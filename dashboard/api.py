"""Talks to the controller's REST API.

Plain blocking calls (requests). The apps run every call in a thread, so the
screen never freezes while a request is waiting. With a key, every request
carries "Authorization: Bearer <key>" and the controller knows from it who is
calling (an admin, or a client that sees only its own objects).
"""
import hashlib
import json
import mimetypes
import os
import tempfile

import requests

BLOCK = 1024 * 1024        # bytes read or written at a time while streaming


class ApiError(Exception):
    """The controller answered with an error, or could not be reached."""

    def __init__(self, message: str, status: int = None):
        super().__init__(message)
        self.status = status


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(BLOCK), b""):
            h.update(block)
    return h.hexdigest()


class Api:
    def __init__(self, base_url: str, api_key: str = None, timeout: float = 5.0):
        base = base_url.rstrip("/")
        if base.endswith("/api/v1"):                 # accept either form
            base = base[: -len("/api/v1")]
        self.base = base                             # e.g. http://127.0.0.1:5000
        self.api_key = api_key or None
        self.timeout = timeout

    @property
    def api_url(self) -> str:
        """http://HOST:PORT/api/v1, the address client.py uses (and saves keys under)."""
        return self.base + "/api/v1"

    # ------------------------------------------------------------ plumbing
    def _call(self, method: str, path: str, timeout: float = None, headers: dict = None, auth: bool = True,
              **kwargs):
        headers = dict(headers or {})
        if auth and self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        try:
            r = requests.request(method, self.base + path, timeout=timeout or self.timeout, headers=headers,
                                 **kwargs)
        except requests.RequestException as e:
            raise ApiError(f"controller not reachable at {self.base} ({type(e).__name__})") from None
        if not r.ok:
            try:
                message = r.json().get("error", r.text)
            except ValueError:
                message = r.text
            r.close()
            raise ApiError(f"{r.status_code}: {message}", r.status_code)
        return r

    def _json(self, method: str, path: str, **kwargs):
        return self._call(method, path, **kwargs).json()

    # ------------------------------------------------------------ who am I
    def me(self) -> dict:
        """{client_id, authenticated, registered, admin, stored_bytes, quota_bytes, ...}"""
        return self._json("GET", "/api/v1/me")

    def register(self, client_id: str = None) -> dict:
        """Register a new client: {client_id, api_key, message}. Needs no key."""
        return self._json("POST", "/api/v1/clients", json={"client_id": client_id} if client_id else {},
                          auth=False)

    def activity(self, limit: int = 100) -> list:
        """What this client did, newest first."""
        return self._json("GET", "/api/v1/me/activity", params={"limit": limit})

    # ------------------------------------------------------------ reading
    def nodes(self) -> list:
        return self._json("GET", "/api/v1/nodes")

    def objects(self) -> list:
        return self._json("GET", "/api/v1/objects")

    def metadata(self, object_id: str) -> dict:
        return self._json("GET", f"/api/v1/objects/{object_id}/metadata")

    def replication(self) -> dict:
        # checks every copy of every chunk, so it may take a moment
        return self._json("GET", "/api/v1/replication", timeout=30)

    def inventory(self, worker_id: str) -> dict:
        return self._json("GET", f"/internal/workers/{worker_id}/chunks")

    # ------------------------------------------------------------ actions
    def upload(self, name: str, data: bytes, custom: dict = None, path: str = None,
               client_id: str = "dashboard") -> dict:
        """Upload bytes held in memory (the admin dashboard's generated files)."""
        form = {"checksum": hashlib.sha256(data).hexdigest()}
        if not self.api_key:          # with a key the controller knows who we are (another id: 403)
            form["client_id"] = client_id
        if custom:
            form["metadata"] = json.dumps(custom)
        if path:
            form["path"] = path
        return self._json("POST", "/api/v1/objects", files={"file": (name, data)}, data=form, timeout=120)

    def upload_file(self, path: str, custom: dict = None) -> dict:
        """Upload a file streamed from disk (never read into memory whole), as
        client.py does: the body is the file, its checksum goes along."""
        name = os.path.basename(path)
        params = {"name": name, "checksum": sha256_file(path), "path": os.path.abspath(path)}
        if custom:
            params["metadata"] = json.dumps(custom)
        content_type = mimetypes.guess_type(name)[0] or "application/octet-stream"
        with open(path, "rb") as f:
            return self._json("POST", "/api/v1/objects", params=params, data=f,
                              headers={"Content-Type": content_type}, timeout=600)

    def download(self, object_id: str) -> bytes:
        r = self._call("GET", f"/api/v1/objects/{object_id}", timeout=120)
        expected = r.headers.get("X-Checksum")
        if expected and hashlib.sha256(r.content).hexdigest() != expected.strip().lower():
            raise ApiError("the download was damaged in transit (checksum mismatch)")
        return r.content

    def download_to(self, object_id: str, dest: str) -> int:
        """Stream the object into a temporary file next to dest, checking its
        size and checksum on the way; only a complete, matching download is
        renamed to dest. Returns the number of bytes."""
        folder = os.path.dirname(os.path.abspath(dest))
        with self._call("GET", f"/api/v1/objects/{object_id}", timeout=600, stream=True) as r:
            expected, length = r.headers.get("X-Checksum"), r.headers.get("Content-Length")
            fd, partial = tempfile.mkstemp(dir=folder, prefix=f".{os.path.basename(dest)}.", suffix=".part")
            try:
                h, received = hashlib.sha256(), 0
                with os.fdopen(fd, "wb") as out:
                    try:
                        for block in r.iter_content(BLOCK):
                            out.write(block)
                            h.update(block)
                            received += len(block)
                    except requests.RequestException as e:
                        raise ApiError(f"the download was cut off after {received} bytes ({type(e).__name__})")
                if length is not None and received != int(length):
                    raise ApiError(f"the download is incomplete: {received} of {length} bytes")
                if expected and h.hexdigest() != expected.strip().lower():
                    raise ApiError("the download was damaged in transit (checksum mismatch)")
                os.replace(partial, dest)
            except BaseException:
                if os.path.exists(partial):
                    os.remove(partial)
                raise
        return received

    def delete(self, object_id: str) -> None:
        self._call("DELETE", f"/api/v1/objects/{object_id}")

    def update_metadata(self, object_id: str, changes: dict) -> dict:
        return self._json("PATCH", f"/api/v1/objects/{object_id}/metadata", json=changes)

    def share(self, object_id: str, readers: list) -> dict:
        return self._json("PUT", f"/api/v1/objects/{object_id}/readers", json={"readers": readers})

    def repair(self) -> dict:
        return self._json("POST", "/api/v1/repair", timeout=300)

    def cleanup(self, min_age: float = None) -> dict:
        params = {} if min_age is None else {"min_age": min_age}
        return self._json("POST", "/api/v1/cleanup", params=params, timeout=300)
