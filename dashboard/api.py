"""Talks to the controller's REST API.

Plain blocking calls (requests). The app runs every call in a thread, so the
screen never freezes while a request is waiting.
"""
import hashlib

import requests


class ApiError(Exception):
    """The controller answered with an error, or could not be reached."""


class Api:
    def __init__(self, base_url: str, timeout: float = 5.0):
        base = base_url.rstrip("/")
        if base.endswith("/api/v1"):                 # accept either form
            base = base[: -len("/api/v1")]
        self.base = base                             # e.g. http://127.0.0.1:5000
        self.timeout = timeout

    # ------------------------------------------------------------ plumbing
    def _call(self, method: str, path: str, timeout: float = None, **kwargs):
        try:
            r = requests.request(method, self.base + path, timeout=timeout or self.timeout, **kwargs)
        except requests.RequestException as e:
            raise ApiError(f"controller not reachable at {self.base} ({type(e).__name__})") from None
        if not r.ok:
            try:
                message = r.json().get("error", r.text)
            except ValueError:
                message = r.text
            raise ApiError(f"{r.status_code}: {message}")
        return r

    def _json(self, method: str, path: str, **kwargs):
        return self._call(method, path, **kwargs).json()

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
        import json
        form = {"checksum": hashlib.sha256(data).hexdigest(), "client_id": client_id}
        if custom:
            form["metadata"] = json.dumps(custom)
        if path:
            form["path"] = path
        return self._json("POST", "/api/v1/objects", files={"file": (name, data)}, data=form, timeout=120)

    def download(self, object_id: str) -> bytes:
        r = self._call("GET", f"/api/v1/objects/{object_id}", timeout=120)
        expected = r.headers.get("X-Checksum")
        if expected and hashlib.sha256(r.content).hexdigest() != expected.strip().lower():
            raise ApiError("the download was damaged in transit (checksum mismatch)")
        return r.content

    def delete(self, object_id: str) -> None:
        self._call("DELETE", f"/api/v1/objects/{object_id}")

    def update_metadata(self, object_id: str, changes: dict) -> dict:
        return self._json("PATCH", f"/api/v1/objects/{object_id}/metadata", json=changes)

    def repair(self) -> dict:
        return self._json("POST", "/api/v1/repair", timeout=300)

    def cleanup(self, min_age: float = None) -> dict:
        params = {} if min_age is None else {"min_age": min_age}
        return self._json("POST", "/api/v1/cleanup", params=params, timeout=300)
