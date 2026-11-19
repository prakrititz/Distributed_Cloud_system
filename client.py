"""Client: talks to the Flask API (the distributor) over HTTP.

Upload  : file path -> checksum it -> stream the file to the API as the body of the request
Download: GET from the API -> write it to a temporary file, checksumming as it
          arrives -> if the size and checksum are right, rename it into place

Both are streamed, a block at a time, so a file of any size needs only a little
memory, here and on the controller.

The bytes stored are exactly the file's bytes, so a download with curl or a
browser gives back the original file too.

Objects are identified by the object_id the server returns on upload; the
list option shows the ids of everything stored.
"""

import json
import mimetypes
import os
import socket
import sys
import tempfile

import requests

from helpers.checksum import compute, compute_file, matches, running

# Where the API is. Override with API_URL (in Docker: http://controller:5000/api/v1).
# Default uses 127.0.0.1, not "localhost": slow on Windows (IPv6 first).
BASE_URL = os.environ.get("API_URL", "http://127.0.0.1:5000/api/v1")
BLOCK = 1024 * 1024   # bytes read or written at a time while streaming

# Where a key this client got by registering is kept: a small JSON file,
# readable only by its owner, with one entry per API address (the format idea
# of ~/.aws/credentials). Override with OBJECTSTORE_CREDENTIALS.
CREDENTIALS = os.environ.get("OBJECTSTORE_CREDENTIALS") or os.path.join(
    os.path.expanduser("~"), ".objectstore", "credentials.json")


def load_credentials(base_url: str, path: str = None):
    """(client_id, api_key) saved for this API address, or (None, None)."""
    try:
        with open(path or CREDENTIALS, encoding="utf-8") as f:
            entry = json.load(f).get(base_url) or {}
        return entry.get("client_id"), entry.get("api_key")
    except (OSError, ValueError, AttributeError):
        return None, None


def save_credentials(base_url: str, client_id: str, api_key: str, path: str = None) -> str:
    """Remember the key for this API address. The file is created readable
    and writable by its owner only (on Windows the user's own folder already
    keeps it private). Returns the file's path."""
    path = path or CREDENTIALS
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    try:
        with open(path, encoding="utf-8") as f:
            saved = json.load(f)
        if not isinstance(saved, dict):
            saved = {}
    except (OSError, ValueError):
        saved = {}
    saved[base_url] = {"client_id": client_id, "api_key": api_key}
    fd = os.open(path + ".tmp", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(saved, f, indent=1)
    os.replace(path + ".tmp", path)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return path


def ordinary_permissions(path: str) -> None:
    """Give a finished download the permissions of any new file (0666 less
    the umask, usually 0644). A temporary file starts readable by its owner
    only (0600); kept that way, a download made inside the client container
    (user 10001) could not be read on a Linux host without sudo."""
    mask = os.umask(0)
    os.umask(mask)
    try:
        os.chmod(path, 0o666 & ~mask)
    except OSError:
        pass


def default_client_id() -> str:
    """Who this client is: CLIENT_ID from the environment, else the machine's
    name. It should stay the same between runs (in Docker, set CLIENT_ID: a
    container's own hostname changes every time it is created)."""
    return os.environ.get("CLIENT_ID") or socket.gethostname()


class Client:
    def __init__(self, base_url: str = BASE_URL, client_id: str = None, api_key: str = None):
        self.base_url = base_url
        self.client_id = client_id or default_client_id()
        # With authentication, every request carries this client's secret key,
        # and the server knows from it who we are: the one given here, else
        # API_KEY, else the key saved when this client registered itself.
        if api_key is None:
            api_key = os.environ.get("API_KEY", "").strip()
            if not api_key:
                saved_id, saved_key = load_credentials(self.base_url)
                if saved_key:
                    api_key = saved_key
                    self.client_id = client_id or saved_id
        self.api_key = api_key

    def _headers(self, extra: dict = None) -> dict:
        headers = dict(extra or {})
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    @staticmethod
    def _check(response) -> None:
        """Raise a readable error using the server's JSON message."""
        if response.ok:
            return
        try:
            message = response.json()["error"]
        except (ValueError, KeyError):
            message = response.text
        raise ValueError(f"Server said {response.status_code}: {message}")

    def upload(self, path: str, metadata: dict = None) -> dict:
        """Upload a file, streamed from disk (never read into memory whole).
        Sent with it: its checksum (so the server can detect damage in
        transit), its content type, its full path and this client's id (two
        uploads from the same path by the same client are refused as the same
        file) and optional custom metadata.
        Returns the new object's metadata (object_id, size, content_type,
        times, chunks...)."""
        name = os.path.basename(path)
        content_type = mimetypes.guess_type(name)[0] or "application/octet-stream"
        params = {
            "name": name,
            "checksum": compute_file(path),          # fingerprint of exactly these bytes
            "path": os.path.abspath(path),
        }
        if not self.api_key:
            params["client_id"] = self.client_id     # with a key, the server knows who we are
        if metadata:
            params["metadata"] = json.dumps(metadata)

        # A raw upload: the body of the request is the file itself. Passing the
        # open file makes requests send it a block at a time.
        with open(path, "rb") as f:
            response = requests.post(f"{self.base_url}/objects", params=params, data=f,
                                     headers=self._headers({"Content-Type": content_type}))
        self._check(response)
        return response.json()

    def retrieve(self, object_id: str, save_to: str = None) -> str:
        """Fetch the object by id and write it to save_to. Without save_to, it
        is saved under its original name (taken from the metadata) in the
        current folder, and an existing file is never overwritten. Returns the
        path it was saved to.

        It arrives a block at a time and goes into a temporary file next to
        the target, checksummed on the way. Only if all of it arrived and the
        checksum matches the one the server sent is it renamed into place;
        otherwise the temporary file is deleted and ValueError is raised, so a
        damaged or incomplete download is never saved (and never replaces an
        existing file)."""
        if save_to is None:
            name = os.path.basename(self.get_metadata(object_id)["name"]) or object_id
            if os.path.exists(name):
                raise ValueError(f"'{name}' already exists here; choose another name to save as")
            save_to = name

        with requests.get(f"{self.base_url}/objects/{object_id}", stream=True,
                          headers=self._headers()) as response:
            self._check(response)
            expected = response.headers.get("X-Checksum")
            length = response.headers.get("Content-Length")
            folder = os.path.dirname(os.path.abspath(save_to))
            fd, partial = tempfile.mkstemp(dir=folder, prefix=f".{os.path.basename(save_to)}.",
                                           suffix=".part")
            try:
                whole, received = running(), 0
                with os.fdopen(fd, "wb") as out:
                    try:
                        for block in response.iter_content(BLOCK):
                            out.write(block)
                            whole.update(block)
                            received += len(block)
                    except requests.RequestException as e:
                        raise ValueError(f"The download was cut off after {received} bytes ({e})") from e
                if length is not None and received != int(length):
                    raise ValueError(f"The download is incomplete: {received} of {length} bytes")
                if expected and not matches(whole.hexdigest(), expected):
                    raise ValueError("Checksum mismatch: the download was damaged in transit")
                ordinary_permissions(partial)
                os.replace(partial, save_to)
            except BaseException:
                os.remove(partial)
                raise
        return save_to


    def get_metadata(self, object_id: str) -> dict:
        """The object's metadata, without downloading it."""
        response = requests.get(f"{self.base_url}/objects/{object_id}/metadata", headers=self._headers())
        self._check(response)
        return response.json()

    def update_metadata(self, object_id: str, updates: dict) -> dict:
        """Add or change custom metadata (a None value removes that key).
        The stored data is not touched. Returns the updated metadata."""
        response = requests.patch(f"{self.base_url}/objects/{object_id}/metadata", json=updates,
                                  headers=self._headers())
        self._check(response)
        return response.json()

    def delete(self, object_id: str) -> None:
        """Delete the object and every copy of it."""
        response = requests.delete(f"{self.base_url}/objects/{object_id}", headers=self._headers())
        self._check(response)

    def list_objects(self, limit: int = None, offset: int = 0) -> list:
        """The stored objects (object_id, name, size, content_type, times)."""
        params = {"offset": offset}
        if limit is not None:
            params["limit"] = limit
        response = requests.get(f"{self.base_url}/objects", params=params, headers=self._headers())
        self._check(response)
        return response.json()

    def register(self, client_id: str = None, save: bool = True) -> dict:
        """Register this client with the controller and get its own API key
        (shown only this once). Without a name, the controller makes one up.
        The key is saved in the credentials file (unless save=False) and used
        from then on. Returns {client_id, api_key, message}."""
        response = requests.post(f"{self.base_url}/clients",
                                 json={"client_id": client_id} if client_id else {})
        self._check(response)
        answer = response.json()
        self.client_id, self.api_key = answer["client_id"], answer["api_key"]
        if save:
            answer["saved_to"] = save_credentials(self.base_url, self.client_id, self.api_key)
        return answer

    def share(self, object_id: str, readers: list) -> dict:
        """Set which other clients may read one of our objects ([] stops
        sharing). Returns {object_id, owner, readers}."""
        response = requests.put(f"{self.base_url}/objects/{object_id}/readers", json={"readers": readers},
                                headers=self._headers())
        self._check(response)
        return response.json()

    def me(self) -> dict:
        """Who the server says we are, what we store, and our limits."""
        response = requests.get(f"{self.base_url}/me", headers=self._headers())
        self._check(response)
        return response.json()

    def show_distribution(self, object_id: str) -> dict:
        """The object's metadata, which includes which nodes hold each chunk."""
        return self.get_metadata(object_id)


def parse_pairs(text: str) -> dict:
    """'a=1, b=two' -> {'a': '1', 'b': 'two'}. 'key=null' means remove the key."""
    result = {}
    for piece in text.split(","):
        piece = piece.strip()
        if not piece:
            continue
        if "=" not in piece:
            raise ValueError(f"'{piece}' is not key=value")
        key, value = (s.strip() for s in piece.split("=", 1))
        if not key:
            raise ValueError(f"'{piece}' has no key")
        result[key] = None if value == "null" else value
    return result


USAGE = """\
python client.py                               the menu (interactive)
python client.py upload FILE [k=v,k2=v2]       upload, with optional custom metadata
python client.py download OBJECT_ID [SAVE_AS]  download (saved under its own name by default)
python client.py list                          list the stored objects
python client.py info OBJECT_ID                show an object's metadata
python client.py update OBJECT_ID k=v,k2=null  change custom metadata (null removes a key)
python client.py delete OBJECT_ID              delete an object
python client.py share OBJECT_ID client2,client3   let other clients read it (share OBJECT_ID - : nobody)
python client.py me                            who the server says you are, your storage and limits
python client.py register [NAME]               get your own API key from the controller (saved for you)

Settings: API_URL (default http://127.0.0.1:5000/api/v1), CLIENT_ID, and API_KEY
(this client's secret key, when the server requires one; without it, the key
saved by `register` is used, from OBJECTSTORE_CREDENTIALS or
~/.objectstore/credentials.json)."""


def run_command(client: "Client", args: list) -> int:
    """One command without the menu (used by the client containers and
    scripts). Prints the result; returns the exit code (0 ok, 1 error, 2 bad usage)."""
    command, rest = args[0], args[1:]
    arity = {"upload": (1, 2), "download": (1, 2), "list": (0, 0), "info": (1, 1),
             "update": (2, 2), "delete": (1, 1), "share": (2, 2), "me": (0, 0), "register": (0, 1)}
    if command not in arity or not arity[command][0] <= len(rest) <= arity[command][1]:
        print(USAGE)
        return 2
    try:
        if command == "upload":
            info = client.upload(rest[0], parse_pairs(rest[1]) if len(rest) > 1 else None)
            print(f"Uploaded '{info['name']}' as client '{info['client_id']}': {info['size']} bytes in "
                  f"{info['chunk_count']} chunk(s)")
            for c in info["chunks"]:
                print(f"  chunk {c['index']}: {', '.join(c['replicas'])}")
            print(f"object_id: {info['object_id']}")
        elif command == "download":
            print(f"Saved to {client.retrieve(rest[0], rest[1] if len(rest) > 1 else None)}")
        elif command == "list":
            objects = client.list_objects()
            if not objects:
                print("Nothing stored yet.")
            for o in objects:
                print(f"{o['object_id']}  {o['name']}  {o['size']} bytes  {o['content_type']}")
        elif command == "info":
            print(json.dumps(client.get_metadata(rest[0]), indent=2))
        elif command == "update":
            print(json.dumps(client.update_metadata(rest[0], parse_pairs(rest[1]))["custom"], indent=2))
        elif command == "delete":
            client.delete(rest[0])
            print("Deleted.")
        elif command == "share":
            readers = [] if rest[1].strip() == "-" else [r.strip() for r in rest[1].split(",") if r.strip()]
            shared = client.share(rest[0], readers)
            print(f"Readers of {shared['object_id']}: {', '.join(shared['readers']) or 'nobody'}")
        elif command == "me":
            print(json.dumps(client.me(), indent=2))
        elif command == "register":
            answer = client.register(rest[0] if rest else None)
            print(f"Registered as '{answer['client_id']}'.")
            print(f"Your API key (shown only this once): {answer['api_key']}")
            print(f"Saved to {answer['saved_to']}; this client uses it from now on.")
        return 0
    except (OSError, ValueError, requests.RequestException) as e:
        print(f"Error: {e}")
        return 1


def main(argv=None):
    """No arguments: the menu. Otherwise one command (see USAGE)."""
    args = sys.argv[1:] if argv is None else list(argv)
    if args:
        if args[0] in ("-h", "--help", "help"):
            print(USAGE)
            return 0
        return run_command(Client(), args)
    client = Client()
    while True:
        print("\n1. Upload\n2. Download\n3. List objects\n4. Show metadata\n"
              "5. Update metadata\n6. Delete\n7. Exit")
        choice = input("Choose: ").strip()

        try:
            if choice == "1":
                path = input("File path to upload: ").strip()
                text = input("Custom metadata (key=value, comma separated; Enter for none): ")
                info = client.upload(path, parse_pairs(text) or None)
                print(f"Uploaded '{info['name']}' ({info['content_type']}): {info['size']} bytes "
                      f"in {info['chunk_count']} chunk(s)\nObject ID: {info['object_id']}")
            elif choice == "2":
                object_id = input("Object ID: ").strip()
                save_to = input("Save as (Enter to use the original name): ").strip() or None
                print(f"Saved to {client.retrieve(object_id, save_to)}")
            elif choice == "3":
                objects = client.list_objects()
                if not objects:
                    print("Nothing stored yet.")
                for o in objects:
                    print(f"{o['object_id']}  {o['name']}  {o['size']} bytes  {o['content_type']}")
            elif choice == "4":
                object_id = input("Object ID: ").strip()
                print(json.dumps(client.get_metadata(object_id), indent=2))
            elif choice == "5":
                object_id = input("Object ID: ").strip()
                text = input("Changes (key=value, comma separated; key=null removes a key): ")
                print(json.dumps(client.update_metadata(object_id, parse_pairs(text))["custom"], indent=2))
            elif choice == "6":
                object_id = input("Object ID: ").strip()
                client.delete(object_id)
                print("Deleted.")
            elif choice == "7":
                return 0
            else:
                print("Please enter 1-7.")
        except (OSError, ValueError, requests.RequestException) as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    sys.exit(main())
