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


def default_client_id() -> str:
    """Who this client is: CLIENT_ID from the environment, else the machine's
    name. It should stay the same between runs (in Docker, set CLIENT_ID: a
    container's own hostname changes every time it is created)."""
    return os.environ.get("CLIENT_ID") or socket.gethostname()


class Client:
    def __init__(self, base_url: str = BASE_URL, client_id: str = None):
        self.base_url = base_url
        self.client_id = client_id or default_client_id()

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
            "client_id": self.client_id,             # the duplicate rule is per client + path
        }
        if metadata:
            params["metadata"] = json.dumps(metadata)

        # A raw upload: the body of the request is the file itself. Passing the
        # open file makes requests send it a block at a time.
        with open(path, "rb") as f:
            response = requests.post(f"{self.base_url}/objects", params=params, data=f,
                                     headers={"Content-Type": content_type})
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

        with requests.get(f"{self.base_url}/objects/{object_id}", stream=True) as response:
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
                os.replace(partial, save_to)
            except BaseException:
                os.remove(partial)
                raise
        return save_to


    def get_metadata(self, object_id: str) -> dict:
        """The object's metadata, without downloading it."""
        response = requests.get(f"{self.base_url}/objects/{object_id}/metadata")
        self._check(response)
        return response.json()

    def update_metadata(self, object_id: str, updates: dict) -> dict:
        """Add or change custom metadata (a None value removes that key).
        The stored data is not touched. Returns the updated metadata."""
        response = requests.patch(f"{self.base_url}/objects/{object_id}/metadata", json=updates)
        self._check(response)
        return response.json()

    def delete(self, object_id: str) -> None:
        """Delete the object and every copy of it."""
        response = requests.delete(f"{self.base_url}/objects/{object_id}")
        self._check(response)

    def list_objects(self, limit: int = None, offset: int = 0) -> list:
        """The stored objects (object_id, name, size, content_type, times)."""
        params = {"offset": offset}
        if limit is not None:
            params["limit"] = limit
        response = requests.get(f"{self.base_url}/objects", params=params)
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

Settings: API_URL (default http://127.0.0.1:5000/api/v1) and CLIENT_ID."""


def run_command(client: "Client", args: list) -> int:
    """One command without the menu (used by the client containers and
    scripts). Prints the result; returns the exit code (0 ok, 1 error, 2 bad usage)."""
    command, rest = args[0], args[1:]
    arity = {"upload": (1, 2), "download": (1, 2), "list": (0, 0), "info": (1, 1),
             "update": (2, 2), "delete": (1, 1)}
    if command not in arity or not arity[command][0] <= len(rest) <= arity[command][1]:
        print(USAGE)
        return 2
    try:
        if command == "upload":
            info = client.upload(rest[0], parse_pairs(rest[1]) if len(rest) > 1 else None)
            print(f"Uploaded '{info['name']}' as client '{client.client_id}': {info['size']} bytes in "
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
