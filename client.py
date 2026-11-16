"""Client: talks to the Flask API (the distributor) over HTTP.

Upload  : file path -> read_document -> serialize -> POST to distributor
Download: GET from distributor -> deserialize -> save_document

Objects are identified by the object_id the server returns on upload; the
list option shows the ids of everything stored.
"""

import json
import mimetypes
import os
import requests

from helpers.checksum import compute, verify
from helpers.file_upload import read_document, save_document
from helpers.serDe import serialize, deserialize

BASE_URL = "http://127.0.0.1:5000/api/v1"   # not "localhost": slow on Windows (IPv6 first)


class Client:
    def __init__(self, base_url: str = BASE_URL):
        self.base_url = base_url

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
        """Read the file, serialize it to binary, send it together with its
        checksum (so the server can detect damage in transit), its content type,
        its full path (two uploads from the same path are refused as the same
        file) and optional custom metadata. Returns the new object's metadata
        (object_id, size, content_type, times, chunks...)."""
        document = read_document(path)
        data = serialize(document)

        name = os.path.basename(path)
        content_type = mimetypes.guess_type(name)[0] or "application/octet-stream"
        form = {
            "checksum": compute(data),               # fingerprint of exactly these bytes
            "path": os.path.abspath(path),
            "size": str(len(document)),              # the original size, not the wrapped one
        }
        if metadata:
            form["metadata"] = json.dumps(metadata)

        response = requests.post(
            f"{self.base_url}/objects",
            files={"file": (name, data, content_type)},
            data=form,
        )
        self._check(response)
        return response.json()

    def retrieve(self, object_id: str, save_to: str = None) -> str:
        """Fetch the object by id, verify it against the checksum the server
        sent, deserialize it, and write it to save_to. Without save_to, it is
        saved under its original name (taken from the metadata) in the current
        folder, and an existing file is never overwritten. Returns the path it
        was saved to. Raises ValueError if the data arrived damaged."""
        response = requests.get(f"{self.base_url}/objects/{object_id}")
        self._check(response)

        expected = response.headers.get("X-Checksum")
        if expected and not verify(response.content, expected):
            raise ValueError("Checksum mismatch: the download was damaged in transit")

        document = deserialize(response.content)

        if save_to is None:
            name = os.path.basename(self.get_metadata(object_id)["name"]) or object_id
            if os.path.exists(name):
                raise ValueError(f"'{name}' already exists here; choose another name to save as")
            save_to = name
        save_document(save_to, document)
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


def main():
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
                break
            else:
                print("Please enter 1-7.")
        except (OSError, ValueError, requests.RequestException) as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()
