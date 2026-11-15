"""Client: talks to the Flask API (the distributor) over HTTP.

Upload  : file path -> read_document -> serialize -> POST to distributor
Download: GET from distributor -> deserialize -> save_document
"""

import os
import requests

from checksum import compute, verify
from file_upload import read_document, save_document
from serDe import serialize, deserialize

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

    def upload(self, path: str) -> dict:
        """Read the file, serialize it to binary, send it together with its
        checksum so the server can detect damage in transit. The file's name
        is its identity. Returns the server's JSON (name, size, chunks)."""
        document = read_document(path)
        data = serialize(document)

        name = os.path.basename(path)
        response = requests.post(
            f"{self.base_url}/objects",
            files={"file": (name, data, "application/octet-stream")},
            data={"checksum": compute(data)},   # fingerprint of exactly these bytes
        )
        self._check(response)
        return response.json()

    def retrieve(self, name: str, save_to: str) -> None:
        """Fetch the object by name, verify it against the checksum the server
        sent, deserialize it, write it to save_to.
        Raises ValueError if the data arrived damaged."""
        response = requests.get(f"{self.base_url}/objects/{name}")
        self._check(response)

        expected = response.headers.get("X-Checksum")
        if not expected:
            raise ValueError("Server sent no checksum, cannot verify the download")
        if not verify(response.content, expected):
            raise ValueError("Checksum mismatch: the download was damaged in transit")

        document = deserialize(response.content)
        save_document(save_to, document)

    def show_distribution(self, name: str) -> dict:
        """GET the chunk -> node map (debug endpoint)."""
        response = requests.get(f"{self.base_url}/objects/{name}/chunks")
        self._check(response)
        return response.json()


def main():
    client = Client()
    while True:
        print("\n1. Upload\n2. Download\n3. Exit")
        choice = input("Choose: ").strip()

        try:
            if choice == "1":
                path = input("File path to upload: ").strip()
                info = client.upload(path)
                print(f"Uploaded '{info['name']}': {info['size']} bytes in {info['chunks']} chunk(s)")
            elif choice == "2":
                name = input("File name to download: ").strip()
                save_to = input("Save as: ").strip()
                client.retrieve(name, save_to)
                print(f"Saved to {save_to}")
            elif choice == "3":
                break
            else:
                print("Please enter 1, 2 or 3.")
        except (OSError, ValueError, requests.RequestException) as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()
