"""File read/write helpers (used by the client).

Files are handled as raw bytes, so any kind of file works: documents,
images, videos, audio, compressed archives, or any other binary data.
No text decoding or newline translation ever happens.
"""


def read_file(path: str) -> bytes:
    """Read any file from disk as raw bytes."""
    with open(path, "rb") as f:
        return f.read()


def save_file(path: str, data: bytes) -> None:
    """Write retrieved raw bytes to disk."""
    with open(path, "wb") as f:
        f.write(data)


# The client still imports these names, so keep them as aliases.
read_document = read_file
save_document = save_file