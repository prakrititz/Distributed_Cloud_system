"""File read/write helpers (used by the client)."""


def read_document(path: str) -> str:
    """Read a text document from disk. newline='' keeps line endings exactly
    as they are, so the round trip stays byte-for-byte identical."""
    with open(path, "r", encoding="utf-8", newline="") as f:
        return f.read()


def save_document(path: str, document: str) -> None:
    """Write a retrieved document to disk."""
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(document)
