"""SerDe: converts a document to binary (and back). Reversible.

Binary layout:
    [ 4 bytes  magic  b"DOC1" ]
    [ 8 bytes  payload length (big-endian) ]
    [ 32 bytes SHA-256 of the payload ]
    [ payload  the document, UTF-8 ]
The header lets deserialize() detect truncated or corrupted data.
"""

import hashlib

MAGIC = b"DOC1"
LENGTH_SIZE = 8
HASH_SIZE = 32
HEADER_SIZE = len(MAGIC) + LENGTH_SIZE + HASH_SIZE


def serialize(document: str) -> bytes:
    """document (text) -> binary data (header + payload)."""
    payload = document.encode("utf-8")
    header = (
        MAGIC
        + len(payload).to_bytes(LENGTH_SIZE, "big")
        + hashlib.sha256(payload).digest()
    )
    return header + payload


def deserialize(data: bytes) -> str:
    """binary data -> original document. Exact reverse of serialize().
    Raises ValueError if the data is not valid or was corrupted."""
    if len(data) < HEADER_SIZE or data[: len(MAGIC)] != MAGIC:
        raise ValueError("Not a valid serialized document (bad header)")

    length_end = len(MAGIC) + LENGTH_SIZE
    length = int.from_bytes(data[len(MAGIC):length_end], "big")
    expected_hash = data[length_end:HEADER_SIZE]
    payload = data[HEADER_SIZE:]

    if len(payload) != length:
        raise ValueError(
            f"Incomplete data: expected {length} bytes, got {len(payload)}"
        )
    if hashlib.sha256(payload).digest() != expected_hash:
        raise ValueError("Checksum mismatch: data is corrupted")

    return payload.decode("utf-8")
