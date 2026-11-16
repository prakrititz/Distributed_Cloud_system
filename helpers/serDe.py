"""SerDe: wraps raw file bytes with a header (and strips it again). Reversible.

Works for any kind of file (documents, images, video, audio, archives, ...)
because the content is treated as opaque bytes and never decoded.

Binary layout:
    [ 4 bytes  magic  b"OBJ1" ]
    [ 8 bytes  payload length (big-endian) ]
    [ 32 bytes SHA-256 of the payload ]
    [ payload  the file's bytes, unchanged ]
The header lets deserialize() detect truncated or corrupted data.
"""

import hashlib

MAGIC = b"OBJ1"
LENGTH_SIZE = 8
HASH_SIZE = 32
HEADER_SIZE = len(MAGIC) + LENGTH_SIZE + HASH_SIZE


def serialize(data: bytes) -> bytes:
    """file bytes -> binary object (header + payload)."""
    header = (
        MAGIC
        + len(data).to_bytes(LENGTH_SIZE, "big")
        + hashlib.sha256(data).digest()
    )
    return header + data


def deserialize(blob: bytes) -> bytes:
    """binary object -> original file bytes. Exact reverse of serialize().
    Raises ValueError if the data is not valid or was corrupted."""
    if len(blob) < HEADER_SIZE or blob[: len(MAGIC)] != MAGIC:
        raise ValueError("Not a valid serialized object (bad header)")

    length_end = len(MAGIC) + LENGTH_SIZE
    length = int.from_bytes(blob[len(MAGIC):length_end], "big")
    expected_hash = blob[length_end:HEADER_SIZE]
    payload = blob[HEADER_SIZE:]

    if len(payload) != length:
        raise ValueError(
            f"Incomplete data: expected {length} bytes, got {len(payload)}"
        )
    if hashlib.sha256(payload).digest() != expected_hash:
        raise ValueError("Checksum mismatch: data is corrupted")

    return payload
