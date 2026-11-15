"""Checksum: one function used everywhere data is checked for corruption.

Treat it as a black box:

    c = compute(data)          # fingerprint of some bytes (64 hex characters)
    ok = verify(data, c)       # True if the bytes still match that fingerprint

The sender computes the checksum, the receiver recomputes it from what it
got and compares. Any change to the data (even one bit) gives a completely
different checksum, so a mismatch means the data was damaged.

Algorithm: SHA-256. Same one the OBJ1 header in serDe.py already uses.
Used for the whole file, for each chunk, and for chunks stored on a node.
"""

import hashlib
import hmac


def compute(data: bytes) -> str:
    """Checksum of `data` as a 64-character lowercase hex string.
    (Hex text, so it can go in JSON, HTTP headers and metadata.)"""
    return hashlib.sha256(data).hexdigest()


def verify(data: bytes, expected: str) -> bool:
    """True if `data` matches the `expected` checksum.
    The comparison takes the same time whether or not the first characters
    match, which is the safe way to compare checksums."""
    if not isinstance(expected, str):
        return False
    return hmac.compare_digest(compute(data), expected.strip().lower())
