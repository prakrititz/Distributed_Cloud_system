"""Checksum: one function used everywhere data is checked for corruption.

Treat it as a black box:

    c = compute(data)          # fingerprint of some bytes (64 hex characters)
    ok = verify(data, c)       # True if the bytes still match that fingerprint

The sender computes the checksum, the receiver recomputes it from what it
got and compares. Any change to the data (even one bit) gives a completely
different checksum, so a mismatch means the data was damaged.

A file too big to hold in memory is checksummed piece by piece: feed each
piece to a running hash, and the result is the same as hashing it all at once.

    h = running()              # an empty running hash
    h.update(piece)            # ... for every piece, in order
    h.hexdigest()              # == compute(all the pieces joined)

    c = compute_file(path)     # checksum of a file on disk, read 1 MB at a time

Algorithm: SHA-256.
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
    return matches(compute(data), expected)


def matches(actual: str, expected: str) -> bool:
    """True if two checksums are the same (case and surrounding spaces of
    `expected` ignored). Constant-time, like verify."""
    if not isinstance(expected, str):
        return False
    return hmac.compare_digest(actual, expected.strip().lower())


def running():
    """An empty running hash: call .update(piece) for every piece in order,
    then .hexdigest() gives the checksum of all of them together."""
    return hashlib.sha256()


def compute_file(path: str, block_size: int = 1024 * 1024) -> str:
    """Checksum of the file at `path`, read `block_size` bytes at a time, so
    a file of any size needs only one block in memory."""
    h = running()
    with open(path, "rb") as f:
        while True:
            block = f.read(block_size)
            if not block:
                return h.hexdigest()
            h.update(block)
