"""Errors shared by the controller side and the node side.

They live here, not in either side's code, so the controller never has to
import node code and the other way round.
"""


class NodeUnavailable(Exception):
    """Raised when a node cannot be reached (used by controller/remote_node.py)."""


class ChunkCorrupt(Exception):
    """Raised when chunk data does not match its checksum: damaged in transit,
    or damaged while stored on a node."""


class ChecksumMismatch(Exception):
    """Raised when an upload's bytes do not match the checksum the client sent
    with them: damaged in transit. Nothing of the upload is kept."""
