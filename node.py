"""A storage node: just holds chunks and knows how much space it has.

A node is deliberately dumb. It does not know which object a chunk belongs
to, what order chunks come in, or that other nodes exist. The distributor
holds all of that knowledge.

A node can be stopped and started to simulate a failure (like
`docker stop worker2` / `docker start worker2` later on). Stopping keeps its
stored chunks, as persistent storage would; it just cannot be reached.
"""


class NodeUnavailable(Exception):
    """Raised when something is asked of a node that is currently stopped."""


class Node:
    def __init__(self, node_id: str, capacity: int):
        self.node_id = node_id
        self.capacity = capacity      # total bytes this node can hold
        self.chunks = {}              # chunk_id -> bytes
        self.alive = True

    # --- up / down simulation -------------------------------------------
    def stop(self) -> None:
        """Simulate the node going down. Its chunks are kept."""
        self.alive = False

    def start(self) -> None:
        """Bring the node back. Its chunks are still there."""
        self.alive = True

    def _require_alive(self) -> None:
        if not self.alive:
            raise NodeUnavailable(f"{self.node_id} is down")

    # --- space accounting (registry info, works even while down) ---------
    def used_space(self) -> int:
        """Total bytes currently stored."""
        return sum(len(data) for data in self.chunks.values())

    def free_space(self) -> int:
        """capacity - used_space."""
        return self.capacity - self.used_space()

    # --- chunk operations (all need the node to be up) -------------------
    def store_chunk(self, chunk_id: str, data: bytes) -> bool:
        """Save a chunk. Return False if there is no space.
        Raises NodeUnavailable if the node is down."""
        self._require_alive()
        if len(data) > self.free_space():
            return False
        self.chunks[chunk_id] = data
        return True

    def get_chunk(self, chunk_id: str) -> bytes:
        """Return a stored chunk. Raises KeyError if this node doesn't have it,
        NodeUnavailable if the node is down."""
        self._require_alive()
        return self.chunks[chunk_id]

    def has_chunk(self, chunk_id: str) -> bool:
        """True only if the node is up AND holds this chunk."""
        return self.alive and chunk_id in self.chunks

    def delete_chunk(self, chunk_id: str) -> None:
        """Remove a chunk (used to clean up a failed upload). No error if absent.
        Raises NodeUnavailable if the node is down."""
        self._require_alive()
        self.chunks.pop(chunk_id, None)
