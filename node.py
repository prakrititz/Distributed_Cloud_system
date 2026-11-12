"""A storage node: just holds chunks and knows how much space it has.

A node is deliberately dumb. It does not know which object a chunk belongs
to, what order chunks come in, or that other nodes exist. The distributor
holds all of that knowledge.
"""


class Node:
    def __init__(self, node_id: str, capacity: int):
        self.node_id = node_id
        self.capacity = capacity      # total bytes this node can hold
        self.chunks = {}              # chunk_id -> bytes

    def used_space(self) -> int:
        """Total bytes currently stored."""
        return sum(len(data) for data in self.chunks.values())

    def free_space(self) -> int:
        """capacity - used_space."""
        return self.capacity - self.used_space()

    def store_chunk(self, chunk_id: str, data: bytes) -> bool:
        """Save a chunk. Return False if there is no space."""
        if len(data) > self.free_space():
            return False
        self.chunks[chunk_id] = data
        return True

    def get_chunk(self, chunk_id: str) -> bytes:
        """Return a stored chunk. Raises KeyError if this node doesn't have it."""
        return self.chunks[chunk_id]

    def delete_chunk(self, chunk_id: str) -> None:
        """Remove a chunk (used to clean up a failed upload). No error if absent."""
        self.chunks.pop(chunk_id, None)
