"""An in-memory stand-in for a RemoteNode, so the distributor can be tested
quickly and deterministically, with failures made to order (a node going down,
a chunk damaged, a write refused) and with every call counted.

It has the same methods and error behavior as controller/remote_node.py.
"""
from helpers.checksum import compute
from helpers.errors import ChunkCorrupt, NodeUnavailable


class FakeNode:
    def __init__(self, node_id: str, capacity: int = 10 ** 9):
        self.node_id = node_id
        self.capacity = capacity
        self.chunks = {}          # chunk_id -> bytes (what is "on disk")
        self.sums = {}            # chunk_id -> checksum saved at write time
        self.down = False         # True: every call raises NodeUnavailable
        self.refuse_writes = False  # True: store_chunk raises ChunkCorrupt (damaged in transit)
        self.on_store = None      # optional callback(chunk_id), called after a successful store
        self.health_calls = 0
        self.store_calls = []     # chunk ids we were asked to store (even if refused)
        self.ages = {}            # chunk_id -> age reported by list_chunks (default: old)

    # --- helpers for tests -------------------------------------------------
    def used(self) -> int:
        return sum(len(v) for v in self.chunks.values())

    def damage(self, chunk_id: str) -> None:
        """Flip a byte of the stored chunk, like damage on disk."""
        data = bytearray(self.chunks[chunk_id])
        data[len(data) // 2] ^= 0xFF
        self.chunks[chunk_id] = bytes(data)

    def lose(self, chunk_id: str) -> None:
        """Remove the chunk behind the node's back."""
        self.chunks.pop(chunk_id, None)
        self.sums.pop(chunk_id, None)

    def _up(self) -> None:
        if self.down:
            raise NodeUnavailable(f"{self.node_id} unreachable")

    # --- what the distributor calls ---------------------------------------
    def health(self) -> dict:
        self._up()
        self.health_calls += 1
        used = self.used()
        return {"node_id": self.node_id, "capacity": self.capacity, "used": used,
                "free": self.capacity - used, "chunks": len(self.chunks)}

    @property
    def alive(self) -> bool:
        return not self.down

    def store_chunk(self, chunk_id: str, data: bytes) -> bool:
        self.store_calls.append(chunk_id)
        self._up()
        if self.refuse_writes:
            raise ChunkCorrupt(f"{self.node_id} received chunk '{chunk_id}' damaged in transit")
        existing = len(self.chunks.get(chunk_id, b""))
        if len(data) - existing > self.capacity - self.used():
            return False
        self.chunks[chunk_id] = bytes(data)
        self.sums[chunk_id] = compute(data)
        if self.on_store:
            self.on_store(chunk_id)
        return True

    def get_chunk(self, chunk_id: str) -> bytes:
        self._up()
        if chunk_id not in self.chunks:
            raise KeyError(chunk_id)
        data = self.chunks[chunk_id]
        if compute(data) != self.sums.get(chunk_id):
            raise ChunkCorrupt(f"{self.node_id} reports its copy of chunk '{chunk_id}' is damaged")
        return data

    def get_checksum(self, chunk_id: str) -> dict:
        self._up()
        if chunk_id not in self.chunks:
            raise KeyError(chunk_id)
        actual = compute(self.chunks[chunk_id])
        stored = self.sums.get(chunk_id)
        return {"chunk_id": chunk_id, "checksum": actual, "stored": stored, "ok": actual == stored}

    def list_chunks(self) -> list:
        self._up()
        return [{"chunk_id": c, "size": len(d), "age": self.ages.get(c, 1000.0)}
                for c, d in sorted(self.chunks.items())]

    def has_chunk(self, chunk_id: str) -> bool:
        return (not self.down) and chunk_id in self.chunks

    def delete_chunk(self, chunk_id: str) -> None:
        self._up()
        self.chunks.pop(chunk_id, None)
        self.sums.pop(chunk_id, None)
