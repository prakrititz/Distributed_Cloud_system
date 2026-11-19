"""A cache of chunks in the controller's memory, so a chunk downloaded again
is not fetched from a worker again.

Why it can never give out old data: a chunk's id is its object's id plus its
position, object ids are never reused, and a chunk's bytes never change after
upload (repair copies the same bytes to another worker). So a cached chunk is
right for as long as its object exists; deleting the object removes its
chunks from the cache. Only chunks whose checksum matched are put in, so a
damaged copy is never cached.

Least recently used first out, within a limit in bytes (CHUNK_CACHE_MB). A
limit of 0 turns the cache off. Safe to use from many threads at once.
"""
import threading
from collections import OrderedDict


class ChunkCache:
    def __init__(self, capacity_bytes: int):
        self.capacity = max(0, int(capacity_bytes))
        self._chunks = OrderedDict()      # chunk_id -> bytes, least recently used first
        self._bytes = 0
        self._lock = threading.Lock()
        self.hits = self.misses = self.evictions = 0

    @property
    def enabled(self) -> bool:
        return self.capacity > 0

    def get(self, chunk_id: str):
        """The chunk's bytes, or None. A hit makes it the most recently used."""
        if not self.enabled:
            return None
        with self._lock:
            data = self._chunks.get(chunk_id)
            if data is None:
                self.misses += 1
                return None
            self._chunks.move_to_end(chunk_id)
            self.hits += 1
            return data

    def __contains__(self, chunk_id: str) -> bool:
        """Is it cached? (Does not count as a hit or change the order.)"""
        with self._lock:
            return chunk_id in self._chunks

    def put(self, chunk_id: str, data: bytes) -> None:
        """Keep a chunk (whose checksum matched), dropping the least recently
        used ones to make room. A chunk bigger than the whole cache is not kept."""
        size = len(data)
        if not self.enabled or size > self.capacity:
            return
        with self._lock:
            old = self._chunks.pop(chunk_id, None)
            if old is not None:
                self._bytes -= len(old)
            while self._chunks and self._bytes + size > self.capacity:
                _, dropped = self._chunks.popitem(last=False)
                self._bytes -= len(dropped)
                self.evictions += 1
            self._chunks[chunk_id] = data
            self._bytes += size

    def discard(self, chunk_ids) -> int:
        """Forget these chunks (their object was deleted). Returns how many were cached."""
        removed = 0
        with self._lock:
            for chunk_id in chunk_ids:
                data = self._chunks.pop(chunk_id, None)
                if data is not None:
                    self._bytes -= len(data)
                    removed += 1
        return removed

    def clear(self) -> int:
        """Forget everything (the counters stay). Returns how many chunks were cached."""
        with self._lock:
            n = len(self._chunks)
            self._chunks.clear()
            self._bytes = 0
            return n

    def stats(self) -> dict:
        with self._lock:
            lookups = self.hits + self.misses
            return {"enabled": self.enabled, "capacity_bytes": self.capacity, "used_bytes": self._bytes,
                    "chunks": len(self._chunks), "hits": self.hits, "misses": self.misses,
                    "hit_rate": round(self.hits / lookups, 3) if lookups else None,
                    "evictions": self.evictions}
