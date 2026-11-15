"""Storage logic for a node: keeps chunks as files in a folder.

A node is deliberately dumb. It does not know which object a chunk belongs
to, what order chunks come in, or that other nodes exist. The distributor
holds all of that knowledge.

Chunks live on disk (one file per chunk id), so they survive a restart, like
persistent storage in a container would. node_server.py wraps this class in
HTTP endpoints.
"""

import os
import re
import threading


class NodeUnavailable(Exception):
    """Raised when a node cannot be reached (used by remote_node.py)."""


# A chunk id becomes a file name, so only allow plain names: letters, digits,
# '.', '_' and '-', not starting with '.'. Anything else (slashes, '..', a
# leading dot) is rejected so a request cannot touch files outside data_dir.
_SAFE_CHUNK_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

# Chunks are written to a temporary file first, then renamed into place, so a
# crash mid-write never leaves a half-written chunk under its real name.
# Temp files start with '.', which no valid chunk id can, so they never clash.
_TEMP_PREFIX = ".tmp-"


class ChunkStore:
    """Keeps chunks as files in a folder, so they survive a restart."""

    def __init__(self, data_dir: str, capacity: int):
        self.data_dir = data_dir
        self.capacity = capacity
        # A web server handles requests on several threads. This lock makes
        # "check free space, then write" one step so two uploads can't both
        # squeeze into the last bit of space.
        self._lock = threading.Lock()

        os.makedirs(self.data_dir, exist_ok=True)
        self._remove_leftover_temp_files()

    def _remove_leftover_temp_files(self) -> None:
        """Delete temp files left behind by a crash during a write."""
        for entry in os.scandir(self.data_dir):
            if entry.is_file() and entry.name.startswith(_TEMP_PREFIX):
                os.remove(entry.path)

    def _path_for(self, chunk_id: str) -> str:
        """File path for a chunk. Raises ValueError for an unsafe id."""
        if not isinstance(chunk_id, str) or not _SAFE_CHUNK_ID.match(chunk_id) or ".." in chunk_id:
            raise ValueError(f"Invalid chunk id: {chunk_id!r}")
        return os.path.join(self.data_dir, chunk_id)

    def _chunk_files(self):
        """Yield the directory entries that are real chunks (not temp files)."""
        for entry in os.scandir(self.data_dir):
            if entry.is_file() and not entry.name.startswith("."):
                yield entry

    def used_space(self) -> int:
        """Total bytes on disk in this store's folder (temp files included, so
        space being written right now is counted)."""
        return sum(e.stat().st_size for e in os.scandir(self.data_dir) if e.is_file())

    def free_space(self) -> int:
        """capacity - used_space."""
        return self.capacity - self.used_space()

    def chunk_count(self) -> int:
        """Number of stored chunks."""
        return sum(1 for _ in self._chunk_files())

    def store(self, chunk_id: str, data: bytes) -> bool:
        """Write the chunk file. Returns False if it doesn't fit.
        Storing an id that already exists replaces it; only the size
        difference needs room. Raises ValueError for an unsafe id."""
        path = self._path_for(chunk_id)

        with self._lock:
            existing = os.path.getsize(path) if os.path.isfile(path) else 0
            if len(data) - existing > self.free_space():
                return False

            temp_path = os.path.join(self.data_dir, _TEMP_PREFIX + chunk_id)
            try:
                with open(temp_path, "wb") as f:
                    f.write(data)
                os.replace(temp_path, path)   # atomic: all of it or none of it
            except BaseException:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                raise
            return True

    def get(self, chunk_id: str) -> bytes:
        """Read the chunk file. Raises KeyError if absent, ValueError for an
        unsafe id."""
        path = self._path_for(chunk_id)
        try:
            with open(path, "rb") as f:
                return f.read()
        except FileNotFoundError:
            raise KeyError(chunk_id) from None

    def has(self, chunk_id: str) -> bool:
        """Does the chunk file exist? Raises ValueError for an unsafe id."""
        return os.path.isfile(self._path_for(chunk_id))

    def delete(self, chunk_id: str) -> None:
        """Remove the chunk file; no error if absent. Raises ValueError for an
        unsafe id."""
        path = self._path_for(chunk_id)
        with self._lock:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
