"""Storage logic for a node: keeps chunks as files in a folder.

A node is deliberately dumb. It does not know which object a chunk belongs
to, what order chunks come in, or that other nodes exist. The distributor
holds all of that knowledge.

Chunks live on disk (one file per chunk id), so they survive a restart, like
persistent storage in a container would. nodes/node_server.py wraps this class in
HTTP endpoints.

Integrity: next to every chunk the node keeps a small file with the chunk's
checksum (in the hidden '.checksums' folder). When a chunk is read, the node
recomputes the checksum and refuses to hand out a chunk that no longer matches
(ChunkCorrupt), for example one that was damaged on disk.
"""

import os
import re
import threading
import time

from helpers.checksum import compute, verify
from helpers.errors import ChunkCorrupt   # raised by get() for a damaged chunk

# A chunk id becomes a file name, so only allow plain names: letters, digits,
# '.', '_' and '-', not starting with '.'. Anything else (slashes, '..', a
# leading dot) is rejected so a request cannot touch files outside data_dir.
_SAFE_CHUNK_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

# Chunks are written to a temporary file first, then renamed into place, so a
# crash mid-write never leaves a half-written chunk under its real name.
# Temp files start with '.', which no valid chunk id can, so they never clash.
_TEMP_PREFIX = ".tmp-"

# Folder (inside data_dir) holding one checksum file per chunk, same name as
# the chunk. Kept in a sub-folder so it can never clash with a chunk id.
_CHECKSUM_DIR = ".checksums"


def _retry_if_busy(action, attempts: int = 25, delay: float = 0.02):
    """Run `action()`, trying again for a moment if the operating system refuses
    because the file is in use.

    On Windows a file cannot be deleted or replaced while another thread has it
    open, for example a download that is reading the very chunk being deleted.
    That lasts only as long as the read, so waiting a few milliseconds and
    trying again is enough. (Without this, the delete failed with a 500 and the
    chunk stayed on the node with no object pointing to it.) If the file is
    still busy after about half a second, the error is raised as before."""
    for attempt in range(attempts):
        try:
            return action()
        except PermissionError:
            if attempt == attempts - 1:
                raise
            time.sleep(delay)


def _read_file(path: str) -> bytes:
    """Read a whole file, waiting briefly if Windows refuses to open it because
    it is being deleted or replaced at that moment. Raises FileNotFoundError if
    the file is (or, after the wait, turns out to be) gone."""
    def read():
        with open(path, "rb") as f:
            return f.read()
    try:
        return _retry_if_busy(read)
    except PermissionError:
        if not os.path.exists(path):
            raise FileNotFoundError(path) from None
        raise


class ChunkStore:
    """Keeps chunks as files in a folder, so they survive a restart."""

    def __init__(self, data_dir: str, capacity: int):
        self.data_dir = data_dir
        self.capacity = capacity
        self._checksum_dir = os.path.join(data_dir, _CHECKSUM_DIR)
        # A web server handles requests on several threads. This lock makes
        # "check free space, then write" one step so two uploads can't both
        # squeeze into the last bit of space.
        self._lock = threading.Lock()

        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self._checksum_dir, exist_ok=True)
        self._remove_leftover_temp_files()

    def _remove_leftover_temp_files(self) -> None:
        """Delete temp files left behind by a crash during a write."""
        for folder in (self.data_dir, self._checksum_dir):
            for entry in os.scandir(folder):
                if entry.is_file() and entry.name.startswith(_TEMP_PREFIX):
                    os.remove(entry.path)

    def _path_for(self, chunk_id: str) -> str:
        """File path for a chunk. Raises ValueError for an unsafe id."""
        if not isinstance(chunk_id, str) or not _SAFE_CHUNK_ID.match(chunk_id) or ".." in chunk_id:
            raise ValueError(f"Invalid chunk id: {chunk_id!r}")
        return os.path.join(self.data_dir, chunk_id)

    def _checksum_path_for(self, chunk_id: str) -> str:
        """Path of the file holding this chunk's stored checksum."""
        self._path_for(chunk_id)   # validates the id
        return os.path.join(self._checksum_dir, chunk_id)

    def _chunk_files(self):
        """Yield the directory entries that are real chunks (not temp files)."""
        for entry in os.scandir(self.data_dir):
            if entry.is_file() and not entry.name.startswith("."):
                yield entry

    def _write_atomically(self, path: str, content: bytes) -> None:
        """Write via a temp file in the same folder, then rename into place."""
        temp_path = os.path.join(os.path.dirname(path), _TEMP_PREFIX + os.path.basename(path))
        try:
            with open(temp_path, "wb") as f:
                f.write(content)
            _retry_if_busy(lambda: os.replace(temp_path, path))   # atomic: all of it or none of it
        except BaseException:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def used_space(self) -> int:
        """Total bytes of chunk data on disk (temp files included, so space
        being written right now is counted). The tiny checksum files are not
        counted."""
        return sum(e.stat().st_size for e in os.scandir(self.data_dir) if e.is_file())

    def free_space(self) -> int:
        """capacity - used_space."""
        return self.capacity - self.used_space()

    def chunk_count(self) -> int:
        """Number of stored chunks."""
        return sum(1 for _ in self._chunk_files())

    def store(self, chunk_id: str, data: bytes) -> bool:
        """Write the chunk file and its checksum. Returns False if it doesn't
        fit. Storing an id that already exists replaces it; only the size
        difference needs room. Raises ValueError for an unsafe id."""
        path = self._path_for(chunk_id)
        checksum_path = self._checksum_path_for(chunk_id)

        with self._lock:
            existing = os.path.getsize(path) if os.path.isfile(path) else 0
            if len(data) - existing > self.free_space():
                return False

            # Drop the old checksum first: if we crash between the two writes
            # below, the chunk is merely unchecked, never wrongly "corrupt".
            if os.path.exists(checksum_path):
                _retry_if_busy(lambda: os.remove(checksum_path))
            self._write_atomically(path, data)
            self._write_atomically(checksum_path, compute(data).encode("ascii"))
            return True

    def get(self, chunk_id: str) -> bytes:
        """Read the chunk file and check it against its stored checksum.
        Raises KeyError if absent, ChunkCorrupt if it no longer matches its
        checksum, ValueError for an unsafe id. (A chunk with no checksum file,
        e.g. stored before checksums existed, is returned unchecked.)"""
        path = self._path_for(chunk_id)
        try:
            data = _read_file(path)
        except FileNotFoundError:
            raise KeyError(chunk_id) from None

        stored = self.stored_checksum(chunk_id)
        if stored is not None and not verify(data, stored):
            raise ChunkCorrupt(f"Chunk '{chunk_id}' no longer matches its checksum")
        return data

    def stored_checksum(self, chunk_id: str):
        """The checksum saved when the chunk was written, or None if there is
        none. Raises ValueError for an unsafe id."""
        try:
            return _read_file(self._checksum_path_for(chunk_id)).decode("ascii").strip()
        except FileNotFoundError:
            return None

    def compute_checksum(self, chunk_id: str) -> str:
        """Checksum of the chunk file as it is on disk right now (recomputed,
        not the saved one). Lets a caller verify a chunk without downloading
        it. Raises KeyError if absent, ValueError for an unsafe id."""
        path = self._path_for(chunk_id)
        try:
            return compute(_read_file(path))
        except FileNotFoundError:
            raise KeyError(chunk_id) from None

    def has(self, chunk_id: str) -> bool:
        """Does the chunk file exist? Raises ValueError for an unsafe id."""
        return os.path.isfile(self._path_for(chunk_id))

    def delete(self, chunk_id: str) -> None:
        """Remove the chunk file and its checksum; no error if absent. Raises
        ValueError for an unsafe id."""
        path = self._path_for(chunk_id)
        checksum_path = self._checksum_path_for(chunk_id)
        with self._lock:
            for p in (path, checksum_path):
                try:
                    _retry_if_busy(lambda: os.remove(p))
                except FileNotFoundError:
                    pass
