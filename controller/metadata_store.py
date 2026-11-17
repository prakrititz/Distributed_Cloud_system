"""Where the controller keeps its metadata so it survives a restart.

The distributor keeps every object's metadata (name, size, chunk list, where
each copy lives, checksums, custom key-values) in memory for speed. With a
store, it also saves all of it after every change (upload, delete, metadata
update, repair) and loads it when it starts, so restarting the controller
keeps every object downloadable: the chunk files are on the workers anyway.

Two stores, same two methods (load / save):
  JsonFileStore  one JSON file. Saving writes a temporary file and renames it
                 over the old one, which is atomic: a crash mid-save leaves the
                 previous complete file, never a half-written one.
  MemoryStore    keeps nothing (metadata lost on restart). For tests, and for
                 METADATA_PATH="" if you want the old behaviour.
"""
import json
import os
import threading
from dataclasses import asdict

FORMAT_VERSION = 1


class MemoryStore:
    persistent = False

    def load(self) -> list:
        return []

    def save(self, objects: list) -> None:
        pass


class JsonFileStore:
    persistent = True

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()   # one save at a time

    def load(self) -> list:
        """The saved objects as a list of plain dicts ([] if nothing was saved
        yet). Raises ValueError if the file exists but cannot be read: better
        to refuse to start than to start empty and lose track of every object."""
        if not os.path.exists(self.path):
            return []
        try:
            with open(self.path, encoding="utf-8") as f:
                doc = json.load(f)
        except (OSError, ValueError) as e:
            raise ValueError(f"Cannot read the metadata file {self.path}: {e}") from None
        if not isinstance(doc, dict) or doc.get("version") != FORMAT_VERSION or not isinstance(doc.get("objects"), list):
            raise ValueError(f"The metadata file {self.path} is not in the expected format")
        return doc["objects"]

    def save(self, objects: list) -> None:
        """Write every object (ObjectMetadata or dicts) atomically."""
        doc = {"version": FORMAT_VERSION,
               "objects": [o if isinstance(o, dict) else asdict(o) for o in objects]}
        text = json.dumps(doc, indent=1, sort_keys=True)
        with self._lock:
            os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)   # on first save
            temp = self.path + ".tmp"
            with open(temp, "w", encoding="utf-8") as f:
                f.write(text)
                f.flush()
                os.fsync(f.fileno())        # on disk before it replaces the old file
            for attempt in range(25):       # Windows: the file may be open for a moment
                try:
                    os.replace(temp, self.path)
                    break
                except PermissionError:
                    if attempt == 24:
                        raise
                    threading.Event().wait(0.02)


def store_from_setting(path: str):
    """METADATA_PATH -> a store. An empty value means: keep nothing."""
    return JsonFileStore(path) if path else MemoryStore()
