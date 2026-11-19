"""Where the controller keeps its metadata so it survives a restart.

The distributor keeps every object's metadata (name, size, chunk list, where
each copy lives, checksums, custom key-values) in memory for speed. With a
store, it also saves each change (upload, delete, metadata update, repair) and
loads everything when it starts, so restarting the controller keeps every
object downloadable: the chunk files are on the workers anyway.

Three stores, same methods (load / apply):
  SqliteStore    a SQLite database file (the default, METADATA_PATH ending in
                 .db). A change writes only the rows it touches, inside one
                 transaction: the cost does not grow with the number of objects
                 stored, and a crash mid-change leaves the previous state.
  JsonFileStore  one JSON file (METADATA_PATH ending in .json). Every change
                 rewrites the whole file: a temporary file, then a rename, which
                 is atomic. Simple, but slower the more objects there are.
  MemoryStore    keeps nothing (metadata lost on restart). For tests, and for
                 METADATA_PATH="".

apply(metadata, objects=(), removed=(), chunks=()) is called after every
change, with the distributor's full metadata (object_id -> ObjectMetadata) and
what changed: object ids added or replaced, object ids deleted, and ChunkInfos
whose list of copies changed (repair, cleanup).

The stores also keep the clients that registered themselves (see
controller/access.py, ClientRegistry): load_clients() -> [(client_id,
key_hash)], add_client(client_id, key_hash, created_at) (ValueError if the id
or the hash is taken). Only a hash of each key is ever stored.
"""
import json
import os
import sqlite3
import threading
from dataclasses import asdict

FORMAT_VERSION = 1


def _as_dict(obj) -> dict:
    return obj if isinstance(obj, dict) else asdict(obj)


class ClientTaken(ValueError):
    """A client id (or key hash) that is already registered."""


class MemoryStore:
    persistent = False

    def __init__(self):
        self._clients = {}          # client_id -> key_hash (lost on restart, like everything here)

    def load(self) -> list:
        return []

    def load_clients(self) -> list:
        return list(self._clients.items())

    def add_client(self, client_id: str, key_hash: str, created_at: str) -> None:
        if client_id in self._clients or key_hash in self._clients.values():
            raise ClientTaken(client_id)
        self._clients[client_id] = key_hash

    def apply(self, metadata: dict, objects=(), removed=(), chunks=()) -> None:
        pass

    def save(self, objects: list) -> None:
        pass

    def close(self) -> None:
        pass


class JsonFileStore:
    persistent = True

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()   # one save at a time
        self._clients = None            # registered clients, read from the file when first needed

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
        self._clients = list(doc.get("clients", []))
        return doc["objects"]

    def _known_clients(self) -> list:
        if self._clients is None:
            try:
                with open(self.path, encoding="utf-8") as f:
                    self._clients = list(json.load(f).get("clients", []))
            except (OSError, ValueError, AttributeError):
                self._clients = []
        return self._clients

    def load_clients(self) -> list:
        return [(c["client_id"], c["key_hash"]) for c in self._known_clients()]

    def add_client(self, client_id: str, key_hash: str, created_at: str) -> None:
        """Add a client and write the file again (with the objects it holds)."""
        objects = self.load() if os.path.exists(self.path) else []   # (re-reads the clients too)
        clients = self._known_clients()
        if any(c["client_id"] == client_id or c["key_hash"] == key_hash for c in clients):
            raise ClientTaken(client_id)
        clients.append({"client_id": client_id, "key_hash": key_hash, "created_at": created_at})
        self.save(objects)

    def apply(self, metadata: dict, objects=(), removed=(), chunks=()) -> None:
        """Whatever changed, the whole file is written again."""
        self.save(list(metadata.values()))

    def save(self, objects: list) -> None:
        """Write every object (ObjectMetadata or dicts) and the registered
        clients atomically."""
        doc = {"version": FORMAT_VERSION, "objects": [_as_dict(o) for o in objects],
               "clients": self._known_clients()}
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

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------
# Three tables, one row per object, per chunk and per copy. Deleting an object
# deletes its chunks and their copies (ON DELETE CASCADE).
SCHEMA = """
CREATE TABLE IF NOT EXISTS info (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS objects (
    object_id    TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    size         INTEGER NOT NULL,
    checksum     TEXT NOT NULL,
    content_type TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    modified_at  TEXT NOT NULL,
    source_path  TEXT NOT NULL DEFAULT '',
    client_id    TEXT NOT NULL DEFAULT '',
    custom       TEXT NOT NULL DEFAULT '{}',         -- JSON object of the custom key-values
    readers      TEXT NOT NULL DEFAULT '[]'          -- JSON list of other clients that may read it
);
-- the duplicate rule, kept by the database too: one object per client and path
CREATE UNIQUE INDEX IF NOT EXISTS objects_one_per_path
    ON objects (client_id, source_path) WHERE source_path <> '';
CREATE TABLE IF NOT EXISTS chunks (
    chunk_id  TEXT PRIMARY KEY,
    object_id TEXT NOT NULL REFERENCES objects (object_id) ON DELETE CASCADE,
    idx       INTEGER NOT NULL,                      -- position in the object ("index" is an SQL word)
    size      INTEGER,                               -- NULL: recorded before sizes were kept
    checksum  TEXT NOT NULL,
    UNIQUE (object_id, idx)
);
CREATE TABLE IF NOT EXISTS replicas (
    chunk_id  TEXT NOT NULL REFERENCES chunks (chunk_id) ON DELETE CASCADE,
    position  INTEGER NOT NULL,                      -- the order of the copies
    worker_id TEXT NOT NULL,
    PRIMARY KEY (chunk_id, position)
);
-- "which chunks are on worker3?" without reading everything
CREATE INDEX IF NOT EXISTS replicas_by_worker ON replicas (worker_id);
-- clients that registered themselves: only a hash of each key is kept
CREATE TABLE IF NOT EXISTS clients (
    client_id  TEXT PRIMARY KEY,
    key_hash   TEXT NOT NULL UNIQUE,                -- SHA-256 of the key, hex; looked up on every request
    created_at TEXT NOT NULL
);
"""


class SqliteStore:
    """The metadata in a SQLite database file.

    SQLite is a library, not a server: the controller process reads and writes
    the file itself (in Docker: /meta/metadata.db in the controller-data
    volume). Every change is one transaction that touches only its own rows.
    WAL mode (a write-ahead log next to the file) keeps it crash-safe: after a
    crash, the next open finishes or undoes the last transaction.

    If the database is new and a metadata.json from the older JSON store sits
    in the same folder, its objects are imported once (the JSON file is left
    as it was)."""
    persistent = True

    def __init__(self, path: str, import_json: bool = True):
        self.path = path
        self._lock = threading.Lock()          # one connection, used by one thread at a time
        new = not os.path.exists(path)
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        try:
            self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
            self._db.execute("PRAGMA journal_mode = WAL")
            self._db.execute("PRAGMA synchronous = FULL")       # on disk before a change counts as done
            self._db.execute("PRAGMA foreign_keys = ON")
            self._db.executescript(SCHEMA)
            # a database made before objects could be shared: add the column
            columns = {row[1] for row in self._db.execute("PRAGMA table_info(objects)")}
            if "readers" not in columns:
                self._db.execute("ALTER TABLE objects ADD COLUMN readers TEXT NOT NULL DEFAULT '[]'")
            version = self._db.execute("SELECT value FROM info WHERE key = 'version'").fetchone()
        except sqlite3.DatabaseError as e:
            raise ValueError(f"Cannot open the metadata database {path}: {e}") from None
        if version is None:
            self._db.execute("INSERT INTO info (key, value) VALUES ('version', ?)", (str(FORMAT_VERSION),))
        elif version[0] != str(FORMAT_VERSION):
            raise ValueError(f"The metadata database {path} has format {version[0]}, expected {FORMAT_VERSION}")
        if new and import_json:
            self._import_json(os.path.join(os.path.dirname(os.path.abspath(path)), "metadata.json"))

    # --- reading ---------------------------------------------------------------
    def load(self) -> list:
        """Every object as a plain dict, with its chunks and their copies."""
        with self._lock:
            replicas = {}
            for chunk_id, worker_id in self._db.execute(
                    "SELECT chunk_id, worker_id FROM replicas ORDER BY chunk_id, position"):
                replicas.setdefault(chunk_id, []).append(worker_id)
            chunks = {}
            for chunk_id, object_id, idx, size, checksum in self._db.execute(
                    "SELECT chunk_id, object_id, idx, size, checksum FROM chunks ORDER BY object_id, idx"):
                chunks.setdefault(object_id, []).append(
                    {"chunk_id": chunk_id, "index": idx, "replicas": replicas.get(chunk_id, []),
                     "checksum": checksum, "size": size})
            return [
                {"object_id": row[0], "name": row[1], "size": row[2], "checksum": row[3],
                 "content_type": row[4], "created_at": row[5], "modified_at": row[6],
                 "source_path": row[7], "client_id": row[8], "custom": json.loads(row[9]),
                 "readers": json.loads(row[10]), "chunks": chunks.get(row[0], [])}
                for row in self._db.execute(
                    "SELECT object_id, name, size, checksum, content_type, created_at, modified_at, "
                    "source_path, client_id, custom, readers FROM objects ORDER BY created_at, object_id")
            ]

    def load_clients(self) -> list:
        """[(client_id, key_hash)] of every registered client."""
        with self._lock:
            return self._db.execute("SELECT client_id, key_hash FROM clients ORDER BY created_at").fetchall()

    def add_client(self, client_id: str, key_hash: str, created_at: str) -> None:
        """Register a client. Raises ClientTaken if the id (or the hash) exists."""
        try:
            with self._lock, self._transaction():
                self._db.execute("INSERT INTO clients (client_id, key_hash, created_at) VALUES (?, ?, ?)",
                                 (client_id, key_hash, created_at))
        except sqlite3.IntegrityError:
            raise ClientTaken(client_id) from None

    def chunks_on(self, worker_id: str) -> list:
        """(object_id, chunk_id) of every copy recorded on a worker, from the
        index: the question repair and cleanup ask after a worker failed."""
        with self._lock:
            return self._db.execute(
                "SELECT c.object_id, r.chunk_id FROM replicas r JOIN chunks c ON c.chunk_id = r.chunk_id "
                "WHERE r.worker_id = ? ORDER BY c.object_id, c.idx", (worker_id,)).fetchall()

    # --- writing ---------------------------------------------------------------
    def apply(self, metadata: dict, objects=(), removed=(), chunks=()) -> None:
        """Write one change, in one transaction: replace the given objects
        (with their chunks and copies), delete the removed ones, and rewrite
        the list of copies of the given chunks. Nothing else is touched."""
        with self._lock, self._transaction():
            for object_id in removed:
                self._db.execute("DELETE FROM objects WHERE object_id = ?", (object_id,))
            for object_id in objects:
                meta = metadata.get(object_id)
                if meta is not None:
                    self._put(_as_dict(meta))
            for info in chunks:
                info = _as_dict(info)
                self._db.execute("DELETE FROM replicas WHERE chunk_id = ?", (info["chunk_id"],))
                self._db.executemany(
                    "INSERT INTO replicas (chunk_id, position, worker_id) VALUES (?, ?, ?)",
                    [(info["chunk_id"], i, w) for i, w in enumerate(info["replicas"])])

    def save(self, objects: list) -> None:
        """Replace everything with these objects (used by tests and the import)."""
        with self._lock, self._transaction():
            self._db.execute("DELETE FROM objects")
            for obj in objects:
                self._put(_as_dict(obj))

    def close(self) -> None:
        with self._lock:
            self._db.close()

    # --- internals -------------------------------------------------------------
    def _transaction(self):
        db = self._db

        class Transaction:
            def __enter__(self):
                db.execute("BEGIN IMMEDIATE")

            def __exit__(self, kind, error, tb):
                db.execute("COMMIT" if kind is None else "ROLLBACK")
                return False
        return Transaction()

    def _put(self, d: dict) -> None:
        """Insert one object with its chunks and copies, replacing any old version."""
        self._db.execute("DELETE FROM objects WHERE object_id = ?", (d["object_id"],))
        self._db.execute(
            "INSERT INTO objects (object_id, name, size, checksum, content_type, created_at, modified_at, "
            "source_path, client_id, custom, readers) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (d["object_id"], d["name"], d["size"], d.get("checksum", ""), d.get("content_type", ""),
             d.get("created_at", ""), d.get("modified_at", ""), d.get("source_path") or "",
             d.get("client_id") or "", json.dumps(d.get("custom") or {}, sort_keys=True),
             json.dumps(sorted(d.get("readers") or []))))
        chunks = d.get("chunks", [])
        self._db.executemany(
            "INSERT INTO chunks (chunk_id, object_id, idx, size, checksum) VALUES (?, ?, ?, ?, ?)",
            [(c["chunk_id"], d["object_id"], c["index"], c.get("size"), c.get("checksum", "")) for c in chunks])
        self._db.executemany(
            "INSERT INTO replicas (chunk_id, position, worker_id) VALUES (?, ?, ?)",
            [(c["chunk_id"], i, w) for c in chunks for i, w in enumerate(c.get("replicas", []))])

    def _import_json(self, json_path: str) -> None:
        if not os.path.exists(json_path):
            return
        objects = JsonFileStore(json_path).load()
        self.save(objects)
        with self._lock:
            self._db.execute("INSERT OR REPLACE INTO info (key, value) VALUES ('imported_from', ?)", (json_path,))
        print(f"[metadata] imported {len(objects)} object(s) from {json_path} into {self.path}")


def store_from_setting(path: str):
    """METADATA_PATH -> a store. Empty: keep nothing. Ending in .json: the JSON
    file store. Anything else (normally .../metadata.db): SQLite."""
    if not path:
        return MemoryStore()
    if path.lower().endswith(".json"):
        return JsonFileStore(path)
    return SqliteStore(path)
