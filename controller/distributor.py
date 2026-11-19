"""Distributor: takes a client's request, splits data into chunks and
decides which nodes store each chunk. Also reassembles on download.

The distributor is the only component that knows the whole picture:
which nodes exist, how full they are, and where every copy of every chunk
of every object lives. Nodes just store bytes and never talk to each other.

Replication: every chunk is stored on REPLICATION_FACTOR different nodes.
The distributor writes each copy itself.

Objects are identified by an object_id the distributor generates. The file
name is only metadata and is not unique; what makes an upload a duplicate is
its source_path (where the client read the file from) together with the
client_id of the client that sent it: the same path from the same client is the
same file, while two different clients may use the same path string.

Workers join by registering (register_node), so their number is not fixed.

Uploads and downloads are streamed: the data passes through one chunk at a
time, so the controller never holds a whole object in memory, whatever its
size (see upload and stream_object). Within one transfer, a few chunks
(parallel_chunks) are moving at once: an upload writes the copies of several
chunks at the same time, a download fetches the next chunks while sending the
current one. The data is still read, checksummed and sent in order.

The nodes it is given are RemoteNode objects (controller/remote_node.py): every call on
them is an HTTP request to that node's server. The distributor only relies on
these methods: health(), alive, store_chunk(), get_chunk(), has_chunk() and
delete_chunk().
"""

import io
import threading
import time
import uuid
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone

from config import HEARTBEAT_TIMEOUT, PARALLEL_CHUNKS
from controller.chunk_cache import ChunkCache
from controller.metadata_store import MemoryStore
from helpers.checksum import compute, matches, running, verify
from helpers.errors import ChecksumMismatch, ChunkCorrupt, NodeUnavailable, QuotaExceeded

CHUNK_SIZE = 1024 * 1024    # 1 MB, as the project spec says
REPLICATION_FACTOR = 2      # copies of every chunk, each on a different node
DEFAULT_CONTENT_TYPE = "application/octet-stream"


def _read_exactly(stream, n: int) -> bytes:
    """Read n bytes from a file-like object; fewer only at the end of the
    data. (One read() may return less than asked, e.g. from a network
    connection, so keep reading.)"""
    parts, got = [], 0
    while got < n:
        part = stream.read(n - got)
        if not part:
            break
        parts.append(part)
        got += len(part)
    return parts[0] if len(parts) == 1 else b"".join(parts)


def _now() -> str:
    """Current time in UTC as an ISO 8601 string, e.g. 2026-11-15T10:12:03Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class ChunkInfo:
    """Where the copies of one chunk live, and what they must contain."""
    chunk_id: str
    index: int                                  # position within the object
    replicas: list = field(default_factory=list)  # node_ids holding a copy
    # Checksum of the chunk's bytes, computed when the file was split. The
    # distributor's own record is the authority: every copy read back from a
    # node must match it.
    checksum: str = ""
    size: int = None                            # bytes in this chunk (None: recorded before sizes were kept)


@dataclass
class ObjectMetadata:
    """Everything the system knows about one object. Kept as a separate object
    so it can be moved/exposed later."""
    object_id: str
    name: str                                   # file name, for display; NOT unique
    size: int                                   # size of the original object, bytes
    chunks: list = field(default_factory=list)  # list of ChunkInfo, in order
    checksum: str = ""                          # checksum of the whole object
    content_type: str = DEFAULT_CONTENT_TYPE
    created_at: str = ""
    modified_at: str = ""                       # changes when custom metadata is patched
    custom: dict = field(default_factory=dict)  # user-defined key-value metadata
    source_path: str = ""                       # where the client read the file
    client_id: str = ""                         # which client sent it (its owner); (client_id, source_path) is unique
    readers: list = field(default_factory=list)  # other clients allowed to read it

    @property
    def stored_bytes(self) -> int:
        """Bytes this object takes on the workers (one copy): the sum of its
        chunks, or its size for objects saved before chunk sizes were kept."""
        sizes = [c.size for c in self.chunks]
        return sum(sizes) if sizes and None not in sizes else self.size

    @staticmethod
    def from_dict(d: dict) -> "ObjectMetadata":
        """Rebuild from what the metadata store saved."""
        d = dict(d)
        d["chunks"] = [ChunkInfo(**c) for c in d.get("chunks", [])]
        return ObjectMetadata(**d)


class _UnregisteredNode:
    """Stands in for a worker that the metadata mentions but that has not
    registered (yet), for example right after the controller restarted. It
    behaves like a worker that is down, so downloads use the other copy and
    repair treats it as unreachable."""

    def __init__(self, node_id: str):
        self.node_id = node_id

    def _down(self, *args, **kwargs):
        raise NodeUnavailable(f"{self.node_id} has not registered")

    health = store_chunk = get_chunk = get_checksum = delete_chunk = list_chunks = _down
    alive = False

    def has_chunk(self, chunk_id: str) -> bool:
        return False


class ChunkStream:
    """The chunks of one download, as an iterator, plus close(): call it when
    the download is finished or abandoned (it is also called when the
    iterator runs out or fails). Usable in a with block."""

    def __init__(self, chunks, on_close):
        self._chunks = chunks
        self._on_close = on_close

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        try:
            return next(self._chunks)
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        """Release the download (once; later calls do nothing)."""
        on_close, self._on_close = self._on_close, None
        if on_close is not None:
            self._chunks.close()
            on_close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class Distributor:
    def __init__(self, nodes: list, chunk_size: int = CHUNK_SIZE,
                 replication_factor: int = REPLICATION_FACTOR,
                 heartbeat_timeout: float = HEARTBEAT_TIMEOUT, store=None,
                 parallel_chunks: int = PARALLEL_CHUNKS, cache: ChunkCache = None):
        # The node registry: a list of RemoteNode. Workers are added while the
        # system runs (register_node), so the list is never changed in place:
        # a new list replaces it, and a request already looping over the old
        # one is not disturbed.
        self.nodes = list(nodes)
        self._nodes_lock = threading.Lock()
        self.heartbeat_timeout = heartbeat_timeout
        # What the nodes last told us, so placing a chunk needs no requests:
        # node_id -> {capacity, used, free, chunks, last_seen}. last_seen is a
        # time.monotonic() value, or None if the node is known to be down.
        self._status = {}
        self._status_lock = threading.Lock()
        self.chunk_size = chunk_size   # a parameter so we can test with tiny chunks
        # How many chunks of one upload or download are moving at once. Each
        # holds one chunk in memory, so this also bounds a transfer's memory.
        self.parallel_chunks = max(1, parallel_chunks)
        # Chunks recently downloaded, kept in memory (controller/chunk_cache.py).
        # Off (size 0) unless one is given.
        self.cache = cache if cache is not None else ChunkCache(0)
        # Choosing nodes reads and updates the cached free space and the pair
        # counts: one choice at a time, so two chunks in flight never both
        # count on the same free space.
        self._place_lock = threading.Lock()
        self.replication_factor = replication_factor
        self.metadata = {}             # object_id -> ObjectMetadata
        self.paths = {}                # (client_id, source_path) -> object_id (duplicate check)
        # The web server handles requests on several threads. This lock makes
        # "check the path is free, then register the object" one step, and
        # protects changes to an object's metadata.
        self._lock = threading.Lock()
        self._repair_lock = threading.Lock()   # only one repair (or cleanup) at a time

        # Persistence: the metadata is saved after every change and loaded here.
        self.store = store or MemoryStore()
        for d in self.store.load():
            meta = ObjectMetadata.from_dict(d)
            self.metadata[meta.object_id] = meta
            if meta.source_path:
                self.paths[(meta.client_id, meta.source_path)] = meta.object_id
        if self.metadata:
            print(f"[metadata] loaded {len(self.metadata)} object(s)")

        # Placement spreads copies over many different PAIRS of workers (see
        # _pick_nodes): how often each pair already holds a chunk together,
        # and a counter that rotates which worker wins a tie.
        self._pair_use = Counter()
        self._rotation = 0
        for meta in self.metadata.values():
            for info in meta.chunks:
                self._note_copies(info.replicas)

        # Object ids whose upload is still storing chunks. Their chunks are on
        # the workers but not in the metadata yet: cleanup must not touch them.
        self._in_flight = set()

        # Downloads in progress: object_id -> how many. An object deleted while
        # it is being downloaded disappears from the metadata at once, but its
        # chunks stay until the last of those downloads has finished
        # (_deleted_while_read holds its metadata until then), so a download
        # never loses its data half-way.
        self._reading = Counter()
        self._deleted_while_read = {}

        # Storage per client, for quotas: bytes of the objects it owns
        # (_used), and bytes of its uploads still arriving (_pending).
        self._used = Counter()
        self._pending = Counter()
        for meta in self.metadata.values():
            self._used[meta.client_id] += meta.stored_bytes

        # Automatic repair (auto_repair_check): when each worker was first
        # seen down, and the workers already repaired for.
        self._down_since = {}
        self._repaired_for = set()
        self.last_auto_repair = None

    # ------------------------------------------------------------------
    # persistence
    # ------------------------------------------------------------------
    def _save(self, objects=(), removed=(), chunks=()) -> None:
        """Save a change: the object ids added or replaced, the object ids
        deleted, and the ChunkInfos whose list of copies changed. A database
        store writes only those rows; the JSON store rewrites everything.
        Call with self._lock held, right after the change, so saves happen in
        the same order as the changes."""
        self.store.apply(self.metadata, objects=objects, removed=removed, chunks=chunks)

    @property
    def persistent(self) -> bool:
        """True if the metadata is saved, i.e. it survives a restart, so it is
        the full truth about what should be on the workers."""
        return self.store.persistent

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _read_chunks(self, source):
        """Yield the data as chunks of chunk_size bytes (the last one may be
        shorter), reading only one chunk at a time. `source` is bytes or a
        binary file-like object (an open file, the body of a request).
        Empty data still gives one (empty) chunk so the object has a record."""
        if isinstance(source, (bytes, bytearray, memoryview)):
            source = io.BytesIO(source)
        index = 0
        while True:
            chunk = _read_exactly(source, self.chunk_size)
            if chunk or index == 0:
                yield chunk
            if len(chunk) < self.chunk_size:
                return
            index += 1

    # ------------------------------------------------------------------
    # what the nodes told us (heartbeats)
    # ------------------------------------------------------------------
    def record_heartbeat(self, node_id: str, capacity: int, used: int, free: int, chunks: int) -> None:
        """A node reports that it is alive and how much space it has. Called
        by the API when a heartbeat arrives. Raises KeyError for an unknown
        node."""
        self._get_node(node_id)
        with self._status_lock:
            self._status[node_id] = {"capacity": capacity, "used": used, "free": free,
                                     "chunks": chunks, "last_seen": time.monotonic()}

    def register_node(self, node, capacity: int, used: int, free: int, chunks: int) -> bool:
        """A worker announces itself (it has just started). Adds it to the
        registry, or, if a worker with this id is already known (a restart),
        replaces the old entry, because its address may have changed. Its
        report counts as a heartbeat, so it is alive at once. Returns True if
        the worker is new."""
        with self._nodes_lock:
            others = [n for n in self.nodes if n.node_id != node.node_id]
            is_new = len(others) == len(self.nodes)
            self.nodes = others + [node] if is_new else \
                [node if n.node_id == node.node_id else n for n in self.nodes]   # keep its place
        self.record_heartbeat(node.node_id, capacity, used, free, chunks)
        return is_new

    def _mark_down(self, node_id: str) -> None:
        """A request to this node just failed: treat it as down at once, rather
        than waiting for its heartbeat to go stale. A heartbeat revives it."""
        with self._status_lock:
            entry = self._status.setdefault(node_id, {"capacity": 0, "used": 0, "free": 0, "chunks": 0})
            entry["last_seen"] = None

    def _reserve(self, node_id: str, nbytes: int) -> None:
        """We just stored nbytes on this node: lower its cached free space now,
        because its next heartbeat may be a moment away and an upload places
        many chunks in between."""
        with self._status_lock:
            entry = self._status.get(node_id)
            if entry is not None:
                entry["free"] = max(0, entry["free"] - nbytes)

    def _cached_state(self, node):
        """(alive, free_space) of a node from the cache, with no request. A node
        that has never been heard of is asked once, so a controller that just
        started can place chunks before the first heartbeats arrive."""
        with self._status_lock:
            entry = self._status.get(node.node_id)
        if entry is None:
            try:
                h = node.health()
                self.record_heartbeat(node.node_id, h["capacity"], h["used"], h["free"], h["chunks"])
            except NodeUnavailable:
                self._mark_down(node.node_id)
            with self._status_lock:
                entry = self._status[node.node_id]
        seen = entry["last_seen"]
        alive = seen is not None and time.monotonic() - seen <= self.heartbeat_timeout
        return alive, entry["free"]

    def counts_as_down(self, node_id: str) -> bool:
        """True if the controller already counts this node as down (no
        heartbeat for heartbeat_timeout, or a request to it just failed),
        without asking it. A node never heard of is not counted down."""
        with self._status_lock:
            entry = self._status.get(node_id)
        if entry is None:
            return False
        seen = entry["last_seen"]
        return seen is None or time.monotonic() - seen > self.heartbeat_timeout

    def heartbeat_age(self, node_id: str):
        """Seconds since the node last reported, or None if it never has."""
        with self._status_lock:
            entry = self._status.get(node_id)
        if entry is None or entry["last_seen"] is None:
            return None
        return round(time.monotonic() - entry["last_seen"], 1)

    def _pick_nodes(self, chunk_size: int, count: int, exclude=(), partners=()):
        """Choose `count` DIFFERENT live nodes that can fit this chunk. Returns
        the list, or None if fewer than `count` nodes qualify. Nodes whose id
        is in `exclude` are never chosen.

        Order of preference:
          1. the node that has shared the fewest chunks with `partners` (the
             nodes already holding a copy of THIS chunk), so the copies of
             different chunks land on many different pairs of workers. With
             fixed pairs, losing both workers of one pair would lose all their
             chunks; spread out, losing any two workers loses few chunks.
          2. most free space
          3. a rotating start, so equal nodes take turns.
        A node without room for the chunk is never chosen.

        Decides from what the nodes last reported (heartbeats), so it sends no
        requests. The picture can be a moment old: if a chosen node turns out
        to be dead or full, the caller sees the failed write and picks
        another."""
        nodes = self.nodes
        start = self._rotation % len(nodes) if nodes else 0
        candidates = []   # (shared_with_partners, -free_space, turn, node)
        for i, node in enumerate(nodes):
            if node.node_id in exclude:
                continue
            alive, free = self._cached_state(node)
            if alive and free >= chunk_size:
                shared = sum(self._pair_use[frozenset((node.node_id, p))] for p in partners)
                candidates.append((shared, -free, (i - start) % len(nodes), node))

        if len(candidates) < count:
            return None
        candidates.sort(key=lambda c: c[:3])
        return [c[3] for c in candidates[:count]]

    def _note_copies(self, node_ids, sign: int = 1) -> None:
        """These nodes now hold copies of the same chunk: count each pair
        (sign=-1 takes a count back, when planned copies change)."""
        ids = list(node_ids)
        with self._status_lock:
            for a in range(len(ids)):
                for b in range(a + 1, len(ids)):
                    self._pair_use[frozenset((ids[a], ids[b]))] += sign

    def _choose(self, size: int, count: int, exclude=(), holders=(), what: str = "a chunk") -> list:
        """Choose `count` more nodes for a chunk of `size` bytes, one at a time
        (each choice counts the earlier ones as partners, so copies spread
        over worker pairs), and reserve the space on them at once, so the
        next chunk in flight sees it as taken. Raises RuntimeError if there
        are not enough live nodes with room."""
        with self._place_lock:
            chosen = []
            for _ in range(count):
                taken = set(exclude) | set(holders) | {n.node_id for n in chosen}
                picked = self._pick_nodes(size, 1, exclude=taken,
                                          partners=list(holders) + [n.node_id for n in chosen])
                if picked is None:
                    for node in chosen:                       # give the space back
                        self._reserve(node.node_id, -size)
                    raise RuntimeError(
                        f"Not enough storage: need {self.replication_factor} different "
                        f"live nodes with room for {what}")
                self._reserve(picked[0].node_id, size)
                chosen.append(picked[0])
            return chosen

    def _get_node(self, node_id: str):
        """Find a registered node by its id. Raises KeyError if unknown."""
        for node in self.nodes:
            if node.node_id == node_id:
                return node
        raise KeyError(f"Unknown node: {node_id}")

    def _node_for(self, node_id: str):
        """The node with this id, or, if it has not registered (yet), a
        stand-in that behaves like a node that is down."""
        try:
            return self._get_node(node_id)
        except KeyError:
            return _UnregisteredNode(node_id)

    def _rollback(self, written: list) -> None:
        """Delete every copy stored so far by a failed upload."""
        for chunk_id, node in written:
            try:
                node.delete_chunk(chunk_id)
            except NodeUnavailable:
                print(f"[upload] warning: {node.node_id} went down, "
                      f"copy of {chunk_id} left behind")

    @staticmethod
    def _duplicate_message(client_id: str, source_path: str, existing_id: str) -> str:
        who = f"client '{client_id}'" if client_id else "this client"
        return (f"An object uploaded by {who} from '{source_path}' already exists "
                f"(object_id {existing_id}); delete it first to upload it again")

    # ------------------------------------------------------------------
    # upload / download
    # ------------------------------------------------------------------
    def upload(self, name: str, data, content_type: str = DEFAULT_CONTENT_TYPE,
               custom: dict = None, source_path: str = None, size: int = None,
               client_id: str = None, checksum: str = None, quota: int = None) -> str:
        """Store the data as chunks, each on REPLICATION_FACTOR different
        nodes, record metadata. Return the new object_id.

        data         the object: bytes, or a binary file-like object (an open
                     file, the body of an HTTP request). It is read one chunk
                     at a time, in order. Up to parallel_chunks chunks are
                     being stored at once (their copies written at the same
                     time); reading waits while all of them are busy, so only
                     about parallel_chunks chunks are ever in memory.
        name         file name, kept as metadata (several objects can share it)
        content_type e.g. 'video/mp4'
        custom       user-defined key-value metadata
        source_path  where the client read the file from. Two uploads with the
                     same source_path from the same client are the same file,
                     so the second is refused. Optional: without it, nothing
                     is checked.
        client_id    which client is uploading. It scopes the duplicate check,
                     so two clients can each upload their own '/files/a.txt'.
                     Optional: uploads without it share one anonymous scope.
        size         size of the original object if it differs from the bytes
                     uploaded (e.g. the client wraps the file in a header);
                     defaults to the number of bytes uploaded
        checksum     the client's checksum of the whole object (optional). The
                     checksum of what arrived is built up chunk by chunk, in
                     reading order, and compared at the end; on a mismatch
                     everything stored is deleted and ChecksumMismatch is raised.
        quota        bytes this client may store in total (optional). The
                     upload is counted against it while it arrives, together
                     with the client's stored objects and its other uploads
                     still arriving; as soon as it would go over, everything
                     stored for it is deleted and QuotaExceeded is raised.

        The object_id is generated here, used to name the chunks, and only
        becomes visible (metadata and path index) once ALL copies of ALL
        chunks are stored and the checksum matches. If anything fails (a copy
        cannot be stored, the data stops arriving, the checksum is wrong),
        the chunks still being stored are waited for, every copy written is
        deleted and the error is raised again, so nothing half-uploaded is
        left behind. (Strict: no chunk is ever stored with fewer copies than
        the replication factor.)

        Raises FileExistsError if this client already has an object from this
        source_path, RuntimeError if there is not enough room on enough live
        nodes, ChecksumMismatch for a damaged upload, QuotaExceeded when the
        client's quota would be exceeded.
        """
        client_id = client_id or ""
        path_key = (client_id, source_path)
        if source_path and path_key in self.paths:
            raise FileExistsError(
                self._duplicate_message(client_id, source_path, self.paths[path_key]))

        object_id = str(uuid.uuid4())
        whole = running()   # checksum of the whole object, built up chunk by chunk
        received = 0        # bytes read so far
        counted = 0         # of those, bytes counted against the client's quota while arriving
        print(f"[upload] '{name}': storing, {self.replication_factor} copies of every chunk, "
              f"up to {self.parallel_chunks} chunks at once")

        # Two lists, two jobs. Both are filled while the chunks are stored.
        #
        # written: "what must I undo if this upload fails?"
        #   One (chunk_id, node) entry per physical COPY stored on a node
        #   (2 per chunk with replication factor 2). Holds node objects
        #   because _rollback calls node.delete_chunk() on them. Chunks are
        #   stored by several threads, so it is guarded by a lock.
        #
        # placed: "what do I record in the metadata?"
        #   One ChunkInfo per CHUNK, listing the node ids of its copies.
        #   Chunks can finish in any order; it is sorted by index at the end.
        written = []
        written_lock = threading.Lock()
        placed = []
        self._in_flight.add(object_id)
        try:
            # The chunks' copies are written by the threads of this pool; the
            # last copy of each chunk is written by the chunk's own thread.
            # Leaving the with block waits for the chunk threads first (they
            # may still hand copies to the pool), then for the copy pool.
            copies = ThreadPoolExecutor(self.parallel_chunks * max(1, self.replication_factor - 1),
                                        thread_name_prefix="copy")
            with copies, ThreadPoolExecutor(self.parallel_chunks, thread_name_prefix="chunk") as chunk_pool:
                in_flight = set()
                # Read in order, one chunk at a time. The whole-object
                # checksum is fed here, at reading time, so it sees the bytes
                # in their real order however the chunks finish.
                for index, chunk in enumerate(self._read_chunks(data)):
                    whole.update(chunk)
                    received += len(chunk)
                    if quota is not None:
                        counted += len(chunk)    # first: the check below counts it even when it refuses
                        self._count_against_quota(client_id, len(chunk), quota)
                    self._rotation += 1        # equal nodes take turns, chunk by chunk
                    # Choosing the nodes is done here, in order, so placement
                    # is the same as if the chunks went one after another.
                    planned = self._choose(len(chunk), self.replication_factor, what=f"chunk {index}")
                    self._note_copies([n.node_id for n in planned])
                    in_flight.add(chunk_pool.submit(
                        self._store_chunk, object_id, index, chunk, planned, copies, written, written_lock))
                    chunk = None               # the task holds it; do not keep a second reference
                    if len(in_flight) >= self.parallel_chunks:
                        done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                        placed.extend(f.result() for f in done)   # a failed chunk raises here
                for f in in_flight:
                    placed.append(f.result())
            placed.sort(key=lambda c: c.index)

            # Every chunk is stored. Now that all of it has gone by, the
            # checksum of the whole object is known: compare it with the
            # client's. A mismatch means the data was damaged on the way here.
            object_checksum = whole.hexdigest()
            if checksum and not matches(object_checksum, checksum):
                raise ChecksumMismatch("Checksum mismatch: the upload was damaged in transit")

            # Register the object. Done under the lock, and the path is checked
            # again: another upload of the same file may have finished while
            # this one was storing chunks.
            with self._lock:
                if source_path and path_key in self.paths:
                    raise FileExistsError(
                        self._duplicate_message(client_id, source_path, self.paths[path_key]))
                now = _now()
                self.metadata[object_id] = ObjectMetadata(
                    object_id=object_id,
                    name=name,
                    size=received if size is None else size,
                    chunks=placed,
                    checksum=object_checksum,
                    content_type=content_type or DEFAULT_CONTENT_TYPE,
                    created_at=now,
                    modified_at=now,
                    custom=dict(custom or {}),
                    source_path=source_path or "",
                    client_id=client_id,
                )
                if source_path:
                    self.paths[path_key] = object_id
                self._used[client_id] += received
                self._save(objects=[object_id])
        except BaseException as e:
            # Whatever went wrong (no space, a node died, the client went
            # away, a bad checksum, a bug), never leave copies behind that no
            # metadata points to. (Leaving the `with` above already waited for
            # every chunk still being stored, so `written` is complete.)
            print(f"[upload] '{name}' failed ({type(e).__name__}: {e}); "
                  f"rolling back {len(written)} copy/copies")
            with self._lock:
                if self.metadata.get(object_id) is not None:   # registered, but the save failed
                    meta = self.metadata.pop(object_id)
                    if meta.source_path and self.paths.get(path_key) == object_id:
                        del self.paths[path_key]
                    self._used[client_id] -= received
            self._rollback(written)
            raise
        finally:
            self._in_flight.discard(object_id)
            if counted:
                with self._lock:
                    self._pending[client_id] -= counted
                    if self._pending[client_id] <= 0:
                        del self._pending[client_id]

        print(f"[upload] done, '{name}': {received} bytes in {len(placed)} chunk(s) -> object_id={object_id}")
        return object_id

    def _store_chunk(self, object_id: str, index: int, chunk: bytes, planned: list,
                     copies: ThreadPoolExecutor, written: list, written_lock) -> "ChunkInfo":
        """Store one chunk's copies on the planned nodes, all at the same time,
        and return its ChunkInfo. Runs on one of the upload's chunk threads.

        A planned node that turns out to be dead, full or to receive the chunk
        damaged is skipped (a dead one is marked down) and a replacement is
        chosen, until the chunk has replication_factor copies. Raises
        RuntimeError when there are not enough nodes left. Every copy stored
        is added to `written`, so the upload can roll it back."""
        chunk_id = f"{object_id}-{index}"
        chunk_checksum = compute(chunk)   # every copy must match it when read back
        size = len(chunk)
        targets = []                      # nodes that now hold a copy
        tried = set()                     # nodes that failed for this chunk
        attempt = list(planned)
        try:
            while True:
                # The extra copies go to the pool; the last one is written by
                # this thread meanwhile, and only then are the others waited for.
                others = [copies.submit(self._store_copy, node, chunk_id, chunk, chunk_checksum)
                          for node in attempt[:-1]]
                mine = self._store_copy(attempt[-1], chunk_id, chunk, chunk_checksum)
                results = [f.result() for f in others] + [mine]
                for node, stored in zip(attempt, results):
                    if stored:
                        with written_lock:
                            written.append((chunk_id, node))   # remember this copy for rollback
                        targets.append(node)
                    else:
                        tried.add(node.node_id)
                        self._reserve(node.node_id, -size)       # it did not take the space
                if len(targets) >= self.replication_factor:
                    break
                attempt = self._choose(size, self.replication_factor - len(targets), exclude=tried,
                                       holders=[n.node_id for n in targets], what=f"chunk {index}")
        finally:
            planned_ids = [n.node_id for n in planned]
            final_ids = [n.node_id for n in targets]
            if sorted(planned_ids) != sorted(final_ids):          # the pair counts follow the real copies
                self._note_copies(planned_ids, -1)
                if len(final_ids) >= self.replication_factor:
                    self._note_copies(final_ids)

        replica_ids = [n.node_id for n in targets]
        print(f"[upload] chunk {index} ({size} bytes) -> {', '.join(replica_ids)}")
        return ChunkInfo(chunk_id, index, replica_ids, chunk_checksum, size)

    def _store_copy(self, node, chunk_id: str, chunk: bytes, chunk_checksum: str) -> bool:
        """Write one copy. True if stored; False if the node is dead (it is
        marked down), full, or says the chunk arrived damaged."""
        try:
            return bool(node.store_chunk(chunk_id, chunk, checksum=chunk_checksum))
        except NodeUnavailable:
            self._mark_down(node.node_id)
            return False
        except ChunkCorrupt:
            return False   # the chunk arrived damaged


    def stream_object(self, object_id: str, use_cache: bool = True):
        """Start a download: returns (metadata, chunks), where chunks is an
        iterator giving the object's bytes one chunk at a time, in order.
        Only one chunk is in memory at a time.

        A chunk in the cache is given out from there, without asking a worker;
        so it can be downloaded even while every worker holding it is down.
        use_cache=False reads every chunk from a worker (what was read is
        still cached).

        Every copy read from a node is checked against the checksum recorded
        at upload; a damaged copy is skipped like a missing one, and the next
        copy is tried. The checksum of the whole object is built up as the
        chunks go by and checked after the last one.

        Problems are found before anything is sent where possible, so the
        caller can still answer with an error:
          - KeyError if there is no such object;
          - RuntimeError if some chunk has no copy on a node that is up, or
            no good copy of the FIRST chunk can be read.
        A problem found later (no good copy of a later chunk, say both copies
        damaged on disk) raises RuntimeError from the iterator, after the
        earlier chunks were already given out: the caller must then abandon
        the transfer (the API cuts the connection, so the client sees it is
        incomplete)."""
        with self._lock:
            meta = self.get_object(object_id)
            self._reading[object_id] += 1        # its chunks now outlive a delete
        try:
            chunks = sorted(meta.chunks, key=lambda c: c.index)
            print(f"[download] '{meta.name}' (object_id={object_id}): {len(chunks)} chunk(s)")
            for info in chunks:
                self._need_live_copy(info, use_cache)
            first = self._fetch_chunk(chunks[0], use_cache) if chunks else b""
        except BaseException:
            self._done_reading(object_id)
            raise
        return meta, ChunkStream(self._chunks_from(meta, chunks, first, use_cache),
                                 lambda: self._done_reading(object_id))

    def _done_reading(self, object_id: str) -> None:
        """A download of this object has finished (or was abandoned). If it
        was the last one and the object was deleted meanwhile, its copies
        can go now."""
        with self._lock:
            self._reading[object_id] -= 1
            if self._reading[object_id] > 0:
                return
            del self._reading[object_id]
            meta = self._deleted_while_read.pop(object_id, None)
        if meta is not None:
            self._delete_copies(meta)

    def _in_use(self, object_id: str) -> bool:
        """True while this object's chunks must stay even if no metadata points
        to them: it is still being uploaded, or still being downloaded."""
        return object_id in self._in_flight or object_id in self._reading

    def _chunks_from(self, meta: ObjectMetadata, chunks: list, first: bytes, use_cache: bool = True):
        """The iterator behind stream_object: the first chunk (already
        fetched), then each later one in order. While one chunk is being
        sent, the next ones (up to parallel_chunks) are already being fetched
        on other threads; they are still given out strictly in index order,
        and the whole-object checksum is fed in that order. If a chunk fails,
        the error is raised when its turn comes, after the earlier chunks."""
        whole = running()
        ahead = deque()                   # futures of the next chunks, in index order
        pool = ThreadPoolExecutor(self.parallel_chunks, thread_name_prefix="fetch") if len(chunks) > 1 else None
        upcoming = iter(chunks[1:])

        def fetch_ahead():
            while pool is not None and len(ahead) < self.parallel_chunks:
                info = next(upcoming, None)
                if info is None:
                    return
                ahead.append(pool.submit(self._fetch_chunk, info, use_cache))

        try:
            fetch_ahead()
            whole.update(first)
            yield first
            first = None                  # do not keep it for the rest of the download
            while ahead:
                data = ahead.popleft().result()   # this chunk's turn; raises if it failed
                fetch_ahead()
                whole.update(data)
                yield data
            if not matches(whole.hexdigest(), meta.checksum):
                raise RuntimeError(f"Object '{meta.object_id}' failed the whole-object checksum")
        finally:
            # finished, failed or abandoned: fetch nothing more
            if pool is not None:
                for f in ahead:
                    f.cancel()
                pool.shutdown(wait=False)


    def _need_live_copy(self, info: ChunkInfo, use_cache: bool = True) -> None:
        """Raise RuntimeError if no node that holds a copy of this chunk is up
        (and it is not in the cache). A node with a recent heartbeat counts as
        up without being asked; the others are asked whether they still have
        it (a HEAD request, no data)."""
        if use_cache and info.chunk_id in self.cache:
            return
        for node_id in info.replicas:
            if not self.counts_as_down(node_id) and not isinstance(self._node_for(node_id), _UnregisteredNode):
                return
        problems = []
        for node_id in info.replicas:
            if self._node_for(node_id).has_chunk(info.chunk_id):
                return
            problems.append((node_id, "down"))
        details = ", ".join(f"{n}: {why}" for n, why in problems) or "no copies recorded"
        raise RuntimeError(f"No good copy of chunk {info.index} ({details})")

    def download(self, object_id: str) -> bytes:
        """The whole object as bytes (all chunks joined). Fine for small
        objects and tests; the API streams instead (stream_object).
        Raises KeyError if there is no such object, and RuntimeError if no
        good copy of some chunk can be found."""
        _, chunks = self.stream_object(object_id)
        with chunks:
            return b"".join(chunks)

    def _fetch_chunk(self, info: ChunkInfo, use_cache: bool = True) -> bytes:
        """The chunk from the cache if it is there (and use_cache); else try
        each replica in order, falling back to the next if one is down, has
        lost the chunk, or returns data that fails the checksum. A good copy
        read from a node is cached."""
        if use_cache:
            data = self.cache.get(info.chunk_id)
            if data is not None:
                print(f"[download] chunk {info.index} <- cache")
                return data
        problems = []   # (node_id, what was wrong), for the error message
        # Nodes known to be down (from heartbeats) go last, so they are not
        # waited for when another copy is available.
        order = sorted(info.replicas, key=lambda n: not self._cached_state(self._node_for(n))[0])
        for node_id in order:
            node = self._node_for(node_id)
            try:
                data = node.get_chunk(info.chunk_id)
            except NodeUnavailable:
                self._mark_down(node_id)
                print(f"[download] chunk {info.index}: {node_id} is down, trying next copy")
                problems.append((node_id, "down"))
                continue
            except KeyError:
                print(f"[download] chunk {info.index}: {node_id} lost the chunk, trying next copy")
                problems.append((node_id, "missing"))
                continue
            except ChunkCorrupt as e:
                # the node itself refused to serve it, or it was damaged on the wire
                print(f"[download] chunk {info.index}: {e}, trying next copy")
                problems.append((node_id, "corrupt"))
                continue

            if not verify(data, info.checksum):
                print(f"[download] chunk {info.index}: copy on {node_id} is CORRUPT "
                      f"(checksum mismatch), trying next copy")
                problems.append((node_id, "corrupt"))
                continue

            print(f"[download] chunk {info.index} <- {node_id}")
            self._cache_chunk(info, data)
            return data

        details = ", ".join(f"{n}: {why}" for n, why in problems)
        raise RuntimeError(f"No good copy of chunk {info.index} ({details})")

    def _cache_chunk(self, info: ChunkInfo, data: bytes) -> None:
        """Keep a chunk whose checksum matched, unless its object was deleted
        meanwhile (a download still running after a delete must not bring it
        back). Under the lock delete() takes, so it is one or the other."""
        if not self.cache.enabled:
            return
        with self._lock:
            if self._object_of(info.chunk_id) in self.metadata:
                self.cache.put(info.chunk_id, data)

    def cache_state(self, meta: ObjectMetadata) -> str:
        """'HIT' if every chunk of the object is cached, 'MISS' if none,
        else 'PARTIAL'. (For the X-Cache header; it may change a moment later.)"""
        cached = sum(1 for info in meta.chunks if info.chunk_id in self.cache)
        return "HIT" if meta.chunks and cached == len(meta.chunks) else "MISS" if not cached else "PARTIAL"

    # ------------------------------------------------------------------
    # metadata, delete, list
    # ------------------------------------------------------------------
    def get_object(self, object_id: str) -> ObjectMetadata:
        """The metadata of one object. Raises KeyError if there is none."""
        meta = self.metadata.get(object_id)
        if meta is None:
            raise KeyError(f"No object with id '{object_id}'")
        return meta

    def list_objects(self) -> list:
        """All objects' metadata, oldest first."""
        return sorted(list(self.metadata.values()), key=lambda m: (m.created_at, m.object_id))

    # ------------------------------------------------------------------
    # per-client storage (quotas) and sharing
    # ------------------------------------------------------------------
    def owns_objects(self, client_id: str) -> bool:
        """True if any stored object belongs to this client id."""
        return any(m.client_id == client_id for m in list(self.metadata.values()))

    def used_bytes(self, client_id: str) -> int:
        """Bytes stored by this client, plus its uploads still arriving."""
        with self._lock:
            return self._used[client_id or ""] + self._pending[client_id or ""]

    def _count_against_quota(self, client_id: str, nbytes: int, quota: int) -> None:
        """Count nbytes of an arriving upload for this client; raise
        QuotaExceeded if its stored bytes plus everything still arriving would
        go over the quota."""
        with self._lock:
            self._pending[client_id] += nbytes
            total = self._used[client_id] + self._pending[client_id]
        if total > quota:
            raise QuotaExceeded(f"Storage quota exceeded: client '{client_id}' may store {quota} bytes, "
                                f"this upload would take it to at least {total}")

    def set_readers(self, object_id: str, readers: list) -> ObjectMetadata:
        """Set which other clients may read an object (download it and read
        its metadata). The owner is never listed. Raises KeyError if there is
        no such object."""
        with self._lock:
            meta = self.get_object(object_id)
            meta.readers = sorted({r for r in readers if r and r != meta.client_id})
            meta.modified_at = _now()
            self._save(objects=[object_id])
            return meta

    def update_custom_metadata(self, object_id: str, updates: dict) -> ObjectMetadata:
        """Add or change custom key-value metadata, without touching the stored
        data. A value of None removes that key. Updates modified_at. Raises
        KeyError if there is no such object."""
        with self._lock:
            meta = self.get_object(object_id)
            for key, value in updates.items():
                if value is None:
                    meta.custom.pop(key, None)
                else:
                    meta.custom[key] = value
            meta.modified_at = _now()
            self._save(objects=[object_id])
            return meta

    def delete(self, object_id: str) -> list:
        """Delete an object and every copy of its chunks. Raises KeyError if
        there is no such object.

        The metadata goes first, so the object is gone for clients at once; then
        each copy is removed from its node. A copy on a node that cannot be
        reached is left behind (an orphan); those are returned as a list of
        (node_id, chunk_id) and printed.

        If the object is being downloaded right now, its copies are removed
        only when the last of those downloads finishes, so they are not cut
        off half-way. (Then [] is returned.)"""
        with self._lock:
            meta = self.get_object(object_id)
            del self.metadata[object_id]
            path_key = (meta.client_id, meta.source_path)
            if meta.source_path and self.paths.get(path_key) == object_id:
                del self.paths[path_key]
            self._used[meta.client_id] -= meta.stored_bytes   # frees the owner's quota
            self._save(removed=[object_id])
            if self._reading[object_id] > 0:
                self._deleted_while_read[object_id] = meta
                print(f"[delete] '{meta.name}' (object_id={object_id}) removed; its copies go when "
                      f"the {self._reading[object_id]} download(s) in progress finish")
                return []
        return self._delete_copies(meta)

    def _delete_copies(self, meta: ObjectMetadata) -> list:
        """Remove every copy of the object's chunks from the nodes (and from
        the cache). Returns the copies left behind on unreachable nodes, as
        (node_id, chunk_id)."""
        self.cache.discard(info.chunk_id for info in meta.chunks)
        left_behind = []
        for info in meta.chunks:
            for node_id in info.replicas:
                try:
                    self._node_for(node_id).delete_chunk(info.chunk_id)
                except NodeUnavailable:
                    left_behind.append((node_id, info.chunk_id))

        print(f"[delete] '{meta.name}' (object_id={meta.object_id}) removed"
              + (f"; {len(left_behind)} copy/copies left on unreachable nodes" if left_behind else ""))
        return left_behind

    # ------------------------------------------------------------------
    # health (read-only report) and repair
    # ------------------------------------------------------------------
    def _check_copy(self, node_id: str, info: ChunkInfo, unreachable: set = None) -> str:
        """Judge one copy of a chunk without downloading it: ask the node to
        recompute the checksum of what is on its disk and compare it with the
        checksum the distributor recorded at upload.

        Returns 'ok', 'down' (node unreachable), 'missing' (node is up but
        has no such chunk) or 'corrupt' (the bytes no longer match).

        A node the controller already counts as down is not asked (asking a
        dead container can take seconds: its name no longer resolves).
        `unreachable`, if given, is the set of nodes that already failed in
        this pass over the chunks; a node that fails is added to it, so a dead
        node costs one failed request per pass, not one per copy."""
        if (unreachable is not None and node_id in unreachable) or self.counts_as_down(node_id):
            return "down"
        try:
            actual = self._node_for(node_id).get_checksum(info.chunk_id)["checksum"]
        except NodeUnavailable:
            if unreachable is not None:
                unreachable.add(node_id)
            return "down"
        except (KeyError, ValueError):
            return "missing"
        return "ok" if actual == info.checksum else "corrupt"

    def replication_report(self) -> dict:
        """For every chunk of every object: the state of each copy (ok, down,
        missing or corrupt) and whether the chunk still has the required
        number of good copies.

        status per chunk:
          healthy          good copies >= replication factor
          under_replicated 1 .. factor-1 good copies (still downloadable)
          unavailable      0 good copies (download will fail)
        """
        chunks = []
        counts = {"healthy": 0, "under_replicated": 0, "unavailable": 0}
        unreachable = set()          # nodes that failed during this report

        # list(...): an upload finishing meanwhile must not change the dict
        # while we loop over it.
        for meta in list(self.metadata.values()):
            for info in sorted(meta.chunks, key=lambda c: c.index):
                replicas = []
                for node_id in info.replicas:
                    state = self._check_copy(node_id, info, unreachable)
                    replicas.append({
                        "node": node_id,
                        "node_up": state != "down",
                        "state": state,
                        "usable": state == "ok",   # a corrupt copy is not usable
                    })
                usable = sum(1 for r in replicas if r["usable"])

                if usable >= self.replication_factor:
                    status = "healthy"
                elif usable >= 1:
                    status = "under_replicated"
                else:
                    status = "unavailable"
                counts[status] += 1

                chunks.append({
                    "object_id": meta.object_id,
                    "name": meta.name,
                    "index": info.index,
                    "chunk_id": info.chunk_id,
                    "usable_copies": usable,
                    "status": status,
                    "replicas": replicas,
                })

        return {
            "replication_factor": self.replication_factor,
            "summary": {"total_chunks": len(chunks), **counts},
            "chunks": chunks,
        }

    def repair(self) -> dict:
        """Bring every chunk back to the full number of good copies.

        For each chunk: judge every recorded copy (_check_copy). If there are
        enough good copies, leave it alone. Otherwise read the chunk from a good
        copy (verified against the recorded checksum), write it to new live
        nodes that do not already hold a good copy, and update the metadata.
        Copies that are missing or corrupt are dropped from the metadata; a
        damaged copy on a node that is up is deleted (or overwritten if that
        node is chosen again). A copy on a node that is down is dropped too and
        reported as left behind, since it cannot be deleted now.
 
        A chunk with no good copy cannot be rebuilt: it is reported as
        unrecoverable. A chunk that gets some but not all of its missing copies
        (not enough live nodes with room) is reported as partial. Raises
        RuntimeError if a repair is already running."""
        if not self._repair_lock.acquire(blocking=False):
            raise RuntimeError("A repair is already running")
        try:
            result = {"chunks_checked": 0, "healthy": 0, "repaired": 0, "partial": 0,
                      "unrecoverable": 0, "copies_created": 0, "left_behind": [], "details": []}
            unreachable = set()      # nodes that failed during this repair
            for meta in list(self.metadata.values()):
                for info in sorted(meta.chunks, key=lambda c: c.index):
                    result["chunks_checked"] += 1
                    outcome = self._repair_chunk(meta, info, result, unreachable)
                    result[outcome] += 1
            print(f"[repair] checked {result['chunks_checked']} chunk(s): "
                  f"{result['repaired']} repaired, {result['partial']} partial, "
                  f"{result['unrecoverable']} unrecoverable, {result['copies_created']} copy/copies created")
            return result
        finally:
            self._repair_lock.release()

    def _repair_chunk(self, meta: ObjectMetadata, info: ChunkInfo, result: dict, unreachable: set = None) -> str:
        """Repair one chunk. Returns 'healthy', 'repaired', 'partial' or
        'unrecoverable'; details go into `result`."""
        states = {node_id: self._check_copy(node_id, info, unreachable) for node_id in list(info.replicas)}
        good = [n for n, s in states.items() if s == "ok"]
        if len(good) >= self.replication_factor:
            return "healthy"

        detail = {"object_id": meta.object_id, "name": meta.name, "index": info.index,
                  "chunk_id": info.chunk_id, "before": states, "added": []}
        result["details"].append(detail)

        if not good:
            detail["outcome"] = "unrecoverable"
            print(f"[repair] chunk {info.chunk_id}: NO good copy left, cannot rebuild")
            return "unrecoverable"

        # Read the chunk from a good copy and check it again against the
        # recorded checksum (it could have been damaged since the check).
        data = None
        for node_id in good:
            try:
                candidate = self._node_for(node_id).get_chunk(info.chunk_id)
            except (NodeUnavailable, KeyError, ChunkCorrupt):
                continue
            if verify(candidate, info.checksum):
                data = candidate
                break
        if data is None:
            detail["outcome"] = "unrecoverable"
            return "unrecoverable"

        # Choose homes for the missing copies. Prefer nodes that hold no copy
        # at all; if there are not enough, a node with a damaged copy may be
        # reused (storing over it replaces it).
        need = self.replication_factor - len(good)
        bad = [n for n in states if n not in good]
        # One copy at a time: a node that turns out to be dead or full is
        # skipped and the next candidate is tried.
        added = []
        tried = set()
        while len(added) < need:
            taken = tried | set(added)
            partners = good + added
            picked = self._pick_nodes(len(data), 1, exclude=set(info.replicas) | taken, partners=partners)
            if picked is None:
                picked = self._pick_nodes(len(data), 1, exclude=set(good) | taken, partners=partners)
            if picked is None:
                break   # no more candidates: the chunk stays short of copies
            node = picked[0]
            try:
                stored = node.store_chunk(info.chunk_id, data, checksum=info.checksum)
            except NodeUnavailable:
                self._mark_down(node.node_id)
                stored = False
            except ChunkCorrupt:
                stored = False
            if stored:
                self._reserve(node.node_id, len(data))
                added.append(node.node_id)
            else:
                tried.add(node.node_id)

        # Record the new layout, unless the object was deleted while we worked
        # (then the new copies must not be left behind).
        # A copy on a node that is only down is dropped only if it was actually
        # replaced; otherwise it stays recorded, since the node may come back
        # with a perfectly good copy.
        down = [n for n in bad if states[n] == "down"]
        still_short = max(0, self.replication_factor - len(good) - len(added))
        kept_down = down[:still_short]
        with self._lock:
            still_there = self.metadata.get(meta.object_id) is meta
            if still_there:
                info.replicas = good + added + kept_down
                self._save(chunks=[info])
        if still_there and added:
            for new in added:
                self._note_copies([new] + [n for n in info.replicas if n != new and n not in added])
        if not still_there:
            for node_id in added:
                try:
                    self._node_for(node_id).delete_chunk(info.chunk_id)
                except NodeUnavailable:
                    pass
            return "healthy"

        # Clean up the dropped copies.
        for node_id in bad:
            if node_id in added or node_id in kept_down:
                continue   # overwritten with the good bytes / still recorded
            if states[node_id] == "down":
                result["left_behind"].append({"node": node_id, "chunk_id": info.chunk_id})
            elif states[node_id] == "corrupt":
                try:
                    self._node_for(node_id).delete_chunk(info.chunk_id)
                except NodeUnavailable:
                    result["left_behind"].append({"node": node_id, "chunk_id": info.chunk_id})

        detail["added"] = added
        result["copies_created"] += len(added)
        complete = len(good) + len(added) >= self.replication_factor
        detail["outcome"] = "repaired" if complete else "partial"
        print(f"[repair] chunk {info.chunk_id}: now on {', '.join(info.replicas)}"
              + ("" if complete else " (still short of copies: not enough live nodes with room)"))
        return detail["outcome"]

    # ------------------------------------------------------------------
    # what is really on a worker: orphans and missing chunks
    # ------------------------------------------------------------------
    def _expected_on(self, node_id: str) -> dict:
        """chunk_id -> object_id of every chunk the metadata says this node holds."""
        with self._lock:
            return {info.chunk_id: meta.object_id
                    for meta in self.metadata.values() for info in meta.chunks
                    if node_id in info.replicas}

    @staticmethod
    def _object_of(chunk_id: str) -> str:
        """Chunk ids are '<object_id>-<index>'."""
        return chunk_id.rsplit("-", 1)[0]

    def worker_inventory(self, node_id: str) -> dict:
        """Compare what a worker really holds with what the metadata says it
        should hold. Raises KeyError for an unknown worker, NodeUnavailable if
        it cannot be reached.

          orphans  held, but no object points to this copy (left by a delete or
                   a repair while the worker was down, or a rolled-back upload).
                   A chunk of an upload still in progress is listed under
                   'in_progress' instead.
          missing  the metadata says it is here, but it is not."""
        node = self._get_node(node_id)
        held = node.list_chunks()
        expected = self._expected_on(node_id)
        held_ids = {c["chunk_id"] for c in held}
        orphans, in_progress = [], []
        for c in held:
            if c["chunk_id"] in expected:
                continue
            (in_progress if self._in_use(self._object_of(c["chunk_id"])) else orphans).append(c)
        return {
            "worker_id": node_id,
            "held": len(held),
            "expected": len(expected),
            "orphans": orphans,
            "in_progress": in_progress,
            "missing": sorted(c for c in expected if c not in held_ids),
        }

    def _chunk_info(self, chunk_id: str):
        """(ObjectMetadata, ChunkInfo) of a chunk, or (None, None) if its object is gone."""
        with self._lock:
            meta = self.metadata.get(self._object_of(chunk_id))
            if meta is None:
                return None, None
            for info in meta.chunks:
                if info.chunk_id == chunk_id:
                    return meta, info
        return None, None

    def _adopt_if_needed(self, node, chunk_id: str) -> bool:
        """An orphaned copy is about to be deleted. If its chunk has FEWER good
        copies than required and this copy is intact (it matches the checksum
        recorded at upload), take it back into the chunk's replica list
        instead, so cleanup never lowers a chunk's good copies. Returns True if
        the copy was adopted (and must not be deleted)."""
        meta, info = self._chunk_info(chunk_id)
        if info is None:
            return False                                  # its object was deleted: just an orphan
        states = {n: self._check_copy(n, info) for n in list(info.replicas)}
        good = [n for n, s in states.items() if s == "ok"]
        if len(good) >= self.replication_factor:
            return False                                  # enough good copies: this one is surplus
        try:
            intact = node.get_checksum(chunk_id)["checksum"] == info.checksum
        except (KeyError, ValueError, NodeUnavailable):
            intact = False
        if not intact:
            return False                                  # damaged too: no use keeping it
        with self._lock:
            if self.metadata.get(meta.object_id) is not meta or node.node_id in info.replicas:
                return False
            new = good + [node.node_id]
            short = max(0, self.replication_factor - len(new))
            # a copy on a worker that is only down may come back: keep it while still short
            new += [n for n in info.replicas if states.get(n) == "down" and n not in new][:short]
            info.replicas = new
            self._save(chunks=[info])
        self._note_copies(info.replicas)
        print(f"[cleanup] chunk {chunk_id} had only {len(good)} good copy/copies: "
              f"kept the intact copy on {node.node_id} instead of deleting it")
        return True

    def cleanup_orphans(self, min_age: float = 60.0) -> dict:
        """Delete orphaned chunks on every reachable worker.

        Never lowers a chunk below the required number of good copies: an
        orphan whose chunk is short of good copies, and that is itself intact,
        is taken back into the chunk's replica list ('adopted') instead of
        being deleted. Only surplus copies (the chunk already has enough good
        copies), damaged ones, and copies of deleted objects are deleted.

        Safe only if the metadata is the full truth: with metadata kept only
        in memory, a controller that has just restarted knows no objects and
        would see EVERY chunk as an orphan. So this refuses (RuntimeError)
        unless the metadata is persistent. It also never deletes a chunk
        younger than min_age seconds or one belonging to an upload still in
        progress, and does not run during a repair (RuntimeError)."""
        if not self.persistent:
            raise RuntimeError("Cleanup needs saved metadata (METADATA_PATH): with metadata only in "
                               "memory, every chunk looks like an orphan after a restart")
        if not self._repair_lock.acquire(blocking=False):
            raise RuntimeError("A repair or cleanup is already running")
        try:
            result = {"deleted": [], "adopted": [], "kept_young": [], "unreachable": []}
            for node in list(self.nodes):
                try:
                    report = self.worker_inventory(node.node_id)
                except NodeUnavailable:
                    result["unreachable"].append(node.node_id)
                    continue
                for c in report["orphans"]:
                    entry = {"node": node.node_id, "chunk_id": c["chunk_id"]}
                    if c["age"] < min_age:
                        result["kept_young"].append(entry)
                        continue
                    # look again right before deleting: an upload or repair may
                    # have claimed it since the inventory was taken
                    if c["chunk_id"] in self._expected_on(node.node_id) \
                            or self._in_use(self._object_of(c["chunk_id"])):
                        continue
                    if self._adopt_if_needed(node, c["chunk_id"]):
                        result["adopted"].append(entry)
                        continue
                    try:
                        node.delete_chunk(c["chunk_id"])
                        result["deleted"].append(entry)
                    except NodeUnavailable:
                        result["unreachable"].append(node.node_id)
                        break
            print(f"[cleanup] deleted {len(result['deleted'])} orphaned chunk(s), "
                  f"adopted {len(result['adopted'])}, kept {len(result['kept_young'])} recent one(s)")
            return result
        finally:
            self._repair_lock.release()

    # ------------------------------------------------------------------
    # automatic repair
    # ------------------------------------------------------------------
    def auto_repair_check(self, grace: float):
        """Called every few seconds by the controller's background thread.

        A worker that has been down for `grace` seconds triggers a repair, so
        the replication factor is restored without anyone calling the repair
        endpoint. A worker that comes back within the grace period (a quick
        restart) triggers nothing, so no copying happens for nothing. Each
        failure is repaired for once; if that repair could not create every
        copy (not enough room), it is tried again after another grace period.
        Returns the repair result, or None if nothing was done."""
        now = time.monotonic()
        overdue = []
        for node in list(self.nodes):
            alive, _ = self._cached_state(node)
            if alive:
                self._down_since.pop(node.node_id, None)
                self._repaired_for.discard(node.node_id)
                continue
            since = self._down_since.setdefault(node.node_id, now)
            if now - since >= grace and node.node_id not in self._repaired_for:
                overdue.append(node.node_id)
        if not overdue:
            return None
        try:
            result = self.repair()
        except RuntimeError:
            return None          # a repair is already running: look again next time
        if result["partial"] or result["unrecoverable"]:
            for n in overdue:
                self._down_since[n] = now        # try again after another grace period
        else:
            self._repaired_for.update(overdue)
        self.last_auto_repair = {
            "at": _now(),
            "because_down": overdue,
            **{k: result[k] for k in ("chunks_checked", "repaired", "partial", "unrecoverable", "copies_created")},
        }
        print(f"[auto-repair] {', '.join(overdue)} down for {grace:g}s or more: "
              f"{result['repaired']} chunk(s) repaired, {result['copies_created']} copy/copies created")
        return result
