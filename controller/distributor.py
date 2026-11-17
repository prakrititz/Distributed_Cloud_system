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

The nodes it is given are RemoteNode objects (controller/remote_node.py): every call on
them is an HTTP request to that node's server. The distributor only relies on
these methods: health(), alive, store_chunk(), get_chunk(), has_chunk() and
delete_chunk().
"""

import threading
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone

from config import HEARTBEAT_TIMEOUT
from controller.metadata_store import MemoryStore
from helpers.checksum import compute, verify
from helpers.errors import ChunkCorrupt, NodeUnavailable

CHUNK_SIZE = 1024 * 1024    # 1 MB, as the project spec says
REPLICATION_FACTOR = 2      # copies of every chunk, each on a different node
DEFAULT_CONTENT_TYPE = "application/octet-stream"


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
    client_id: str = ""                         # which client sent it; (client_id, source_path) is unique

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


class Distributor:
    def __init__(self, nodes: list, chunk_size: int = CHUNK_SIZE,
                 replication_factor: int = REPLICATION_FACTOR,
                 heartbeat_timeout: float = HEARTBEAT_TIMEOUT, store=None):
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

        # Automatic repair (auto_repair_check): when each worker was first
        # seen down, and the workers already repaired for.
        self._down_since = {}
        self._repaired_for = set()
        self.last_auto_repair = None

    # ------------------------------------------------------------------
    # persistence
    # ------------------------------------------------------------------
    def _save(self) -> None:
        """Save all metadata. Call with self._lock held, right after a change,
        so saves happen in the same order as the changes."""
        self.store.save(list(self.metadata.values()))

    @property
    def persistent(self) -> bool:
        """True if the metadata is saved, i.e. it survives a restart, so it is
        the full truth about what should be on the workers."""
        return self.store.persistent

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _split(self, data: bytes) -> list:
        """bytes -> list of chunks of at most chunk_size.
        Empty data still gives one (empty) chunk so the object has a record."""
        if not data:
            return [b""]
        return [data[i:i + self.chunk_size] for i in range(0, len(data), self.chunk_size)]

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

    def _note_copies(self, node_ids) -> None:
        """These nodes now hold copies of the same chunk: count each pair."""
        ids = list(node_ids)
        with self._status_lock:
            for a in range(len(ids)):
                for b in range(a + 1, len(ids)):
                    self._pair_use[frozenset((ids[a], ids[b]))] += 1

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
    def upload(self, name: str, data: bytes, content_type: str = DEFAULT_CONTENT_TYPE,
               custom: dict = None, source_path: str = None, size: int = None,
               client_id: str = None) -> str:
        """Split data, store every chunk on REPLICATION_FACTOR different
        nodes, record metadata. Return the new object_id.

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
        size         size of the original object if it differs from len(data)
                     (e.g. the client wraps the file in a header); defaults to
                     len(data)

        The object_id is generated here, used to name the chunks, and only
        becomes visible (metadata and path index) once ALL copies of ALL
        chunks are stored. If any copy cannot be stored, every copy already
        written is deleted and a RuntimeError is raised, so nothing
        half-uploaded is left behind. (Strict: no chunk is ever stored with
        fewer copies than the replication factor.)

        Raises FileExistsError if this client already has an object from this
        source_path.
        """
        client_id = client_id or ""
        path_key = (client_id, source_path)
        if source_path and path_key in self.paths:
            raise FileExistsError(
                self._duplicate_message(client_id, source_path, self.paths[path_key]))

        object_id = str(uuid.uuid4())
        chunk_list = self._split(data)
        object_checksum = compute(data)   # fingerprint of the whole object
        print(f"[upload] '{name}': {len(data)} bytes -> {len(chunk_list)} chunk(s), "
              f"{self.replication_factor} copies each")

        # Two lists, two jobs. Both are filled in the loop below.
        #
        # written: "what must I undo if this upload fails?"
        #   One (chunk_id, node) entry per physical COPY stored on a node
        #   (2 per chunk with replication factor 2). Holds node objects
        #   because _rollback calls node.delete_chunk() on them. Only
        #   _rollback reads it.
        #
        # placed: "what do I record in the metadata?"
        #   One ChunkInfo per CHUNK, listing the node ids of its copies.
        #   Only added once every copy of that chunk is stored. Only the
        #   ObjectMetadata built at the end reads it; if the upload fails
        #   it is simply thrown away.
        written = []
        placed = []
        self._in_flight.add(object_id)
        try:
            for index, chunk in enumerate(chunk_list):
                chunk_id = f"{object_id}-{index}"
                self._rotation += 1        # equal nodes take turns, chunk by chunk

                # Place the copies one at a time. Placement uses what the nodes
                # last reported, which can be a moment old, so a chosen node may
                # turn out to be dead or full: then it is skipped (and a dead
                # one is marked down) and the next best node is tried. The
                # upload fails only when there are not enough nodes left.
                targets = []   # nodes that now hold a copy of this chunk
                tried = set()  # nodes that failed for this chunk
                while len(targets) < self.replication_factor:
                    holders = [n.node_id for n in targets]
                    picked = self._pick_nodes(len(chunk), 1, exclude=tried | set(holders),
                                              partners=holders)
                    if picked is None:
                        raise RuntimeError(
                            f"Not enough storage: need {self.replication_factor} different "
                            f"live nodes with room for chunk {index}"
                        )
                    node = picked[0]
                    try:
                        stored = node.store_chunk(chunk_id, chunk)
                    except NodeUnavailable:
                        self._mark_down(node.node_id)
                        stored = False
                    except ChunkCorrupt:
                        stored = False   # the chunk arrived damaged
                    if not stored:
                        tried.add(node.node_id)
                        continue
                    self._reserve(node.node_id, len(chunk))
                    written.append((chunk_id, node))   # remember this copy for rollback
                    targets.append(node)

                replica_ids = [n.node_id for n in targets]
                self._note_copies(replica_ids)
                print(f"[upload] chunk {index} ({len(chunk)} bytes) -> {', '.join(replica_ids)}")
                # all copies stored: record the chunk, with the checksum every
                # copy must match when it is read back
                placed.append(ChunkInfo(chunk_id, index, replica_ids, compute(chunk)))

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
                    size=len(data) if size is None else size,
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
                self._save()
        except Exception as e:
            # Whatever went wrong (no space, a node died, a bug), never leave
            # copies behind that no metadata points to.
            print(f"[upload] '{name}' failed ({e}); rolling back {len(written)} copy/copies")
            with self._lock:
                if self.metadata.get(object_id) is not None:   # registered, but the save failed
                    meta = self.metadata.pop(object_id)
                    if meta.source_path and self.paths.get(path_key) == object_id:
                        del self.paths[path_key]
            self._rollback(written)
            raise
        finally:
            self._in_flight.discard(object_id)

        print(f"[upload] done, '{name}' -> object_id={object_id}")
        return object_id

    def download(self, object_id: str) -> bytes:
        """Fetch each chunk of the object from the first replica that is up AND
        intact, join in order.

        Every copy read from a node is checked against the checksum recorded
        at upload; a damaged copy is skipped like a missing one. The joined
        result is checked against the whole-object checksum as a last step.

        Raises KeyError if there is no such object, and RuntimeError if no
        good copy of some chunk can be found."""
        meta = self.get_object(object_id)
        print(f"[download] '{meta.name}' (object_id={object_id}): fetching {len(meta.chunks)} chunk(s)")

        pieces = []
        for info in sorted(meta.chunks, key=lambda c: c.index):
            pieces.append(self._fetch_chunk(info))
        result = b"".join(pieces)

        if not verify(result, meta.checksum):
            raise RuntimeError(f"Object '{object_id}' failed the whole-object checksum after reassembly")
        return result

    def _fetch_chunk(self, info: ChunkInfo) -> bytes:
        """Try each replica in order; fall back to the next if one is down,
        has lost the chunk, or returns data that fails the checksum."""
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
            return data

        details = ", ".join(f"{n}: {why}" for n, why in problems)
        raise RuntimeError(f"No good copy of chunk {info.index} ({details})")

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
            self._save()
            return meta

    def delete(self, object_id: str) -> list:
        """Delete an object and every copy of its chunks. Raises KeyError if
        there is no such object.

        The metadata goes first, so the object is gone for clients at once; then
        each copy is removed from its node. A copy on a node that cannot be
        reached is left behind (an orphan); those are returned as a list of
        (node_id, chunk_id) and printed."""
        with self._lock:
            meta = self.get_object(object_id)
            del self.metadata[object_id]
            path_key = (meta.client_id, meta.source_path)
            if meta.source_path and self.paths.get(path_key) == object_id:
                del self.paths[path_key]
            self._save()

        left_behind = []
        for info in meta.chunks:
            for node_id in info.replicas:
                try:
                    self._node_for(node_id).delete_chunk(info.chunk_id)
                except NodeUnavailable:
                    left_behind.append((node_id, info.chunk_id))

        print(f"[delete] '{meta.name}' (object_id={object_id}) removed"
              + (f"; {len(left_behind)} copy/copies left on unreachable nodes" if left_behind else ""))
        return left_behind

    # ------------------------------------------------------------------
    # health (read-only report) and repair
    # ------------------------------------------------------------------
    def _check_copy(self, node_id: str, info: ChunkInfo) -> str:
        """Judge one copy of a chunk without downloading it: ask the node to
        recompute the checksum of what is on its disk and compare it with the
        checksum the distributor recorded at upload.

        Returns 'ok', 'down' (node unreachable), 'missing' (node is up but
        has no such chunk) or 'corrupt' (the bytes no longer match)."""
        try:
            actual = self._node_for(node_id).get_checksum(info.chunk_id)["checksum"]
        except NodeUnavailable:
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

        # list(...): an upload finishing meanwhile must not change the dict
        # while we loop over it.
        for meta in list(self.metadata.values()):
            for info in sorted(meta.chunks, key=lambda c: c.index):
                replicas = []
                for node_id in info.replicas:
                    state = self._check_copy(node_id, info)
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
            for meta in list(self.metadata.values()):
                for info in sorted(meta.chunks, key=lambda c: c.index):
                    result["chunks_checked"] += 1
                    outcome = self._repair_chunk(meta, info, result)
                    result[outcome] += 1
            print(f"[repair] checked {result['chunks_checked']} chunk(s): "
                  f"{result['repaired']} repaired, {result['partial']} partial, "
                  f"{result['unrecoverable']} unrecoverable, {result['copies_created']} copy/copies created")
            return result
        finally:
            self._repair_lock.release()

    def _repair_chunk(self, meta: ObjectMetadata, info: ChunkInfo, result: dict) -> str:
        """Repair one chunk. Returns 'healthy', 'repaired', 'partial' or
        'unrecoverable'; details go into `result`."""
        states = {node_id: self._check_copy(node_id, info) for node_id in list(info.replicas)}
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
                stored = node.store_chunk(info.chunk_id, data)
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
                self._save()
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
            (in_progress if self._object_of(c["chunk_id"]) in self._in_flight else orphans).append(c)
        return {
            "worker_id": node_id,
            "held": len(held),
            "expected": len(expected),
            "orphans": orphans,
            "in_progress": in_progress,
            "missing": sorted(c for c in expected if c not in held_ids),
        }

    def cleanup_orphans(self, min_age: float = 60.0) -> dict:
        """Delete orphaned chunks on every reachable worker.

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
            result = {"deleted": [], "kept_young": [], "unreachable": []}
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
                            or self._object_of(c["chunk_id"]) in self._in_flight:
                        continue
                    try:
                        node.delete_chunk(c["chunk_id"])
                        result["deleted"].append(entry)
                    except NodeUnavailable:
                        result["unreachable"].append(node.node_id)
                        break
            print(f"[cleanup] deleted {len(result['deleted'])} orphaned chunk(s), "
                  f"kept {len(result['kept_young'])} recent one(s)")
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
