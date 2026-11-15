"""Distributor: takes a client's request, splits data into chunks and
decides which nodes store each chunk. Also reassembles on download.

The distributor is the only component that knows the whole picture:
which nodes exist, how full they are, and where every copy of every chunk
of every object lives. Nodes just store bytes and never talk to each other.

Replication: every chunk is stored on REPLICATION_FACTOR different nodes.
The distributor writes each copy itself.

Objects are identified by an object_id the distributor generates. The file
name is only metadata and is not unique; what makes an upload a duplicate is
its source_path (where the client read the file from).

The nodes it is given are RemoteNode objects (remote_node.py): every call on
them is an HTTP request to that node's server. The distributor only relies on
these methods: health(), alive, store_chunk(), get_chunk(), has_chunk() and
delete_chunk().
"""

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from checksum import compute, verify
from node import ChunkCorrupt, NodeUnavailable

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
    source_path: str = ""                       # where the client read the file; unique


class Distributor:
    def __init__(self, nodes: list, chunk_size: int = CHUNK_SIZE,
                 replication_factor: int = REPLICATION_FACTOR):
        self.nodes = nodes             # list of RemoteNode (the node registry, for now)
        self.chunk_size = chunk_size   # a parameter so we can test with tiny chunks
        self.replication_factor = replication_factor
        self.metadata = {}             # object_id -> ObjectMetadata
        self.paths = {}                # source_path -> object_id (duplicate check)
        # The web server handles requests on several threads. This lock makes
        # "check the path is free, then register the object" one step, and
        # protects changes to an object's metadata.
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _split(self, data: bytes) -> list:
        """bytes -> list of chunks of at most chunk_size.
        Empty data still gives one (empty) chunk so the object has a record."""
        if not data:
            return [b""]
        return [data[i:i + self.chunk_size] for i in range(0, len(data), self.chunk_size)]

    def _pick_nodes(self, chunk_size: int, count: int):
        """Choose `count` DIFFERENT live nodes that can fit this chunk, most
        free space first (ties keep node-list order). Returns the list, or
        None if fewer than `count` nodes qualify.

        Asks each node for its health ONCE and decides from that snapshot. An
        unreachable node is skipped. (Asking separately 'are you alive?' and
        'how much space?' would cost more requests, and the node could die in
        between.)"""
        candidates = []   # (free_space, node)
        for node in self.nodes:
            try:
                free = node.health()["free"]   # one HTTP request per node
            except NodeUnavailable:
                continue                       # unreachable: not a candidate
            if free >= chunk_size:
                candidates.append((free, node))

        if len(candidates) < count:
            return None
        candidates.sort(key=lambda c: -c[0])   # stable: ties keep node-list order
        return [node for _, node in candidates[:count]]

    def _get_node(self, node_id: str):
        """Find a node by its id."""
        for node in self.nodes:
            if node.node_id == node_id:
                return node
        raise KeyError(f"Unknown node: {node_id}")

    def _rollback(self, written: list) -> None:
        """Delete every copy stored so far by a failed upload."""
        for chunk_id, node in written:
            try:
                node.delete_chunk(chunk_id)
            except NodeUnavailable:
                print(f"[upload] warning: {node.node_id} went down, "
                      f"copy of {chunk_id} left behind")

    @staticmethod
    def _duplicate_message(source_path: str, existing_id: str) -> str:
        return (f"An object uploaded from '{source_path}' already exists "
                f"(object_id {existing_id}); delete it first to upload it again")

    # ------------------------------------------------------------------
    # upload / download
    # ------------------------------------------------------------------
    def upload(self, name: str, data: bytes, content_type: str = DEFAULT_CONTENT_TYPE,
               custom: dict = None, source_path: str = None, size: int = None) -> str:
        """Split data, store every chunk on REPLICATION_FACTOR different
        nodes, record metadata. Return the new object_id.

        name         file name, kept as metadata (several objects can share it)
        content_type e.g. 'video/mp4'
        custom       user-defined key-value metadata
        source_path  where the client read the file from. Two uploads with the
                     same source_path are the same file, so the second is
                     refused. Optional: without it, nothing is checked.
        size         size of the original object if it differs from len(data)
                     (e.g. the client wraps the file in a header); defaults to
                     len(data)

        The object_id is generated here, used to name the chunks, and only
        becomes visible (metadata and path index) once ALL copies of ALL
        chunks are stored. If any copy cannot be stored, every copy already
        written is deleted and a RuntimeError is raised, so nothing
        half-uploaded is left behind. (Strict: no chunk is ever stored with
        fewer copies than the replication factor.)

        Raises FileExistsError if an object from this source_path exists.
        """
        if source_path and source_path in self.paths:
            raise FileExistsError(self._duplicate_message(source_path, self.paths[source_path]))

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
        try:
            for index, chunk in enumerate(chunk_list):
                chunk_id = f"{object_id}-{index}"
                targets = self._pick_nodes(len(chunk), self.replication_factor)

                if targets is None:
                    raise RuntimeError(
                        f"Not enough storage: need {self.replication_factor} different "
                        f"live nodes with room for chunk {index}"
                    )

                for node in targets:
                    try:
                        stored = node.store_chunk(chunk_id, chunk)
                    except (NodeUnavailable, ChunkCorrupt):
                        stored = False   # down, or the chunk arrived damaged
                    if not stored:
                        raise RuntimeError(
                            f"Could not store a copy of chunk {index} on {node.node_id}"
                        )
                    written.append((chunk_id, node))   # remember this copy for rollback

                replica_ids = [n.node_id for n in targets]
                print(f"[upload] chunk {index} ({len(chunk)} bytes) -> {', '.join(replica_ids)}")
                # all copies stored: record the chunk, with the checksum every
                # copy must match when it is read back
                placed.append(ChunkInfo(chunk_id, index, replica_ids, compute(chunk)))

            # Register the object. Done under the lock, and the path is checked
            # again: another upload of the same file may have finished while
            # this one was storing chunks.
            with self._lock:
                if source_path and source_path in self.paths:
                    raise FileExistsError(
                        self._duplicate_message(source_path, self.paths[source_path]))
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
                )
                if source_path:
                    self.paths[source_path] = object_id
        except Exception as e:
            # Whatever went wrong (no space, a node died, a bug), never leave
            # copies behind that no metadata points to.
            print(f"[upload] '{name}' failed ({e}); rolling back {len(written)} copy/copies")
            self._rollback(written)
            raise

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
        for node_id in info.replicas:
            node = self._get_node(node_id)
            try:
                data = node.get_chunk(info.chunk_id)
            except NodeUnavailable:
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
            if meta.source_path and self.paths.get(meta.source_path) == object_id:
                del self.paths[meta.source_path]

        left_behind = []
        for info in meta.chunks:
            for node_id in info.replicas:
                try:
                    self._get_node(node_id).delete_chunk(info.chunk_id)
                except NodeUnavailable:
                    left_behind.append((node_id, info.chunk_id))

        print(f"[delete] '{meta.name}' (object_id={object_id}) removed"
              + (f"; {len(left_behind)} copy/copies left on unreachable nodes" if left_behind else ""))
        return left_behind

    # ------------------------------------------------------------------
    # health (read-only: detects problems, does not repair them)
    # ------------------------------------------------------------------
    def replication_report(self) -> dict:
        """For every chunk of every object: which replicas are usable right
        now (node up and chunk present), and whether the chunk still has the
        required number of copies.

        status per chunk:
          healthy          usable copies >= replication factor
          under_replicated 1 .. factor-1 usable copies (still downloadable)
          unavailable      0 usable copies (download will fail)
        """
        chunks = []
        counts = {"healthy": 0, "under_replicated": 0, "unavailable": 0}

        # Ask every node once whether it is up, instead of once per chunk copy.
        up = {n.node_id: n.alive for n in self.nodes}

        # list(...): an upload finishing meanwhile must not change the dict
        # while we loop over it.
        for meta in list(self.metadata.values()):
            for info in sorted(meta.chunks, key=lambda c: c.index):
                replicas = []
                for node_id in info.replicas:
                    node_up = up.get(node_id, False)
                    replicas.append({
                        "node": node_id,
                        "node_up": node_up,
                        # only ask a live node whether it holds the chunk
                        "usable": node_up and self._get_node(node_id).has_chunk(info.chunk_id),
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
