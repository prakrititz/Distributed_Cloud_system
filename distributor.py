"""Distributor: takes a client's request, splits data into chunks and
decides which nodes store each chunk. Also reassembles on download.

The distributor is the only component that knows the whole picture:
which nodes exist, how full they are, and where every copy of every chunk
of every object lives. Nodes just store bytes and never talk to each other.

Replication: every chunk is stored on REPLICATION_FACTOR different nodes.
The distributor writes each copy itself.

The nodes it is given are RemoteNode objects (remote_node.py): every call on
them is an HTTP request to that node's server. The distributor only relies on
these methods: health(), alive, store_chunk(), get_chunk(), has_chunk() and
delete_chunk().
"""

import threading
import uuid
from dataclasses import dataclass, field

from checksum import compute, verify
from node import ChunkCorrupt, NodeUnavailable

CHUNK_SIZE = 1024 * 1024    # 1 MB, as the project spec says
REPLICATION_FACTOR = 2      # copies of every chunk, each on a different node


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
    """Kept as a separate object so it can be moved/exposed later."""
    object_id: str
    name: str
    size: int
    chunks: list = field(default_factory=list)  # list of ChunkInfo, in order
    checksum: str = ""                          # checksum of the whole object


class Distributor:
    def __init__(self, nodes: list, chunk_size: int = CHUNK_SIZE,
                 replication_factor: int = REPLICATION_FACTOR):
        self.nodes = nodes             # list of RemoteNode (the node registry, for now)
        self.chunk_size = chunk_size   # a parameter so we can test with tiny chunks
        self.replication_factor = replication_factor
        self.metadata = {}             # object_id -> ObjectMetadata
        self.names = {}                # name -> object_id (what the client looks up by)
        # The web server handles requests on several threads. This lock makes
        # "check the name is free, then register it" one step.
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

    # ------------------------------------------------------------------
    # upload / download
    # ------------------------------------------------------------------
    def upload(self, name: str, data: bytes) -> str:
        """Split data, store every chunk on REPLICATION_FACTOR different
        nodes, record metadata. Return the new object_id.

        The client identifies an object by its name only. The object_id is
        internal: it is generated here, used to name the chunks, and only
        becomes visible (name -> id map and metadata) once ALL copies of ALL
        chunks are stored. If any copy cannot be stored, every copy already
        written is deleted and a RuntimeError is raised, so nothing
        half-uploaded is left behind. (Strict: no chunk is ever stored with
        fewer copies than the replication factor.)

        Raises FileExistsError if an object with this name already exists.
        """
        if name in self.names:
            raise FileExistsError(f"An object named '{name}' already exists")

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

            # Register the object. Done under the lock, and the name is checked
            # again: another upload with the same name may have finished while
            # this one was storing chunks.
            with self._lock:
                if name in self.names:
                    raise FileExistsError(f"An object named '{name}' already exists")
                self.metadata[object_id] = ObjectMetadata(
                    object_id=object_id,
                    name=name,
                    size=len(data),
                    chunks=placed,
                    checksum=object_checksum,
                )
                self.names[name] = object_id
        except Exception as e:
            # Whatever went wrong (no space, a node died, a bug), never leave
            # copies behind that no metadata points to.
            print(f"[upload] '{name}' failed ({e}); rolling back {len(written)} copy/copies")
            self._rollback(written)
            raise

        print(f"[upload] done, '{name}' -> object_id={object_id}")
        return object_id

    def download(self, name: str) -> bytes:
            """Find the object by name, fetch each chunk from the first replica
            that is up AND intact, join in order.

            Every copy read from a node is checked against the checksum recorded
            at upload; a damaged copy is skipped like a missing one. The joined
            result is checked against the whole-object checksum as a last step.

            Raises KeyError if no object has that name, and RuntimeError if no
            good copy of some chunk can be found."""
            object_id = self.names.get(name)
            if object_id is None:
                raise KeyError(f"No object named '{name}'")
            meta = self.metadata[object_id]
            print(f"[download] '{name}' (object_id={object_id}): fetching {len(meta.chunks)} chunk(s)")

            pieces = []
            for info in sorted(meta.chunks, key=lambda c: c.index):
                pieces.append(self._fetch_chunk(info))
            result = b"".join(pieces)

            if not verify(result, meta.checksum):
                raise RuntimeError(f"'{name}' failed the whole-object checksum after reassembly")
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
        for name, object_id in list(self.names.items()):
            meta = self.metadata[object_id]
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
                    "name": name,
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
