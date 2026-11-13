"""Distributor: takes a client's request, splits data into chunks and
decides which nodes store each chunk. Also reassembles on download.

The distributor is the only component that knows the whole picture:
which nodes exist, how full they are, and where every copy of every chunk
of every object lives. Nodes just store bytes and never talk to each other.

Replication: every chunk is stored on REPLICATION_FACTOR different nodes.
The distributor writes each copy itself.
"""

import uuid
from dataclasses import dataclass, field

from node import NodeUnavailable

CHUNK_SIZE = 1024 * 1024    # 1 MB, as the project spec says
REPLICATION_FACTOR = 2      # copies of every chunk, each on a different node


@dataclass
class ChunkInfo:
    """Where the copies of one chunk live."""
    chunk_id: str
    index: int                                  # position within the object
    replicas: list = field(default_factory=list)  # node_ids holding a copy


@dataclass
class ObjectMetadata:
    """Kept as a separate object so it can be moved/exposed later."""
    object_id: str
    name: str
    size: int
    chunks: list = field(default_factory=list)  # list of ChunkInfo, in order


class Distributor:
    def __init__(self, nodes: list, chunk_size: int = CHUNK_SIZE,
                 replication_factor: int = REPLICATION_FACTOR):
        self.nodes = nodes             # list of Node (the node registry, for now)
        self.chunk_size = chunk_size   # a parameter so we can test with tiny chunks
        self.replication_factor = replication_factor
        self.metadata = {}             # object_id -> ObjectMetadata
        self.names = {}                # name -> object_id (what the client looks up by)

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
        None if fewer than `count` nodes qualify."""
        candidates = [n for n in self.nodes if n.alive and n.free_space() >= chunk_size]
        if len(candidates) < count:
            return None
        candidates.sort(key=lambda n: -n.free_space())  # stable sort
        return candidates[:count]

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
        for index, chunk in enumerate(chunk_list):
            chunk_id = f"{object_id}-{index}"
            targets = self._pick_nodes(len(chunk), self.replication_factor)

            if targets is None:
                print(f"[upload] chunk {index}: fewer than {self.replication_factor} "
                      f"live nodes have room for {len(chunk)} bytes, rolling back")
                self._rollback(written)
                raise RuntimeError(
                    f"Not enough storage: need {self.replication_factor} different "
                    f"live nodes with room for chunk {index}"
                )

            for node in targets:
                try:
                    stored = node.store_chunk(chunk_id, chunk)
                except NodeUnavailable:
                    stored = False
                if not stored:
                    print(f"[upload] chunk {index}: could not store a copy on "
                          f"{node.node_id}, rolling back")
                    self._rollback(written)
                    raise RuntimeError(
                        f"Could not store a copy of chunk {index} on {node.node_id}"
                    )
                written.append((chunk_id, node))   # remember this copy for rollback

            replica_ids = [n.node_id for n in targets]
            print(f"[upload] chunk {index} ({len(chunk)} bytes) -> {', '.join(replica_ids)}")
            placed.append(ChunkInfo(chunk_id, index, replica_ids))   # all copies stored: record the chunk

        self.metadata[object_id] = ObjectMetadata(
            object_id=object_id,
            name=name,
            size=len(data),
            chunks=placed,
        )
        self.names[name] = object_id
        print(f"[upload] done, '{name}' -> object_id={object_id}")
        return object_id

    def download(self, name: str) -> bytes:
        """Find the object by name, fetch each chunk from the first replica
        that answers, join in order.

        Raises KeyError if no object has that name, and RuntimeError if every
        copy of some chunk is unavailable."""
        object_id = self.names.get(name)
        if object_id is None:
            raise KeyError(f"No object named '{name}'")
        meta = self.metadata[object_id]
        print(f"[download] '{name}' (object_id={object_id}): fetching {len(meta.chunks)} chunk(s)")

        pieces = []
        for info in sorted(meta.chunks, key=lambda c: c.index):
            pieces.append(self._fetch_chunk(info))
        return b"".join(pieces)

    def _fetch_chunk(self, info: ChunkInfo) -> bytes:
        """Try each replica in order; fall back to the next if one fails."""
        for node_id in info.replicas:
            node = self._get_node(node_id)
            try:
                data = node.get_chunk(info.chunk_id)
            except NodeUnavailable:
                print(f"[download] chunk {info.index}: {node_id} is down, trying next copy")
                continue
            except KeyError:
                print(f"[download] chunk {info.index}: {node_id} lost the chunk, trying next copy")
                continue
            print(f"[download] chunk {info.index} <- {node_id}")
            return data
        raise RuntimeError(
            f"All {len(info.replicas)} copies of chunk {info.index} are unavailable"
        )

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

        for name, object_id in self.names.items():
            meta = self.metadata[object_id]
            for info in sorted(meta.chunks, key=lambda c: c.index):
                replicas = []
                for node_id in info.replicas:
                    node = self._get_node(node_id)
                    replicas.append({
                        "node": node_id,
                        "node_up": node.alive,
                        "usable": node.has_chunk(info.chunk_id),
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
