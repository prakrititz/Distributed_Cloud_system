"""Distributor: takes a client's request, splits data into chunks and
decides which node stores each chunk. Also reassembles on download.

The distributor is the only component that knows the whole picture:
which nodes exist, how full they are, and where every chunk of every
object lives. Nodes just store bytes.
"""

import uuid
from dataclasses import dataclass, field

CHUNK_SIZE = 1024 * 1024  # 1 MB, as the project spec says


@dataclass
class ObjectMetadata:
    """Kept as a separate object so it can be moved/exposed later."""
    object_id: str
    name: str
    size: int
    # list of (chunk_id, index, node_id), in order
    chunks: list = field(default_factory=list)


class Distributor:
    def __init__(self, nodes: list, chunk_size: int = CHUNK_SIZE):
        self.nodes = nodes             # list of Node (the node registry, for now)
        self.chunk_size = chunk_size   # a parameter so we can test with tiny chunks
        self.metadata = {}             # object_id -> ObjectMetadata
        self.names = {}                # name -> object_id (what the client looks up by)

    def _split(self, data: bytes) -> list:
        """bytes -> list of chunks of at most chunk_size.
        Empty data still gives one (empty) chunk so the object has a record."""
        if not data:
            return [b""]
        return [data[i:i + self.chunk_size] for i in range(0, len(data), self.chunk_size)]

    def _pick_node(self, chunk_size: int):
        """Choose the node with the most free space that can fit this chunk.
        Ties go to the first node in the list. Return None if no node fits."""
        candidates = [n for n in self.nodes if n.free_space() >= chunk_size]
        if not candidates:
            return None
        return max(candidates, key=lambda n: n.free_space())

    def _get_node(self, node_id: str):
        """Find a node by its id."""
        for node in self.nodes:
            if node.node_id == node_id:
                return node
        raise KeyError(f"Unknown node: {node_id}")
    
    def upload(self, name: str, data: bytes) -> str:
        """Split data, store each chunk on a node, record metadata.
        Return the new object_id.

        The client identifies an object by its name only. The object_id is
        internal: it is generated here, used to name the chunks, and only
        becomes visible (name -> id map and metadata) once ALL chunks are
        stored. If any chunk cannot be stored, the chunks already placed are
        deleted and a RuntimeError is raised, so nothing half-uploaded is
        left behind.

        Raises FileExistsError if an object with this name already exists.
        """
        if name in self.names:
            raise FileExistsError(f"An object named '{name}' already exists")

        object_id = str(uuid.uuid4())
        chunk_list = self._split(data)
        print(f"[upload] '{name}': {len(data)} bytes -> {len(chunk_list)} chunk(s)")

        placed = []  # (chunk_id, index, node) for rollback and metadata
        for index, chunk in enumerate(chunk_list):
            chunk_id = f"{object_id}-{index}"
            node = self._pick_node(len(chunk))

            if node is None or not node.store_chunk(chunk_id, chunk):
                print(f"[upload] no node can hold chunk {index} ({len(chunk)} bytes), rolling back")
                for placed_id, _, placed_node in placed:
                    placed_node.delete_chunk(placed_id)
                raise RuntimeError("Not enough storage space across nodes")

            print(f"[upload] chunk {index} ({len(chunk)} bytes) -> {node.node_id}")
            placed.append((chunk_id, index, node))

        self.metadata[object_id] = ObjectMetadata(
            object_id=object_id,
            name=name,
            size=len(data),
            chunks=[(cid, idx, node.node_id) for cid, idx, node in placed],
        )
        self.names[name] = object_id
        print(f"[upload] done, '{name}' -> object_id={object_id}")
        return object_id

    def download(self, name: str) -> bytes:
        """Find the object by name, fetch its chunks from their nodes, join
        in order. Raises KeyError if no object has that name."""
        object_id = self.names.get(name)
        if object_id is None:
            raise KeyError(f"No object named '{name}'")
        meta = self.metadata[object_id]
        print(f"[download] '{name}' (object_id={object_id}): fetching {len(meta.chunks)} chunk(s)")

        pieces = []
        for chunk_id, index, node_id in sorted(meta.chunks, key=lambda c: c[1]):
            node = self._get_node(node_id)
            print(f"[download] chunk {index} <- {node_id}")
            pieces.append(node.get_chunk(chunk_id))

        return b"".join(pieces)
    
