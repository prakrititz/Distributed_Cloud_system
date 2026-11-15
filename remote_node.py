"""RemoteNode: how the distributor talks to a node over HTTP.

This is the CLIENT side of the wire; node_server.py is the SERVER side. The
distributor creates one RemoteNode per node and calls its methods. Each
method sends one HTTP request to that node's server.

It offers the same methods and attributes the distributor already uses, so
distributor.py needs little or no change.

Error mapping the distributor already understands:
    node unreachable / timeout / server error  -> raise NodeUnavailable
    chunk not found (404)                      -> raise KeyError
    no space (507)                             -> store_chunk returns False
    invalid chunk id (400)                     -> raise ValueError (a bug on
                                                  our side, not a node failure)
"""
import requests

from config import REQUEST_TIMEOUT
from node import NodeUnavailable


class RemoteNode:
    def __init__(self, node_id: str, base_url: str, timeout: float = REQUEST_TIMEOUT):
        self.node_id = node_id
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    # --- the one place that actually sends HTTP --------------------------
    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        """Send one request to this node. If the node cannot be reached (not
        running, connection refused, too slow), raise NodeUnavailable."""
        try:
            return requests.request(
                method, self.base_url + path, timeout=self.timeout, **kwargs
            )
        except requests.RequestException as e:
            raise NodeUnavailable(f"{self.node_id} unreachable: {type(e).__name__}") from None

    def _unexpected(self, response: requests.Response) -> NodeUnavailable:
        """The node answered, but not in a way we can use (e.g. a 500)."""
        return NodeUnavailable(f"{self.node_id} returned unexpected status {response.status_code}")

    # --- node state ------------------------------------------------------
    def health(self) -> dict:
        """GET /internal/health -> {node_id, capacity, used, free, chunks}.
        Raises NodeUnavailable if the node can't be reached."""
        response = self._request("GET", "/internal/health")
        if response.status_code != 200:
            raise self._unexpected(response)
        return response.json()

    @property
    def alive(self) -> bool:
        """True if the node answers GET /internal/health in time."""
        try:
            self.health()
            return True
        except NodeUnavailable:
            return False

    @property
    def capacity(self) -> int:
        """Total bytes the node can hold. Raises NodeUnavailable if down."""
        return self.health()["capacity"]

    def used_space(self) -> int:
        """Bytes stored on the node. Raises NodeUnavailable if down."""
        return self.health()["used"]

    def free_space(self) -> int:
        """Free bytes on the node, or 0 if it can't be reached, so an
        unreachable node is simply never chosen for placement.
        Note: this is a network call, made for every node on every chunk
        placement. Fine for now; heartbeats can cache it later."""
        try:
            return self.health()["free"]
        except NodeUnavailable:
            return 0

    def chunk_count(self) -> int:
        """Number of chunks on the node. Raises NodeUnavailable if down."""
        return self.health()["chunks"]

    # --- chunk operations ------------------------------------------------
    def store_chunk(self, chunk_id: str, data: bytes) -> bool:
        """PUT the chunk's raw bytes. True if stored, False if the node has no
        space. Raises NodeUnavailable if the node is unreachable."""
        response = self._request(
            "PUT", f"/internal/chunks/{chunk_id}",
            data=data, headers={"Content-Type": "application/octet-stream"},
        )
        if response.status_code == 201:
            return True
        if response.status_code == 507:
            return False
        if response.status_code == 400:
            raise ValueError(response.json().get("error", "invalid chunk id"))
        raise self._unexpected(response)

    def get_chunk(self, chunk_id: str) -> bytes:
        """GET the chunk's bytes. Raises KeyError if the node doesn't have it,
        NodeUnavailable if it can't be reached."""
        response = self._request("GET", f"/internal/chunks/{chunk_id}")
        if response.status_code == 200:
            return response.content
        if response.status_code == 404:
            raise KeyError(chunk_id)
        if response.status_code == 400:
            raise ValueError(response.json().get("error", "invalid chunk id"))
        raise self._unexpected(response)

    def has_chunk(self, chunk_id: str) -> bool:
        """True only if the node is up AND holds the chunk (HEAD request, so no
        data is transferred). Used by the replication health report."""
        try:
            response = self._request("HEAD", f"/internal/chunks/{chunk_id}")
        except NodeUnavailable:
            return False
        return response.status_code == 200

    def delete_chunk(self, chunk_id: str) -> None:
        """DELETE the chunk. No error if it was already gone. Raises
        NodeUnavailable if the node can't be reached (the distributor's
        rollback already tolerates that)."""
        response = self._request("DELETE", f"/internal/chunks/{chunk_id}")
        if response.status_code == 204:
            return
        if response.status_code == 400:
            raise ValueError(response.json().get("error", "invalid chunk id"))
        raise self._unexpected(response)
