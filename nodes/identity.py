"""Who am I? How a worker started by Docker Compose finds its own name.

docker-compose.yml starts N copies of ONE worker service (`replicas: N`).
They all get the same settings, so none of them is told "you are worker 2".
But Docker names the containers object-store-worker-1, -2, ..., and keeps
that number when a container is re-created, so the number is a stable id.

A container cannot read its own name directly (its hostname is a random
container id). Docker's built-in DNS server (127.0.0.11, inside every
container on a compose network) knows it, though: asking it "who has my IP
address?" (a reverse, PTR, lookup) answers
"object-store-worker-2.object-store_store". The standard library cannot ask
a specific DNS server, so the tiny query is built by hand below.

Outside Docker there is no such server and the lookup returns None.
"""
import re
import socket
import struct

DOCKER_DNS = "127.0.0.11"


def _read_name(buf: bytes, pos: int):
    """Read a (possibly compressed) DNS name starting at pos. Returns (name, next_pos)."""
    labels, end = [], None
    for _ in range(64):                       # bounded: a broken packet cannot loop forever
        length = buf[pos]
        if length == 0:
            return ".".join(labels), (end if end is not None else pos + 1)
        if length & 0xC0 == 0xC0:             # pointer to a name earlier in the packet
            if end is None:
                end = pos + 2
            pos = ((length & 0x3F) << 8) | buf[pos + 1]
            continue
        labels.append(buf[pos + 1:pos + 1 + length].decode("ascii", "replace"))
        pos += 1 + length
    raise ValueError("DNS name too long")


def reverse_lookup(ip: str, server: str = DOCKER_DNS, timeout: float = 2.0):
    """The name the DNS server `server` has for `ip`, or None."""
    query_id = 0x4F53
    name = ".".join(reversed(ip.split("."))) + ".in-addr.arpa"
    packet = struct.pack(">HHHHHH", query_id, 0x0100, 1, 0, 0, 0)          # header: 1 question
    packet += b"".join(bytes([len(p)]) + p.encode("ascii") for p in name.split(".")) + b"\x00"
    packet += struct.pack(">HH", 12, 1)                                    # type PTR, class IN
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(timeout)
            s.sendto(packet, (server, 53))
            reply, _ = s.recvfrom(1024)
        answer_id, _, questions, answers, _, _ = struct.unpack(">HHHHHH", reply[:12])
        if answer_id != query_id or answers == 0:
            return None
        pos = 12
        for _ in range(questions):
            _, pos = _read_name(reply, pos)
            pos += 4
        for _ in range(answers):
            _, pos = _read_name(reply, pos)
            rtype, _, _, rdlength = struct.unpack(">HHIH", reply[pos:pos + 10])
            pos += 10
            if rtype == 12:
                return _read_name(reply, pos)[0]
            pos += rdlength
    except (OSError, ValueError, IndexError, struct.error):
        return None
    return None


def own_ip() -> str:
    """This container's address on its network."""
    return socket.gethostbyname(socket.gethostname())


def docker_container_name():
    """'object-store-worker-2' when running under Docker Compose, else None."""
    try:
        full = reverse_lookup(own_ip())
    except OSError:
        return None
    return full.split(".")[0] if full else None


def number_from_container_name(container_name: str):
    """'object-store-worker-2' -> 2 (the replica number compose gives it), or None."""
    match = re.search(r"-(\d+)$", container_name or "")
    return int(match.group(1)) if match else None


def choose_identity(node_id: str, host: str, port: int, advertise_url: str = None,
                    name_prefix: str = "worker", container_name: str = None):
    """(node_id, url): who this worker is and where the controller can reach it.

    node_id "auto": inside Docker Compose the container is called e.g.
    object-store-worker-2, which gives the id worker2 (with name_prefix
    "worker") and the address http://object-store-worker-2:<port>, a name the
    other containers can resolve. Anywhere else "auto" falls back to the
    machine's name.
    Any other node_id is used as it is. advertise_url, if given, wins."""
    if node_id == "auto":
        number = number_from_container_name(container_name)
        node_id = f"{name_prefix}{number}" if number is not None else socket.gethostname()
    if advertise_url:
        return node_id, advertise_url.rstrip("/")
    if container_name:
        return node_id, f"http://{container_name}:{port}"
    if host in ("0.0.0.0", "", "::"):
        host = own_ip()
    return node_id, f"http://{host}:{port}"
