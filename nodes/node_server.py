"""Node server: ONE storage node running as its own process.

Start one per node, each with its own port and data folder, e.g.

    NODE_ID=worker1 PORT=5001 python -m nodes.node_server
    NODE_ID=worker2 PORT=5002 python -m nodes.node_server
    ...

(PowerShell: $env:NODE_ID="worker1"; $env:PORT="5001"; python -m nodes.node_server)

Run these from the project's top folder (the one with app.py), so that the
helpers/ and nodes/ packages can be found. Start as many as you like, in any
order: each one registers itself with the controller.

Settings (environment variables):
  NODE_ID             this worker's id. "auto" (used in Docker) means: take the
                      number Docker Compose gave the container, so replica 2
                      becomes worker2 (see nodes/identity.py)
  WORKER_NAME         the prefix for "auto" ids (default "worker")
  PORT, HOST          where it listens (HOST=0.0.0.0 inside a container)
  CAPACITY            bytes it may store (default 5 MB)
  DATA_DIR            its chunk folder; "{node_id}" in it is replaced by the id,
                      so many workers can share one volume (default ./data/{node_id})
  ADVERTISE_URL       the address the controller should use to reach it
                      (default: worked out from HOST/PORT, or the container name)
  CONTROLLER_URL      where it registers and sends heartbeats
  HEARTBEAT_INTERVAL  seconds between heartbeats (default 2)
  WORKER_THREADS      requests it answers at the same time (default 32)

The controller talks to it only through the endpoints below, never by
importing it. Workers never talk to each other.

It is served by waitress, a production web server (not Flask's development
server): it keeps connections open between requests (HTTP keep-alive), so
the controller can reuse them, and answers with a fixed pool of threads.
"""
import os
import threading
import time

import requests
from flask import Flask, Response, request, jsonify
from waitress.server import create_server

from config import HEARTBEAT_INTERVAL as DEFAULT_HEARTBEAT_INTERVAL
from config import CONTROLLER_URL as DEFAULT_CONTROLLER_URL
from config import WORKER_THREADS
from helpers.checksum import compute, verify
from helpers.errors import ChunkCorrupt
from nodes import identity
from nodes.node import ChunkStore

app = Flask(__name__)

HOST = os.environ.get("HOST", "127.0.0.1")
PORT = int(os.environ.get("PORT", "5001"))
CAPACITY = int(os.environ.get("CAPACITY", 5 * 1024 * 1024))   # bytes
CONTROLLER_URL = os.environ.get("CONTROLLER_URL", DEFAULT_CONTROLLER_URL)
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", DEFAULT_HEARTBEAT_INTERVAL))


# Who am I, and where can the controller reach me? (see nodes/identity.py)
_requested_id = os.environ.get("NODE_ID", "worker1").strip() or "worker1"
NODE_ID, ADVERTISE_URL = identity.choose_identity(
    _requested_id, HOST, PORT,
    advertise_url=os.environ.get("ADVERTISE_URL", "").strip() or None,
    name_prefix=os.environ.get("WORKER_NAME", "worker").strip() or "worker",
    # only look the container up when asked to (it is a DNS query)
    container_name=identity.docker_container_name() if _requested_id == "auto" else None,
)
DATA_DIR = os.environ.get("DATA_DIR", "./data/{node_id}").replace("{node_id}", NODE_ID)

store = ChunkStore(DATA_DIR, CAPACITY)


def error(message: str, status: int):
    """Every error response is JSON with a meaningful message."""
    return jsonify({"error": message}), status


@app.route("/internal/chunks/<chunk_id>", methods=["PUT"])
def put_chunk(chunk_id):
    """Store a chunk. The request body is the raw bytes and the 'X-Checksum'
    header is the sender's checksum of them.
    201 on success, 507 if there is no space, 400 for a bad chunk_id or a
    missing header, 422 if the bytes don't match the checksum (damaged in
    transit: nothing is stored). Storing an id that already exists replaces it.
    """
    expected = request.headers.get("X-Checksum")
    if not expected:
        return error("Missing 'X-Checksum' header", 400)

    data = request.get_data()
    # Check B (distributor -> node): refuse a chunk damaged on the way here.
    if not verify(data, expected):
        return error(f"{NODE_ID}: checksum mismatch, chunk '{chunk_id}' was damaged in transit", 422)

    try:
        stored = store.store(chunk_id, data)
    except ValueError as e:
        return error(str(e), 400)

    if not stored:
        return error(
            f"{NODE_ID} has no space for {len(data)} bytes ({store.free_space()} free)",
            507,
        )
    print(f"[{NODE_ID}] stored {chunk_id} ({len(data)} bytes)")
    return jsonify({"chunk_id": chunk_id, "size": len(data)}), 201


@app.route("/internal/chunks/<chunk_id>", methods=["GET", "HEAD"])
def get_chunk(chunk_id):
    """Return the chunk's raw bytes (application/octet-stream), with their
    checksum in the 'X-Checksum' header so the receiver can verify them.
    404 if this node doesn't have it, 400 for a bad chunk_id, 409 if the
    stored chunk no longer matches its checksum (damaged on disk: it is NOT
    sent). HEAD answers 200 or 404 without sending the data: the cheap way to
    ask 'do you have this chunk?'.
    """
    try:
        if request.method == "HEAD":
            return Response(status=200 if store.has(chunk_id) else 404)
        data = store.get(chunk_id)   # checks the chunk against its stored checksum
    except ValueError as e:
        return error(str(e), 400)
    except KeyError:
        return error(f"{NODE_ID} has no chunk '{chunk_id}'", 404)
    except ChunkCorrupt as e:
        print(f"[{NODE_ID}] CORRUPT chunk {chunk_id}: refusing to serve it")
        return error(f"{NODE_ID}: {e}", 409)

    response = Response(data, mimetype="application/octet-stream")
    response.headers["X-Checksum"] = compute(data)
    return response


@app.route("/internal/chunks/<chunk_id>/checksum", methods=["GET"])
def chunk_checksum(chunk_id):
    """Recompute the checksum of a stored chunk right now, without sending the
    chunk itself, and compare it with the one saved at write time.
    Returns {chunk_id, checksum, stored, ok}. 404 if absent, 400 for a bad id.
    ('ok' is true when there is nothing to compare against.)
    """
    try:
        actual = store.compute_checksum(chunk_id)
        stored = store.stored_checksum(chunk_id)
    except ValueError as e:
        return error(str(e), 400)
    except KeyError:
        return error(f"{NODE_ID} has no chunk '{chunk_id}'", 404)

    return jsonify({
        "chunk_id": chunk_id,
        "checksum": actual,
        "stored": stored,
        "ok": stored is None or actual == stored,
    })


@app.route("/internal/chunks/<chunk_id>", methods=["DELETE"])
def delete_chunk(chunk_id):
    """Delete a chunk. 204 even if it was already absent (deleting twice is
    harmless), 400 for a bad chunk_id."""
    try:
        store.delete(chunk_id)
    except ValueError as e:
        return error(str(e), 400)
    print(f"[{NODE_ID}] deleted {chunk_id}")
    return "", 204


@app.route("/internal/chunks", methods=["GET"])
def list_chunks():
    """Every chunk this worker holds: {node_id, chunks: [{chunk_id, size, age}]}
    (age in seconds since the chunk was written). Used by the controller to
    find orphaned and missing chunks."""
    return jsonify({"node_id": NODE_ID, "chunks": store.list_chunks()})


@app.route("/internal/health", methods=["GET"])
def health():
    """Tell the distributor this node is alive and how much space it has."""
    return jsonify({
        "node_id": NODE_ID,
        "capacity": store.capacity,
        "used": store.used_space(),
        "free": store.free_space(),
        "chunks": store.chunk_count(),
    })


def space_report() -> dict:
    """What this worker tells the controller about itself."""
    return {
        "worker_id": NODE_ID,
        "capacity": store.capacity,
        "used": store.used_space(),
        "free": store.free_space(),
        "chunks": store.chunk_count(),
    }


def send_heartbeats(stop: threading.Event = None) -> None:
    """Runs forever in a background thread (until `stop` is set, if given:
    the tests use that).

    First REGISTER (POST /internal/workers/register with this worker's id and
    address), so the controller knows it exists. Then every
    HEARTBEAT_INTERVAL seconds send a heartbeat (alive + free space).

    If the controller cannot be reached, try again next round (it may not be
    started yet, or may be restarting). If it answers a heartbeat with 404 it
    has forgotten this worker (it restarted), so register again."""
    base = CONTROLLER_URL.rstrip("/")
    registered = False
    connected = None   # only to print when the state changes
    while stop is None or not stop.is_set():
        try:
            if not registered:
                response = requests.post(base + "/internal/workers/register",
                                         json={**space_report(), "url": ADVERTISE_URL}, timeout=(0.5, 2))
                registered = response.status_code in (200, 201)
                now_connected = registered
                if registered:
                    print(f"[{NODE_ID}] registered with {base} as {ADVERTISE_URL}")
            else:
                response = requests.post(base + "/internal/workers/heartbeat", json=space_report(), timeout=(0.5, 2))
                if response.status_code == 404:          # the controller does not know us (any more)
                    registered = False
                    print(f"[{NODE_ID}] the controller does not know this worker; registering again")
                    continue
                now_connected = response.status_code == 200
        except requests.RequestException:
            now_connected = False
        except Exception as e:
            # Anything else (a disk error while counting space, ...): say so and
            # try again next round. This thread must never die: without
            # heartbeats the controller counts a worker down that still works.
            print(f"[{NODE_ID}] heartbeat failed: {type(e).__name__}: {e}; trying again", flush=True)
            now_connected = False
        if now_connected != connected:
            connected = now_connected
            print(f"[{NODE_ID}] controller {base}: " + ("connected" if connected else "not reachable, retrying"))
        if stop is not None:
            stop.wait(HEARTBEAT_INTERVAL)
        else:
            time.sleep(HEARTBEAT_INTERVAL)


if __name__ == "__main__":
    print(f"[{NODE_ID}] listening on {HOST}:{PORT} (reachable as {ADVERTISE_URL}), "
          f"data in {DATA_DIR}, capacity {CAPACITY} bytes")
    # waitress, not app.run(): connections stay open for the controller to
    # reuse, and WORKER_THREADS requests are answered at once. A connection
    # idle for 120 s is closed (the controller then opens a new one).
    # Order matters: create_server already listens on the port, and only then
    # does the heartbeat thread register with the controller, so the
    # controller never hears "alive" from a worker that cannot be reached yet.
    server = create_server(app, host=HOST, port=PORT, threads=WORKER_THREADS,
                           connection_limit=max(100, 4 * WORKER_THREADS), channel_timeout=120,
                           ident=NODE_ID)
    threading.Thread(target=send_heartbeats, daemon=True).start()
    server.run()
