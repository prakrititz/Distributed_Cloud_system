"""Node server: ONE storage node running as its own process.

Start one per node, each with its own port and data folder, e.g.

    NODE_ID=Node1 PORT=5001 DATA_DIR=./data/Node1 python node_server.py
    NODE_ID=Node2 PORT=5002 DATA_DIR=./data/Node2 python node_server.py
    ...

(PowerShell: $env:NODE_ID="Node1"; $env:PORT="5001"; python node_server.py)

Settings (environment variables): NODE_ID, PORT, CAPACITY (bytes), DATA_DIR,
HOST (use 0.0.0.0 inside a container so other containers can reach it),
CONTROLLER_URL (where to send heartbeats, default http://127.0.0.1:5000) and
HEARTBEAT_INTERVAL (seconds, default 2).

Later each of these becomes one Docker container built from the same image.
The distributor talks to it only through the endpoints below, never by
importing it. Nodes never talk to each other.
"""
import os
import threading
import time

import requests
from flask import Flask, Response, request, jsonify

from checksum import compute, verify
from config import HEARTBEAT_INTERVAL as DEFAULT_HEARTBEAT_INTERVAL
from config import CONTROLLER_URL as DEFAULT_CONTROLLER_URL
from node import ChunkCorrupt, ChunkStore

app = Flask(__name__)

NODE_ID = os.environ.get("NODE_ID", "Node1")
HOST = os.environ.get("HOST", "127.0.0.1")
PORT = int(os.environ.get("PORT", "5001"))
CAPACITY = int(os.environ.get("CAPACITY", 5 * 1024 * 1024))   # bytes
DATA_DIR = os.environ.get("DATA_DIR", f"./data/{NODE_ID}")
CONTROLLER_URL = os.environ.get("CONTROLLER_URL", DEFAULT_CONTROLLER_URL)
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", DEFAULT_HEARTBEAT_INTERVAL))

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


def send_heartbeats() -> None:
    """Runs forever in a background thread: every HEARTBEAT_INTERVAL seconds
    tell the controller this node is alive and how much space it has. If the
    controller cannot be reached, just try again next round (it may not be
    started yet, or may be restarting)."""
    url = CONTROLLER_URL.rstrip("/") + "/internal/workers/heartbeat"
    connected = None   # only to print when the connection state changes
    while True:
        try:
            response = requests.post(url, json={
                "worker_id": NODE_ID,
                "capacity": store.capacity,
                "used": store.used_space(),
                "free": store.free_space(),
                "chunks": store.chunk_count(),
            }, timeout=(0.5, 2))
            now_connected = response.status_code == 200
        except requests.RequestException:
            now_connected = False
        if now_connected != connected:
            connected = now_connected
            print(f"[{NODE_ID}] heartbeats to {url}: " + ("accepted" if connected else "not getting through"))
        time.sleep(HEARTBEAT_INTERVAL)


if __name__ == "__main__":
    print(f"[{NODE_ID}] listening on {HOST}:{PORT}, data in {DATA_DIR}, capacity {CAPACITY} bytes")
    threading.Thread(target=send_heartbeats, daemon=True).start()
    app.run(host=HOST, port=PORT)
