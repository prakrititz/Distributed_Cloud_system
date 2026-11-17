"""Flask app: the client-facing API. It owns the distributor, which talks to
the node servers (nodes/node_server.py) over HTTP through RemoteNode objects.

Objects are identified by the object_id returned from an upload. The file name
is only metadata: several objects can share it.

Workers register themselves when they start (POST /internal/workers/register),
so start them in any order, and as many as you like. Each worker's capacity is
set on its own server (CAPACITY), not here. The chunk size and the number of
copies per chunk are CHUNK_SIZE and REPLICATION_FACTOR (see config.py).
"""
import io
import json
import mimetypes
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, request, jsonify, send_file
from werkzeug.exceptions import HTTPException

from config import (AUTO_REPAIR_GRACE, CHUNK_SIZE, HEARTBEAT_INTERVAL, METADATA_PATH, NODE_ADDRESSES,
                    ORPHAN_MIN_AGE, REPLICATION_FACTOR)
from controller.distributor import DEFAULT_CONTENT_TYPE, Distributor
from controller.metadata_store import store_from_setting
from controller.remote_node import RemoteNode
from helpers.checksum import compute, verify
from helpers.errors import NodeUnavailable

app = Flask(__name__)

MAX_CLIENT_ID_LENGTH = 128

# One RemoteNode per worker. The distributor calls their methods (store_chunk,
# get_chunk, ...) and each call becomes an HTTP request to that worker's server.
# Workers listed in config.WORKERS are known from the start; every other worker
# is added when it registers.
# The metadata is saved to METADATA_PATH after every change and loaded here,
# so restarting the controller keeps every object.
distributor = Distributor([RemoteNode(node_id, url) for node_id, url in NODE_ADDRESSES.items()],
                          chunk_size=CHUNK_SIZE, replication_factor=REPLICATION_FACTOR,
                          store=store_from_setting(METADATA_PATH))
auto_repair = {"enabled": False, "grace_seconds": AUTO_REPAIR_GRACE}   # switched on in __main__


def auto_repair_loop(grace: float, interval: float = HEARTBEAT_INTERVAL) -> None:
    """Background thread: every `interval` seconds see whether a worker has
    been down for `grace` seconds, and if so repair (Distributor.auto_repair_check)."""
    while True:
        try:
            distributor.auto_repair_check(grace)
        except Exception as e:            # never let the thread die
            print(f"[auto-repair] check failed: {e}")
        time.sleep(interval)


def start_auto_repair(grace: float = AUTO_REPAIR_GRACE) -> bool:
    if grace <= 0:
        return False
    auto_repair.update(enabled=True, grace_seconds=grace)
    threading.Thread(target=auto_repair_loop, args=(grace,), daemon=True).start()
    return True


def error(message: str, status: int):
    """Every error response is JSON with a meaningful message."""
    return jsonify({"error": message}), status


@app.errorhandler(HTTPException)
def handle_http_error(e):
    """Flask's own errors (unknown URL, wrong method, ...) as JSON too."""
    return error(e.description, e.code)


@app.errorhandler(Exception)
def handle_unexpected_error(e):
    """Anything unexpected: log it, answer with JSON instead of an HTML page."""
    app.logger.exception("Unhandled error")
    return error("Internal server error", 500)


def parse_custom(value, what: str) -> dict:
    """Check that `value` is a JSON object of key-value pairs, with string keys
    and string/number/boolean/null values. Raises ValueError if not."""
    if not isinstance(value, dict):
        raise ValueError(f"{what} must be a JSON object of key-value pairs")
    for key, val in value.items():
        if not isinstance(key, str) or not key:
            raise ValueError(f"{what}: keys must be non-empty strings")
        if val is not None and not isinstance(val, (str, int, float, bool)):
            raise ValueError(f"{what}: the value of '{key}' must be a string, number, boolean or null")
    return value


def object_json(meta) -> dict:
    """An object's full metadata as JSON: what the system keeps (size, type,
    times, checksum, where every chunk copy lives) plus the custom metadata."""
    return {
        "object_id": meta.object_id,
        "name": meta.name,
        "size": meta.size,
        "content_type": meta.content_type,
        "created_at": meta.created_at,
        "modified_at": meta.modified_at,
        "checksum": meta.checksum,
        "source_path": meta.source_path,
        "client_id": meta.client_id,
        "chunk_count": len(meta.chunks),
        "copies_per_chunk": distributor.replication_factor,
        "chunks": [
            {"index": c.index, "chunk_id": c.chunk_id, "checksum": c.checksum, "replicas": c.replicas}
            for c in sorted(meta.chunks, key=lambda c: c.index)
        ],
        "custom": meta.custom,
    }


@app.route("/api/v1/objects", methods=["POST"])
def upload_object():
    """Upload an object (multipart/form-data).

    Fields: 'file' (required) and, all optional:
      'metadata'  JSON object of custom key-value metadata
      'checksum'  checksum of the file; verified before anything is stored
      'path'      where the client read the file from. Two uploads from the
                  same path by the same client are the same file, so the
                  second is refused
      'client_id' which client is uploading (at most 128 characters). It
                  scopes the 'path' rule: two clients may each upload their
                  own '/files/a.txt'. Without it, uploads share one anonymous
                  scope
      'size'      size to record, if it differs from the uploaded size
                  (defaults to the uploaded size)

    201 with the new object's metadata. 400 for a bad field or a checksum
    mismatch (nothing is stored), 409 if an object from this path exists,
    503 if there is not enough space on enough live nodes to store every
    chunk twice.
    """
    file = request.files.get("file")
    if file is None or not file.filename:
        return error("Missing 'file' field in the multipart upload", 400)

    data = file.read()
    name = os.path.basename(file.filename.replace("\\", "/"))

    # Check A (client -> server): if the client sent a checksum, the bytes that
    # arrived must match it. Reject before storing anything.
    expected = request.form.get("checksum")
    if expected and not verify(data, expected):
        return error("Checksum mismatch: the upload was damaged in transit", 400)

    # content type: what the client said, else guessed from the file name
    content_type = file.mimetype
    if not content_type or content_type == DEFAULT_CONTENT_TYPE:
        content_type = mimetypes.guess_type(name)[0] or DEFAULT_CONTENT_TYPE

    try:
        custom = {}
        if request.form.get("metadata"):
            try:
                raw = json.loads(request.form["metadata"])
            except ValueError:
                raise ValueError("'metadata' must be valid JSON")
            custom = {k: v for k, v in parse_custom(raw, "'metadata'").items() if v is not None}

        size = None
        if request.form.get("size"):
            try:
                size = int(request.form["size"])
            except ValueError:
                raise ValueError("'size' must be an integer")
            if size < 0:
                raise ValueError("'size' must not be negative")

        client_id = (request.form.get("client_id") or "").strip()
        if len(client_id) > MAX_CLIENT_ID_LENGTH:
            raise ValueError(f"'client_id' must be at most {MAX_CLIENT_ID_LENGTH} characters")
    except ValueError as e:
        return error(str(e), 400)

    try:
        object_id = distributor.upload(
            name, data, content_type=content_type, custom=custom,
            source_path=request.form.get("path") or None, size=size,
            client_id=client_id or None,
        )
    except FileExistsError as e:
        return error(str(e), 409)
    except RuntimeError as e:
        return error(str(e), 503)

    return jsonify(object_json(distributor.get_object(object_id))), 201


@app.route("/api/v1/objects", methods=["GET"])
def list_objects():
    """List the stored objects (object_id, name, size, content_type and times),
    oldest first. Optional 'limit' and 'offset' query parameters paginate; the
    total number of objects is in the 'X-Total-Count' header."""
    try:
        offset = int(request.args.get("offset", 0))
        limit = request.args.get("limit")
        limit = None if limit is None else int(limit)
        if offset < 0 or (limit is not None and limit < 0):
            raise ValueError
    except ValueError:
        return error("'limit' and 'offset' must be non-negative integers", 400)

    objects = distributor.list_objects()
    page = objects[offset:] if limit is None else objects[offset:offset + limit]
    response = jsonify([
        {
            "object_id": m.object_id,
            "name": m.name,
            "size": m.size,
            "content_type": m.content_type,
            "created_at": m.created_at,
            "modified_at": m.modified_at,
        }
        for m in page
    ])
    response.headers["X-Total-Count"] = str(len(objects))
    return response


@app.route("/api/v1/objects/<object_id>", methods=["GET"])
def download_object(object_id):
    """Send the object's bytes back with its content type, and their checksum
    in the 'X-Checksum' header so the client can verify them.
    404 if there is no such object, 503 if no good copy of a chunk is left.
    """
    try:
        meta = distributor.get_object(object_id)
        data = distributor.download(object_id)
    except KeyError:
        return error(f"No object with id '{object_id}'", 404)
    except RuntimeError as e:
        return error(str(e), 503)

    response = send_file(io.BytesIO(data), mimetype=meta.content_type)
    response.headers["X-Checksum"] = compute(data)
    return response


@app.route("/api/v1/objects/<object_id>/metadata", methods=["GET"])
def get_metadata(object_id):
    """The object's metadata, without downloading its data. 404 if unknown."""
    try:
        return jsonify(object_json(distributor.get_object(object_id)))
    except KeyError:
        return error(f"No object with id '{object_id}'", 404)


@app.route("/api/v1/objects/<object_id>/metadata", methods=["PATCH"])
def update_metadata(object_id):
    """Add or update custom metadata. The body is a JSON object of key-value
    pairs; a null value removes that key. The stored data is not touched.
    400 for a bad body, 404 if unknown. Returns the updated metadata."""
    try:
        updates = parse_custom(request.get_json(silent=True), "The request body")
    except ValueError as e:
        return error(str(e), 400)

    try:
        meta = distributor.update_custom_metadata(object_id, updates)
    except KeyError:
        return error(f"No object with id '{object_id}'", 404)
    return jsonify(object_json(meta))


@app.route("/api/v1/objects/<object_id>", methods=["DELETE"])
def delete_object(object_id):
    """Delete the object and every copy of its chunks. 204, or 404 if unknown."""
    try:
        distributor.delete(object_id)
    except KeyError:
        return error(f"No object with id '{object_id}'", 404)
    return "", 204


@app.route("/api/v1/nodes", methods=["GET"])
def show_nodes():
    """Debug endpoint: up/down state and free/used space of each node.
    Asks each node's server for its health, all at once; a node that cannot
    be reached is listed as down. A node the controller already counts as
    down (no heartbeat for HEARTBEAT_TIMEOUT) is not asked: asking a stopped
    container can take seconds, because its name no longer resolves. To test
    a failure, stop that node's server process."""
    def probe(n):
        down = {"node": n.node_id, "alive": False, "heartbeat_age": distributor.heartbeat_age(n.node_id)}
        if distributor.counts_as_down(n.node_id):
            return down
        try:
            h = n.health()
        except NodeUnavailable:
            return down
        return {"node": n.node_id, "alive": True, "capacity": h["capacity"], "used": h["used"],
                "free": h["free"], "chunks": h["chunks"], "heartbeat_age": distributor.heartbeat_age(n.node_id)}

    nodes = list(distributor.nodes)
    if not nodes:
        return jsonify([])
    with ThreadPoolExecutor(max_workers=min(32, len(nodes))) as pool:
        return jsonify(list(pool.map(probe, nodes)))


@app.route("/api/v1/replication", methods=["GET"])
def show_replication():
    """Debug endpoint: health of every chunk (healthy / under_replicated /
    unavailable). Read-only: it reports problems, it does not repair them.
    'auto_repair' says whether automatic repair is on and what it last did."""
    report = distributor.replication_report()
    report["auto_repair"] = {**auto_repair, "last": distributor.last_auto_repair}
    return jsonify(report)


SPACE_FIELDS = ("capacity", "used", "free", "chunks")
MAX_WORKER_ID_LENGTH = 64


def worker_report(body):
    """Check a register/heartbeat body: a 'worker_id' string and non-negative
    integers for the space fields. Returns an error message, or None if fine."""
    if (not isinstance(body, dict) or not isinstance(body.get("worker_id"), str)
            or not body["worker_id"].strip() or len(body["worker_id"]) > MAX_WORKER_ID_LENGTH
            or any(type(body.get(f)) is not int or body[f] < 0 for f in SPACE_FIELDS)):
        return (f"Body must be JSON with a 'worker_id' string (1-{MAX_WORKER_ID_LENGTH} characters) "
                "and non-negative integers 'capacity', 'used', 'free' and 'chunks'")
    return None


@app.route("/internal/workers/register", methods=["POST"])
def register_worker():
    """A worker announces itself when it starts:
    {"worker_id", "url", "capacity", "used", "free", "chunks"}, where url is the
    address the controller can reach it at. From then on it is used for
    placement. A worker that registers again with the same id (a restart)
    replaces its old entry, so its address is updated. 201 for a new worker,
    200 for a known one, 400 for a bad body."""
    body = request.get_json(silent=True)
    problem = worker_report(body)
    url = body.get("url") if isinstance(body, dict) else None
    if problem is None and (not isinstance(url, str) or not url.startswith(("http://", "https://"))):
        problem = "'url' must be the worker's address, starting with http:// or https://"
    if problem:
        return error(problem, 400)
    worker_id = body["worker_id"].strip()
    is_new = distributor.register_node(RemoteNode(worker_id, url), *(body[f] for f in SPACE_FIELDS))
    print(f"[controller] worker {worker_id} {'registered' if is_new else 're-registered'} at {url}", flush=True)
    return jsonify({"status": "registered", "worker_id": worker_id, "new": is_new}), 201 if is_new else 200


@app.route("/internal/workers/heartbeat", methods=["POST"])
def worker_heartbeat():
    """A worker reports that it is alive and how much space it has:
    {"worker_id", "capacity", "used", "free", "chunks"}. The controller keeps the
    latest report per worker and places chunks from it without asking the
    workers. 400 for a bad body, 404 for a worker that has not registered
    (for example after the controller restarted): the worker then registers."""
    body = request.get_json(silent=True)
    problem = worker_report(body)
    if problem:
        return error(problem, 400)
    try:
        distributor.record_heartbeat(body["worker_id"], *(body[f] for f in SPACE_FIELDS))
    except KeyError:
        return error(f"Unknown worker '{body['worker_id']}': register first", 404)
    return jsonify({"status": "ok"})


@app.route("/internal/workers/<worker_id>/chunks", methods=["GET"])
def worker_chunks(worker_id):
    """What a worker really holds compared with what the metadata says:
    {worker_id, held, expected, orphans, in_progress, missing}.
    404 unknown worker, 503 if it cannot be reached."""
    try:
        return jsonify(distributor.worker_inventory(worker_id))
    except KeyError:
        return error(f"Unknown worker '{worker_id}'", 404)
    except NodeUnavailable as e:
        return error(str(e), 503)


@app.route("/api/v1/cleanup", methods=["POST"])
def cleanup():
    """Delete orphaned chunks (held by a worker, but no object points to that
    copy) on every reachable worker. Chunks younger than 'min_age' seconds
    (query parameter, default ORPHAN_MIN_AGE) are left alone. 409 if the
    metadata is not saved (then it is not safe) or a repair is running."""
    try:
        min_age = float(request.args.get("min_age", ORPHAN_MIN_AGE))
        if min_age < 0:
            raise ValueError
    except ValueError:
        return error("'min_age' must be a non-negative number of seconds", 400)
    try:
        return jsonify(distributor.cleanup_orphans(min_age))
    except RuntimeError as e:
        return error(str(e), 409)


@app.route("/api/v1/repair", methods=["POST"])
def repair():
    """Re-replicate: bring every chunk back to the full number of good copies
    (after a node failure or a corrupt copy) and report what was done.
    409 if a repair is already running."""
    try:
        return jsonify(distributor.repair())
    except RuntimeError as e:
        return error(str(e), 409)


if __name__ == "__main__":
    # use_reloader=False: the reloader would start a second process with its
    # own empty copy of the nodes, which is confusing while we're learning.
    # HOST=0.0.0.0 is needed inside a container, otherwise nothing outside it
    # can connect. Debug mode (the interactive debugger) is off unless DEBUG=1.
    if start_auto_repair():
        print(f"[controller] automatic repair on: after a worker is down for {AUTO_REPAIR_GRACE} s")
    print(f"[controller] metadata: {os.path.abspath(METADATA_PATH) if METADATA_PATH else 'in memory only'}")
    app.run(host=os.environ.get("HOST", "127.0.0.1"),
            port=int(os.environ.get("PORT", "5000")),
            debug=os.environ.get("DEBUG") == "1",
            use_reloader=False)
