"""Flask app: the client-facing API. It owns the distributor, which talks to
the node servers (nodes/node_server.py) over HTTP through RemoteNode objects.

Objects are identified by the object_id returned from an upload. The file name
is only metadata: several objects can share it.

Start the 4 node servers first (see nodes/node_server.py), then this app. Each
node's capacity is set on its own server (CAPACITY), not here.
"""
import io
import json
import mimetypes
import os

from flask import Flask, request, jsonify, send_file
from werkzeug.exceptions import HTTPException

from config import NODE_ADDRESSES
from controller.distributor import DEFAULT_CONTENT_TYPE, Distributor
from controller.remote_node import RemoteNode
from helpers.checksum import compute, verify
from helpers.errors import NodeUnavailable

app = Flask(__name__)

MAX_CLIENT_ID_LENGTH = 128

# One RemoteNode per address in config.py. The distributor calls their methods
# (store_chunk, get_chunk, ...) and each call becomes an HTTP request to that
# node's server.
nodes = [RemoteNode(node_id, url) for node_id, url in NODE_ADDRESSES.items()]
distributor = Distributor(nodes)


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
      'size'      size of the original object, if the uploaded bytes are a
                  wrapped form of it (defaults to the uploaded size)

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
    Asks each node's server for its health; a node that cannot be reached
    is listed as down. To test a failure, stop that node's server process."""
    report = []
    for n in nodes:
        try:
            h = n.health()   # one request per node
            report.append({
                "node": n.node_id,
                "alive": True,
                "capacity": h["capacity"],
                "used": h["used"],
                "free": h["free"],
                "chunks": h["chunks"],
                "heartbeat_age": distributor.heartbeat_age(n.node_id),
            })
        except NodeUnavailable:
            report.append({"node": n.node_id, "alive": False,
                           "heartbeat_age": distributor.heartbeat_age(n.node_id)})
    return jsonify(report)


@app.route("/api/v1/replication", methods=["GET"])
def show_replication():
    """Debug endpoint: health of every chunk (healthy / under_replicated /
    unavailable). Read-only: it reports problems, it does not repair them."""
    return jsonify(distributor.replication_report())


@app.route("/internal/workers/heartbeat", methods=["POST"])
def worker_heartbeat():
    """A worker reports that it is alive and how much space it has:
    {"worker_id", "capacity", "used", "free", "chunks"}. The controller keeps the
    latest report per worker and places chunks from it without asking the
    workers. 400 for a bad body, 404 for a worker that is not in config.py."""
    body = request.get_json(silent=True)
    fields = ("capacity", "used", "free", "chunks")
    if (not isinstance(body, dict) or not isinstance(body.get("worker_id"), str)
            or any(type(body.get(f)) is not int or body[f] < 0 for f in fields)):
        return error("Body must be JSON with a 'worker_id' string and non-negative "
                     "integers 'capacity', 'used', 'free' and 'chunks'", 400)
    try:
        distributor.record_heartbeat(body["worker_id"], *(body[f] for f in fields))
    except KeyError:
        return error(f"Unknown worker '{body['worker_id']}'", 404)
    return jsonify({"status": "ok"})


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
    app.run(host=os.environ.get("HOST", "127.0.0.1"),
            port=int(os.environ.get("PORT", "5000")),
            debug=os.environ.get("DEBUG") == "1",
            use_reloader=False)
