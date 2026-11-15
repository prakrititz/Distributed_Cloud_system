"""Flask app: the client-facing API. It owns the distributor, which talks to
the node servers (node_server.py) over HTTP through RemoteNode objects.

The client identifies an object by its name only. Object IDs stay internal.

Start the 4 node servers first (see node_server.py), then this app. Each
node's capacity is set on its own server (CAPACITY), not here.
"""
import io

from flask import Flask, request, jsonify, send_file

from config import NODE_ADDRESSES
from distributor import Distributor
from node import NodeUnavailable
from remote_node import RemoteNode

app = Flask(__name__)

# One RemoteNode per address in config.py. The distributor calls their methods
# (store_chunk, get_chunk, ...) and each call becomes an HTTP request to that
# node's server.
nodes = [RemoteNode(node_id, url) for node_id, url in NODE_ADDRESSES.items()]
distributor = Distributor(nodes)


def error(message: str, status: int):
    """Every error response is JSON with a meaningful message."""
    return jsonify({"error": message}), status


@app.route("/api/v1/objects", methods=["POST"])
def upload_object():
    """Receive a file (multipart field 'file'), pass it to distributor.upload.
    Return JSON with name, object_id, size, number of chunks. Status 201.
    400 if no file / empty name, 409 if the name exists, 503 if there is not
    enough space on enough live nodes to store every chunk twice.
    """
    file = request.files.get("file")
    if file is None or not file.filename:
        return error("Missing 'file' field in the multipart upload", 400)

    name = file.filename
    data = file.read()

    try:
        object_id = distributor.upload(name, data)
    except FileExistsError as e:
        return error(str(e), 409)
    except RuntimeError as e:
        return error(str(e), 503)

    meta = distributor.metadata[object_id]
    return jsonify({
        "name": meta.name,
        "object_id": object_id,
        "size": meta.size,
        "chunks": len(meta.chunks),
        "copies_per_chunk": distributor.replication_factor,
    }), 201


@app.route("/api/v1/objects/<name>", methods=["GET"])
def download_object(name):
    """Ask distributor.download for the bytes and send them back.
    404 if no object has that name, 503 if every copy of a chunk is down.
    """
    if name not in distributor.names:
        return error(f"No object named '{name}'", 404)

    try:
        data = distributor.download(name)
    except RuntimeError as e:
        return error(str(e), 503)

    return send_file(io.BytesIO(data), mimetype="application/octet-stream")


@app.route("/api/v1/objects/<name>/chunks", methods=["GET"])
def show_distribution(name):
    """Debug endpoint (our own, not in the spec): return which nodes hold
    each chunk's copies, so we can SEE how the distributor spread the file.
    """
    object_id = distributor.names.get(name)
    if object_id is None:
        return error(f"No object named '{name}'", 404)

    meta = distributor.metadata[object_id]
    return jsonify({
        "name": meta.name,
        "object_id": meta.object_id,
        "size": meta.size,
        "chunks": [
            {"index": c.index, "chunk_id": c.chunk_id, "replicas": c.replicas}
            for c in sorted(meta.chunks, key=lambda c: c.index)
        ],
    })


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
            })
        except NodeUnavailable:
            report.append({"node": n.node_id, "alive": False})
    return jsonify(report)


@app.route("/api/v1/replication", methods=["GET"])
def show_replication():
    """Debug endpoint: health of every chunk (healthy / under_replicated /
    unavailable). Read-only: it reports problems, it does not repair them."""
    return jsonify(distributor.replication_report())


if __name__ == "__main__":
    # use_reloader=False: the reloader would start a second process with its
    # own empty copy of the nodes, which is confusing while we're learning.
    app.run(port=5000, debug=True, use_reloader=False)
