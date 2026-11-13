"""Flask app: the client-facing API. It owns the distributor and the 4 nodes.

The client identifies an object by its name only. Object IDs stay internal.
"""
import io

from flask import Flask, request, jsonify, send_file

from node import Node
from distributor import Distributor

app = Flask(__name__)

# 4 nodes with small capacity so a big file visibly spills over several nodes.
# Every chunk is stored twice, so 20 MB of raw capacity holds about 10 MB of files.
NODE_CAPACITY = 5 * 1024 * 1024  # 5 MB each, 20 MB in total
nodes = [Node(f"Node{i}", NODE_CAPACITY) for i in range(1, 5)]
distributor = Distributor(nodes)


def error(message: str, status: int):
    """Every error response is JSON with a meaningful message."""
    return jsonify({"error": message}), status


def find_node(node_id: str):
    """Return the node with this id, or None."""
    return next((n for n in nodes if n.node_id == node_id), None)


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
    """Debug endpoint: up/down state and free/used space of each node."""
    return jsonify([
        {
            "node": n.node_id,
            "alive": n.alive,
            "capacity": n.capacity,
            "used": n.used_space(),
            "free": n.free_space(),
            "chunks": len(n.chunks),
        }
        for n in nodes
    ])


@app.route("/api/v1/nodes/<node_id>/stop", methods=["POST"])
def stop_node(node_id):
    """Simulate a node failure (stands in for `docker stop`). Chunks are kept."""
    node = find_node(node_id)
    if node is None:
        return error(f"No node named '{node_id}'", 404)
    node.stop()
    print(f"[node] {node_id} stopped")
    return jsonify({"node": node_id, "alive": node.alive})


@app.route("/api/v1/nodes/<node_id>/start", methods=["POST"])
def start_node(node_id):
    """Bring a stopped node back (stands in for `docker start`)."""
    node = find_node(node_id)
    if node is None:
        return error(f"No node named '{node_id}'", 404)
    node.start()
    print(f"[node] {node_id} started")
    return jsonify({"node": node_id, "alive": node.alive})


@app.route("/api/v1/replication", methods=["GET"])
def show_replication():
    """Debug endpoint: health of every chunk (healthy / under_replicated /
    unavailable). Read-only: it reports problems, it does not repair them."""
    return jsonify(distributor.replication_report())


if __name__ == "__main__":
    # use_reloader=False: the reloader would start a second process with its
    # own empty copy of the nodes, which is confusing while we're learning.
    app.run(port=5000, debug=True, use_reloader=False)
