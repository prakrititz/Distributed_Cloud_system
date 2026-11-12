"""Flask app: the client-facing API. It owns the distributor and the 4 nodes.

The client identifies an object by its name only. Object IDs stay internal.
"""
import io

from flask import Flask, request, jsonify, send_file

from node import Node
from distributor import Distributor

app = Flask(__name__)

# 4 nodes with small capacity so a big file visibly spills over several nodes.
NODE_CAPACITY = 5 * 1024 * 1024  # 5 MB each, 20 MB in total
nodes = [Node(f"Node{i}", NODE_CAPACITY) for i in range(1, 5)]
distributor = Distributor(nodes)


def error(message: str, status: int):
    """Every error response is JSON with a meaningful message."""
    return jsonify({"error": message}), status


@app.route("/api/v1/objects", methods=["POST"])
def upload_object():
    """Receive a file (multipart field 'file'), pass it to distributor.upload.
    Return JSON with name, object_id, size, number of chunks. Status 201.
    400 if no file / empty name, 409 if the name exists, 503 if no space.
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
    }), 201


@app.route("/api/v1/objects/<name>", methods=["GET"])
def download_object(name):
    """Ask distributor.download for the bytes and send them back.
    404 if no object has that name.
    """
    if name not in distributor.names:
        return error(f"No object named '{name}'", 404)

    try:
        data = distributor.download(name)
    except KeyError as e:
        # The object exists but a node lost one of its chunks.
        return error(f"Stored data is unavailable: {e}", 503)

    return send_file(io.BytesIO(data), mimetype="application/octet-stream")


@app.route("/api/v1/objects/<name>/chunks", methods=["GET"])
def show_distribution(name):
    """Debug endpoint (our own, not in the spec): return which chunk
    went to which node, so we can SEE how the distributor spread the file.
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
            {"index": index, "chunk_id": chunk_id, "node": node_id}
            for chunk_id, index, node_id in meta.chunks
        ],
    })


@app.route("/api/v1/nodes", methods=["GET"])
def show_nodes():
    """Debug endpoint: free/used space of each node."""
    return jsonify([
        {
            "node": n.node_id,
            "capacity": n.capacity,
            "used": n.used_space(),
            "free": n.free_space(),
            "chunks": len(n.chunks),
        }
        for n in nodes
    ])


if __name__ == "__main__":
    # use_reloader=False: the reloader would start a second process with its
    # own empty copy of the nodes, which is confusing while we're learning.
    app.run(port=5000, debug=True, use_reloader=False)
