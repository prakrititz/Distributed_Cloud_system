"""Settings shared by the distributor side: where the nodes are.

This dict is the node registry for now. Later, nodes could register
themselves (POST /internal/workers/register) instead.
"""

# node_id -> base URL of that node's server (see node_server.py)
# TODO: adjust if you change ports, or read this from environment variables
NODE_ADDRESSES = {
    "Node1": "http://localhost:5001",
    "Node2": "http://localhost:5002",
    "Node3": "http://localhost:5003",
    "Node4": "http://localhost:5004",
}

# Seconds to wait for a node before treating it as down.
# TODO: pick a value. Too long = a dead node stalls every upload.
REQUEST_TIMEOUT = 2
