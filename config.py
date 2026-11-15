"""Settings shared by the distributor side: where the nodes are.

This dict is the node registry for now. Later, nodes could register
themselves (POST /internal/workers/register) instead.
"""

# node_id -> base URL of that node's server (see node_server.py)
# TODO: adjust if you change ports, or read this from environment variables
# Use 127.0.0.1, not "localhost": on Windows "localhost" tries IPv6 first and
# waits about 2 seconds before falling back, which slowed every request.
NODE_ADDRESSES = {
    "Node1": "http://127.0.0.1:5001",
    "Node2": "http://127.0.0.1:5002",
    "Node3": "http://127.0.0.1:5003",
    "Node4": "http://127.0.0.1:5004",
}

# Seconds to wait for a node before treating it as down.
# Two separate limits: how long to wait for a connection (short, so a dead
# node is given up on quickly) and how long to wait for an answer once
# connected (longer, so a slow disk write on a live node isn't mistaken for a
# failure).
CONNECT_TIMEOUT = 0.5
READ_TIMEOUT = 5
REQUEST_TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)
