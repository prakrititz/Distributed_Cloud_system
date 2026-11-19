"""Settings of the controller (and the defaults the workers share).

Where do the workers come from? They REGISTER themselves: every worker calls
POST /internal/workers/register when it starts, with its id and address, so
the controller needs no list and any number of workers can join. (In Docker
the number is set in docker-compose.yml / .env.)

WORKERS is an optional fixed list on top of that, for workers that are known
in advance (the tests use it):
    WORKERS=worker1=http://127.0.0.1:5001,worker2=http://127.0.0.1:5002
Each id must be the NODE_ID that worker uses.

Every setting can be given as an environment variable.
"""

import os


def _int_setting(name: str, default: int, minimum: int = 1) -> int:
    """A whole-number setting from the environment, at least `minimum`."""
    text = os.environ.get(name, "").strip()
    if not text:
        return default
    try:
        value = int(text)
    except ValueError:
        raise ValueError(f"{name} must be a whole number, got '{text}'") from None
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}, got {value}")
    return value


def parse_workers(text: str) -> dict:
    """'worker1=http://worker1:5000,worker2=http://worker2:5000' -> {id: url}.
    Raises ValueError for anything that is not id=url pairs."""
    result = {}
    for piece in text.split(","):
        piece = piece.strip()
        if not piece:
            continue
        node_id, sep, url = piece.partition("=")
        node_id, url = node_id.strip(), url.strip()
        if not sep or not node_id or not url.startswith(("http://", "https://")):
            raise ValueError(f"WORKERS: '{piece}' is not id=http://address")
        result[node_id] = url
    if not result:
        raise ValueError("WORKERS is set but lists no workers")
    return result


# worker id -> base URL: workers known before any of them registers (optional).
NODE_ADDRESSES = parse_workers(os.environ["WORKERS"]) if os.environ.get("WORKERS", "").strip() else {}

# How objects are stored.
CHUNK_SIZE = _int_setting("CHUNK_SIZE", 1024 * 1024)        # bytes per chunk (1 MB, as the spec says)
REPLICATION_FACTOR = _int_setting("REPLICATION_FACTOR", 2)  # copies of every chunk, each on a different worker

# Uploads and downloads are streamed, one chunk at a time, so each needs only
# about one chunk of the controller's memory. At most this many run at once
# (more are refused with 503, "try again"), so the controller's memory use
# stays bounded however many clients there are.
MAX_TRANSFERS = _int_setting("MAX_TRANSFERS", 32)
# Within one upload or download, this many chunks move at once: an upload
# writes the copies of several chunks at the same time, a download fetches the
# next chunks while sending the current one. Each holds one chunk in memory,
# so a transfer needs about PARALLEL_CHUNKS chunks of memory. 1 = one chunk
# at a time (a chunk's copies are still written at the same time).
PARALLEL_CHUNKS = _int_setting("PARALLEL_CHUNKS", 4)

# Connections. The controller keeps a pool of open connections to each worker
# and reuses them (HTTP keep-alive) instead of opening a new connection for
# every request; WORKER_CONNECTIONS is how many it keeps open per worker (a
# burst beyond that opens extra connections that are closed after use).
# Each worker answers with WORKER_THREADS threads (waitress), so that many
# requests to one worker are handled at the same time.
WORKER_CONNECTIONS = _int_setting("WORKER_CONNECTIONS", 32)
WORKER_THREADS = _int_setting("WORKER_THREADS", 32)

# Authentication. API_KEYS gives every client its secret key:
#     API_KEYS=client1=KEY1,client2=KEY2,admin=KEY3
# A client sends "Authorization: Bearer <its key>"; who it is comes from the
# key. Each client sees and changes only its own objects (and reads the ones
# shared with it); the clients named in ADMINS may do everything, including
# repair and cleanup. Empty API_KEYS: no authentication (everyone may do
# everything, as before).
API_KEYS = os.environ.get("API_KEYS", "").strip()
ADMINS = os.environ.get("ADMINS", "admin").strip()
# AUTH=1 turns authentication on even with no API_KEYS (then every client
# registers itself). It is on anyway when API_KEYS is set.
AUTH_REQUIRED = os.environ.get("AUTH", "").strip().lower() in ("1", "on", "true", "yes")
# Self-registration (with authentication on): a new client calls
# POST /api/v1/clients and the controller gives it a key of its own, once.
# At most REGISTRATIONS_PER_MINUTE registrations per network address.
ALLOW_REGISTRATION = os.environ.get("ALLOW_REGISTRATION", "1").strip().lower() not in ("0", "off", "false", "no")
REGISTRATIONS_PER_MINUTE = _int_setting("REGISTRATIONS_PER_MINUTE", 10)

# A fair share of capacity for every client (per authenticated client, or per
# client_id when there is no authentication):
#   MAX_TRANSFERS_PER_CLIENT  uploads and downloads of one client at once (of
#                             the MAX_TRANSFERS in total); more get 503
#   RATE_PER_CLIENT           requests per second of one client, with bursts of
#   RATE_BURST                up to this many; more get 429. 0 = no limit
#   QUOTA_PER_CLIENT          bytes one client may store; 0 = no limit
#                             (admins have no quota)
MAX_TRANSFERS_PER_CLIENT = _int_setting("MAX_TRANSFERS_PER_CLIENT", 8)
RATE_PER_CLIENT = _int_setting("RATE_PER_CLIENT", 20, minimum=0)
RATE_BURST = _int_setting("RATE_BURST", 40)
QUOTA_PER_CLIENT = _int_setting("QUOTA_PER_CLIENT", 0, minimum=0)

# Where the controller saves its metadata, so a restart keeps every object:
# a SQLite database file (a path ending in .json uses the older JSON file
# instead). Empty ("") keeps it only in memory.
METADATA_PATH = os.environ.get("METADATA_PATH", "./data/controller/metadata.db").strip()

# Automatic repair: a worker that has been down this many seconds triggers a
# repair (a quick restart within that time causes no copying). 0 turns it off.
AUTO_REPAIR_GRACE = _int_setting("AUTO_REPAIR_GRACE", 30, minimum=0)
# Orphan cleanup leaves alone chunks younger than this (seconds), so it can
# never remove a chunk of an upload that is still running.
ORPHAN_MIN_AGE = _int_setting("ORPHAN_MIN_AGE", 60, minimum=0)

# Heartbeats: every worker tells the controller "I'm alive, this much space is
# free" every HEARTBEAT_INTERVAL seconds. The controller keeps the latest
# report per worker and treats one that has not reported for HEARTBEAT_TIMEOUT
# seconds as down (3 missed beats), so placing a chunk needs no requests.
HEARTBEAT_INTERVAL = 2
HEARTBEAT_TIMEOUT = 6
# Where the workers register and send their heartbeats (the Flask API in app.py).
CONTROLLER_URL = "http://127.0.0.1:5000"

# Seconds to wait for a worker before treating it as down.
# Two separate limits: how long to wait for a connection (short, so a dead
# worker is given up on quickly) and how long to wait for an answer once
# connected (longer, so a slow disk write on a live worker isn't mistaken for a
# failure).
CONNECT_TIMEOUT = 0.5
READ_TIMEOUT = 5
REQUEST_TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)
