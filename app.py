"""Flask app: the client-facing API. It owns the distributor, which talks to
the node servers (nodes/node_server.py) over HTTP through RemoteNode objects.

Objects are identified by the object_id returned from an upload. The file name
is only metadata: several objects can share it.

Workers register themselves when they start (POST /internal/workers/register),
so start them in any order, and as many as you like. Each worker's capacity is
set on its own server (CAPACITY), not here. The chunk size and the number of
copies per chunk are CHUNK_SIZE and REPLICATION_FACTOR (see config.py).
"""
import functools
import json
import math
import mimetypes
import os
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, Response, g, request, jsonify
from werkzeug.exceptions import HTTPException

from config import (ADMINS, ALLOW_REGISTRATION, API_KEYS, AUTH_REQUIRED, AUTO_REPAIR_GRACE, CHUNK_SIZE,
                    HEARTBEAT_INTERVAL, MAX_TRANSFERS, MAX_TRANSFERS_PER_CLIENT, METADATA_PATH, NODE_ADDRESSES,
                    ORPHAN_MIN_AGE, QUOTA_PER_CLIENT, RATE_BURST, RATE_PER_CLIENT, REGISTRATIONS_PER_MINUTE,
                    REPLICATION_FACTOR)
from controller.access import CLIENT_ID_PATTERN, ClientRegistry, ClientSlots, KeyRing, RateLimiter
from controller.distributor import DEFAULT_CONTENT_TYPE, Distributor
from controller.metadata_store import store_from_setting
from controller.remote_node import RemoteNode
from helpers.errors import ChecksumMismatch, NodeUnavailable, QuotaExceeded

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


class TransferSlots:
    """At most `limit` uploads and downloads at once. Each streams one chunk
    at a time, so this bounds the controller's memory: about limit x chunk
    size, whatever the size of the files."""

    def __init__(self, limit: int):
        self.limit = limit
        self._free = threading.BoundedSemaphore(limit)

    def take(self) -> bool:
        """Take a slot if one is free (never waits). False if all are in use."""
        return self._free.acquire(blocking=False)

    def give_back(self) -> None:
        self._free.release()


transfers = TransferSlots(MAX_TRANSFERS)

# Who is calling and what is each client allowed (controller/access.py).
# With no API_KEYS, keyring.enabled is False: no authentication, and none of
# the per-client limits below apply (without authentication a client_id is
# only a claim, so there is nobody to be fair between).
keyring = KeyRing.from_setting(API_KEYS, ADMINS, required=AUTH_REQUIRED,
                               registry=ClientRegistry(distributor.store))   # + clients that registered themselves
allow_registration = ALLOW_REGISTRATION
registration_limiter = RateLimiter(REGISTRATIONS_PER_MINUTE / 60, REGISTRATIONS_PER_MINUTE)   # per network address
rate_limiter = RateLimiter(RATE_PER_CLIENT, RATE_BURST)    # requests per second per client
client_slots = ClientSlots(MAX_TRANSFERS_PER_CLIENT)        # transfers at once per client
quota_per_client = QUOTA_PER_CLIENT                         # bytes per client (0: no quota)


def busy():
    """503 when every transfer slot is in use."""
    response, status = error(f"The controller is busy: {transfers.limit} uploads and downloads are "
                             "already running. Try again in a moment", 503)
    response.headers["Retry-After"] = "1"
    return response, status


def take_transfer_slots():
    """Take a transfer slot, and when clients are authenticated one of this
    client's own slots too. None if the transfer may start; otherwise a 503
    to answer with (nothing is held then)."""
    if not transfers.take():
        return busy()
    if keyring.enabled and not client_slots.take(g.client):
        transfers.give_back()
        response, status = error(f"Client '{g.client}' already has {client_slots.limit} uploads and downloads "
                                 "running. Try again when one has finished", 503)
        response.headers["Retry-After"] = "1"
        return response, status
    return None


def give_back_transfer_slots(client) -> None:
    """The opposite of take_transfer_slots (client: g.client when it was taken)."""
    if keyring.enabled:
        client_slots.give_back(client)
    transfers.give_back()


# ---------------------------------------------------------------------------
# Authentication and access rules
# ---------------------------------------------------------------------------
def protected(path: str, method: str = "GET") -> bool:
    """The client API needs a key, except registering (that is how a client
    gets one); so does a worker's chunk inventory (an operator tool). The
    other /internal endpoints are for the workers."""
    if path == "/api/v1/clients" and method == "POST":
        return False
    return path.startswith("/api/") or (path.startswith("/internal/workers/") and path.endswith("/chunks"))


@app.before_request
def authenticate():
    """Find out who is calling (from 'Authorization: Bearer <key>') and apply
    the client's request rate. Without API_KEYS everyone may do everything."""
    g.client, g.admin = None, True
    if not keyring.enabled or not protected(request.path, request.method):
        return None
    client = keyring.identify(request.headers.get("Authorization"))
    if client is None:
        discard_body()
        response, status = error("Missing or wrong API key: send 'Authorization: Bearer <your key>'", 401)
        response.headers["WWW-Authenticate"] = 'Bearer realm="object-store"'
        return response, status
    g.client, g.admin = client, keyring.is_admin(client)
    allowed, wait = rate_limiter.allow(client)
    if not allowed:
        discard_body()
        response, status = error(f"Too many requests from client '{client}': at most "
                                 f"{rate_limiter.rate} per second on average", 429)
        response.headers["Retry-After"] = str(max(1, math.ceil(wait)))
        return response, status
    return None


def admin_only(view):
    """For the operator endpoints: with authentication, admins only."""
    @functools.wraps(view)
    def checked(*args, **kwargs):
        if keyring.enabled and not g.admin:
            return error("Only an admin may do this", 403)
        return view(*args, **kwargs)
    return checked


def may_read(meta) -> bool:
    """The owner, the clients it shared the object with, and admins."""
    return not keyring.enabled or g.admin or meta.client_id == g.client or g.client in meta.readers


def may_change(meta) -> bool:
    """Only the owner (and admins) may change, delete or share an object."""
    return not keyring.enabled or g.admin or meta.client_id == g.client


def find_object(object_id: str, change: bool = False):
    """(metadata, None) if the caller may use the object, else (None, an
    error response). An object the caller may not read answers 404, exactly
    like one that does not exist, so nobody learns what others store."""
    try:
        meta = distributor.get_object(object_id)
    except KeyError:
        meta = None
    if meta is None or not may_read(meta):
        return None, error(f"No object with id '{object_id}'", 404)
    if change and not may_change(meta):
        return None, error(f"Only the owner of object '{object_id}' may change, delete or share it", 403)
    return meta, None


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
            {"index": c.index, "chunk_id": c.chunk_id, "size": c.size, "checksum": c.checksum,
             "replicas": c.replicas}
            for c in sorted(meta.chunks, key=lambda c: c.index)
        ],
        "custom": meta.custom,
        # who else may read it: shown to the owner (and admins) only
        **({"readers": meta.readers} if may_change(meta) else {}),
    }


@app.route("/api/v1/objects", methods=["POST"])
def upload_object():
    """Upload an object. Either way, it is stored one chunk at a time as it is
    read, so the controller never holds the whole file in memory.

    Two ways to send it:
      multipart/form-data (curl -F, HTML forms): the file in the 'file' field,
          the fields below as form fields. The web server first writes the
          whole upload to a temporary file, which is then read chunk by chunk.
      raw (client.py): the request body is the file itself, the fields below
          go in the query string, 'name' (the file name) is required, and the
          Content-Type header is the object's content type. Chunks go to the
          workers while the body is still arriving. With curl:
          curl -X POST -T report.pdf "http://HOST/api/v1/objects?name=report.pdf"

    Fields, all optional (except 'name' for a raw upload):
      'metadata'  JSON object of custom key-value metadata
      'checksum'  checksum of the file. The checksum of what arrives is
                  computed as the chunks go by and compared at the end; on a
                  mismatch everything stored is deleted again
      'path'      where the client read the file from. Two uploads from the
                  same path by the same client are the same file, so the
                  second is refused
      'client_id' which client is uploading (at most 128 characters). It
                  scopes the 'path' rule: two clients may each upload their
                  own '/files/a.txt'. Without it, uploads share one anonymous
                  scope. With authentication the client is the one its key
                  belongs to (the object's owner); a different client_id is
                  refused (403)
      'size'      size to record, if it differs from the uploaded size
                  (defaults to the uploaded size)

    201 with the new object's metadata. 400 for a bad field, a checksum
    mismatch or an upload that stopped before its end (nothing is stored),
    409 if an object from this path exists, 413 if it would take the client
    over its storage quota (nothing is stored), 503 if there is not enough
    space on enough live nodes to store every chunk twice, or if MAX_TRANSFERS
    uploads and downloads are already running (or this client's
    MAX_TRANSFERS_PER_CLIENT).
    """
    if request.mimetype == "multipart/form-data":
        file = request.files.get("file")
        if file is None or not file.filename:
            return error("Missing 'file' field in the multipart upload", 400)
        return store_upload(request.form, file.stream, file.filename, file.mimetype)

    if "name" in request.args:
        response = store_upload(request.args, request.stream, request.args["name"], request.mimetype)
        if response[1] >= 400:
            discard_body()
        return response

    return error("Missing 'file' field in the multipart upload (or 'name' in the query string "
                 "for a raw upload)", 400)


def store_upload(fields, stream, filename: str, content_type: str):
    """The part of an upload shared by both ways of sending it: check the
    fields, then let the distributor store the stream chunk by chunk."""
    name = os.path.basename(filename.replace("\\", "/"))
    if not name:
        return error("The file name must not be empty", 400)

    # content type: what the client said, else guessed from the file name
    if not content_type or content_type == DEFAULT_CONTENT_TYPE:
        content_type = mimetypes.guess_type(name)[0] or DEFAULT_CONTENT_TYPE

    try:
        custom = {}
        if fields.get("metadata"):
            try:
                raw = json.loads(fields["metadata"])
            except ValueError:
                raise ValueError("'metadata' must be valid JSON")
            custom = {k: v for k, v in parse_custom(raw, "'metadata'").items() if v is not None}

        size = None
        if fields.get("size"):
            try:
                size = int(fields["size"])
            except ValueError:
                raise ValueError("'size' must be an integer")
            if size < 0:
                raise ValueError("'size' must not be negative")

        client_id = (fields.get("client_id") or "").strip()
        if len(client_id) > MAX_CLIENT_ID_LENGTH:
            raise ValueError(f"'client_id' must be at most {MAX_CLIENT_ID_LENGTH} characters")
    except ValueError as e:
        return error(str(e), 400)

    quota = None
    if keyring.enabled:
        # the uploader is who the key says, never who the request claims
        if client_id and client_id != g.client:
            return error(f"Your key belongs to client '{g.client}', not '{client_id}'", 403)
        client_id = g.client
        if quota_per_client and not g.admin:
            quota = quota_per_client
            known = request.content_length if request.mimetype != "multipart/form-data" else None
            if known is not None and distributor.used_bytes(client_id) + known > quota:   # refuse at once
                return error(f"Storage quota exceeded: client '{client_id}' may store {quota} bytes and "
                             f"already uses {distributor.used_bytes(client_id)}; this upload is {known}", 413)

    refused = take_transfer_slots()
    if refused:
        return refused
    slot_client = g.client
    try:
        object_id = distributor.upload(
            name, stream, content_type=content_type, custom=custom,
            source_path=fields.get("path") or None, size=size,
            client_id=client_id or None, checksum=fields.get("checksum") or None, quota=quota,
        )
    except FileExistsError as e:
        return error(str(e), 409)
    except ChecksumMismatch as e:
        return error(str(e), 400)
    except QuotaExceeded as e:
        return error(str(e), 413)
    except RuntimeError as e:
        return error(str(e), 503)
    finally:
        give_back_transfer_slots(slot_client)

    return jsonify(object_json(distributor.get_object(object_id))), 201


def discard_body() -> None:
    """Read and drop what is left of a raw upload's body. A client sends the
    whole body before it reads the answer, so without this a refused upload
    (409, 503, ...) would look like a broken connection to it instead of
    showing the error."""
    try:
        while request.stream.read(CHUNK_SIZE):
            pass
    except Exception:
        pass   # the client went away: nobody is waiting for the answer



@app.route("/api/v1/objects", methods=["GET"])
def list_objects():
    """List the stored objects (object_id, name, size, content_type and times),
    oldest first: with authentication, only the caller's own objects and the
    ones shared with it ('owner' says whose each is). Optional 'limit' and
    'offset' query parameters paginate; the total number of objects is in the
    'X-Total-Count' header."""
    try:
        offset = int(request.args.get("offset", 0))
        limit = request.args.get("limit")
        limit = None if limit is None else int(limit)
        if offset < 0 or (limit is not None and limit < 0):
            raise ValueError
    except ValueError:
        return error("'limit' and 'offset' must be non-negative integers", 400)

    objects = [m for m in distributor.list_objects() if may_read(m)]
    page = objects[offset:] if limit is None else objects[offset:offset + limit]
    response = jsonify([
        {
            "object_id": m.object_id,
            "name": m.name,
            "size": m.size,
            "content_type": m.content_type,
            "created_at": m.created_at,
            "modified_at": m.modified_at,
            "owner": m.client_id,
        }
        for m in page
    ])
    response.headers["X-Total-Count"] = str(len(objects))
    return response


@app.route("/api/v1/objects/<object_id>", methods=["GET"])
def download_object(object_id):
    """Send the object's bytes, one chunk at a time as each is read from a
    worker, so the controller never holds the whole object in memory.
    Headers: its Content-Type, Content-Length, and X-Checksum, the checksum
    recorded at upload, so the client can verify what it received.

    404 if there is no such object (or the caller may not read it). 503 if
    some chunk has no copy on a live worker or the first chunk has no good
    copy (both found before anything is sent), or if MAX_TRANSFERS uploads
    and downloads are already running (or this client's
    MAX_TRANSFERS_PER_CLIENT). If a later chunk turns out to have no good
    copy (e.g. both copies damaged on disk), the connection is closed before
    Content-Length bytes were sent: the client sees the download is
    incomplete and must not keep it.
    """
    _, refused = find_object(object_id)
    if refused:
        return refused
    refused = take_transfer_slots()
    if refused:
        return refused
    slot_client = g.client
    try:
        meta, chunks = distributor.stream_object(object_id)
    except KeyError:
        give_back_transfer_slots(slot_client)
        return error(f"No object with id '{object_id}'", 404)
    except RuntimeError as e:
        give_back_transfer_slots(slot_client)
        return error(str(e), 503)
    except BaseException:
        give_back_transfer_slots(slot_client)
        raise

    finished = threading.Lock()

    def finish():
        """End the download (a delete waiting for it can go ahead) and free its
        slots. Once only: it is called both when the response is closed and
        when the body stops, whichever comes first."""
        if finished.acquire(blocking=False):
            chunks.close()
            give_back_transfer_slots(slot_client)

    def body():
        try:
            yield from chunks
        except Exception as e:
            print(f"[download] '{meta.name}' (object_id={object_id}) stopped part-way: {e}; "
                  "closing the connection")
            raise
        finally:
            # also runs when the client went away part-way (the server then
            # drops this generator), even if the server never closes the response
            finish()

    response = Response(body(), mimetype=meta.content_type)
    sizes = [c.size for c in meta.chunks]
    if None not in sizes:                # unknown only for objects stored before sizes were kept
        response.headers["Content-Length"] = str(sum(sizes))
    response.headers["X-Checksum"] = meta.checksum
    response.call_on_close(finish)       # finished, or never started (e.g. a HEAD request)
    return response



@app.route("/api/v1/objects/<object_id>/metadata", methods=["GET"])
def get_metadata(object_id):
    """The object's metadata, without downloading its data. 404 if unknown
    (or the caller may not read it)."""
    meta, refused = find_object(object_id)
    return refused or jsonify(object_json(meta))


@app.route("/api/v1/objects/<object_id>/metadata", methods=["PATCH"])
def update_metadata(object_id):
    """Add or update custom metadata. The body is a JSON object of key-value
    pairs; a null value removes that key. The stored data is not touched.
    400 for a bad body, 404 if unknown, 403 if the caller may read but not
    change it. Returns the updated metadata."""
    _, refused = find_object(object_id, change=True)
    if refused:
        return refused
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
    """Delete the object and every copy of its chunks. 204, 404 if unknown,
    403 if the caller may read but not delete it."""
    _, refused = find_object(object_id, change=True)
    if refused:
        return refused
    try:
        distributor.delete(object_id)
    except KeyError:
        return error(f"No object with id '{object_id}'", 404)
    return "", 204


MAX_READERS = 100


@app.route("/api/v1/objects/<object_id>/readers", methods=["GET"])
def get_readers(object_id):
    """Who else may read the object: {object_id, owner, readers}. Owner (and
    admins) only: 403 for a reader, 404 for anyone else."""
    meta, refused = find_object(object_id, change=True)
    return refused or jsonify({"object_id": meta.object_id, "owner": meta.client_id, "readers": meta.readers})


@app.route("/api/v1/objects/<object_id>/readers", methods=["PUT"])
def put_readers(object_id):
    """Share the object: the body {"readers": ["client2", ...]} is the full
    list of other clients that may read it (download it and read its
    metadata); [] stops sharing. Owner (and admins) only."""
    _, refused = find_object(object_id, change=True)
    if refused:
        return refused
    body = request.get_json(silent=True)
    readers = body.get("readers") if isinstance(body, dict) else None
    if (not isinstance(readers, list) or len(readers) > MAX_READERS
            or any(not isinstance(r, str) or not r.strip() or len(r) > MAX_CLIENT_ID_LENGTH for r in readers)):
        return error(f"The body must be {{\"readers\": [client ids]}}, at most {MAX_READERS} of them", 400)
    meta = distributor.set_readers(object_id, [r.strip() for r in readers])
    return jsonify({"object_id": meta.object_id, "owner": meta.client_id, "readers": meta.readers})


@app.route("/api/v1/clients", methods=["POST"])
def register_client():
    """A new client registers itself and gets its own API key. No key is
    needed for this. Body (optional): {"client_id": "client7"}, the name it
    wants (letters, digits, . _ -, at most 64); without one, a name is made up.

    201 {"client_id", "api_key"}: the key is shown only this once; the
    controller keeps only its hash, so a lost key cannot be recovered. 400
    for a bad name, 404 when registration is off (no authentication, or
    ALLOW_REGISTRATION=0), 409 if the name is taken (a fixed client, an
    admin, a registered client, or the owner of stored objects), 429 after
    too many registrations from one network address."""
    if not keyring.enabled or not allow_registration or keyring.registry is None:
        return error("Registration is off on this controller", 404)
    allowed, wait = registration_limiter.allow(request.remote_addr or "unknown")
    if not allowed:
        response, status = error("Too many registrations from this address. Try again later", 429)
        response.headers["Retry-After"] = str(max(1, math.ceil(wait)))
        return response, status

    body = request.get_json(silent=True)
    if body is not None and not isinstance(body, dict):
        return error("The body must be a JSON object, e.g. {\"client_id\": \"client7\"}", 400)
    client_id = (body or {}).get("client_id")
    if client_id is None:
        client_id = "client-" + secrets.token_hex(4)                 # no name asked for: make one up
        while keyring.taken(client_id):
            client_id = "client-" + secrets.token_hex(4)
    if not isinstance(client_id, str) or not CLIENT_ID_PATTERN.match(client_id):
        return error("'client_id' must be 1-64 letters, digits, '.', '_' or '-', starting with a letter or digit",
                     400)
    # A name that already means someone must never be handed out again: that
    # would give the newcomer their rights (an admin) or their objects (an
    # owner from before authentication was on).
    if keyring.taken(client_id) or distributor.owns_objects(client_id):
        return error(f"The client id '{client_id}' is taken; choose another", 409)
    try:
        key = keyring.registry.register(client_id, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    except ValueError:                                                # registered a moment ago by someone else
        return error(f"The client id '{client_id}' is taken; choose another", 409)
    print(f"[controller] client {client_id} registered itself", flush=True)
    return jsonify({
        "client_id": client_id,
        "api_key": key,
        "message": "Keep this key safe: it is shown only this once. Send it as 'Authorization: Bearer <key>'.",
    }), 201


@app.route("/api/v1/me", methods=["GET"])
def who_am_i():
    """Who the caller is and what it may use: its client id, whether it is an
    admin, the bytes it stores and its quota (null: none), and its transfers
    running and allowed at once."""
    client = g.client
    return jsonify({
        "client_id": client,
        "authenticated": keyring.enabled,
        "registered": bool(client and keyring.registry is not None and keyring.registry.known(client)),
        "admin": bool(g.admin),
        "stored_bytes": distributor.used_bytes(client or ""),
        "quota_bytes": quota_per_client if keyring.enabled and quota_per_client and not g.admin else None,
        "transfers_running": client_slots.running(client) if keyring.enabled else None,
        "transfers_allowed": client_slots.limit if keyring.enabled else transfers.limit,
        "requests_per_second": rate_limiter.rate if keyring.enabled and rate_limiter.enabled else None,
    })


@app.route("/api/v1/nodes", methods=["GET"])
@admin_only
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
@admin_only
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
@admin_only
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
@admin_only
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
@admin_only
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
