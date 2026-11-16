# Mini Distributed Object Store

A basic storage system where a client uploads and retrieves files of any type
(documents, images, video, audio, archives, other binary data). A distributor
splits each file into 1 MB chunks and stores every chunk on two different nodes
(replication factor 2) across four nodes.

Covers upload, download, delete, object metadata (including custom key-value
metadata), replication, corruption detection (checksums) and repair
(re-replication). No Docker yet.

## Architecture

```
Client -> SerDe (file bytes <-> binary object) -> API (app.py) -> Distributor -> RemoteNode -(HTTP)-> node server x4
```

The distributor is the only component that knows where everything lives. Each
node is its own server process with its own storage; nodes never talk to each
other, and the distributor writes every copy itself through a `RemoteNode`.

The code is split into three folders plus the entry points at the top level:

```
app.py  client.py  config.py       entry points and shared settings
controller/                        everything that runs in the controller
  distributor.py
  remote_node.py
nodes/                             everything that runs in a storage node
  node.py
  node_server.py
helpers/                           small pieces both sides (and the client) use
  checksum.py
  serDe.py
  file_upload.py
  errors.py
```

| File | Role |
|---|---|
| `app.py` | Flask endpoints (client-facing API); owns the distributor |
| `client.py` | Menu (upload / download / list / metadata / delete), talks to the Flask API over HTTP |
| `config.py` | Node addresses, request timeouts and heartbeat settings |
| `controller/distributor.py` | Splits into chunks, places replicas, tracks metadata, reassembles, reports health, repairs |
| `controller/remote_node.py` | `RemoteNode`: the distributor's HTTP client for one node server |
| `nodes/node_server.py` | One storage node as its own Flask process; sends the heartbeats |
| `nodes/node.py` | `ChunkStore`: the node's file-backed chunk storage, with checksums |
| `helpers/checksum.py` | `compute` / `verify`: the one checksum function (SHA-256) used everywhere |
| `helpers/serDe.py` | Adds/strips a header with length and SHA-256 checksum |
| `helpers/file_upload.py` | Reads and saves files on disk as raw bytes |
| `helpers/errors.py` | `NodeUnavailable` and `ChunkCorrupt`, shared so the controller and the nodes never import each other |

Run everything from the top folder (the one with `app.py`).

## How the distributor works

**Upload:** split the bytes into chunks of at most 1 MB. For each chunk, pick two
different live nodes with room (most free space first) and store a copy on each.
The name and metadata are registered only after every copy of every chunk is
stored. If any copy cannot be stored, every copy already written is deleted and
the upload fails with 503, so a chunk is never kept with fewer than two copies.

**Heartbeats:** every node sends `POST /internal/workers/heartbeat` to the
controller every 2 seconds with its capacity and free space. The controller keeps
the latest report per node and treats a node that has not reported for 6 seconds
as down. Placing a chunk reads that table, so it sends no requests to the nodes,
and an upload is as fast with a dead node as without. Between heartbeats the
controller lowers a node's cached free space itself each time it stores a chunk.
If a chosen node turns out to be dead or full, it is skipped (a dead one is
marked down at once) and the next best node is tried. Downloads try nodes known
to be down last. A node the controller has never heard from is asked once.

**Download:** look up each chunk's replicas by object ID and read from the first
one that answers; if its node is down or lost the chunk, fall back to the other
copy. It fails with 503 only if every copy of some chunk is unavailable.

**Health:** `GET /api/v1/replication` reports each chunk as `healthy` (2 good
copies), `under_replicated` (1 good copy, still downloadable) or `unavailable`
(0 good copies). Each copy is `ok`, `down` (node unreachable), `missing` or
`corrupt` (a node recomputes the checksum of what is on its disk and the
distributor compares it with the checksum it recorded). It only reports.

**Repair:** `POST /api/v1/repair` brings every chunk back to 2 good copies. For
each chunk with fewer, it reads a verified good copy and writes it to live nodes
that hold no copy, then updates the metadata. A damaged copy on a live node is
deleted (or overwritten); a copy on a node that is down is dropped only once it
has been replaced, and is listed as `left_behind` since it cannot be deleted
now. A chunk with no good copy is reported `unrecoverable` and is never
invented; one that cannot get all its copies (too few live nodes with room) is
`partial`. Repair is triggered by this call, not automatic, and only one runs
at a time (409 otherwise).

Objects are identified by the `object_id` returned from an upload. The file name
is only metadata, so several objects can share it. What makes an upload a
duplicate is its source path (the full path the client read the file from) **for
the same client**: a second upload of the same path by the same `client_id` is
refused with 409 until the first is deleted. Two different clients may each
upload their own `/files/report.txt` (in Docker, two containers can easily have
the same path), and the same file name from different folders is always fine.
Uploads that send no path (for example plain `curl`) are never refused, and
uploads without a `client_id` share one anonymous scope.

The client sends its id from the `CLIENT_ID` environment variable (default: the
machine's name). Keep it stable between runs: in Docker, set it explicitly,
because a container's own hostname changes every time it is created.

## Metadata

Each object has: `object_id`, `name`, `size`, `content_type`, `created_at`,
`modified_at`, `checksum`, `source_path`, `client_id`, the chunk list (each chunk's index,
ID, checksum and the nodes holding its copies), and `custom`, user-defined
key-value pairs. `PATCH` adds, changes or (with a `null` value) removes custom
keys and updates `modified_at`; the stored data is never touched.

```
GET /api/v1/objects/<object_id>/metadata
{ "object_id": "3f2a...", "name": "movie.mp4", "size": 3145737, "content_type": "video/mp4",
  "created_at": "2026-11-15T10:12:03Z", "modified_at": "2026-11-15T10:12:03Z",
  "checksum": "ab12...", "chunk_count": 4,
  "chunks": [{"index": 0, "chunk_id": "...-0", "checksum": "...", "replicas": ["Node1", "Node2"]}, ...],
  "custom": {"project": "demo"} }
```

## API

| Method | Endpoint | Purpose | Errors |
|---|---|---|---|
| POST | `/api/v1/objects` | Upload (multipart): `file` is required; optional `metadata` (JSON object), `checksum`, `path`, `client_id`, `size`. Returns 201 with the object's metadata | 400 no file / bad field / checksum mismatch, 409 same path already uploaded by the same client, 503 not enough space on 2 live nodes |
| GET | `/api/v1/objects` | List objects (`object_id`, `name`, `size`, `content_type`, times); optional `limit` and `offset`, total in the `X-Total-Count` header | 400 bad limit/offset |
| GET | `/api/v1/objects/<object_id>` | Download the object with its content type | 404 unknown id, 503 all copies of a chunk down or corrupt |
| GET | `/api/v1/objects/<object_id>/metadata` | The object's metadata | 404 unknown id |
| PATCH | `/api/v1/objects/<object_id>/metadata` | Add/update custom metadata (JSON object; `null` removes a key) | 400 bad body, 404 unknown id |
| DELETE | `/api/v1/objects/<object_id>` | Delete the object and all copies of its chunks, returns 204 | 404 unknown id |
| POST | `/internal/workers/heartbeat` | Sent by each node every 2 s: `worker_id`, `capacity`, `used`, `free`, `chunks` | 400 bad body, 404 unknown worker |
| GET | `/api/v1/nodes` | Debug: up/down, capacity, used, free, chunk count and seconds since the last heartbeat per node | |
| GET | `/api/v1/replication` | Debug: health of every chunk and the state of each copy | |
| POST | `/api/v1/repair` | Re-replicate under-replicated and corrupt chunks; returns what was done | 409 a repair is already running |

Errors are always JSON: `{"error": "..."}`, including unknown URLs (404), wrong
methods (405) and unexpected failures (500). `checksum` is optional: when given,
the server verifies it before storing anything. The debug and simulation
endpoints are our own additions, not part of the project spec.

The command-line client (`python client.py`) has a menu for all of this: upload
(with optional `key=value` metadata), download by object ID (saved under the
original name unless you give another), list, show metadata, update metadata and
delete.

## Serialized format

```
[4 bytes "OBJ1"] [8 bytes length] [32 bytes SHA-256] [payload: the file's bytes]
```

The content is never decoded, so any file type round-trips unchanged. The
distributor stores and returns these bytes as they are. Only the client
deserializes them, and it rejects truncated or corrupted data.

## Integrity checks (checksums)

Every check uses one function, `checksum.compute(data)` (SHA-256, 64 hex
characters), and `checksum.verify(data, expected)`.

| Where | What is checked | If it fails |
|---|---|---|
| Client -> server | The client sends the checksum of the upload (`checksum` form field); the server verifies it before storing anything. | 400, nothing stored |
| Server -> client | The server sends the checksum of the download in the `X-Checksum` header; the client verifies it before decoding. | The client raises an error |
| Distributor -> node | Each chunk is sent with an `X-Checksum` header; the node verifies it before writing. | 422, nothing stored, the upload rolls back |
| On the node | The node saves a checksum next to each chunk (in `.checksums/`) and rechecks it on every read. | 409, the damaged chunk is not sent |
| Distributor, on read | Every chunk read from a node is checked against the checksum the distributor recorded at upload, and the reassembled file against the whole-file checksum. | The copy is skipped and the other replica is used |

The distributor's own record is the authority: a node cannot vouch for itself.
If both copies of a chunk are damaged, the download fails with a clear 503
(`No good copy of chunk 0 (Node1: corrupt, Node2: corrupt)`) instead of
returning bad data. A node can also recompute a stored chunk's checksum without
sending it (`GET /internal/chunks/<id>/checksum`), for scans.

To try it, upload a file larger than 1 MB, change a byte in one of the chunk
files under `data/<node>/`, and download: the file still comes back correct.

## Setup and run

Start the four node servers first (one terminal each, PowerShell shown), then the
API, then the client:

```
pip install flask requests

$env:NODE_ID="Node1"; $env:PORT="5001"; python -m nodes.node_server    # Node2/5002, Node3/5003, Node4/5004

python app.py       # the API on port 5000
python client.py    # menu: upload, download, list, show/update metadata, delete
```

The client reads two optional settings from the environment: `API_URL` (default
`http://127.0.0.1:5000/api/v1`) and `CLIENT_ID` (see above).

Each node reports to the controller at `CONTROLLER_URL` (default
`http://127.0.0.1:5000`; set it on the node server if the API runs elsewhere).
Each node holds 5 MB by default (`CAPACITY` on the node server) and keeps its
chunks under `./data/<NODE_ID>`. Every chunk is stored twice, so four nodes hold
about 10 MB of files. Node addresses are in `config.py`.

To see replication and failure handling, upload a file larger than 1 MB, then:

```
curl http://127.0.0.1:5000/api/v1/objects/<object_id>/metadata   # chunks[].replicas: two nodes per chunk
# stop one node server (Ctrl+C in its terminal)
curl http://127.0.0.1:5000/api/v1/nodes                          # that node is listed as not alive
curl http://127.0.0.1:5000/api/v1/replication                    # chunks are under_replicated
python client.py                                                 # download still works
# start the node server again, then check /replication
```

## Docker files

`Dockerfile.controller` and `Dockerfile.worker` build the two images (the worker
image is started four times, each with its own `NODE_ID` and its own volume for
`/data`); `Dockerfile.*.dockerignore` lists the only files each image contains.
Build from this folder, for example
`docker build -f Dockerfile.worker -t object-store-worker .`. In Docker the
controller finds the workers through the `WORKERS` environment variable
(`worker1=http://worker1:5000,worker2=http://worker2:5000,...`). A compose file
that starts the whole system is not written yet.

## Running the tests

```
pip install -r requirements-dev.txt
python -m pytest tests            # everything, about 90 seconds
python -m pytest tests -m "not cluster"   # only the fast tests that start no servers
```

| Test file | What it checks |
|---|---|
| `test_helpers.py`, `test_chunkstore.py` | checksum, serDe, file helpers; the node's chunk storage (capacity, damage, unsafe ids) |
| `test_distributor.py` | the distributor against in-memory fake nodes: upload, rollback, path rules, fallback, heartbeats, health report, repair |
| `test_node_server.py` | the node's HTTP API called directly |
| `test_api.py` | the client REST API end to end |
| `test_integrity.py` | damage on disk and in transit |
| `test_heartbeat.py`, `test_failure_and_repair.py` | heartbeats, a worker stopping and returning, repair |
| `test_multi_client.py` | several clients at once: the same path from different clients, concurrent uploads, races, parallel patches and deletes, ending with no orphaned chunks |
| `test_client.py` | the command-line client and its menu |
| `test_docker_files.py` | each image's allowlist is enough to start it, the settings work, the Dockerfiles say what the spec requires |

The slower tests start four real node servers and the API on free local ports
and stop them afterwards.

## Limitations

- Metadata is kept in the API process's memory, so restarting it forgets every
  object (the chunk files stay on the nodes).
- Whole files are held in memory during upload and download.
- Repair runs only when `POST /api/v1/repair` is called; nothing triggers it
  automatically yet.
- Copies left on a node that was down during repair stay on its disk as orphans
  (they are listed, not cleaned up).
- Nodes are listed in `config.py`; there is no registration yet (heartbeats exist,
  but only for the nodes in that list).
- The controller's picture of the nodes is up to a heartbeat old (2 s). A node
  that has just died can still be chosen once: the failed write is noticed, the
  node is marked down at once and the next node is used.
- The client wraps each file in a small header (see "Serialized format"), so the
  bytes stored on the nodes are that wrapped form. `size` still reports the
  original size, but a download with a browser or `curl` returns the wrapped
  bytes; uploads made without the client (plain `curl`) are stored and returned
  exactly as sent.
- Docker is not set up yet.
