# Mini Distributed Object Store

A basic storage system where clients upload and retrieve files of any type
(documents, images, video, audio, archives, other binary data). A controller
splits each file into 1 MB chunks and stores every chunk on two different
workers (replication factor 2). It runs in Docker as 1 controller, any number
of workers (4 by default) and one or more clients.

Covers upload, download, delete, object metadata (including custom key-value
metadata), replication spread over the workers, worker registration and
heartbeats, corruption detection (checksums), manual and automatic repair,
orphan cleanup, persistent metadata, and a Docker Compose deployment.

**To run it and try everything yourself, see [GUIDE.md](GUIDE.md).**

## Architecture

```
client(s) --HTTP--> controller (app.py: API + distributor) --HTTP--> worker x N (nodes/node_server.py)
                         |                                              |
                    metadata file                                  chunk files
                (controller-data volume)                       (worker-data volume)
```

The controller is the only component that knows where everything lives. Each
worker is its own server process with its own storage; workers never talk to
each other, and the controller writes every copy itself through a `RemoteNode`.

```
app.py  client.py  config.py  demo.py      entry points, settings, the demonstrations
docker-compose.yml                         the whole system in Docker (settings at the top)
Dockerfile.controller / .worker / .client  the three images (+ a .dockerignore each)
files/                                     files the client containers can upload (/files)
controller/                                everything that runs in the controller
  distributor.py  remote_node.py  metadata_store.py
nodes/                                     everything that runs in a worker
  node.py  node_server.py  identity.py
helpers/                                   small pieces both sides (and the client) use
  checksum.py  file_upload.py  errors.py
tests/                                     the test suite (needs no Docker)
```

| File | Role |
|---|---|
| `app.py` | Flask endpoints (client API and internal worker API); owns the distributor; runs automatic repair |
| `client.py` | The client: a menu, or one command per run (`python client.py upload FILE`) |
| `config.py` | Settings: chunk size, copies per chunk, metadata file, auto-repair grace, timeouts, heartbeats |
| `demo.py` | Runs the spec's demonstrations against the running system |
| `controller/distributor.py` | Chunking, placement, metadata, reassembly, health report, repair, auto-repair, orphan detection |
| `controller/remote_node.py` | `RemoteNode`: the controller's HTTP client for one worker |
| `controller/metadata_store.py` | Saves the metadata to a JSON file atomically and loads it on start |
| `nodes/node_server.py` | One worker as its own Flask process; registers itself and sends heartbeats |
| `nodes/identity.py` | How a worker started by Docker Compose finds its own id (worker1, worker2, ...) |
| `nodes/node.py` | `ChunkStore`: the worker's file-backed chunk storage, with checksums |
| `helpers/checksum.py` | `compute` / `verify`: the one checksum function (SHA-256) used everywhere |
| `helpers/file_upload.py` | Reads and saves files on disk as raw bytes |
| `helpers/errors.py` | `NodeUnavailable` and `ChunkCorrupt`, shared so the controller and the workers never import each other |

## How it works

**Upload:** the file is read `CHUNK_SIZE` bytes at a time. Each chunk gets
`REPLICATION_FACTOR` copies on different live workers with room, and only then
is the next chunk read, so the controller holds about one chunk, never the whole
file. The checksum of the whole file is built up as the chunks go by and
compared with the client's at the end. The name and metadata are registered (and
saved) only after every copy of every chunk is stored and the checksum matches.
If anything fails (a copy cannot be stored: 503; the checksum is wrong: 400; the
client stops sending), every copy already written is deleted, so nothing
half-uploaded is ever visible and a chunk is never kept with fewer copies.

**Placement:** for each copy, prefer the worker that has shared the fewest
chunks with the workers already holding this chunk, then the one with the most
free space, then take turns. So the copies are spread over all pairs of
workers instead of fixed mirror pairs: losing any two workers loses only a few
chunks. A worker without room is never chosen.

**Registration:** the controller has no list of workers. Every worker calls
`POST /internal/workers/register` when it starts, with its id and address, and
is used from then on. A worker keeps trying until the controller answers, and
registers again if the controller has forgotten it (its heartbeat gets a 404).

**Heartbeats:** every worker sends `POST /internal/workers/heartbeat` every 2 s
with its capacity and free space. A worker silent for 6 s is treated as down.
Placement reads this table, so it sends no requests to the workers. A chosen
worker that turns out to be dead or full is skipped and the next one is tried.

**Download:** streamed too: each chunk is read from the first copy that is up
and intact (checked against the checksum recorded at upload, falling back to
the other copy), sent, and dropped before the next. The response carries
`Content-Length` and `X-Checksum` (the checksum recorded at upload). Before
anything is sent, the controller checks that every chunk has a copy on a live
worker and reads the first chunk, so the usual failures are a clear 503. If a
later chunk turns out to have no good copy (say both copies were damaged on
disk), the response has already started: the connection is closed early, the
client sees fewer bytes than `Content-Length`, and `client.py` throws the
download away.

`client.py` streams as well: it uploads the file straight from disk, and
downloads into a temporary file next to the target, checksumming it on the way;
only if the size and checksum are right is it renamed into place, so a damaged
or incomplete download never replaces anything.

**Delete during a download:** the object disappears from the metadata at once
(new requests get 404), but its copies are deleted only when the downloads
already running have finished, so they are never cut off half-way. Cleanup
leaves those copies alone meanwhile.

**Transfers at once:** at most `MAX_TRANSFERS` uploads and downloads run at the
same time (503 with `Retry-After: 1` beyond that), so the controller's memory
stays bounded: about one chunk per transfer.

**Health:** `GET /api/v1/replication` reports each chunk as `healthy`,
`under_replicated` (still downloadable) or `unavailable`, and each copy as `ok`,
`down`, `missing` or `corrupt`.

**Repair:** brings every chunk back to `REPLICATION_FACTOR` good copies: it
reads a verified good copy and writes it to live workers that hold none. A
damaged copy on a live worker is deleted or overwritten; a copy on a worker that
is down is dropped once replaced. A chunk with no good copy is `unrecoverable`
(never invented); one that cannot get all its copies is `partial`. Only one
repair runs at a time (409 otherwise).
- **Automatic:** when a worker has been down for `AUTO_REPAIR_GRACE` seconds (30
  by default), the controller repairs by itself. A worker back within that time
  causes no copying. `/api/v1/replication` shows what it last did.
- **Manual:** `POST /api/v1/repair`.

**Orphans:** copies no object points to any more, for example old copies on a
worker that was down while its chunks were repaired elsewhere, or a delete that
could not reach a worker. `GET /internal/workers/<id>/chunks` compares what a
worker holds with the metadata (orphans and missing chunks).
`POST /api/v1/cleanup` deletes the orphans. It never lowers a chunk below the
required number of good copies: if a chunk is short of good copies and an
orphan of it is intact (it matches the recorded checksum), that copy is taken
back into the chunk's replica list ("adopted") instead of being deleted. It
never touches a chunk of an upload in progress or one younger than `min_age`
seconds (60 by default; the age is the time since the copy was written on that
worker), and it works only with saved metadata, since an empty in-memory
controller would see every chunk as an orphan.

**Persistent metadata:** all metadata is saved to `METADATA_PATH` after every
change (written to a temp file, then renamed, so a crash never leaves a
half-written file) and loaded on start. Restarting the controller keeps every
object. Workers it has not heard from yet count as down until they register.

**Duplicates:** objects are identified by `object_id`; the file name is only
metadata. An upload is a duplicate if the same client (`client_id`) uploads the
same source path again (409). Different clients may use the same path, and the
same name from different folders is always fine. Uploads with no path (plain
`curl`) are never refused.

## Metadata

`object_id`, `name`, `size`, `content_type`, `created_at`, `modified_at`,
`checksum`, `source_path`, `client_id`, the chunk list (index, id, size,
checksum and the workers holding each copy) and `custom` key-value pairs. `PATCH` adds,
changes or (with `null`) removes custom keys; the stored data is never touched.

```
GET /api/v1/objects/<object_id>/metadata
{ "object_id": "3f2a...", "name": "movie.mp4", "size": 3145737, "content_type": "video/mp4",
  "created_at": "2026-11-15T10:12:03Z", "modified_at": "2026-11-15T10:12:03Z",
  "checksum": "ab12...", "chunk_count": 4, "copies_per_chunk": 2,
  "chunks": [{"index": 0, "chunk_id": "...-0", "size": 1048576, "checksum": "...",
              "replicas": ["worker1", "worker3"]}, ...],
  "custom": {"project": "demo"} }
```

## API

| Method | Endpoint | Purpose | Errors |
|---|---|---|---|
| POST | `/api/v1/objects` | Upload, two ways (below). Optional fields: `metadata` (JSON object), `checksum`, `path`, `client_id`, `size`. 201 with the metadata | 400 bad field / checksum mismatch / upload cut off, 409 same client + path, 503 not enough room on enough live workers or `MAX_TRANSFERS` busy |
| GET | `/api/v1/objects` | List objects; optional `limit`, `offset`; total in `X-Total-Count` | 400 |
| GET | `/api/v1/objects/<id>` | Download, streamed: the original bytes, with the content type, `Content-Length` and an `X-Checksum` header | 404, 503 (no live copy of a chunk, or busy) |
| GET | `/api/v1/objects/<id>/metadata` | The object's metadata | 404 |
| PATCH | `/api/v1/objects/<id>/metadata` | Add/update custom metadata (`null` removes a key) | 400, 404 |
| DELETE | `/api/v1/objects/<id>` | Delete the object and all copies, 204 | 404 |
| GET | `/api/v1/nodes` | Workers: up/down, capacity, used, free, chunks, seconds since the last heartbeat | |
| GET | `/api/v1/replication` | Health of every chunk and copy; `auto_repair` status | |
| POST | `/api/v1/repair` | Repair now; returns what was done | 409 already running |
| POST | `/api/v1/cleanup` | Delete orphaned copies; `?min_age=` seconds (default 60) | 400, 409 no saved metadata / repair running |
| POST | `/internal/workers/register` | Sent by a worker when it starts: `worker_id`, `url`, `capacity`, `used`, `free`, `chunks`. 201 new, 200 known | 400 |
| POST | `/internal/workers/heartbeat` | Sent by a worker every 2 s: `worker_id`, `capacity`, `used`, `free`, `chunks` | 400, 404 not registered |
| GET | `/internal/workers/<id>/chunks` | What the worker holds vs the metadata: `held`, `expected`, `orphans`, `in_progress`, `missing` | 404, 503 unreachable |

Errors are always JSON: `{"error": "..."}`. The workers' own endpoints
(`/internal/chunks/...`, `/internal/health`) are used only by the controller.

The two ways to upload:

```
# multipart form, the file in the 'file' field (what curl -F and HTML forms send).
# The web server first writes the whole upload to a temporary file, then it is read chunk by chunk.
curl -F "file=@movie.mp4" -F 'metadata={"project": "demo"}' http://localhost:5000/api/v1/objects

# raw: the body is the file, the fields go in the query string ('name' required),
# Content-Type is the object's type. Chunks go to the workers while the body is arriving.
# This is what client.py sends.
curl -X POST -T movie.mp4 -H "Content-Type: video/mp4" \
     "http://localhost:5000/api/v1/objects?name=movie.mp4&client_id=me"
```

## Integrity checks (checksums)

| Where | What is checked | If it fails |
|---|---|---|
| Client -> controller | The client sends the file's checksum; the controller builds up the checksum of what arrives, chunk by chunk, and compares at the end | 400, everything stored is deleted |
| Controller -> client | The download carries `X-Checksum` (recorded at upload) and `Content-Length`; the client checks both while writing a temporary file | the client deletes it and reports an error |
| Controller -> worker | Each chunk is sent with `X-Checksum`; the worker verifies it before writing | 422, the upload rolls back |
| On the worker | A checksum is saved next to each chunk (`.checksums/`) and rechecked on every read | 409, the damaged chunk is not sent |
| Controller, on read | Every chunk is checked against the checksum recorded at upload, the whole file against its checksum as the chunks go by | that copy is skipped, the other is used |

The controller's record is the authority: a worker cannot vouch for itself. If
every copy of a chunk is damaged, the download fails with a clear 503 instead of
returning bad data.

## Settings

In Docker they are at the top of `docker-compose.yml` (see [GUIDE.md](GUIDE.md)).

| Setting | Default | Read by | What it is |
|---|---|---|---|
| `WORKERS` | 4 | compose | number of worker containers |
| `CHUNK_SIZE` | 1048576 | controller | bytes per chunk |
| `REPLICATION_FACTOR` | 2 | controller | copies of every chunk |
| `MAX_TRANSFERS` | 32 | controller | uploads and downloads at once; more get 503 |
| `WORKER_CAPACITY` (`CAPACITY`) | 5242880 | worker | bytes a worker may store |
| `WORKER_NAME` | worker | worker | prefix of the worker ids |
| `API_PORT` (`PORT`) | 5000 | controller | the API's port |
| `AUTO_REPAIR_GRACE` | 30 | controller | seconds down before automatic repair; 0 = off |
| `METADATA_PATH` | ./data/controller/metadata.json (Docker: /meta/metadata.json) | controller | where metadata is saved; empty = memory only |
| `ORPHAN_MIN_AGE` | 60 | controller | default `min_age` of cleanup |
| `API_URL`, `CLIENT_ID` | http://127.0.0.1:5000/api/v1, machine name | client | where the API is, who this client is |
| `NODE_ID`, `PORT`, `DATA_DIR`, `CONTROLLER_URL` | worker1, 5001, ./data/{node_id}, http://127.0.0.1:5000 | worker | for running a worker by hand |

## Running the tests

```
pip install -r requirements-dev.txt
python -m pytest tests                    # everything, about 2 minutes; no Docker needed
python -m pytest tests -m "not cluster"   # only the fast tests that start no servers
```

| Test file | What it checks |
|---|---|
| `test_helpers.py`, `test_chunkstore.py` | checksum and file helpers; the worker's chunk storage |
| `test_distributor.py` | the distributor against in-memory fake workers: upload, rollback, path rules, fallback, heartbeats, health report, repair, how evenly copies are spread |
| `test_node_server.py` | the worker's HTTP API |
| `test_api.py` | the REST API end to end |
| `test_integrity.py` | damage on disk and in transit |
| `test_heartbeat.py`, `test_failure_and_repair.py` | heartbeats, a worker stopping and returning, repair |
| `test_auto_repair.py` | repair after the grace period, none for a quick restart, retry when it could not finish |
| `test_metadata_store.py` | saving and loading the metadata, a crash during a save, a controller restart |
| `test_cleanup.py` | orphans and missing chunks, cleanup, uploads in progress left alone |
| `test_multi_client.py` | several clients at once: same paths, races, parallel patches and deletes |
| `test_client.py` | the client: menu, commands, original bytes on download |
| `test_streaming.py` | streamed uploads and downloads: one chunk at a time, raw uploads, checksum at the end, uploads and downloads cut off half-way, delete during a download, `MAX_TRANSFERS`, a 50 MB file in a few MB of memory |
| `test_registration.py` | workers registering and re-registering, more workers joining, finding the id in Docker |
| `test_docker_files.py` | each image's allowlist starts its program, the settings work, the compose file and Dockerfiles say what the spec requires |

The slower tests start four real worker processes and the API on free local
ports and stop them afterwards.

## Memory

Uploads and downloads are streamed, so the controller's memory does not grow
with the file size. Extra memory of the controller process during one transfer
(local cluster, 4 workers, `client.py` streaming from and to disk):

| File | Upload, before | Upload, now | Download, before | Download, now |
|---|---|---|---|---|
| 20 MB | +51 MB | +16 MB | +41 MB | +0 MB |
| 100 MB | +210 MB | +20 MB | +201 MB | +3 MB |
| 500 MB | not measured (about 2x the file) | +17 MB | not measured (about 2x the file) | +11 MB |

Most of what is left on upload is the development server itself: after each
request it reads the rest of the connection into a fresh 10 MB buffer.
`tests/test_streaming.py` checks that a 50 MB file passes through the client and
the controller together in under 10 MB (about 3 MB up and 5 MB down; it was 200
MB up and 100 MB down before).

## Limitations

- A multipart upload (`curl -F`) is first written in full to a temporary file
  on the controller's disk by the web server; a raw upload (`client.py`) is not.
- If a later chunk of a download has no good copy, the response has already
  started, so the connection is closed early instead of answering 503 (the
  client notices and discards it).
- In Docker all workers share one volume (each in its own folder), because
  Compose cannot give each copy of a scaled service its own named volume.
- The metadata file is rewritten in full on every change: fine for a project,
  slow for millions of objects.
- The controller's picture of the workers is up to a heartbeat old (2 s). A
  worker that has just died can still be chosen once; the failed write is
  noticed and the next worker is used.
- There is one controller: if it is down, nothing can be uploaded or downloaded
  (the data stays safe on the workers).
