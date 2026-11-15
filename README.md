# Mini Distributed Object Store (Stage 1)

A basic storage system where a client uploads and retrieves files of any type
(documents, images, video, audio, archives, other binary data). A distributor
splits each file into 1 MB chunks and stores every chunk on two different nodes
(replication factor 2) across four nodes.

Covers upload, download, delete, object metadata (including custom key-value
metadata), replication, and corruption detection (checksums). No Docker or
repair yet.

## Architecture

```
Client -> SerDe (file bytes <-> binary object) -> API (app.py) -> Distributor -> RemoteNode -(HTTP)-> node server x4
```

The distributor is the only component that knows where everything lives. Each
node is its own server process with its own storage; nodes never talk to each
other, and the distributor writes every copy itself through a `RemoteNode`.

| File | Role |
|---|---|
| `client.py` | Menu (upload / download / list / metadata / delete), talks to the Flask API over HTTP |
| `file_upload.py` | Reads and saves files on disk as raw bytes |
| `serDe.py` | Adds/strips a header with length and SHA-256 checksum |
| `checksum.py` | `compute` / `verify`: the one checksum function (SHA-256) used everywhere |
| `app.py` | Flask endpoints (client-facing API); owns the distributor |
| `distributor.py` | Splits into chunks, places replicas, tracks metadata, reassembles, reports health |
| `remote_node.py` | `RemoteNode`: the distributor's HTTP client for one node server |
| `node_server.py` | One storage node as its own Flask process |
| `node.py` | `ChunkStore`: the node's file-backed chunk storage, with checksums |
| `config.py` | Node addresses and request timeouts |

## How the distributor works

**Upload:** split the bytes into chunks of at most 1 MB. For each chunk, pick two
different live nodes with room (most free space first) and store a copy on each.
The name and metadata are registered only after every copy of every chunk is
stored. If any copy cannot be stored, every copy already written is deleted and
the upload fails with 503, so a chunk is never kept with fewer than two copies.

**Download:** look up each chunk's replicas by object ID and read from the first
one that answers; if its node is down or lost the chunk, fall back to the other
copy. It fails with 503 only if every copy of some chunk is unavailable.

**Health:** `GET /api/v1/replication` reports each chunk as `healthy` (2 usable
copies), `under_replicated` (1 usable copy, still downloadable) or `unavailable`
(0 usable copies). It only reports; it does not repair.

Objects are identified by the `object_id` returned from an upload. The file name
is only metadata, so several objects can share it. What makes an upload a
duplicate is its source path (the full path the client read the file from): a
second upload from the same path is refused with 409 until the first is deleted.
Uploads that send no path (for example plain `curl`) are never refused.

## Metadata

Each object has: `object_id`, `name`, `size`, `content_type`, `created_at`,
`modified_at`, `checksum`, `source_path`, the chunk list (each chunk's index,
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
| POST | `/api/v1/objects` | Upload (multipart): `file` is required; optional `metadata` (JSON object), `checksum`, `path`, `size`. Returns 201 with the object's metadata | 400 no file / bad field / checksum mismatch, 409 same path already uploaded, 503 not enough space on 2 live nodes |
| GET | `/api/v1/objects` | List objects (`object_id`, `name`, `size`, `content_type`, times); optional `limit` and `offset`, total in the `X-Total-Count` header | 400 bad limit/offset |
| GET | `/api/v1/objects/<object_id>` | Download the object with its content type | 404 unknown id, 503 all copies of a chunk down or corrupt |
| GET | `/api/v1/objects/<object_id>/metadata` | The object's metadata | 404 unknown id |
| PATCH | `/api/v1/objects/<object_id>/metadata` | Add/update custom metadata (JSON object; `null` removes a key) | 400 bad body, 404 unknown id |
| DELETE | `/api/v1/objects/<object_id>` | Delete the object and all copies of its chunks, returns 204 | 404 unknown id |
| GET | `/api/v1/nodes` | Debug: up/down, capacity, used, free and chunk count per node | |
| GET | `/api/v1/replication` | Debug: health of every chunk | |

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

$env:NODE_ID="Node1"; $env:PORT="5001"; python node_server.py    # Node2/5002, Node3/5003, Node4/5004

python app.py       # the API on port 5000
python client.py    # menu: upload, download, list, show/update metadata, delete
```

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

## Limitations

- Metadata is kept in the API process's memory, so restarting it forgets every
  object (the chunk files stay on the nodes).
- Whole files are held in memory during upload and download.
- No repair: after a failure, chunks stay under-replicated until the node returns.
- Corruption is detected and avoided but not repaired, and the health report
  (`/api/v1/replication`) does not yet flag corrupt copies.
- Nodes are listed in `config.py`; there is no registration or heartbeat yet.
- The client wraps each file in a small header (see "Serialized format"), so the
  bytes stored on the nodes are that wrapped form. `size` still reports the
  original size, but a download with a browser or `curl` returns the wrapped
  bytes; uploads made without the client (plain `curl`) are stored and returned
  exactly as sent.
- Docker is not set up yet.
