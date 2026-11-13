# Mini Distributed Object Store (Stage 1)

A basic storage system where a client uploads and retrieves files of any type
(documents, images, video, audio, archives, other binary data). A distributor
splits each file into 1 MB chunks and stores every chunk on two different nodes
(replication factor 2) across four nodes.

Covers upload, download, replication, and failure simulation. No Docker,
repair, per-chunk integrity checks, or custom metadata yet.

## Architecture

```
Client -> SerDe (file bytes <-> binary object) -> Distributor -> Node1..Node4
```

The distributor is the only component that knows where everything lives. Nodes
are simple chunk stores and never talk to each other; the distributor writes
every copy itself.

| File | Role |
|---|---|
| `client.py` | Menu (upload / download), talks to the Flask API over HTTP |
| `file_upload.py` | Reads and saves files on disk as raw bytes |
| `serDe.py` | Adds/strips a header with length and SHA-256 checksum |
| `app.py` | Flask endpoints (client-facing API); owns the distributor and 4 nodes |
| `distributor.py` | Splits into chunks, places replicas, tracks metadata, reassembles, reports health |
| `node.py` | In-memory chunk store with free-space tracking and up/down simulation |

## How the distributor works

**Upload:** split the bytes into chunks of at most 1 MB. For each chunk, pick two
different live nodes with room (most free space first) and store a copy on each.
The name and metadata are registered only after every copy of every chunk is
stored. If any copy cannot be stored, every copy already written is deleted and
the upload fails with 503, so a chunk is never kept with fewer than two copies.

**Download:** look up each chunk's replicas by name and read from the first one
that answers; if its node is down or lost the chunk, fall back to the other copy.
It fails with 503 only if every copy of some chunk is unavailable.

**Health:** `GET /api/v1/replication` reports each chunk as `healthy` (2 usable
copies), `under_replicated` (1 usable copy, still downloadable) or `unavailable`
(0 usable copies). It only reports; it does not repair.

Objects are identified by file name. Uploading a name that already exists is
rejected. Object IDs are internal and used only to name chunks.

## API

| Method | Endpoint | Purpose | Errors |
|---|---|---|---|
| POST | `/api/v1/objects` | Upload a file (multipart field `file`), returns 201 | 400 no file, 409 name exists, 503 not enough space on 2 live nodes |
| GET | `/api/v1/objects/<name>` | Download the file | 404 unknown name, 503 all copies of a chunk down |
| GET | `/api/v1/objects/<name>/chunks` | Debug: which nodes hold each chunk's copies | 404 unknown name |
| GET | `/api/v1/nodes` | Debug: up/down, capacity, used, free and chunk count per node | |
| POST | `/api/v1/nodes/<id>/stop` | Simulate a node failure (chunks are kept) | 404 unknown node |
| POST | `/api/v1/nodes/<id>/start` | Bring a stopped node back | 404 unknown node |
| GET | `/api/v1/replication` | Debug: health of every chunk | |

Errors are JSON: `{"error": "..."}`. The debug and simulation endpoints are our
own additions, not part of the project spec.

## Serialized format

```
[4 bytes "OBJ1"] [8 bytes length] [32 bytes SHA-256] [payload: the file's bytes]
```

The content is never decoded, so any file type round-trips unchanged. The
distributor stores and returns these bytes as they are. Only the client
deserializes them, and it rejects truncated or corrupted data.

## Setup and run

```
pip install flask requests
python app.py       # terminal 1: start the API on port 5000
python client.py    # terminal 2: 1 Upload, 2 Download, 3 Exit
```

Each node holds 5 MB (`NODE_CAPACITY` in `app.py`), 20 MB raw. Because every
chunk is stored twice, that holds about 10 MB of files.

To see replication and failure handling, upload a file larger than 1 MB, then:

```
curl http://localhost:5000/api/v1/objects/<file name>/chunks   # two nodes per chunk
curl -X POST http://localhost:5000/api/v1/nodes/Node1/stop     # simulate a failure
curl http://localhost:5000/api/v1/replication                  # see under_replicated chunks
python client.py                                               # download still works
curl -X POST http://localhost:5000/api/v1/nodes/Node1/start    # bring it back
```

## Limitations

- Storage is in memory, so restarting the server loses all objects.
- Whole files are held in memory during upload and download.
- No repair: after a failure, chunks stay under-replicated until the node returns.
- No per-chunk integrity checks, so a corrupted copy is not detected by the
  distributor.
- Content type and custom metadata are not stored yet.
- Nodes are in-process objects; heartbeats and Docker are not implemented.
