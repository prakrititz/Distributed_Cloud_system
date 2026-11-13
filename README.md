# Mini Distributed Object Store (Stage 1)

A basic storage system where a client uploads and retrieves files of any type
(documents, images, video, audio, archives, other binary data). A distributor
splits each file into 1 MB chunks and spreads the chunks across four nodes.

Stage 1 covers upload, download and chunk distribution only. No Docker,
replication, failure handling, or custom metadata yet.

## Architecture

```
Client -> SerDe (file bytes <-> binary object) -> Distributor -> Node1..Node4
```

The distributor is the only component that knows where everything lives. Nodes
are simple chunk stores and never talk to each other.

| File | Role |
|---|---|
| `client.py` | Menu (upload / download), talks to the Flask API over HTTP |
| `file_upload.py` | Reads and saves files on disk as raw bytes |
| `serDe.py` | Adds/strips a header with length and SHA-256 checksum |
| `app.py` | Flask endpoints (client-facing API); owns the distributor and 4 nodes |
| `distributor.py` | Splits into chunks, picks a node per chunk, tracks metadata, reassembles |
| `node.py` | In-memory chunk store with free-space tracking |

## How the distributor works

**Upload:** split the bytes into chunks of at most 1 MB, then place each chunk
on the node with the most free space (ties go to the first node). The name and
metadata are registered only after every chunk is stored. If any chunk cannot
be placed, the chunks already stored are deleted and nothing is left behind.

**Download:** look up the chunk list by name, fetch each chunk from its node,
join them in index order.

Objects are identified by file name. Uploading a name that already exists is
rejected. Object IDs are internal and used only to name chunks.

## API

| Method | Endpoint | Purpose | Errors |
|---|---|---|---|
| POST | `/api/v1/objects` | Upload a file (multipart field `file`), returns 201 | 400 no file, 409 name exists, 503 not enough space |
| GET | `/api/v1/objects/<name>` | Download the file | 404 unknown name |
| GET | `/api/v1/objects/<name>/chunks` | Debug: which chunk is on which node | 404 unknown name |
| GET | `/api/v1/nodes` | Debug: capacity, used, free and chunk count per node | |

Errors are JSON: `{"error": "..."}`.

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

Each node holds 5 MB (`NODE_CAPACITY` in `app.py`), 20 MB in total. Open
`http://localhost:5000/api/v1/nodes` and
`http://localhost:5000/api/v1/objects/<file name>/chunks` in a browser to see
how a large file was spread.

## Limitations (stage 1)

- Storage is in memory, so restarting the server loses all objects.
- Whole files are held in memory during upload and download.
- No replication, heartbeats, or recovery from a failed node.
- Content type and custom metadata are not stored yet.
