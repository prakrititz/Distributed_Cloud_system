# Mini Distributed Object Store (Stage 1)

A basic storage system where a client uploads and retrieves documents, and a
distributor splits them into chunks and spreads the chunks across four nodes.

Stage 1 only covers documents and distribution. No Docker, replication,
failure handling or custom metadata yet.

## Architecture

```
Client -> SerDe (document <-> binary) -> Distributor -> Node1..Node4
```

| File | Role | Status |
|---|---|---|
| `client.py` | Menu (upload / download), talks to the Flask API | done |
| `file_upload.py` | Read and save documents on disk | done |
| `serDe.py` | Document <-> binary, with header and checksum | done |
| `app.py` | Flask endpoints (client-facing API) | skeleton |
| `distributor.py` | Splits data into 1 MB chunks, picks a node per chunk, tracks `ObjectMetadata` | skeleton |
| `node.py` | Holds chunks, reports free space | skeleton |

## API

| Method | Endpoint | Purpose |
|---|---|---|
| POST | `/api/v1/objects` | Upload a file (multipart field `file`) |
| GET | `/api/v1/objects/<object_id>` | Download the object |
| GET | `/api/v1/objects/<object_id>/chunks` | Debug: which chunk is on which node |
| GET | `/api/v1/nodes` | Debug: free/used space per node |

## Serialized format

```
[4 bytes  "DOC1"] [8 bytes length] [32 bytes SHA-256] [payload, UTF-8]
```

The distributor stores and returns these bytes unchanged. Only the client
deserializes them.

## Setup and run

```
pip install flask requests
python app.py       # start the distributor API on port 5000
python client.py    # in another terminal: 1 Upload, 2 Download, 3 Exit
```

The server and client only work end to end once `distributor.py`, `node.py`
and the `app.py` endpoints are implemented.
