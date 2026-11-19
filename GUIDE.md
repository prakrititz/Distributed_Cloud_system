# Guide: run it, deploy it, try everything yourself

Commands are shown for **PowerShell** on Windows. In Git Bash / Linux / macOS
they are the same, except where noted. Run everything from this folder (the one
with `docker-compose.yml`).

> In PowerShell, `curl` is not real curl. Always type **`curl.exe`**.

---

## 0. What you need

| For | Install |
|---|---|
| the system | Docker Desktop (running), with `docker compose` |
| the demo script and the client on this machine | Python 3.11+ and `pip install -r requirements.txt` |
| the tests | `pip install -r requirements-dev.txt` |

---

## 1. Start the system

```powershell
docker compose up --build -d
```

That builds the images (the first time takes a minute) and starts:

```
object-store-controller-1     the API, http://127.0.0.1:5000/api/v1
object-store-worker-1 .. -4   the workers (not reachable from outside, only through the controller)
```

Check that it is up:

```powershell
docker compose ps                                  # all "healthy" after a few seconds
curl.exe -H "Authorization: Bearer admin-dev-key" http://127.0.0.1:5000/api/v1/nodes   # worker1..worker4, "alive": true
docker compose logs controller                     # "worker workerN registered at ..."
```

What happened behind the scenes:

```
1. Docker starts 1 controller + 4 copies of the worker image on the network "store"
2. each worker asks Docker's DNS "what is my name?" -> object-store-worker-2 -> id worker2
3. each worker: POST http://controller:5000/internal/workers/register   (id + address)
4. each worker: POST .../internal/workers/heartbeat every 2 s           (alive + free space)
5. the controller loads its saved metadata (/meta/metadata.db, a SQLite database) if there is any
```

---

## 2. Use it

### a) The client on this machine

```powershell
$env:API_KEY = "client1-dev-key"                   # the compose file turns authentication on:
                                                   # act as client1 (keys: docker-compose.yml)
# ...or get a key of your own instead (saved for you, used from then on):
#   Remove-Item Env:API_KEY; python client.py register myname
python client.py                                   # the menu
python client.py upload files\hello.txt "owner=me,course=cloud"
python client.py list
python client.py info <object_id>
python client.py download <object_id>              # saved under its own name in this folder
python client.py update <object_id> "status=final,owner=null"
python client.py delete <object_id>
```

### b) The client containers (several clients)

`client1` and `client2` are two separate clients (each has its own `CLIENT_ID`).
They see this folder's `files\` as `/files`, and save downloads to
`downloads\client1\` and `downloads\client2\` on this machine.

```powershell
docker compose run --rm client1 upload /files/hello.txt
docker compose run --rm client2 upload /files/hello.txt    # same path, other client: allowed, a separate object
docker compose run --rm client1 upload /files/hello.txt    # same client, same path again: refused (409)
docker compose run --rm client1 list
docker compose run --rm client2 download <object_id>       # -> downloads\client2\hello.txt
docker compose run --rm client1                            # the menu, inside the container
```

Put your own files into `files\` to upload them from the containers. The first
`run` builds the client image.

To see two clients at **the same moment**, run the two uploads in two
terminals at once, or:

```powershell
Start-Job { docker compose -f $using:PWD\docker-compose.yml run --rm -T client1 upload /files/hello.txt }
Start-Job { docker compose -f $using:PWD\docker-compose.yml run --rm -T client2 upload /files/hello.txt }
Get-Job | Receive-Job -Wait
```

### c) Plain HTTP (curl)

```powershell
$h = "Authorization: Bearer client1-dev-key"     # act as client1 (keys: docker-compose.yml)
curl.exe -H $h -F "file=@files/hello.txt" -F 'metadata={"owner":"me"}' http://127.0.0.1:5000/api/v1/objects
curl.exe -H $h http://127.0.0.1:5000/api/v1/objects
curl.exe -H $h http://127.0.0.1:5000/api/v1/objects/<object_id> -o copy.txt        # the original bytes
curl.exe -H $h http://127.0.0.1:5000/api/v1/objects/<object_id>/metadata
curl.exe -H $h -X PATCH -H "Content-Type: application/json" -d '{"status":"final"}' http://127.0.0.1:5000/api/v1/objects/<object_id>/metadata
curl.exe -H $h -X DELETE http://127.0.0.1:5000/api/v1/objects/<object_id>
```

JSON on the command line: Windows PowerShell 5 drops the inner `"` when it
calls `curl.exe`, so there write each `"` as `\"`
(`'metadata={\"owner\":\"me\"}'`, `-d '{\"status\":\"final\"}'`), or use the
client for metadata. Git Bash, Linux and PowerShell 7 take them as shown.
A browser works for every GET, for example http://127.0.0.1:5000/api/v1/replication.

---

## 3. The demonstrations (spec section 10)

### All at once

```powershell
python demo.py              # all seven, prints what to look at
python demo.py --pause      # waits for Enter after each one: use this to present
python demo.py failure      # only one (names: python demo.py --list)
```

The failure demo waits for automatic repair (30 s after the worker stops). Start
with `$env:AUTO_REPAIR_GRACE=15; docker compose up -d` for a shorter wait.

### By hand, step by step

**Storage + chunking + replication:** upload something larger than 1 MB.

```powershell
fsutil file createnew files\big.bin 3500000          # a 3.5 MB file (Linux: head -c 3500000 /dev/urandom > files/big.bin)
python client.py upload files\big.bin
python client.py info <object_id>                    # 4 chunks, each "replicas": two different workers
python client.py download <object_id> big-copy.bin
(Get-FileHash files\big.bin).Hash -eq (Get-FileHash big-copy.bin).Hash    # True
```

**Metadata:** `upload ... "k=v"`, `info`, `update ... "k=new,old=null"`, `info`
again. `checksum` and the data stay the same, `modified_at` changes.

**Worker failure:**

```powershell
$a = "Authorization: Bearer admin-dev-key"       # these are admin endpoints
curl.exe -H $a http://127.0.0.1:5000/api/v1/replication    # BEFORE: everything healthy
docker stop object-store-worker-2
curl.exe -H $a http://127.0.0.1:5000/api/v1/nodes          # after ~6 s: worker2 "alive": false
curl.exe -H $a http://127.0.0.1:5000/api/v1/replication    # DURING: chunks on worker2 are under_replicated
python client.py download <object_id> during.bin     # still works (the other copy is used)
# wait 30 s: automatic repair copies those chunks to the other workers
curl.exe -H $a http://127.0.0.1:5000/api/v1/replication    # healthy again; "auto_repair.last" says what it did
docker start object-store-worker-2                   # it registers again; its old chunks are still on its volume
curl.exe -H $a http://127.0.0.1:5000/internal/workers/worker2/chunks   # those old copies are now "orphans"
curl.exe -H $a -X POST "http://127.0.0.1:5000/api/v1/cleanup?min_age=0"  # removes them
```

Don't want to wait? `curl.exe -H $a -X POST http://127.0.0.1:5000/api/v1/repair`
repairs at once. Restart the worker within 30 s and nothing is copied at all.

**Data integrity:** damage one stored copy on purpose.

```powershell
python client.py info <object_id>                    # pick a chunk_id and one worker holding it, e.g. worker3
docker exec object-store-worker-3 ls /data/worker3   # the chunk files
docker exec object-store-worker-3 python -c "f=open('/data/worker3/<chunk_id>','r+b'); f.seek(10); f.write(b'XX'); f.close()"
$a = "Authorization: Bearer admin-dev-key"
curl.exe -H $a http://127.0.0.1:5000/api/v1/replication    # that copy: "state": "corrupt"
python client.py download <object_id> check.bin     # still correct: the damaged copy is skipped
curl.exe -H $a -X POST http://127.0.0.1:5000/api/v1/repair # the damaged copy is rebuilt from the good one
```

Damage **both** copies of a chunk and the download fails with a clear 503
(`No good copy of chunk ...`): bad data is never returned.

---

## 4. Change the settings

All settings are at the top of `docker-compose.yml`. Either edit the default
there, or override it for one run:

```powershell
$env:WORKERS=6; docker compose up -d                 # 6 workers
docker compose up -d --scale worker=6                # same, changes only the number of workers
$env:CHUNK_SIZE=524288; docker compose up -d         # 512 KB chunks
$env:REPLICATION_FACTOR=3; docker compose up -d      # 3 copies of every chunk (needs >= 3 workers)
$env:WORKER_CAPACITY=20000000; docker compose up -d  # 20 MB per worker
$env:AUTO_REPAIR_GRACE=0; docker compose up -d       # automatic repair off
Remove-Item Env:WORKERS                              # back to the default
```

(Git Bash / Linux: `WORKERS=6 docker compose up -d`.)

Notes:
- A new chunk size applies to new uploads; old objects keep their chunks.
- A higher replication factor applies to new uploads, and `POST /api/v1/repair` adds the missing copies to old ones.
- Fewer workers: the removed ones count as down, and automatic repair moves their chunks.

---

## 5. Restarts and stored data

| Command | Containers | Chunks and metadata |
|---|---|---|
| `docker stop` / `docker start <name>` | stopped / started | kept |
| `docker compose restart controller` | controller restarted | kept: every object is still there |
| `docker compose down` then `up -d` | removed and re-created | kept (in the volumes `worker-data`, `controller-data`) |
| `docker compose up --build -d` | rebuilt | kept |
| `docker compose down -v` | removed | **deleted** (a clean start) |

```powershell
docker compose restart controller
python client.py list                                # still everything
```

---

## 6. Stop

```powershell
docker compose down          # stop; keeps all data
docker compose down -v       # stop and delete all data
```

---

## 7. Run without Docker

One terminal per worker, then the controller, then the client:

```powershell
$env:NODE_ID="worker1"; $env:PORT="5001"; python -m nodes.node_server
$env:NODE_ID="worker2"; $env:PORT="5002"; python -m nodes.node_server
$env:NODE_ID="worker3"; $env:PORT="5003"; python -m nodes.node_server
$env:NODE_ID="worker4"; $env:PORT="5004"; python -m nodes.node_server
python app.py
python client.py
```

Start as many workers as you like, in any order (each registers itself).
Chunks are kept in `data\workerN\`, the metadata in `data\controller\metadata.db` (SQLite).
Ctrl+C in a worker's terminal is the "worker failure". `demo.py` works too,
except for its failure and integrity parts, which use `docker`.

---

## 8. Run the tests

```powershell
pip install -r requirements-dev.txt
python -m pytest tests                    # all ~380 tests, about 2 minutes, no Docker needed
python -m pytest tests -m "not cluster"   # the fast ones only (seconds)
python -m pytest tests/test_auto_repair.py -v     # one file, every test name shown
```

---

## 9. Troubleshooting

| Problem | Fix |
|---|---|
| `port is already allocated` / 5000 in use | something else uses port 5000: `$env:API_PORT=5050; docker compose up -d`, then use `http://127.0.0.1:5050` (`$env:API_URL="http://127.0.0.1:5050/api/v1"` for the client and the demo) |
| a worker never shows up in `/nodes` | `docker compose logs worker`: it prints "registered" or "not reachable, retrying" |
| upload gives 503 "Not enough storage" | fewer than 2 live workers with room: start workers, or raise `WORKER_CAPACITY` |
| `curl` in PowerShell prints odd errors | use `curl.exe` |
| Git Bash turns `/files/x` into `C:/Program Files/Git/files/x` | prefix the command with `MSYS_NO_PATHCONV=1` |
| the controller will not start: "Cannot read the metadata file" | the saved metadata is damaged; `docker compose down -v` starts clean |
| Linux: the client container cannot save downloads | `mkdir -p downloads/client1 downloads/client2 && chmod 777 downloads/*` |
