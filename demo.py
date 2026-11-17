"""Run the project's demonstrations (spec section 10) against the running system.

Start the system first (docker compose up --build -d), then:

    python demo.py                    every demonstration, one after another
    python demo.py --pause            wait for Enter after each one (for presenting)
    python demo.py failure integrity  only some of them
    python demo.py --list             the names

Demonstrations:
    storage      a file goes in and comes back byte for byte
    chunking     a file larger than 1 MB is split into chunks spread over the workers
    replication  every chunk has two copies, on two different workers
    metadata     custom metadata is stored, read and changed without re-uploading
    failure      a worker container is stopped: before, during and after, repair, restart
    integrity    a stored chunk is damaged on purpose: detected and handled
    clients      two clients at the same time, with the same file paths

The failure and integrity demos use the docker command (docker stop / docker
exec), so they need the Docker deployment. Settings: API_URL (default
http://127.0.0.1:5000/api/v1) and COMPOSE_PROJECT (default object-store).
"""
import argparse
import os
import re
import subprocess
import sys
import tempfile
import threading
import time

import requests

from client import Client
from helpers.checksum import compute

API = os.environ.get("API_URL", "http://127.0.0.1:5000/api/v1").rstrip("/")
ROOT = API.rsplit("/api/v1", 1)[0]          # the internal endpoints live outside /api/v1
PROJECT = os.environ.get("COMPOSE_PROJECT", "object-store")
HERE = os.path.dirname(os.path.abspath(__file__))
WORK = tempfile.mkdtemp(prefix="object-store-demo-")


# ------------------------------------------------------------------ helpers
def title(text):
    print("\n" + "=" * 72 + f"\n  {text}\n" + "=" * 72)


def say(text=""):
    print(f"  {text}")


def ok(text):
    print(f"  [OK] {text}")


def fail(text):
    print(f"  [!!] {text}")
    raise SystemExit(1)


def get(path):
    r = requests.get(API + path, timeout=30)
    r.raise_for_status()
    return r.json()


def nodes():
    return {n["node"]: n for n in get("/nodes")}


def show_nodes():
    for name, n in sorted(nodes().items(), key=lambda kv: worker_number(kv[0]) or 0):
        state = "up  " if n["alive"] else "DOWN"
        extra = f"{n['chunks']} chunk(s), {n['free']} bytes free" if n["alive"] else ""
        say(f"  {name:10} {state} {extra}")


def summary():
    return get("/replication")["summary"]


def show_summary():
    s = summary()
    say(f"  replication: {s['healthy']} healthy, {s['under_replicated']} under-replicated, "
        f"{s['unavailable']} unavailable (of {s['total_chunks']} chunks)")
    return s


def wait_for(condition, seconds, what):
    end = time.time() + seconds
    while time.time() < end:
        if condition():
            return True
        time.sleep(1)
    fail(f"timed out waiting for: {what}")


def worker_number(node_id):
    m = re.search(r"(\d+)$", node_id)
    return int(m.group(1)) if m else None


def container_of(node_id):
    return f"{PROJECT}-worker-{worker_number(node_id)}"


def docker(*args):
    r = subprocess.run(["docker", *args], capture_output=True, text=True)
    if r.returncode != 0:
        fail(f"docker {' '.join(args)} failed: {r.stderr.strip()}")
    return r.stdout


def make_file(name, size):
    path = os.path.join(WORK, name)
    with open(path, "wb") as f:
        f.write(os.urandom(size))
    return path


def download_bytes(object_id):
    r = requests.get(f"{API}/objects/{object_id}", timeout=60)
    r.raise_for_status()
    return r.content


def show_chunks(meta):
    for c in meta["chunks"]:
        say(f"  chunk {c['index']}  ->  {' + '.join(c['replicas'])}")


client = Client(base_url=API, client_id="demo")


# ----------------------------------------------------------- demonstrations
def demo_storage():
    title("1. Object storage: what goes in comes back unchanged")
    path = os.path.join(HERE, "files", "hello.txt")
    if not os.path.exists(path):
        path = make_file("hello.txt", 100)
    original = open(path, "rb").read()
    meta = Client(base_url=API, client_id=f"demo-{time.time_ns()}").upload(path)
    say(f"uploaded {meta['name']} ({meta['size']} bytes) -> object_id {meta['object_id']}")
    back = download_bytes(meta["object_id"])
    say(f"checksum before: {compute(original)[:16]}...  after: {compute(back)[:16]}...")
    if back != original:
        fail("the downloaded file differs")
    ok("the downloaded object is byte-for-byte the original")


STATE = {}


def big_object():
    if "big" not in STATE:
        path = make_file("big-3.5MB.bin", 3 * 1024 * 1024 + 512 * 1024)
        STATE["big_data"] = open(path, "rb").read()
        STATE["big"] = client.upload(path, {"demo": "chunking"})
    return STATE["big"]


def demo_chunking():
    title("2. Chunking and distribution: a 3.5 MB file")
    meta = big_object()
    say(f"{meta['name']}: {meta['size']} bytes -> {meta['chunk_count']} chunks of at most 1 MB")
    show_chunks(meta)
    used = {r for c in meta["chunks"] for r in c["replicas"]}
    say(f"the chunks are spread over {len(used)} worker(s): {', '.join(sorted(used))}")
    if download_bytes(meta["object_id"]) != STATE["big_data"]:
        fail("reassembled file differs")
    ok("downloaded and reassembled from the chunks: identical to the original")


def demo_replication():
    title("3. Replication: every chunk has 2 copies on 2 different workers")
    meta = big_object()
    show_chunks(meta)
    for c in meta["chunks"]:
        if len(set(c["replicas"])) != meta["copies_per_chunk"]:
            fail(f"chunk {c['index']} does not have {meta['copies_per_chunk']} different workers")
    ok(f"every chunk is stored {meta['copies_per_chunk']} times, never twice on the same worker")
    show_summary()


def demo_metadata():
    title("4. Metadata: stored with the object, read and changed without re-uploading")
    path = make_file("report.pdf", 2000)
    meta = client.upload(path, {"author": "team", "course": "cloud computing"})
    oid = meta["object_id"]
    say(f"uploaded {meta['name']} with custom metadata {meta['custom']}")
    m = client.get_metadata(oid)
    for k in ("object_id", "name", "size", "content_type", "created_at", "checksum", "chunk_count"):
        say(f"  {k:13} {m[k]}")
    say(f"  custom        {m['custom']}")
    m2 = client.update_metadata(oid, {"status": "final", "author": None})
    say(f"PATCH status=final, author removed -> custom is now {m2['custom']}")
    say(f"created_at {m2['created_at']} stays, modified_at is updated: {m2['modified_at']}")
    if m2["checksum"] != m["checksum"] or download_bytes(oid) != open(path, "rb").read():
        fail("the data changed")
    ok("the metadata changed, the stored data did not (no re-upload)")


def demo_failure():
    title("5. Worker failure: stop a worker container")
    meta = client.get_metadata(big_object()["object_id"])
    victim = meta["chunks"][0]["replicas"][0]
    container = container_of(victim)
    status = get("/replication")["auto_repair"]
    say("BEFORE:")
    show_nodes()
    show_summary()

    say(f"\n$ docker stop {container}")
    docker("stop", container)
    say("waiting for the controller to notice (no heartbeat for 6 s)...")
    wait_for(lambda: not nodes()[victim]["alive"], 30, f"{victim} marked down")
    say("DURING:")
    show_nodes()
    s = show_summary()
    if s["under_replicated"] == 0:
        fail("expected under-replicated chunks")
    if download_bytes(meta["object_id"]) != STATE["big_data"]:
        fail("download failed during the failure")
    ok("download still works: every chunk still has its other copy")

    if status["enabled"]:
        say(f"automatic repair is on: it starts after {status['grace_seconds']} s of downtime. Waiting...")
        wait_for(lambda: summary()["under_replicated"] == 0, status["grace_seconds"] + 60, "automatic repair")
        say(f"auto-repair: {get('/replication')['auto_repair']['last']}")
    else:
        say("$ curl -X POST .../repair")
        r = requests.post(API + "/repair", timeout=120).json()
        say(f"repair: {r['repaired']} chunk(s) repaired, {r['copies_created']} new copies")
    show_summary()
    ok("the replication factor is restored on the remaining workers")
    show_chunks(client.get_metadata(meta["object_id"]))

    say(f"\n$ docker start {container}")
    docker("start", container)
    wait_for(lambda: nodes()[victim]["alive"], 60, f"{victim} back")
    say("AFTER:")
    show_nodes()
    inv = requests.get(f"{ROOT}/internal/workers/{victim}/chunks", timeout=30).json()
    say(f"{victim} came back with {inv['held']} chunk file(s); {len(inv['orphans'])} are old copies "
        f"no object points to any more (replaced while it was down)")
    if inv["orphans"]:
        r = requests.post(API + "/cleanup?min_age=0", timeout=60).json()
        say(f"$ curl -X POST .../cleanup  -> deleted {len(r['deleted'])} orphaned copy/copies")
    show_summary()
    ok("worker back, data intact, every chunk healthy")


def demo_integrity():
    title("6. Data integrity: damage a stored chunk on purpose")
    meta = client.get_metadata(big_object()["object_id"])     # current locations (repair may have moved copies)
    chunk = meta["chunks"][1] if len(meta["chunks"]) > 1 else meta["chunks"][0]
    holder = chunk["replicas"][0]
    container = container_of(holder)
    path = f"/data/{holder}/{chunk['chunk_id']}"
    say(f"chunk {chunk['index']} has copies on {' + '.join(chunk['replicas'])}")
    say(f"$ docker exec {container}  (flip one byte in {path})")
    docker("exec", container, "python", "-c",
           f"f = open('{path}', 'r+b'); f.seek(1000); b = f.read(1); f.seek(1000); f.write(bytes([b[0] ^ 255])); f.close()")
    report = get("/replication")
    states = next(c for c in report["chunks"] if c["chunk_id"] == chunk["chunk_id"])["replicas"]
    for r in states:
        say(f"  copy on {r['node']:10} {r['state']}")
    if not any(r["state"] == "corrupt" for r in states):
        fail("the damage was not detected")
    ok("detected: the worker's checksum no longer matches the one recorded at upload")
    if download_bytes(meta["object_id"]) != STATE["big_data"]:
        fail("download returned damaged data")
    ok("download is still correct: the damaged copy is skipped, the good copy is used")
    r = requests.post(API + "/repair", timeout=120).json()
    say(f"repair: {r['repaired']} chunk(s) repaired, {r['copies_created']} new copies (the bad copy is replaced)")
    show_summary()
    ok("the damaged copy was rebuilt from the good one")


def demo_clients():
    title("7. Several clients at the same time")
    path = make_file("shared-name.txt", 1500)
    alice = Client(base_url=API, client_id=f"client1-{time.time_ns()}")
    bob = Client(base_url=API, client_id=f"client2-{time.time_ns()}")
    results, errors = {}, []

    def up(name, c):
        try:
            results[name] = c.upload(path)
        except Exception as e:
            errors.append(f"{name}: {e}")

    threads = [threading.Thread(target=up, args=(n, c)) for n, c in (("client1", alice), ("client2", bob))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        fail("; ".join(errors))
    say(f"both clients uploaded {path} at the same moment:")
    for name, m in results.items():
        say(f"  {name}: object_id {m['object_id']}")
    ok("same path, different clients: two separate objects, no conflict")
    try:
        alice.upload(path)
        fail("the same client uploading the same path twice was accepted")
    except ValueError as e:
        ok(f"the same client uploading the same path again is refused: {str(e)[:70]}...")
    other = alice.list_objects()
    say(f"each client can see and download every object ({len(other)} stored in total)")


DEMOS = {
    "storage": demo_storage, "chunking": demo_chunking, "replication": demo_replication,
    "metadata": demo_metadata, "failure": demo_failure, "integrity": demo_integrity,
    "clients": demo_clients,
}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("names", nargs="*", help="which demonstrations (default: all)")
    parser.add_argument("--pause", action="store_true", help="wait for Enter after each demonstration")
    parser.add_argument("--list", action="store_true", help="list the demonstrations")
    args = parser.parse_args(argv)
    if args.list:
        print("\n".join(DEMOS))
        return 0
    unknown = [n for n in args.names if n not in DEMOS]
    if unknown:
        parser.error(f"unknown: {', '.join(unknown)} (choose from {', '.join(DEMOS)})")
    try:
        n = nodes()
    except requests.RequestException as e:
        print(f"The API at {API} is not reachable ({type(e).__name__}). Start it first: docker compose up --build -d")
        return 1
    print(f"API {API}: {sum(1 for x in n.values() if x['alive'])} of {len(n)} worker(s) up")
    for name in args.names or list(DEMOS):
        DEMOS[name]()
        if args.pause:
            input("\n  (Enter for the next one) ")
    print("\nAll demonstrations done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
