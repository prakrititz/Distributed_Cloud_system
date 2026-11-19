"""nodes/node_server.py: the internal worker API, called directly over HTTP."""
import requests

from helpers.checksum import compute
from tests.util import TIMEOUT

NODE = "worker1"


def put(cluster, chunk_id, data, checksum="auto"):
    headers = {"Content-Type": "application/octet-stream"}
    if checksum == "auto":
        headers["X-Checksum"] = compute(data)
    elif checksum is not None:
        headers["X-Checksum"] = checksum
    return requests.put(f"{cluster.node_url(NODE)}/internal/chunks/{chunk_id}", data=data,
                        headers=headers, timeout=TIMEOUT)


def url(cluster, chunk_id, suffix=""):
    return f"{cluster.node_url(NODE)}/internal/chunks/{chunk_id}{suffix}"


class TestChunkEndpoints:
    def test_store_and_read_back(self, cluster):
        data = b"chunk bytes \x00\xff" * 100
        r = put(cluster, "t-0", data)
        assert r.status_code == 201 and r.json() == {"chunk_id": "t-0", "size": len(data)}
        g = requests.get(url(cluster, "t-0"), timeout=TIMEOUT)
        assert g.status_code == 200 and g.content == data
        assert g.headers["X-Checksum"] == compute(data)
        assert g.headers["Content-Type"].startswith("application/octet-stream")

    def test_head_asks_without_transferring(self, cluster):
        put(cluster, "t-0", b"abc")
        assert requests.head(url(cluster, "t-0"), timeout=TIMEOUT).status_code == 200
        assert requests.head(url(cluster, "nope"), timeout=TIMEOUT).status_code == 404

    def test_missing_chunk_is_404_json(self, cluster):
        r = requests.get(url(cluster, "nope"), timeout=TIMEOUT)
        assert r.status_code == 404 and "error" in r.json()

    def test_storing_the_same_id_again_replaces_it(self, cluster):
        put(cluster, "t-0", b"old")
        assert put(cluster, "t-0", b"newer").status_code == 201
        assert requests.get(url(cluster, "t-0"), timeout=TIMEOUT).content == b"newer"

    def test_delete_is_idempotent(self, cluster):
        put(cluster, "t-0", b"abc")
        assert requests.delete(url(cluster, "t-0"), timeout=TIMEOUT).status_code == 204
        assert requests.delete(url(cluster, "t-0"), timeout=TIMEOUT).status_code == 204
        assert requests.get(url(cluster, "t-0"), timeout=TIMEOUT).status_code == 404

    def test_a_missing_checksum_header_is_refused(self, cluster):
        r = put(cluster, "t-0", b"abc", checksum=None)
        assert r.status_code == 400 and "X-Checksum" in r.json()["error"]
        assert requests.head(url(cluster, "t-0"), timeout=TIMEOUT).status_code == 404

    def test_data_damaged_in_transit_is_refused_and_not_stored(self, cluster):
        r = put(cluster, "t-0", b"the real bytes", checksum=compute(b"different bytes"))
        assert r.status_code == 422 and "damaged in transit" in r.json()["error"]
        assert requests.head(url(cluster, "t-0"), timeout=TIMEOUT).status_code == 404

    def test_unsafe_chunk_ids_are_refused(self, cluster):
        for bad in (".hidden", "a..b", "..", ".checksums"):
            assert put(cluster, bad, b"x").status_code in (400, 404), bad
        assert not any(f.startswith(".hidden") for f in cluster.chunk_files(NODE))

    def test_a_damaged_chunk_on_disk_is_never_sent(self, cluster):
        data = b"precious" * 50
        put(cluster, "t-0", data)
        cluster.damage_chunk(NODE, "t-0")
        r = requests.get(url(cluster, "t-0"), timeout=TIMEOUT)
        assert r.status_code == 409 and r.content != data and "error" in r.json()


class TestChecksumEndpoint:
    def test_matches_for_an_intact_chunk(self, cluster):
        data = b"intact" * 30
        put(cluster, "t-0", data)
        j = requests.get(url(cluster, "t-0", "/checksum"), timeout=TIMEOUT).json()
        assert j == {"chunk_id": "t-0", "checksum": compute(data), "stored": compute(data), "ok": True}

    def test_recomputes_from_disk_so_damage_is_visible_without_sending_the_chunk(self, cluster):
        put(cluster, "t-0", b"intact" * 30)
        cluster.damage_chunk(NODE, "t-0")
        j = requests.get(url(cluster, "t-0", "/checksum"), timeout=TIMEOUT).json()
        assert j["ok"] is False and j["checksum"] != j["stored"]

    def test_missing_chunk(self, cluster):
        assert requests.get(url(cluster, "nope", "/checksum"), timeout=TIMEOUT).status_code == 404


class TestHealthEndpoint:
    def test_reports_capacity_and_usage(self, cluster):
        before = requests.get(f"{cluster.node_url(NODE)}/internal/health", timeout=TIMEOUT).json()
        assert before["node_id"] == NODE and before["chunks"] == 0
        assert before["capacity"] == cluster.capacity and before["free"] == before["capacity"]
        put(cluster, "t-0", b"x" * 1000)
        after = requests.get(f"{cluster.node_url(NODE)}/internal/health", timeout=TIMEOUT).json()
        assert after["used"] == 1000 and after["free"] == before["free"] - 1000 and after["chunks"] == 1


def test_the_heartbeat_thread_survives_an_unexpected_error(tmp_path, monkeypatch):
    """An error other than a network one (here: counting the disk space
    fails) must not stop the heartbeats: without them the controller counts a
    worker down that still works."""
    import importlib
    import sys
    import threading
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "w"))
    monkeypatch.setenv("NODE_ID", "worker9")
    sys.modules.pop("nodes.node_server", None)
    server = importlib.import_module("nodes.node_server")
    monkeypatch.setattr(server, "HEARTBEAT_INTERVAL", 0.01)
    sent, calls = [], {"n": 0}

    class Ok:
        status_code = 200

    def post(url, json=None, timeout=None):
        sent.append(url.rsplit("/", 1)[-1])
        return Ok()

    def space_report():
        calls["n"] += 1
        if calls["n"] in (2, 3):                     # the 1st heartbeat after registering, and the next
            raise FileNotFoundError("a chunk vanished while counting")
        return {"worker_id": "worker9", "capacity": 1, "used": 0, "free": 1, "chunks": 0}
    monkeypatch.setattr(server.requests, "post", post)
    monkeypatch.setattr(server, "space_report", space_report)
    stop = threading.Event()
    t = threading.Thread(target=server.send_heartbeats, args=(stop,), daemon=True)
    t.start()
    try:
        for _ in range(200):
            if sent.count("heartbeat") >= 3:
                break
            threading.Event().wait(0.01)
    finally:
        stop.set()
        t.join(2)
        sys.modules.pop("nodes.node_server", None)
    assert sent[0] == "register" and sent.count("heartbeat") >= 3 and calls["n"] >= 5
    assert not t.is_alive()
