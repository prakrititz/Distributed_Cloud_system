"""Heartbeats between the real node processes and the real controller."""
import os
import time

import requests

from controller.remote_node import RemoteNode
from controller.distributor import Distributor
from tests.cluster import HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, NODE_IDS, wait_until
from tests.util import MB, TIMEOUT, download, post_heartbeat, replicas_of, upload, upload_ok

GOOD = {"worker_id": "Node1", "capacity": 100, "used": 10, "free": 90, "chunks": 1}


class TestHeartbeatEndpoint:
    def test_a_valid_heartbeat_is_accepted(self, cluster):
        assert post_heartbeat(cluster, GOOD).status_code == 200

    def test_bad_bodies_are_400(self, cluster):
        bad = [{}, [], "text", {**GOOD, "free": -1}, {**GOOD, "free": "x"}, {**GOOD, "free": 1.5},
               {**GOOD, "free": True}, {**GOOD, "worker_id": 5},
               {k: v for k, v in GOOD.items() if k != "chunks"}]
        for body in bad:
            r = post_heartbeat(cluster, body)
            assert r.status_code == 400 and "error" in r.json(), body

    def test_a_worker_that_is_not_in_the_list_is_404(self, cluster):
        r = post_heartbeat(cluster, {**GOOD, "worker_id": "Nope"})
        assert r.status_code == 404 and "Nope" in r.json()["error"]

    def test_get_is_not_allowed(self, cluster):
        assert requests.get(cluster.root + "/internal/workers/heartbeat", timeout=TIMEOUT).status_code == 405


class TestNodesReportThemselves:
    def test_every_node_is_heard_from_without_being_asked(self, cluster):
        for n in NODE_IDS:
            assert wait_until(lambda: cluster.distributor.heartbeat_age(n) is not None, 5)
            assert cluster.is_alive(n)

    def test_the_reports_keep_coming(self, cluster):
        time.sleep(HEARTBEAT_INTERVAL * 3)
        assert all(cluster.distributor.heartbeat_age(n) <= HEARTBEAT_INTERVAL * 2 + 0.5 for n in NODE_IDS)

    def test_the_reported_numbers_are_the_nodes_real_ones(self, cluster):
        oid = upload_ok(cluster, "f", os.urandom(MB))["object_id"]
        node = replicas_of(cluster, oid)[0][1][0]
        assert wait_until(lambda: cluster.distributor._status[node]["chunks"] == 1, 5)
        assert cluster.distributor._status[node]["used"] == MB


class TestPlacementUsesTheCache:
    def test_placing_chunks_sends_no_health_requests(self, cluster, monkeypatch):
        calls = []
        original = RemoteNode.health
        monkeypatch.setattr(RemoteNode, "health", lambda self: (calls.append(self.node_id), original(self))[1])
        time.sleep(HEARTBEAT_INTERVAL * 2)                 # let the table fill from heartbeats
        upload_ok(cluster, "big", os.urandom(4 * MB))     # 4 chunks, 8 copies
        assert calls == []

    def test_a_fresh_controller_asks_each_node_once_then_uses_its_cache(self, cluster, monkeypatch):
        calls = []
        original = RemoteNode.health
        monkeypatch.setattr(RemoteNode, "health", lambda self: (calls.append(self.node_id), original(self))[1])
        fresh = Distributor([RemoteNode(n, cluster.node_url(n)) for n in NODE_IDS], chunk_size=1000)
        fresh.upload("a", os.urandom(5000))
        fresh.upload("b", os.urandom(5000))
        assert sorted(calls) == sorted(NODE_IDS)           # once each, not once per chunk


class TestFailureDetection:
    def test_a_dead_node_is_declared_down_after_the_timeout_and_not_before(self, cluster):
        started = time.time()
        cluster.kill("Node2")
        assert cluster.is_alive("Node2") or time.time() - started > HEARTBEAT_TIMEOUT   # not instantly
        assert cluster.wait_down("Node2")
        assert time.time() - started <= HEARTBEAT_TIMEOUT + 4
        assert all(cluster.is_alive(n) for n in NODE_IDS if n != "Node2")

    def test_the_nodes_endpoint_shows_it(self, cluster):
        cluster.kill("Node3")
        cluster.wait_down("Node3")
        nodes = {n["node"]: n for n in requests.get(cluster.api + "/nodes", timeout=TIMEOUT).json()}
        assert nodes["Node3"]["alive"] is False and nodes["Node1"]["alive"] is True

    def test_uploads_during_the_failure_are_fast_and_avoid_the_dead_node(self, cluster):
        cluster.kill("Node2")
        assert cluster.wait_down("Node2")
        started = time.time()
        m = upload_ok(cluster, "f", os.urandom(6 * MB))
        elapsed = time.time() - started
        assert elapsed < 8                                   # no probe timeout per chunk
        assert "Node2" not in {r for _, reps in replicas_of(cluster, m["object_id"]) for r in reps}

    def test_downloads_during_the_failure_work_and_are_fast(self, cluster):
        data = os.urandom(4 * MB)
        oid = upload_ok(cluster, "f", data)["object_id"]
        victim = replicas_of(cluster, oid)[0][1][0]
        cluster.kill(victim)
        assert cluster.wait_down(victim)
        started = time.time()
        r = download(cluster, oid)
        assert r.status_code == 200 and r.content == data and time.time() - started < 5

    def test_a_node_that_dies_just_before_it_is_declared_down_does_not_fail_the_upload(self, cluster):
        cluster.distributor.heartbeat_timeout = 60           # pretend the silence has not been noticed yet
        victim = "Node1"
        cluster.kill(victim)
        assert cluster.is_alive(victim)                       # the cache still believes it is alive
        m = upload_ok(cluster, "f", os.urandom(3 * MB))       # a copy is attempted on it, fails, is skipped
        assert victim not in {r for _, reps in replicas_of(cluster, m["object_id"]) for r in reps}
        assert not cluster.is_alive(victim)                   # found out by the failed write

    def test_a_restarted_node_is_alive_again_and_used(self, cluster):
        cluster.kill("Node4")
        assert cluster.wait_down("Node4")
        cluster.restart("Node4")
        assert cluster.wait_alive("Node4")
        used = set()
        for i in range(6):
            used |= {r for _, reps in replicas_of(cluster, upload_ok(cluster, f"f{i}", os.urandom(2 * MB))["object_id"])
                     for r in reps}
        assert "Node4" in used

    def test_with_too_few_nodes_left_it_is_a_clean_503_not_a_hang(self, cluster):
        for n in ("Node1", "Node2", "Node3"):
            cluster.kill(n)
        for n in ("Node1", "Node2", "Node3"):
            assert cluster.wait_down(n)
        started = time.time()
        r = upload(cluster, "f", os.urandom(MB))
        assert r.status_code == 503 and time.time() - started < 5
