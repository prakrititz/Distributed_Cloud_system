"""Workers registering themselves (POST /internal/workers/register), so the
number of workers is not fixed, and how a worker started by Docker Compose
finds its own id (nodes/identity.py)."""
import os
import socket
import struct
import subprocess
import sys
import threading
import time

import pytest
import requests
from flask import Flask, jsonify, request
from werkzeug.serving import make_server

from nodes import identity
from tests.cluster import HEARTBEAT_INTERVAL, NODE_IDS, REPO_ROOT, free_port, wait_until
from tests.fakes import FakeNode
from tests.util import MB, TIMEOUT, replicas_of, upload_ok

REPORT = {"capacity": 1000, "used": 0, "free": 1000, "chunks": 0}


def register(cluster, body):
    return requests.post(cluster.root + "/internal/workers/register", json=body, timeout=TIMEOUT)


# ----------------------------------------------------------- the distributor
class TestDistributorRegistry:
    def test_a_new_worker_is_added_and_alive_at_once(self, make_distributor):
        d, _ = make_distributor(2)
        assert d.register_node(FakeNode("N9"), **REPORT) is True
        assert [n.node_id for n in d.nodes] == ["N1", "N2", "N9"]
        assert d._cached_state(d._get_node("N9"))[0] is True

    def test_registering_again_replaces_the_entry_in_its_place(self, make_distributor):
        d, _ = make_distributor(3)
        replacement = FakeNode("N2")
        assert d.register_node(replacement, **REPORT) is False
        assert [n.node_id for n in d.nodes] == ["N1", "N2", "N3"]
        assert d._get_node("N2") is replacement

    def test_the_list_is_replaced_not_changed_so_a_running_loop_is_safe(self, make_distributor):
        d, _ = make_distributor(2)
        before = d.nodes
        d.register_node(FakeNode("N3"), **REPORT)
        assert [n.node_id for n in before] == ["N1", "N2"] and d.nodes is not before

    def test_with_no_workers_an_upload_is_refused_then_works_once_they_register(self):
        from controller.distributor import Distributor
        d = Distributor([], chunk_size=100)
        with pytest.raises(RuntimeError):
            d.upload("a", b"x" * 250)
        for i in (1, 2):
            d.register_node(FakeNode(f"W{i}"), capacity=10 ** 6, used=0, free=10 ** 6, chunks=0)
        oid = d.upload("a", b"x" * 250)
        assert d.download(oid) == b"x" * 250
        assert all(sorted(c.replicas) == ["W1", "W2"] for c in d.get_object(oid).chunks)

    def test_many_workers_registering_at_once(self, make_distributor):
        d, _ = make_distributor(0)
        threads = [threading.Thread(target=d.register_node, args=(FakeNode(f"W{i}"),), kwargs=REPORT)
                   for i in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sorted(n.node_id for n in d.nodes) == sorted(f"W{i}" for i in range(50))


# -------------------------------------------------------------- the endpoint
class TestRegisterEndpoint:
    def test_a_new_worker_is_201_and_listed(self, cluster):
        port = free_port()   # nothing listens there: /nodes lists it as down, that is fine
        r = register(cluster, {"worker_id": "workerX", "url": f"http://127.0.0.1:{port}", **REPORT})
        assert r.status_code == 201 and r.json()["new"] is True
        assert "workerX" in [n["node"] for n in requests.get(cluster.api + "/nodes", timeout=TIMEOUT).json()]
        cluster.extra["workerX"] = port          # so reset() removes it again

    def test_registering_again_is_200_and_updates_the_address(self, cluster):
        cluster.extra["workerY"] = 1
        register(cluster, {"worker_id": "workerY", "url": "http://127.0.0.1:1", **REPORT})
        r = register(cluster, {"worker_id": "workerY", "url": "http://127.0.0.1:2", **REPORT})
        assert r.status_code == 200 and r.json()["new"] is False
        assert cluster.distributor._get_node("workerY").base_url == "http://127.0.0.1:2"
        assert [n.node_id for n in cluster.distributor.nodes].count("workerY") == 1

    @pytest.mark.parametrize("body", [
        {}, [], "text",
        {"worker_id": "w", **REPORT},                                    # no url
        {"worker_id": "w", "url": "ftp://x", **REPORT},
        {"worker_id": "w", "url": 5, **REPORT},
        {"worker_id": "", "url": "http://x:1", **REPORT},
        {"worker_id": "   ", "url": "http://x:1", **REPORT},
        {"worker_id": "w" * 65, "url": "http://x:1", **REPORT},
        {"worker_id": "w", "url": "http://x:1", **{**REPORT, "free": -1}},
        {"worker_id": "w", "url": "http://x:1", "capacity": 1},          # missing fields
    ])
    def test_bad_bodies_are_400_and_register_nothing(self, cluster, body):
        before = [n.node_id for n in cluster.distributor.nodes]
        r = register(cluster, body)
        assert r.status_code == 400 and "error" in r.json()
        assert [n.node_id for n in cluster.distributor.nodes] == before


# ------------------------------------------------------------ real processes
class TestWorkersJoinOnTheirOwn:
    def test_a_worker_nobody_listed_registers_and_is_used(self, cluster):
        cluster.start_extra("worker5")
        assert cluster.wait_alive("worker5")
        for n in ("worker1", "worker2", "worker3"):
            cluster.kill(n)
        for n in ("worker1", "worker2", "worker3"):
            assert cluster.wait_down(n)
        m = upload_ok(cluster, "f", os.urandom(2 * MB))       # possible only with worker4 + worker5
        assert all(sorted(reps) == ["worker4", "worker5"] for _, reps in replicas_of(cluster, m["object_id"]))

    def test_a_controller_that_forgot_a_worker_gets_it_back(self, cluster):
        # what a controller restart looks like to a worker: its heartbeats get 404
        d = cluster.distributor
        with d._nodes_lock:
            d.nodes = [n for n in d.nodes if n.node_id != "worker1"]
        with d._status_lock:
            d._status.pop("worker1", None)
        assert wait_until(lambda: "worker1" in [n.node_id for n in d.nodes], HEARTBEAT_INTERVAL * 6)
        assert cluster.wait_alive("worker1")

    def test_two_more_workers_join_and_share_the_load(self, cluster):
        for n in ("worker5", "worker6"):
            cluster.start_extra(n)
        for n in ("worker5", "worker6"):
            assert cluster.wait_alive(n)
        assert sorted(n.node_id for n in cluster.distributor.nodes) == sorted(NODE_IDS + ["worker5", "worker6"])
        used = set()
        for i in range(6):     # most free space first, so the newcomers get chunks within a few uploads
            used |= {r for _, reps in replicas_of(cluster, upload_ok(cluster, f"f{i}", os.urandom(MB))["object_id"])
                     for r in reps}
        assert {"worker5", "worker6"} <= used


class TestAWorkerStartedBeforeTheController:
    """In Docker the workers may start first. A worker keeps trying to register,
    and registers again if the controller later forgets it."""

    @pytest.fixture
    def fake_controller(self):
        calls = {"register": [], "heartbeat": 0}
        forget = threading.Event()
        app = Flask("fake-controller")

        @app.post("/internal/workers/register")
        def reg():
            calls["register"].append(request.get_json())
            return jsonify({"status": "registered"}), 201

        @app.post("/internal/workers/heartbeat")
        def beat():
            calls["heartbeat"] += 1
            if forget.is_set():
                forget.clear()
                return jsonify({"error": "unknown"}), 404
            return jsonify({"status": "ok"})

        port = free_port()
        yield port, calls, forget, app

    def test_it_keeps_trying_then_registers_and_re_registers_when_forgotten(self, fake_controller, tmp_path):
        port, calls, forget, app = fake_controller
        worker_port = free_port()
        env = dict(os.environ, NODE_ID="early", PORT=str(worker_port), HOST="127.0.0.1", CAPACITY="1000",
                   DATA_DIR=str(tmp_path / "{node_id}"), CONTROLLER_URL=f"http://127.0.0.1:{port}",
                   HEARTBEAT_INTERVAL="0.2")
        env.pop("ADVERTISE_URL", None)
        proc = subprocess.Popen([sys.executable, "-m", "nodes.node_server"], cwd=REPO_ROOT, env=env,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        server = None
        try:
            time.sleep(1.5)                                   # the worker tries while nothing listens
            server = make_server("127.0.0.1", port, app, threaded=True)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            assert wait_until(lambda: len(calls["register"]) == 1 and calls["heartbeat"] >= 2, 10)
            body = calls["register"][0]
            assert body["worker_id"] == "early" and body["url"] == f"http://127.0.0.1:{worker_port}"
            assert body["capacity"] == 1000
            assert (tmp_path / "early").is_dir()              # {node_id} in DATA_DIR became the id
            forget.set()
            assert wait_until(lambda: len(calls["register"]) == 2, 10)
        finally:
            proc.terminate()
            proc.wait(timeout=10)
            if server:
                server.shutdown()


# ------------------------------------------------------------ who am I
class TestIdentity:
    @pytest.mark.parametrize("name,number", [("object-store-worker-2", 2), ("proj-worker-13", 13),
                                             ("worker", None), ("", None), (None, None), ("w-2x", None)])
    def test_number_from_container_name(self, name, number):
        assert identity.number_from_container_name(name) == number

    def test_choose_identity(self):
        choose_identity = identity.choose_identity
        assert choose_identity("auto", "0.0.0.0", 5000, container_name="object-store-worker-3") == \
            ("worker3", "http://object-store-worker-3:5000")
        assert choose_identity("auto", "0.0.0.0", 5000, name_prefix="store", container_name="p-worker-3") == \
            ("store3", "http://p-worker-3:5000")
        assert choose_identity("worker7", "127.0.0.1", 5007) == ("worker7", "http://127.0.0.1:5007")
        assert choose_identity("w", "127.0.0.1", 1, advertise_url="http://me:9/") == ("w", "http://me:9")
        node_id, _ = choose_identity("auto", "127.0.0.1", 1, container_name=None)   # outside Docker
        assert node_id == socket.gethostname()

    @staticmethod
    def fake_dns(answer_name):
        """A UDP 'DNS server' on localhost answering every PTR query with
        answer_name, using a compression pointer like real servers do."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("127.0.0.1", 0))
        send = socket.socket.sendto      # the real one: the tests redirect the client's sendto

        def serve():
            query, addr = sock.recvfrom(512)
            qid = query[:2]
            question = query[12:]
            header = qid + struct.pack(">HHHHH", 0x8180, 1, 1 if answer_name else 0, 0, 0)
            reply = header + question
            if answer_name:
                rdata = b"".join(bytes([len(p)]) + p.encode() for p in answer_name.split(".")) + b"\x00"
                reply += b"\xc0\x0c" + struct.pack(">HHIH", 12, 1, 60, len(rdata)) + rdata
            send(sock, reply, addr)
            sock.close()

        threading.Thread(target=serve, daemon=True).start()
        return sock.getsockname()

    def test_reverse_lookup_reads_the_answer(self, monkeypatch):
        host, port = self.fake_dns("object-store-worker-4.object-store_store")
        real = socket.socket.sendto
        monkeypatch.setattr(socket.socket, "sendto", lambda s, data, addr: real(s, data, (host, port)))
        assert identity.reverse_lookup("172.20.0.5") == "object-store-worker-4.object-store_store"

    def test_reverse_lookup_without_an_answer_is_none(self, monkeypatch):
        host, port = self.fake_dns(None)
        real = socket.socket.sendto
        monkeypatch.setattr(socket.socket, "sendto", lambda s, data, addr: real(s, data, (host, port)))
        assert identity.reverse_lookup("172.20.0.5") is None

    def test_reverse_lookup_with_nobody_answering_is_none_after_the_timeout(self, monkeypatch):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as silent:   # receives, never answers
            silent.bind(("127.0.0.1", 0))
            target = silent.getsockname()
            real = socket.socket.sendto
            monkeypatch.setattr(socket.socket, "sendto", lambda s, data, addr: real(s, data, target))
            started = time.time()
            assert identity.reverse_lookup("10.0.0.1", timeout=0.3) is None
            assert time.time() - started < 2

    def test_a_garbage_reply_is_none(self, monkeypatch):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("127.0.0.1", 0))
        real = socket.socket.sendto
        answered = threading.Event()

        def serve():
            query, addr = sock.recvfrom(512)
            # 1 question, 1 answer, then a truncated name: the parser must not crash
            real(sock, query[:2] + b"\x81\x80\x00\x01\x00\x01\x00\x00\x00\x00" + b"\x3f" * 5, addr)
            answered.set()
            sock.close()

        threading.Thread(target=serve, daemon=True).start()
        target = sock.getsockname()
        monkeypatch.setattr(socket.socket, "sendto", lambda s, data, addr: real(s, data, target))
        assert identity.reverse_lookup("172.20.0.5") is None
        assert answered.is_set()          # it really parsed a reply, it did not just time out
