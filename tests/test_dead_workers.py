"""A dead worker must not slow everything down.

Asking a stopped container takes seconds before it fails (in Docker its name
no longer resolves). So the controller does not ask a worker it already counts
as down (no heartbeat for HEARTBEAT_TIMEOUT, or a request just failed), and in
one pass over the chunks it asks a failing worker only once.
"""
import pytest
import requests

from tests.util import TIMEOUT, upload_ok


def count_checks(node):
    """Count get_checksum calls on a FakeNode."""
    calls = []
    real = node.get_checksum

    def counted(chunk_id):
        calls.append(chunk_id)
        return real(chunk_id)
    node.get_checksum = counted
    return calls


def heartbeat_all(d, nodes):
    for n in nodes:
        d.record_heartbeat(n.node_id, n.capacity, n.used(), n.capacity - n.used(), len(n.chunks))


class TestCountsAsDown:
    def test_from_heartbeats_and_failed_requests(self, make_distributor, clock):
        d, nodes = make_distributor()
        assert d.counts_as_down("N1") is False                  # never heard of: not judged yet
        heartbeat_all(d, nodes)
        assert d.counts_as_down("N1") is False
        clock.advance(d.heartbeat_timeout + 0.1)
        assert d.counts_as_down("N1") is True                   # heartbeat too old
        heartbeat_all(d, nodes)
        d._mark_down("N2")                                      # a request to it just failed
        assert d.counts_as_down("N2") is True and d.counts_as_down("N1") is False


class TestCopyChecks:
    def test_a_worker_counted_down_is_not_asked(self, make_distributor, clock):
        d, nodes = make_distributor()
        heartbeat_all(d, nodes)
        oid = d.upload("f", b"x" * 1000)                        # 10 chunks
        victim = nodes[0]
        victim.down = True
        clock.advance(d.heartbeat_timeout + 0.1)
        heartbeat_all(d, nodes[1:])                             # the others keep reporting
        calls = count_checks(victim)
        report = d.replication_report()
        held = [c for c in report["chunks"] for r in c["replicas"] if r["node"] == victim.node_id]
        assert held and all(r["state"] == "down" for c in held for r in c["replicas"] if r["node"] == victim.node_id)
        assert calls == []
        assert d.repair()["repaired"] == len(held) and calls == []
        assert all(victim.node_id not in c.replicas for c in d.get_object(oid).chunks)

    def test_a_worker_that_just_died_is_asked_once_per_pass(self, make_distributor, clock):
        d, nodes = make_distributor()
        heartbeat_all(d, nodes)
        d.upload("f", b"x" * 1000)
        victim = nodes[0]
        victim.down = True                                      # dead, but its heartbeat is still fresh
        calls = count_checks(victim)
        report = d.replication_report()
        on_victim = sum(1 for c in report["chunks"] for r in c["replicas"] if r["node"] == victim.node_id)
        assert on_victim > 1 and len(calls) == 1                # not once per copy
        calls.clear()
        d.repair()
        assert len(calls) == 1

    def test_a_worker_back_with_a_heartbeat_is_asked_again(self, make_distributor, clock):
        d, nodes = make_distributor()
        heartbeat_all(d, nodes)
        d.upload("f", b"x" * 300)
        d._mark_down("N1")
        assert all(r["state"] != "ok" for c in d.replication_report()["chunks"]
                   for r in c["replicas"] if r["node"] == "N1")
        heartbeat_all(d, nodes)                                 # it reports again
        assert d.replication_report()["summary"]["healthy"] == 3


class TestNodesEndpoint:
    def test_a_worker_counted_down_is_listed_without_being_asked(self, cluster, monkeypatch):
        upload_ok(cluster, "f", b"abc")
        victim = "worker2"
        cluster.kill(victim)
        assert cluster.wait_down(victim)
        node = next(n for n in cluster.distributor.nodes if n.node_id == victim)
        monkeypatch.setattr(node, "health", lambda: pytest.fail("a worker counted down was asked"))
        report = {n["node"]: n for n in requests.get(f"{cluster.api}/nodes", timeout=TIMEOUT).json()}
        assert report[victim]["alive"] is False
        assert all(report[n]["alive"] for n in report if n != victim)
