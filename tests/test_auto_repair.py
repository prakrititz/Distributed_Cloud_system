"""Automatic repair: a worker down for the grace period triggers a repair,
a quick restart does not."""
import os

import pytest
import requests

from tests.util import TIMEOUT

GRACE = 30


@pytest.fixture
def system(make_distributor, clock):
    d, nodes = make_distributor(4)
    by_id = {n.node_id: n for n in nodes}

    def tick(seconds=0):
        """Let time pass; every node that is up sends its heartbeat; the
        controller's background check runs once."""
        clock.advance(seconds)
        for n in nodes:
            if not n.down:
                d.record_heartbeat(n.node_id, n.capacity, n.used(), n.capacity - n.used(), len(n.chunks))
        return d.auto_repair_check(GRACE)

    tick()
    return d, by_id, tick


def healthy(d):
    s = d.replication_report()["summary"]
    return s["healthy"] == s["total_chunks"]


class TestAutoRepair:
    def test_nothing_happens_while_everyone_is_up(self, system):
        d, _, tick = system
        d.upload("f", os.urandom(400))
        for _ in range(10):
            assert tick(10) is None

    def test_a_worker_down_past_the_grace_period_is_repaired_for(self, system):
        d, nodes, tick = system
        oid = d.upload("f", os.urandom(400))
        victim = d.get_object(oid).chunks[0].replicas[0]
        nodes[victim].down = True
        assert tick(10) is None                     # not yet noticed (heartbeat timeout 6 s)
        assert tick(10) is None                     # down, but within the grace period
        result = tick(GRACE)
        assert result is not None and result["repaired"] >= 1
        assert healthy(d) and all(victim not in c.replicas for c in d.get_object(oid).chunks)
        assert d.last_auto_repair["because_down"] == [victim]
        assert tick(GRACE) is None                  # repaired once, not again and again

    def test_a_quick_restart_causes_no_copying(self, system):
        d, nodes, tick = system
        d.upload("f", os.urandom(400))
        stores_before = {n: len(x.store_calls) for n, x in nodes.items()}
        nodes["N2"].down = True
        tick(10)
        tick(10)                                    # down for a while, but less than GRACE
        nodes["N2"].down = False
        for _ in range(5):
            assert tick(10) is None
        assert {n: len(x.store_calls) for n, x in nodes.items()} == stores_before

    def test_a_second_failure_later_is_repaired_for_too(self, system):
        d, nodes, tick = system
        oid = d.upload("f", os.urandom(400))
        nodes["N1"].down = True
        tick(10)
        tick(GRACE)
        nodes["N3"].down = True
        tick(10)
        assert tick(GRACE) is not None
        assert healthy(d)
        assert all(set(c.replicas) <= {"N2", "N4"} for c in d.get_object(oid).chunks)

    def test_a_repair_that_could_not_finish_is_tried_again_later(self, system):
        d, nodes, tick = system
        oid = d.upload("f", os.urandom(400))
        for n in ("N1", "N2"):
            nodes[n].down = True
        for n in ("N3", "N4"):                      # the two that are left cannot take more
            nodes[n].capacity = nodes[n].used()
        tick(10)
        first = tick(GRACE)
        assert first is not None and first["partial"] + first["unrecoverable"] >= 1
        assert tick(1) is None                      # not immediately again
        nodes["N1"].down = False                    # room again: the next attempt can finish
        nodes["N3"].capacity = nodes["N4"].capacity = 10 ** 9
        second = tick(GRACE)                        # N2 is still down: tried again after another grace period
        assert second is not None and second["partial"] == second["unrecoverable"] == 0
        assert healthy(d) and all("N2" not in c.replicas for c in d.get_object(oid).chunks)

    def test_skipped_while_a_repair_is_running(self, system):
        d, nodes, tick = system
        d.upload("f", os.urandom(400))
        nodes["N1"].down = True
        tick(10)
        with d._repair_lock:
            assert tick(GRACE) is None
        assert tick(1) is not None                  # done as soon as it is free


class TestSwitch:
    def test_off_with_zero(self, cluster):
        assert cluster.app_module.start_auto_repair(0) is False
        assert cluster.app_module.auto_repair["enabled"] is False

    def test_the_report_says_whether_it_is_on(self, cluster):
        r = requests.get(cluster.api + "/replication", timeout=TIMEOUT).json()
        assert set(r["auto_repair"]) == {"enabled", "grace_seconds", "last"}
