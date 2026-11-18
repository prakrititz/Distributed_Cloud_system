"""Reused connections to the workers (issue #24).

The controller keeps a pool of open connections to every worker (one
requests.Session per RemoteNode) and the workers run on waitress, which keeps
connections open, so a request does not pay for a new connection. A worker
that dies must still be noticed, and a restarted one used again.
"""
import threading

import pytest

from config import WORKER_CONNECTIONS
from controller.remote_node import RemoteNode
from helpers.errors import NodeUnavailable
from tests.util import upload_ok


def connections_opened(node: RemoteNode) -> int:
    """How many connections the RemoteNode's pool(s) have opened so far."""
    pools = node.session.get_adapter(node.base_url).poolmanager.pools
    return sum(pools[key].num_connections for key in pools.keys())


class TestThePool:
    def test_each_worker_gets_its_own_pool_of_the_configured_size(self):
        a = RemoteNode("a", "http://127.0.0.1:1")
        b = RemoteNode("b", "http://127.0.0.1:2")
        assert a.session is not b.session
        assert a.session.get_adapter(a.base_url)._pool_maxsize == WORKER_CONNECTIONS


class TestAgainstRealWorkers:
    def test_requests_to_one_worker_reuse_one_connection(self, cluster):
        node = RemoteNode("worker1", cluster.node_url("worker1"))
        for _ in range(50):
            node.health()
        assert connections_opened(node) == 1                # 50 requests, one connection

    def test_chunk_traffic_reuses_connections_too(self, cluster):
        node = RemoteNode("worker1", cluster.node_url("worker1"))
        for i in range(20):
            assert node.store_chunk(f"pool-test-{i}", b"x" * 1000)
            assert node.get_chunk(f"pool-test-{i}") == b"x" * 1000
            node.delete_chunk(f"pool-test-{i}")
        assert connections_opened(node) == 1

    def test_many_threads_at_once_each_get_a_connection(self, cluster):
        node = RemoteNode("worker2", cluster.node_url("worker2"))
        errors = []

        def hammer():
            try:
                for _ in range(10):
                    node.health()
            except Exception as e:                           # pragma: no cover - reported below
                errors.append(e)
        threads = [threading.Thread(target=hammer) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        assert 1 <= connections_opened(node) <= 16          # reused across the 160 requests

    def test_a_dead_worker_is_still_noticed_and_a_restarted_one_reused(self, cluster):
        node = RemoteNode("worker3", cluster.node_url("worker3"))
        node.health()                                       # a pooled connection now exists
        cluster.kill("worker3")
        with pytest.raises(NodeUnavailable):
            node.health()                                   # the pooled connection is dead: noticed
        assert cluster.wait_down("worker3")
        cluster.restart("worker3")
        assert cluster.wait_alive("worker3")
        assert node.health()["node_id"] == "worker3"        # a fresh connection, no error

    def test_a_restarted_worker_can_be_used_as_soon_as_it_counts_as_alive(self, cluster):
        """The worker listens on its port before it registers, so the
        controller is never told 'alive' by a worker it cannot reach yet."""
        oid = upload_ok(cluster, "f", b"y" * 5000)["object_id"]
        cluster.kill("worker4")
        assert cluster.wait_down("worker4")
        cluster.restart("worker4")
        assert cluster.wait_alive("worker4")
        assert cluster.distributor._get_node("worker4").health()["node_id"] == "worker4"
        assert cluster.distributor.replication_report()["summary"]["healthy"] == 1
        assert oid
