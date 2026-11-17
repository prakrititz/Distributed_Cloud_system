import os

import pytest

from tests.cluster import Cluster
from tests.fakes import FakeNode


# --------------------------------------------------------------------------
# real cluster (integration tests): one for the whole session, reset per test
# --------------------------------------------------------------------------
@pytest.fixture(scope="session")
def _cluster_session(tmp_path_factory):
    cluster = Cluster(str(tmp_path_factory.mktemp("cluster")))
    cluster.start()
    yield cluster
    cluster.stop()
    cluster.cleanup_dirs()


@pytest.fixture
def cluster(_cluster_session):
    """The real cluster, clean at the start and at the end of every test."""
    _cluster_session.reset()
    yield _cluster_session
    _cluster_session.reset()


def pytest_collection_modifyitems(items):
    for item in items:
        if "cluster" in getattr(item, "fixturenames", ()):
            item.add_marker(pytest.mark.cluster)


# --------------------------------------------------------------------------
# helpers for the distributor unit tests
# --------------------------------------------------------------------------
@pytest.fixture
def make_distributor():
    """make_distributor(n=4, chunk_size=100, **kwargs) -> (distributor, [FakeNode, ...])"""
    from controller.distributor import Distributor

    def make(n=4, chunk_size=100, capacity=10 ** 9, **kwargs):
        nodes = [FakeNode(f"N{i + 1}", capacity) for i in range(n)]
        return Distributor(nodes, chunk_size=chunk_size, **kwargs), nodes

    return make


@pytest.fixture
def clock(monkeypatch):
    """Controllable time for the heartbeat table: clock.now moves only when told."""
    import controller.distributor as dist

    class Clock:
        now = 1000.0

        def monotonic(self):
            return self.now

        def advance(self, seconds):
            self.now += seconds

    c = Clock()
    monkeypatch.setattr(dist, "time", c)
    return c


@pytest.fixture
def files(tmp_path):
    """files('dir/name.ext', b'content') -> absolute path of a new file."""
    def make(relative, content):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return str(path)
    return make


def random_bytes(n: int) -> bytes:
    return os.urandom(n)
