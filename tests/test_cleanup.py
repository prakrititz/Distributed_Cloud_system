"""What is really on a worker (orphans, missing chunks) and removing orphans."""
import os

import pytest
import requests

from controller.distributor import Distributor
from controller.metadata_store import JsonFileStore
from tests.fakes import FakeNode
from tests.util import MB, TIMEOUT, upload_ok


@pytest.fixture
def setup(tmp_path):
    nodes = [FakeNode(f"N{i}") for i in range(1, 5)]
    d = Distributor(nodes, chunk_size=100, store=JsonFileStore(str(tmp_path / "m.json")))
    return d, {n.node_id: n for n in nodes}


class TestInventory:
    def test_a_clean_worker(self, setup):
        d, nodes = setup
        d.upload("f", os.urandom(300))
        for n in nodes:
            r = d.worker_inventory(n)
            assert r["held"] == r["expected"] == len(nodes[n].chunks)
            assert r["orphans"] == r["missing"] == r["in_progress"] == []

    def test_a_copy_left_by_a_delete_while_the_worker_was_down_is_an_orphan(self, setup):
        d, nodes = setup
        oid = d.upload("f", os.urandom(100))
        holder = d.get_object(oid).chunks[0].replicas[0]
        nodes[holder].down = True
        assert d.delete(oid) == [(holder, f"{oid}-0")]           # could not be deleted there
        nodes[holder].down = False
        assert [c["chunk_id"] for c in d.worker_inventory(holder)["orphans"]] == [f"{oid}-0"]

    def test_a_lost_chunk_is_missing(self, setup):
        d, nodes = setup
        oid = d.upload("f", os.urandom(100))
        holder = d.get_object(oid).chunks[0].replicas[0]
        nodes[holder].lose(f"{oid}-0")
        assert d.worker_inventory(holder)["missing"] == [f"{oid}-0"]

    def test_chunks_of_an_upload_in_progress_are_not_orphans(self, setup):
        d, nodes = setup
        seen = []

        def look(chunk_id):        # called by the node right after it stored a chunk, mid-upload
            if not seen:
                node = next(n for n in nodes.values() if chunk_id in n.chunks)
                seen.append(d.worker_inventory(node.node_id))
                seen.append(d.cleanup_orphans(min_age=0))
        for n in nodes.values():
            n.on_store = look
        oid = d.upload("f", os.urandom(300))
        report, cleanup = seen
        assert report["orphans"] == [] and [c["chunk_id"] for c in report["in_progress"]] == [f"{oid}-0"]
        assert cleanup["deleted"] == []
        assert d.download(oid)                                    # nothing was taken away

    def test_unknown_worker(self, setup):
        d, _ = setup
        with pytest.raises(KeyError):
            d.worker_inventory("nobody")


class TestCleanup:
    def make_orphan(self, d, nodes):
        oid = d.upload("f", os.urandom(100))
        holder = d.get_object(oid).chunks[0].replicas[0]
        nodes[holder].down = True
        d.delete(oid)
        nodes[holder].down = False
        return holder, f"{oid}-0"

    def test_orphans_are_deleted_and_nothing_else(self, setup):
        d, nodes = setup
        keep = d.upload("keep", os.urandom(300))
        holder, orphan = self.make_orphan(d, nodes)
        r = d.cleanup_orphans(min_age=0)
        assert r["deleted"] == [{"node": holder, "chunk_id": orphan}]
        assert orphan not in nodes[holder].chunks
        assert d.download(keep)
        assert d.cleanup_orphans(min_age=0)["deleted"] == []      # nothing left to do

    def test_young_chunks_are_kept(self, setup):
        d, nodes = setup
        holder, orphan = self.make_orphan(d, nodes)
        nodes[holder].ages[orphan] = 5.0
        r = d.cleanup_orphans(min_age=60)
        assert r["deleted"] == [] and r["kept_young"] == [{"node": holder, "chunk_id": orphan}]
        assert orphan in nodes[holder].chunks

    def test_copies_dropped_by_repair_while_the_worker_was_down_are_cleaned_up_once_it_is_back(self, setup):
        d, nodes = setup
        oid = d.upload("f", os.urandom(300))
        victim = d.get_object(oid).chunks[0].replicas[0]
        on_victim = [c.chunk_id for c in d.get_object(oid).chunks if victim in c.replicas]
        nodes[victim].down = True
        d.repair()                                                # new copies elsewhere
        nodes[victim].down = False                                # it comes back with its old copies
        d.record_heartbeat(victim, 10 ** 9, 0, 10 ** 9, 0)
        assert sorted(c["chunk_id"] for c in d.worker_inventory(victim)["orphans"]) == sorted(on_victim)
        d.cleanup_orphans(min_age=0)
        assert nodes[victim].chunks == {}
        assert d.download(oid)

    def test_a_down_worker_is_skipped(self, setup):
        d, nodes = setup
        d.upload("f", os.urandom(100))
        nodes["N1"].down = True
        assert "N1" in d.cleanup_orphans(min_age=0)["unreachable"]

    def test_refused_when_the_metadata_is_not_saved(self):
        d = Distributor([FakeNode("N1"), FakeNode("N2")], chunk_size=100)
        with pytest.raises(RuntimeError, match="METADATA_PATH"):
            d.cleanup_orphans()

    def test_refused_during_a_repair(self, setup):
        d, _ = setup
        with d._repair_lock:
            with pytest.raises(RuntimeError, match="already running"):
                d.cleanup_orphans()


class TestEndpoints:
    def test_the_worker_lists_its_chunks(self, cluster):
        m = upload_ok(cluster, "f", os.urandom(MB + 10))
        holder = m["chunks"][0]["replicas"][0]
        r = requests.get(cluster.node_url(holder) + "/internal/chunks", timeout=TIMEOUT).json()
        listed = {c["chunk_id"]: c for c in r["chunks"]}
        assert m["chunks"][0]["chunk_id"] in listed and listed[m["chunks"][0]["chunk_id"]]["size"] == MB
        assert all(c["age"] >= 0 for c in r["chunks"])

    def test_the_controller_compares_it_with_the_metadata(self, cluster):
        m = upload_ok(cluster, "f", os.urandom(10))
        holder = m["chunks"][0]["replicas"][0]
        r = requests.get(f"{cluster.root}/internal/workers/{holder}/chunks", timeout=TIMEOUT)
        assert r.status_code == 200
        assert r.json()["held"] == r.json()["expected"] == 1 and r.json()["orphans"] == []

    def test_unknown_and_unreachable_workers(self, cluster):
        assert requests.get(f"{cluster.root}/internal/workers/nobody/chunks", timeout=TIMEOUT).status_code == 404
        cluster.kill("worker3")
        assert requests.get(f"{cluster.root}/internal/workers/worker3/chunks", timeout=TIMEOUT).status_code == 503

    def test_cleanup_end_to_end(self, cluster):
        m = upload_ok(cluster, "f", os.urandom(1000))
        holder = m["chunks"][0]["replicas"][0]
        cluster.kill(holder)
        assert cluster.wait_down(holder)
        requests.delete(f"{cluster.api}/objects/{m['object_id']}", timeout=TIMEOUT)
        cluster.restart(holder)
        assert cluster.wait_alive(holder)
        assert cluster.chunk_files(holder) == [m["chunks"][0]["chunk_id"]]          # the orphan
        kept = requests.post(cluster.api + "/cleanup", timeout=TIMEOUT).json()    # default: it is too young
        assert kept["deleted"] == [] and len(kept["kept_young"]) == 1
        r = requests.post(cluster.api + "/cleanup?min_age=0", timeout=TIMEOUT)
        assert r.status_code == 200 and r.json()["deleted"] == [{"node": holder, "chunk_id": m["chunks"][0]["chunk_id"]}]
        assert cluster.chunk_files(holder) == []

    @pytest.mark.parametrize("bad", ["-1", "soon"])
    def test_bad_min_age(self, cluster, bad):
        assert requests.post(f"{cluster.api}/cleanup?min_age={bad}", timeout=TIMEOUT).status_code == 400
