"""Parallel chunk transfers within one upload or download (issue #22).

An upload still READS its data in order, one chunk after another, and feeds
the whole-object checksum in that order; what runs in parallel is storing:
the copies of a chunk are written at the same time, and up to
parallel_chunks chunks are being stored at once. A download fetches the next
chunks while the current one is sent, but gives them out strictly in order.
"""
import os
import threading
import time

import pytest
import requests

from helpers.checksum import compute
from helpers.errors import ChunkCorrupt
from tests.util import MB, TIMEOUT, download, upload_ok


def wrap_stores(nodes, before=None, after=None):
    """Call before(node, chunk_id) / after(node, chunk_id) around every store_chunk."""
    for node in nodes:
        real = node.store_chunk

        def store(chunk_id, data, checksum=None, real=real, node=node):
            if before:
                before(node, chunk_id)
            try:
                return real(chunk_id, data, checksum=checksum)
            finally:
                if after:
                    after(node, chunk_id)
        node.store_chunk = store


def index_of(chunk_id: str) -> int:
    return int(chunk_id.rsplit("-", 1)[1])


def stored_anywhere(nodes) -> int:
    return sum(len(n.chunks) for n in nodes)


class TestParallelUpload:
    def test_the_two_copies_of_a_chunk_are_written_at_the_same_time(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100)
        both_started = threading.Barrier(2, timeout=5)      # broken unless two writes overlap
        wrap_stores(nodes, before=lambda node, chunk_id: both_started.wait())
        oid = d.upload("f", os.urandom(80))                  # one chunk, two copies
        assert len(d.get_object(oid).chunks[0].replicas) == 2

    def test_several_chunks_are_stored_at_once_but_never_more_than_allowed(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100, parallel_chunks=3)
        active, most, lock = [0], [0], threading.Lock()

        def before(node, chunk_id):
            with lock:
                active[0] += 1
                most[0] = max(most[0], active[0])
            time.sleep(0.03)

        def after(node, chunk_id):
            with lock:
                active[0] -= 1
        wrap_stores(nodes, before, after)
        data = os.urandom(1200)                              # 12 chunks
        oid = d.upload("f", data)
        assert 2 < most[0] <= 3 * 2                          # more than one chunk's copies, at most 3 chunks
        assert d.download(oid) == data

    def test_chunks_that_finish_out_of_order_give_correct_metadata(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100, parallel_chunks=4)
        finished = []
        wrap_stores(nodes,
                    before=lambda node, chunk_id: time.sleep(0.25 if index_of(chunk_id) == 0 else 0),
                    after=lambda node, chunk_id: finished.append(index_of(chunk_id)))
        data = os.urandom(450)
        oid = d.upload("f", data, checksum=compute(data))
        assert finished.index(0) > finished.index(1)         # chunk 0 really finished after chunk 1
        meta = d.get_object(oid)
        assert [c.index for c in meta.chunks] == [0, 1, 2, 3, 4]
        assert [c.size for c in meta.chunks] == [100, 100, 100, 100, 50]
        assert meta.checksum == compute(data) and d.download(oid) == data

    def test_a_failure_in_one_chunk_leaves_nothing_even_with_others_in_flight(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100, parallel_chunks=4)

        def before(node, chunk_id):
            time.sleep(0.02)
            if index_of(chunk_id) == 5:                       # every node receives chunk 5 damaged
                raise ChunkCorrupt(f"{node.node_id}: chunk 5 damaged in transit")
        wrap_stores(nodes, before=before)
        with pytest.raises(RuntimeError, match="Not enough storage"):
            d.upload("f", os.urandom(1200), source_path="/a/f")
        assert stored_anywhere(nodes) == 0 and d.list_objects() == []
        assert not d._in_flight and ("", "/a/f") not in d.paths

    def test_a_bad_checksum_rolls_back_every_chunk_in_flight(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100, parallel_chunks=4)
        wrap_stores(nodes, before=lambda node, chunk_id: time.sleep(0.01))
        with pytest.raises(Exception, match="damaged in transit"):
            d.upload("f", os.urandom(1000), checksum=compute(b"other"))
        assert stored_anywhere(nodes) == 0 and d.list_objects() == []

    def test_a_dead_planned_node_is_replaced_and_the_space_given_back(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100, parallel_chunks=4)
        for n in nodes:
            d.record_heartbeat(n.node_id, 10 ** 6, 0, 10 ** 6, 0)   # the cache says all are alive
        nodes[0].down = True                                        # ...but N1 just died
        data = os.urandom(800)
        oid = d.upload("f", data)
        assert all("N1" not in c.replicas and len(set(c.replicas)) == 2 for c in d.get_object(oid).chunks)
        assert d.download(oid) == data
        # the space reserved on N1 for its planned copies was given back
        assert d._status["N1"]["free"] == 10 ** 6

    def test_one_at_a_time_still_works(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100, parallel_chunks=1)
        data = os.urandom(777)
        assert d.download(d.upload("f", data)) == data

    def test_several_uploads_at_once_all_succeed(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100, parallel_chunks=4)
        datas = [os.urandom(900 + i) for i in range(8)]
        results = {}

        def go(i):
            results[i] = d.upload(f"f{i}", datas[i])
        threads = [threading.Thread(target=go, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for i, oid in results.items():
            assert d.download(oid) == datas[i]
        held = sum(len(c.replicas) for m in d.list_objects() for c in m.chunks)
        assert held == stored_anywhere(nodes)                 # no copy lost or left behind


class TestParallelDownload:
    def test_a_slow_early_chunk_is_still_given_out_first(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100, parallel_chunks=4)
        data = os.urandom(600)
        oid = d.upload("f", data)
        for n in nodes:
            real = n.get_chunk

            def slow(chunk_id, real=real):
                if index_of(chunk_id) == 1:
                    time.sleep(0.2)                              # chunk 1 arrives after 2, 3, 4
                return real(chunk_id)
            n.get_chunk = slow
        _, chunks = d.stream_object(oid)
        assert b"".join(chunks) == data

    def test_fetches_run_at_the_same_time(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100, parallel_chunks=4)
        oid = d.upload("f", os.urandom(900))
        active, most, lock = [0], [0], threading.Lock()
        for n in nodes:
            real = n.get_chunk

            def slow(chunk_id, real=real):
                with lock:
                    active[0] += 1
                    most[0] = max(most[0], active[0])
                time.sleep(0.03)
                with lock:
                    active[0] -= 1
                return real(chunk_id)
            n.get_chunk = slow
        _, chunks = d.stream_object(oid)
        for _ in chunks:
            time.sleep(0.01)
        assert 1 < most[0] <= 4

    def test_a_bad_chunk_stops_the_download_at_its_turn(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100, parallel_chunks=4)
        data = os.urandom(800)
        oid = d.upload("f", data)
        bad = d.get_object(oid).chunks[2]
        for node_id in bad.replicas:
            next(n for n in nodes if n.node_id == node_id).damage(bad.chunk_id)
        _, chunks = d.stream_object(oid)
        assert next(chunks) + next(chunks) == data[:200]      # chunks 3.. may already be fetched
        with pytest.raises(RuntimeError, match="No good copy of chunk 2"):
            next(chunks)
        assert not d._reading

    def test_an_abandoned_download_fetches_nothing_more(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100, parallel_chunks=2)
        oid = d.upload("f", os.urandom(3000))                # 30 chunks
        fetched = []
        for n in nodes:
            real = n.get_chunk

            def slow(chunk_id, real=real):
                fetched.append(chunk_id)
                time.sleep(0.02)
                return real(chunk_id)
            n.get_chunk = slow
        _, chunks = d.stream_object(oid)
        next(chunks)
        next(chunks)
        chunks.close()
        time.sleep(0.3)
        assert len(fetched) <= 2 + 2 + 1                      # what was taken, plus at most the window
        assert not d._reading


class TestAgainstTheCluster:
    def test_a_multi_chunk_round_trip_over_http(self, cluster):
        data = os.urandom(9 * MB + 123)
        m = upload_ok(cluster, "big.bin", data, checksum=compute(data))
        assert [c["index"] for c in m["chunks"]] == list(range(10))
        assert all(len(set(c["replicas"])) == 2 for c in m["chunks"])
        assert download(cluster, m["object_id"]).content == data
        assert cluster.total_chunk_files() == 20

    def test_raw_uploads_and_downloads_at_once(self, cluster):
        datas = [os.urandom(3 * MB + 1 + i) for i in range(4)]     # 4 chunks each
        results = [None] * 4

        def go(i):
            r = requests.post(cluster.api + "/objects", params={"name": f"f{i}", "checksum": compute(datas[i])},
                              data=datas[i], timeout=TIMEOUT)
            results[i] = r.json()["object_id"]
        threads = [threading.Thread(target=go, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for i, oid in enumerate(results):
            assert download(cluster, oid).content == datas[i]
        assert cluster.total_chunk_files() == 4 * 4 * 2
