"""The chunk cache (controller/chunk_cache.py): chunks downloaded once are
kept in the controller's memory, so downloading them again asks no worker.

- the cache itself: least recently used out first, within its byte limit
- with the distributor: a hit asks no worker; only checked data is cached;
  a delete empties it (also a delete during a download); no-cache reads the
  workers; a cached chunk survives its workers being down
- through the API: X-Cache, Cache-Control: no-cache, GET / DELETE /cache
"""
import threading

import pytest
import requests

from controller.chunk_cache import ChunkCache
from helpers.checksum import compute
from tests.test_client_dashboard import bearer, secured  # noqa: F401  (secured: the fixture)
from tests.util import MB, TIMEOUT, upload_ok


# ======================================================================== the cache
class TestTheCache:
    def test_least_recently_used_goes_first(self):
        c = ChunkCache(30)
        c.put("a", b"x" * 10)
        c.put("b", b"x" * 10)
        c.put("c", b"x" * 10)
        assert c.get("a") is not None                 # a is now the most recently used
        c.put("d", b"x" * 10)                         # room for d: b goes, not a
        assert "b" not in c and all(k in c for k in "acd")
        assert c.stats()["used_bytes"] == 30 and c.stats()["evictions"] == 1

    def test_never_over_its_limit(self):
        c = ChunkCache(25)
        for i in range(20):
            c.put(str(i), b"x" * (i % 7 + 1))
            assert c.stats()["used_bytes"] <= 25
        c.put("huge", b"x" * 26)                      # bigger than the whole cache: not kept
        assert "huge" not in c

    def test_putting_the_same_chunk_again_counts_it_once(self):
        c = ChunkCache(100)
        c.put("a", b"x" * 10)
        c.put("a", b"x" * 10)
        assert c.stats()["used_bytes"] == 10 and c.stats()["chunks"] == 1

    def test_hits_misses_and_rate(self):
        c = ChunkCache(100)
        assert c.stats()["hit_rate"] is None
        c.put("a", b"1")
        c.get("a"), c.get("a"), c.get("nope")
        s = c.stats()
        assert (s["hits"], s["misses"], s["hit_rate"]) == (2, 1, 0.667)
        assert "a" in c and c.stats()["hits"] == 2   # 'in' is not a lookup

    def test_discard_and_clear(self):
        c = ChunkCache(100)
        for k in "abc":
            c.put(k, b"12")
        assert c.discard(["a", "zzz"]) == 1 and "a" not in c and c.stats()["used_bytes"] == 4
        c.get("b")
        assert c.clear() == 2 and c.stats()["used_bytes"] == 0 and c.stats()["hits"] == 1   # counters stay

    def test_size_zero_is_off(self):
        c = ChunkCache(0)
        c.put("a", b"1")
        assert not c.enabled and c.get("a") is None and "a" not in c and c.stats()["misses"] == 0

    def test_many_threads_at_once(self):
        c = ChunkCache(64 * 50)

        def work(n):
            for i in range(300):
                key = f"{(n * 7 + i) % 120}"
                if c.get(key) is None:
                    c.put(key, bytes([n]) * 64)
        threads = [threading.Thread(target=work, args=(n,)) for n in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        s = c.stats()
        assert s["used_bytes"] == s["chunks"] * 64 <= c.capacity
        assert s["hits"] + s["misses"] == 8 * 300


# ================================================================ with the distributor
def counting(nodes):
    """Count each node's get_chunk calls (calls[node_id])."""
    calls = {n.node_id: 0 for n in nodes}
    for n in nodes:
        real = n.get_chunk

        def get_chunk(chunk_id, node=n, real=real):
            calls[node.node_id] += 1
            return real(chunk_id)
        n.get_chunk = get_chunk
    return calls


@pytest.fixture
def cached(make_distributor):
    """A distributor with a cache, 4 fake workers, 100-byte chunks."""
    d, nodes = make_distributor(chunk_size=100, cache=ChunkCache(10_000))
    return d, nodes, counting(nodes)


class TestWithTheDistributor:
    def test_a_second_download_asks_no_worker(self, cached):
        d, nodes, calls = cached
        data = bytes(range(256)) * 2                              # 6 chunks
        oid = d.upload("f.bin", data)
        assert sum(calls.values()) == 0 and d.cache.stats()["chunks"] == 0   # uploads are not cached
        assert d.download(oid) == data
        first = sum(calls.values())
        assert first == 6 and d.cache.stats()["chunks"] == 6
        assert d.download(oid) == data
        assert sum(calls.values()) == first and d.cache.stats()["hits"] == 6

    def test_a_damaged_copy_is_never_cached(self, cached):
        d, nodes, _ = cached
        oid = d.upload("f.bin", b"a" * 100)
        info = d.get_object(oid).chunks[0]
        first = next(n for n in nodes if n.node_id == info.replicas[0])
        first.damage(info.chunk_id)
        assert d.download(oid) == b"a" * 100                     # the good copy, from the second worker
        assert compute(d.cache.get(info.chunk_id)) == info.checksum

    def test_a_cached_chunk_survives_its_workers_going_down(self, cached):
        d, nodes, _ = cached
        oid = d.upload("f.bin", b"b" * 250)
        d.download(oid)
        for n in nodes:
            n.down = True
        assert d.download(oid) == b"b" * 250                     # every chunk from memory
        with pytest.raises(RuntimeError):
            d.stream_object(oid, use_cache=False)                # without the cache: no live copy

    def test_no_cache_reads_the_workers(self, cached):
        d, nodes, calls = cached
        oid = d.upload("f.bin", b"c" * 300)
        d.download(oid)
        before = sum(calls.values())
        _, chunks = d.stream_object(oid, use_cache=False)
        with chunks:
            assert b"".join(chunks) == b"c" * 300
        assert sum(calls.values()) == before + 3

    def test_deleting_empties_the_cache_of_that_object(self, cached):
        d, _, _ = cached
        keep, gone = d.upload("k.bin", b"k" * 200), d.upload("g.bin", b"g" * 200)
        d.download(keep), d.download(gone)
        d.delete(gone)
        cached_ids = {i.chunk_id for i in d.get_object(keep).chunks}
        assert d.cache.stats()["chunks"] == 2 and all(c in d.cache for c in cached_ids)

    def test_a_download_running_during_a_delete_does_not_bring_chunks_back(self, cached):
        d, _, _ = cached
        oid = d.upload("f.bin", bytes(range(200)) * 3)             # 6 chunks
        _, chunks = d.stream_object(oid)
        with chunks:
            next(iter(chunks))                                    # under way
            d.delete(oid)                                         # deleted meanwhile
            rest = b"".join(chunks)                               # the download still finishes
        assert len(rest) == 500
        assert d.cache.stats()["chunks"] == 0                     # and nothing of it stays cached

    def test_cache_state(self, cached):
        d, _, _ = cached
        oid = d.upload("f.bin", b"s" * 300)
        meta = d.get_object(oid)
        assert d.cache_state(meta) == "MISS"
        _, chunks = d.stream_object(oid)
        next(iter(chunks))
        chunks.close()
        assert d.cache_state(meta) in ("PARTIAL", "HIT")
        d.download(oid)
        assert d.cache_state(meta) == "HIT"

    def test_off_by_default(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100)
        calls = counting(nodes)
        oid = d.upload("f.bin", b"x" * 200)
        d.download(oid), d.download(oid)
        assert sum(calls.values()) == 4 and not d.cache.enabled


# ========================================================================= the API
@pytest.fixture
def with_cache(cluster, monkeypatch):
    cache = ChunkCache(64 * MB)
    monkeypatch.setattr(cluster.distributor, "cache", cache)
    return cluster


def get(cluster, oid, **headers):
    return requests.get(f"{cluster.api}/objects/{oid}", headers=headers, timeout=TIMEOUT)


class TestThroughTheApi:
    def test_x_cache_says_where_it_came_from(self, with_cache):
        data = bytes(range(256)) * (10 * 1024)                    # 2.5 MB: 3 chunks
        oid = upload_ok(with_cache, "f.bin", data)["object_id"]
        first, second = get(with_cache, oid), get(with_cache, oid)
        assert (first.headers["X-Cache"], second.headers["X-Cache"]) == ("MISS", "HIT")
        assert first.content == second.content == data
        fresh = get(with_cache, oid, **{"Cache-Control": "no-cache"})
        assert fresh.headers["X-Cache"] == "BYPASS" and fresh.content == data
        assert get(with_cache, oid, Pragma="no-cache").headers["X-Cache"] == "BYPASS"

    def test_the_stats_and_emptying_the_cache(self, with_cache):
        oid = upload_ok(with_cache, "f.bin", b"z" * (MB + 10))["object_id"]
        get(with_cache, oid), get(with_cache, oid)
        s = requests.get(with_cache.api + "/cache", timeout=TIMEOUT).json()
        assert s["enabled"] and s["chunks"] == 2 and s["hits"] == 2 and s["used_bytes"] == MB + 10
        r = requests.delete(with_cache.api + "/cache", timeout=TIMEOUT).json()
        assert r["dropped"] == 2 and r["chunks"] == 0
        assert get(with_cache, oid).headers["X-Cache"] == "MISS"

    def test_a_chunk_damaged_on_disk_after_caching_is_still_found_by_repair(self, with_cache):
        oid = upload_ok(with_cache, "f.bin", b"r" * 500)["object_id"]
        get(with_cache, oid)
        info = with_cache.distributor.get_object(oid).chunks[0]
        with_cache.damage_chunk(info.replicas[0], info.chunk_id)
        assert get(with_cache, oid).content == b"r" * 500                    # from the cache
        report = requests.get(with_cache.api + "/replication", timeout=TIMEOUT).json()
        assert report["summary"]["under_replicated"] == 1                     # the disks are checked, not the cache
        fresh = get(with_cache, oid, **{"Cache-Control": "no-cache"})
        assert fresh.status_code == 200 and fresh.content == b"r" * 500       # the other copy

    def test_a_cache_of_size_zero_says_off(self, cluster, monkeypatch):
        monkeypatch.setattr(cluster.distributor, "cache", ChunkCache(0))
        oid = upload_ok(cluster, "f.bin", b"o" * 10)["object_id"]
        assert get(cluster, oid).headers["X-Cache"] == "OFF"
        assert requests.get(cluster.api + "/cache", timeout=TIMEOUT).json()["enabled"] is False

    def test_only_admins_see_or_empty_the_cache(self, secured):
        for method in ("GET", "DELETE"):
            assert requests.request(method, secured.api + "/cache", headers=bearer("alice"),
                                    timeout=TIMEOUT).status_code == 403
            assert requests.request(method, secured.api + "/cache", headers=bearer("admin"),
                                    timeout=TIMEOUT).status_code == 200
