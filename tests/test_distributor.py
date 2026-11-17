"""controller/distributor.py, tested against in-memory fake nodes: fast,
deterministic, with every failure made to order and every call counted."""
import os

import pytest

import controller.distributor as dist
from helpers.checksum import compute


def chunks_of(d, object_id):
    return sorted(d.get_object(object_id).chunks, key=lambda c: c.index)


def nodes_holding(nodes, chunk_id):
    return sorted(n.node_id for n in nodes if chunk_id in n.chunks)


def all_chunk_ids(nodes):
    return {cid for n in nodes for cid in n.chunks}


# ===================================================================== upload
class TestUploadAndDownload:
    def test_round_trip_over_several_chunks(self, make_distributor):
        d, _ = make_distributor(chunk_size=100)
        data = os.urandom(1050)
        oid = d.upload("a.bin", data)
        assert d.download(oid) == data
        meta = d.get_object(oid)
        assert meta.size == 1050 and len(meta.chunks) == 11
        assert meta.checksum == compute(data)

    @pytest.mark.parametrize("size,expected_chunks", [(0, 1), (1, 1), (99, 1), (100, 1), (101, 2), (200, 2), (201, 3)])
    def test_chunk_boundaries(self, make_distributor, size, expected_chunks):
        d, _ = make_distributor(chunk_size=100)
        data = os.urandom(size)
        oid = d.upload("f", data)
        assert len(d.get_object(oid).chunks) == expected_chunks
        assert d.download(oid) == data

    def test_only_the_last_chunk_may_be_smaller(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100)
        oid = d.upload("f", os.urandom(250))
        sizes = [len(nodes[0].chunks.get(c.chunk_id) or next(n.chunks[c.chunk_id] for n in nodes if c.chunk_id in n.chunks))
                 for c in chunks_of(d, oid)]
        assert sizes == [100, 100, 50]

    def test_every_chunk_has_a_unique_id_and_a_position(self, make_distributor):
        d, _ = make_distributor()
        oid = d.upload("f", os.urandom(450))
        chunks = chunks_of(d, oid)
        assert [c.index for c in chunks] == [0, 1, 2, 3, 4]
        assert [c.chunk_id for c in chunks] == [f"{oid}-{i}" for i in range(5)]
        assert len({c.chunk_id for c in chunks}) == 5

    def test_every_chunk_is_stored_on_two_different_nodes(self, make_distributor):
        d, nodes = make_distributor()
        oid = d.upload("f", os.urandom(1000))
        for c in chunks_of(d, oid):
            assert len(c.replicas) == 2 and len(set(c.replicas)) == 2
            assert nodes_holding(nodes, c.chunk_id) == sorted(c.replicas)

    def test_each_chunk_records_the_checksum_of_its_bytes(self, make_distributor):
        d, nodes = make_distributor()
        data = os.urandom(300)
        oid = d.upload("f", data)
        for i, c in enumerate(chunks_of(d, oid)):
            assert c.checksum == compute(data[i * 100:(i + 1) * 100])

    def test_chunks_are_spread_across_all_nodes(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100)
        d.upload("f", os.urandom(800))                   # 8 chunks, 16 copies, 4 nodes
        assert [len(n.chunks) for n in nodes] == [4, 4, 4, 4]

    def test_metadata_fields(self, make_distributor, monkeypatch):
        monkeypatch.setattr(dist, "_now", lambda: "2030-05-06T07:08:09Z")
        d, _ = make_distributor()
        oid = d.upload("movie.mp4", os.urandom(120), content_type="video/mp4",
                       custom={"project": "demo"}, source_path="/a/movie.mp4", size=99, client_id="c1")
        m = d.get_object(oid)
        assert (m.name, m.content_type, m.custom, m.size) == ("movie.mp4", "video/mp4", {"project": "demo"}, 99)
        assert (m.source_path, m.client_id) == ("/a/movie.mp4", "c1")
        assert m.created_at == m.modified_at == "2030-05-06T07:08:09Z"

    def test_defaults(self, make_distributor):
        d, _ = make_distributor()
        data = os.urandom(10)
        m = d.get_object(d.upload("f", data))
        assert m.content_type == "application/octet-stream" and m.custom == {} and m.size == 10

    def test_the_custom_dict_is_copied(self, make_distributor):
        d, _ = make_distributor()
        custom = {"a": "1"}
        oid = d.upload("f", b"x", custom=custom)
        custom["a"] = "changed"
        assert d.get_object(oid).custom == {"a": "1"}

    def test_object_ids_are_unique(self, make_distributor):
        d, _ = make_distributor()
        assert len({d.upload("same", b"x") for _ in range(20)}) == 20


# ============================================================ upload failures
class TestUploadFailures:
    def test_if_every_node_refuses_writes_nothing_is_left(self, make_distributor):
        d, nodes = make_distributor()
        for n in nodes:
            n.refuse_writes = True
        with pytest.raises(RuntimeError, match="Not enough storage"):
            d.upload("f", os.urandom(300))
        assert all_chunk_ids(nodes) == set() and d.list_objects() == []

    def test_one_live_node_is_not_enough_and_the_first_copy_is_rolled_back(self, make_distributor):
        d, nodes = make_distributor(n=3)
        nodes[1].down = nodes[2].down = True
        with pytest.raises(RuntimeError, match="Not enough storage"):
            d.upload("f", os.urandom(300))
        assert all_chunk_ids(nodes) == set() and d.list_objects() == []

    def test_a_failure_halfway_removes_the_chunks_already_written(self, make_distributor):
        d, nodes = make_distributor(n=2, chunk_size=100, capacity=250)    # room for 2 chunks per node
        with pytest.raises(RuntimeError, match="Not enough storage"):
            d.upload("f", os.urandom(500))                                # needs 5 chunks x 2 copies
        assert all_chunk_ids(nodes) == set() and d.list_objects() == []
        assert [n.used() for n in nodes] == [0, 0]

    def test_a_full_node_is_skipped_and_the_next_one_used(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100)
        nodes[0].capacity = 50                                            # can never fit a 100-byte chunk
        d.record_heartbeat("N1", 10 ** 9, 0, 10 ** 9, 0)                  # the cache wrongly says it has room
        oid = d.upload("f", os.urandom(300))
        assert "N1" not in {r for c in chunks_of(d, oid) for r in c.replicas}
        assert d.download(oid) is not None

    def test_a_dead_node_found_by_a_failed_write_is_skipped_and_marked_down(self, make_distributor):
        d, nodes = make_distributor()
        for n in nodes:
            d.record_heartbeat(n.node_id, 10 ** 9, 0, 10 ** 9, 0)         # the cache says all are alive
        nodes[0].down = True                                              # ...but N1 just died
        oid = d.upload("f", os.urandom(400))
        assert "N1" not in {r for c in chunks_of(d, oid) for r in c.replicas}
        assert d._cached_state(nodes[0])[0] is False                      # marked down at once
        assert d.download(oid) is not None

    def test_a_node_that_damages_chunks_in_transit_is_skipped(self, make_distributor):
        d, nodes = make_distributor()
        nodes[0].refuse_writes = True
        oid = d.upload("f", os.urandom(400))
        assert "N1" not in {r for c in chunks_of(d, oid) for r in c.replicas}

    def test_failed_uploads_do_not_register_the_path(self, make_distributor):
        d, nodes = make_distributor(n=2)
        nodes[1].down = True
        with pytest.raises(RuntimeError):
            d.upload("f", b"x" * 10, source_path="/a/f", client_id="c1")
        nodes[1].down = False
        d._mark_down("N2")
        d.record_heartbeat("N2", 10 ** 9, 0, 10 ** 9, 0)
        d.upload("f", b"x" * 10, source_path="/a/f", client_id="c1")      # allowed: the first never happened


# =============================================================== path rules
class TestPathRules:
    def test_same_name_in_different_directories_is_two_objects(self, make_distributor):
        d, _ = make_distributor()
        a = d.upload("report.txt", b"A", source_path="/a/report.txt", client_id="c1")
        b = d.upload("report.txt", b"B", source_path="/b/report.txt", client_id="c1")
        assert a != b and d.download(a) == b"A" and d.download(b) == b"B"

    def test_same_path_from_the_same_client_is_refused(self, make_distributor):
        d, nodes = make_distributor()
        first = d.upload("f", b"A", source_path="/a/f", client_id="c1")
        before = all_chunk_ids(nodes)
        with pytest.raises(FileExistsError) as e:
            d.upload("f", b"A", source_path="/a/f", client_id="c1")
        assert first in str(e.value) and "c1" in str(e.value) and "/a/f" in str(e.value)
        assert all_chunk_ids(nodes) == before and len(d.list_objects()) == 1     # nothing stored

    def test_the_same_path_from_different_clients_is_allowed(self, make_distributor):
        d, _ = make_distributor()
        a = d.upload("f", b"A", source_path="/files/f", client_id="clientA")
        b = d.upload("f", b"B", source_path="/files/f", client_id="clientB")
        assert a != b
        assert d.get_object(a).client_id == "clientA" and d.get_object(b).client_id == "clientB"
        with pytest.raises(FileExistsError):
            d.upload("f", b"A2", source_path="/files/f", client_id="clientA")
        with pytest.raises(FileExistsError):
            d.upload("f", b"B2", source_path="/files/f", client_id="clientB")

    def test_without_a_path_nothing_is_ever_refused(self, make_distributor):
        d, _ = make_distributor()
        ids = {d.upload("f", b"x", client_id="c1") for _ in range(5)}
        assert len(ids) == 5

    def test_without_a_client_id_there_is_one_shared_scope(self, make_distributor):
        d, _ = make_distributor()
        d.upload("f", b"x", source_path="/a/f")
        with pytest.raises(FileExistsError):
            d.upload("f", b"x", source_path="/a/f")
        d.upload("f", b"x", source_path="/a/f", client_id="c1")           # a named client is a different scope

    def test_delete_frees_the_path_for_that_client_only(self, make_distributor):
        d, _ = make_distributor()
        a = d.upload("f", b"A", source_path="/p", client_id="c1")
        b = d.upload("f", b"B", source_path="/p", client_id="c2")
        d.delete(a)
        d.upload("f", b"A2", source_path="/p", client_id="c1")           # c1 can upload again
        with pytest.raises(FileExistsError):
            d.upload("f", b"B2", source_path="/p", client_id="c2")      # c2's is still there
        assert d.download(b) == b"B"


# ============================================================== download
class TestDownloadFallback:
    @pytest.fixture
    def stored(self, make_distributor):
        d, nodes = make_distributor()
        data = os.urandom(400)
        oid = d.upload("f", data)
        return d, nodes, oid, data

    def test_unknown_object(self, stored):
        d, *_ = stored
        with pytest.raises(KeyError):
            d.download("no-such-id")

    def test_the_first_copy_being_down_is_survivable(self, stored):
        d, nodes, oid, data = stored
        for n in nodes:
            if n.node_id == chunks_of(d, oid)[0].replicas[0]:
                n.down = True
        assert d.download(oid) == data

    def test_a_damaged_copy_is_skipped(self, stored):
        d, nodes, oid, data = stored
        c = chunks_of(d, oid)[0]
        next(n for n in nodes if n.node_id == c.replicas[0]).damage(c.chunk_id)
        assert d.download(oid) == data

    def test_a_lost_copy_is_skipped(self, stored):
        d, nodes, oid, data = stored
        c = chunks_of(d, oid)[1]
        next(n for n in nodes if n.node_id == c.replicas[0]).lose(c.chunk_id)
        assert d.download(oid) == data

    def test_a_node_that_serves_wrong_bytes_without_noticing_is_caught_by_the_recorded_checksum(self, stored):
        d, nodes, oid, data = stored
        c = chunks_of(d, oid)[0]
        bad = next(n for n in nodes if n.node_id == c.replicas[0])
        bad.chunks[c.chunk_id] = b"Z" * 100
        bad.sums[c.chunk_id] = compute(b"Z" * 100)       # the node's own checksum agrees with its wrong bytes
        assert d.download(oid) == data                    # the distributor's record is the authority

    def test_both_copies_bad_gives_a_clear_error_and_no_data(self, stored):
        d, nodes, oid, _ = stored
        c = chunks_of(d, oid)[0]
        holders = [n for n in nodes if n.node_id in c.replicas]
        holders[0].damage(c.chunk_id)
        holders[1].down = True
        with pytest.raises(RuntimeError, match="No good copy of chunk 0") as e:
            d.download(oid)
        assert "corrupt" in str(e.value) and "down" in str(e.value)

    def test_the_whole_object_checksum_is_the_last_line_of_defense(self, stored):
        d, _, oid, _ = stored
        d.get_object(oid).checksum = "0" * 64
        with pytest.raises(RuntimeError, match="whole-object checksum"):
            d.download(oid)

    def test_nodes_known_to_be_down_are_not_waited_for(self, stored):
        d, nodes, oid, data = stored
        c = chunks_of(d, oid)[0]
        dead = next(n for n in nodes if n.node_id == c.replicas[0])
        dead.down = True
        d._mark_down(dead.node_id)
        calls = []
        orig = dead.get_chunk
        dead.get_chunk = lambda cid: (calls.append(cid), orig(cid))[1]
        assert d.download(oid) == data
        assert calls == []                                # the live copy was read first, the dead node never asked


# ================================================================== metadata
class TestMetadataOperations:
    def test_update_adds_changes_and_removes_keys(self, make_distributor, monkeypatch):
        d, nodes = make_distributor()
        monkeypatch.setattr(dist, "_now", lambda: "2030-01-01T00:00:00Z")
        oid = d.upload("f", b"data", custom={"a": "1", "b": "2"})
        before = {n.node_id: dict(n.chunks) for n in nodes}
        monkeypatch.setattr(dist, "_now", lambda: "2030-01-02T00:00:00Z")
        m = d.update_custom_metadata(oid, {"a": "changed", "c": 3, "b": None, "never-existed": None})
        assert m.custom == {"a": "changed", "c": 3}
        assert m.created_at == "2030-01-01T00:00:00Z" and m.modified_at == "2030-01-02T00:00:00Z"
        assert {n.node_id: dict(n.chunks) for n in nodes} == before        # the data was not touched
        assert d.download(oid) == b"data"

    def test_update_unknown_object(self, make_distributor):
        d, _ = make_distributor()
        with pytest.raises(KeyError):
            d.update_custom_metadata("nope", {"a": 1})

    def test_get_object_unknown(self, make_distributor):
        d, _ = make_distributor()
        with pytest.raises(KeyError):
            d.get_object("nope")

    def test_list_is_oldest_first(self, make_distributor, monkeypatch):
        d, _ = make_distributor()
        stamps = iter(["2030-01-03T00:00:00Z", "2030-01-01T00:00:00Z", "2030-01-02T00:00:00Z"])
        monkeypatch.setattr(dist, "_now", lambda: next(stamps))
        ids = [d.upload(f"f{i}", b"x") for i in range(3)]
        assert [m.object_id for m in d.list_objects()] == [ids[1], ids[2], ids[0]]

    def test_delete_removes_every_copy(self, make_distributor):
        d, nodes = make_distributor()
        keep = d.upload("keep", os.urandom(200))
        gone = d.upload("gone", os.urandom(350))
        gone_ids = {c.chunk_id for c in chunks_of(d, gone)}
        assert d.delete(gone) == []
        assert all_chunk_ids(nodes).isdisjoint(gone_ids)
        with pytest.raises(KeyError):
            d.get_object(gone)
        assert d.download(keep) is not None

    def test_delete_unknown_object(self, make_distributor):
        d, _ = make_distributor()
        with pytest.raises(KeyError):
            d.delete("nope")

    def test_delete_with_a_node_down_reports_the_copies_left_behind(self, make_distributor):
        d, nodes = make_distributor()
        oid = d.upload("f", os.urandom(300))
        victim = chunks_of(d, oid)[0].replicas[0]
        next(n for n in nodes if n.node_id == victim).down = True
        left = d.delete(oid)
        assert left and all(node_id == victim for node_id, _ in left)
        with pytest.raises(KeyError):
            d.get_object(oid)


# ================================================================ heartbeats
class TestHeartbeatTable:
    def test_unknown_node(self, make_distributor):
        d, _ = make_distributor()
        with pytest.raises(KeyError):
            d.record_heartbeat("ghost", 1, 0, 1, 0)

    def test_alive_until_the_timeout_then_down(self, make_distributor, clock):
        d, nodes = make_distributor(heartbeat_timeout=6)
        d.record_heartbeat("N1", 1000, 0, 1000, 0)
        assert d._cached_state(nodes[0])[0] is True
        clock.advance(5.9)
        assert d._cached_state(nodes[0])[0] is True
        clock.advance(0.2)
        assert d._cached_state(nodes[0])[0] is False

    def test_a_new_heartbeat_revives_a_silent_node(self, make_distributor, clock):
        d, nodes = make_distributor(heartbeat_timeout=6)
        d.record_heartbeat("N1", 1000, 0, 1000, 0)
        clock.advance(10)
        assert d._cached_state(nodes[0])[0] is False
        d.record_heartbeat("N1", 1000, 0, 1000, 0)
        assert d._cached_state(nodes[0])[0] is True

    def test_heartbeat_age(self, make_distributor, clock):
        d, _ = make_distributor()
        assert d.heartbeat_age("N1") is None
        d.record_heartbeat("N1", 1000, 0, 1000, 0)
        clock.advance(2.5)
        assert d.heartbeat_age("N1") == 2.5

    def test_mark_down_is_immediate_and_a_heartbeat_undoes_it(self, make_distributor):
        d, nodes = make_distributor()
        d.record_heartbeat("N1", 1000, 0, 1000, 0)
        d._mark_down("N1")
        assert d._cached_state(nodes[0])[0] is False and d.heartbeat_age("N1") is None
        d.record_heartbeat("N1", 1000, 0, 1000, 0)
        assert d._cached_state(nodes[0])[0] is True

    def test_reserve_lowers_the_cached_free_space_and_never_below_zero(self, make_distributor):
        d, nodes = make_distributor()
        d.record_heartbeat("N1", 1000, 0, 1000, 0)
        d._reserve("N1", 300)
        assert d._cached_state(nodes[0])[1] == 700
        d._reserve("N1", 10_000)
        assert d._cached_state(nodes[0])[1] == 0
        d._reserve("ghost", 5)                       # no entry: ignored, no error

    def test_the_next_heartbeat_replaces_the_estimate(self, make_distributor):
        d, nodes = make_distributor()
        d.record_heartbeat("N1", 1000, 0, 1000, 0)
        d._reserve("N1", 300)
        d.record_heartbeat("N1", 1000, 300, 700, 3)
        assert d._cached_state(nodes[0])[1] == 700

    def test_placement_sends_no_requests_to_the_nodes(self, make_distributor):
        d, nodes = make_distributor()
        for n in nodes:
            d.record_heartbeat(n.node_id, 10 ** 9, 0, 10 ** 9, 0)
        d.upload("f", os.urandom(1000))              # 10 chunks
        assert [n.health_calls for n in nodes] == [0, 0, 0, 0]

    def test_a_node_never_heard_from_is_asked_once(self, make_distributor):
        d, nodes = make_distributor()
        d.upload("f", os.urandom(1000))
        d.upload("g", os.urandom(1000))
        assert [n.health_calls for n in nodes] == [1, 1, 1, 1]

    def test_a_node_that_is_down_when_first_asked_is_marked_down_not_asked_again(self, make_distributor):
        d, nodes = make_distributor()
        nodes[0].down = True
        d.upload("f", os.urandom(500))
        d.upload("g", os.urandom(500))
        assert nodes[0].store_calls == [] and nodes[0].health_calls == 0

    def test_the_node_with_the_most_free_space_is_chosen_first(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100)
        for node_id, free in [("N1", 500), ("N2", 100), ("N3", 900), ("N4", 300)]:
            d.record_heartbeat(node_id, 1000, 0, free, 0)
        oid = d.upload("f", os.urandom(50))          # one chunk
        assert sorted(chunks_of(d, oid)[0].replicas) == ["N1", "N3"]

    def test_a_node_without_room_for_the_chunk_is_not_a_candidate(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100)
        for node_id, free in [("N1", 99), ("N2", 100), ("N3", 5000), ("N4", 10)]:
            d.record_heartbeat(node_id, 10 ** 9, 0, free, 0)
        oid = d.upload("f", os.urandom(100))
        assert sorted(chunks_of(d, oid)[0].replicas) == ["N2", "N3"]

    def test_a_known_down_node_is_never_tried(self, make_distributor):
        d, nodes = make_distributor()
        for n in nodes:
            d.record_heartbeat(n.node_id, 10 ** 9, 0, 10 ** 9, 0)
        d._mark_down("N1")
        d.upload("f", os.urandom(800))
        assert nodes[0].store_calls == []

    def test_silent_nodes_are_not_used_and_with_too_few_left_the_upload_fails(self, make_distributor, clock):
        d, nodes = make_distributor(heartbeat_timeout=6)
        for n in nodes:
            d.record_heartbeat(n.node_id, 10 ** 9, 0, 10 ** 9, 0)
        clock.advance(7)
        for n in nodes[2:]:
            d.record_heartbeat(n.node_id, 10 ** 9, 0, 10 ** 9, 0)     # only N3, N4 still report
        oid = d.upload("f", os.urandom(300))
        assert {r for c in chunks_of(d, oid) for r in c.replicas} == {"N3", "N4"}
        clock.advance(7)                                               # now nobody reports
        with pytest.raises(RuntimeError, match="Not enough storage"):
            d.upload("g", os.urandom(300))

    def test_the_replica_order_puts_known_down_nodes_last_on_download(self, make_distributor):
        d, nodes = make_distributor()
        oid = d.upload("f", os.urandom(100))
        c = chunks_of(d, oid)[0]
        d._mark_down(c.replicas[0])
        order = sorted(c.replicas, key=lambda n: not d._cached_state(d._get_node(n))[0])
        assert order[-1] == c.replicas[0]


# ============================================================ health report
class TestReplicationReport:
    def test_all_healthy(self, make_distributor):
        d, _ = make_distributor()
        d.upload("f", os.urandom(300))
        r = d.replication_report()
        assert r["replication_factor"] == 2
        assert r["summary"] == {"total_chunks": 3, "healthy": 3, "under_replicated": 0, "unavailable": 0}
        assert all(rep["state"] == "ok" and rep["usable"] for ch in r["chunks"] for rep in ch["replicas"])

    def test_each_kind_of_problem_is_named(self, make_distributor):
        d, nodes = make_distributor()
        oid = d.upload("f", os.urandom(400))
        chunks = chunks_of(d, oid)
        by_id = {n.node_id: n for n in nodes}
        # One fault of each kind, chosen from wherever the copies actually landed:
        down = chunks[0].replicas[0]                                   # a whole node goes down
        lost = (chunks[2].index, next(n for n in chunks[2].replicas if n != down))
        damaged = (chunks[1].index, next(n for n in chunks[1].replicas if n != down))
        by_id[down].down = True
        by_id[lost[1]].lose(chunks[2].chunk_id)
        by_id[damaged[1]].damage(chunks[1].chunk_id)

        def expected_state(index, node):
            if node == down:
                return "down"
            if (index, node) == lost:
                return "missing"
            if (index, node) == damaged:
                return "corrupt"
            return "ok"

        r = d.replication_report()
        states = {ch["index"]: {rep["node"]: rep["state"] for rep in ch["replicas"]} for ch in r["chunks"]}
        for c in chunks:
            assert states[c.index] == {n: expected_state(c.index, n) for n in c.replicas}
        assert {"down", "missing", "corrupt", "ok"} <= {s for st in states.values() for s in st.values()}
        good = {i: sum(s == "ok" for s in st.values()) for i, st in states.items()}
        assert r["summary"] == {"total_chunks": 4,
                                "healthy": sum(g == 2 for g in good.values()),
                                "under_replicated": sum(g == 1 for g in good.values()),
                                "unavailable": sum(g == 0 for g in good.values())}

    def test_a_corrupt_copy_is_not_counted_as_usable(self, make_distributor):
        d, nodes = make_distributor()
        oid = d.upload("f", os.urandom(100))
        c = chunks_of(d, oid)[0]
        for n in nodes:
            if n.node_id in c.replicas:
                n.damage(c.chunk_id)
        r = d.replication_report()
        assert r["summary"]["unavailable"] == 1 and r["chunks"][0]["usable_copies"] == 0

    def test_includes_object_id_and_name(self, make_distributor):
        d, _ = make_distributor()
        oid = d.upload("named.bin", b"x")
        ch = d.replication_report()["chunks"][0]
        assert ch["object_id"] == oid and ch["name"] == "named.bin"


# ===================================================================== repair
def summary(result):
    return {k: v for k, v in result.items() if k not in ("details", "left_behind")}


class TestRepair:
    @pytest.fixture
    def setup(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100)
        data = os.urandom(600)                       # 6 chunks, 12 copies
        oid = d.upload("f", data)
        return d, nodes, oid, data

    def test_a_healthy_system_is_left_alone(self, setup):
        d, nodes, oid, _ = setup
        before = {n.node_id: dict(n.chunks) for n in nodes}
        r = d.repair()
        assert summary(r) == {"chunks_checked": 6, "healthy": 6, "repaired": 0, "partial": 0,
                              "unrecoverable": 0, "copies_created": 0}
        assert {n.node_id: dict(n.chunks) for n in nodes} == before

    def test_after_a_node_dies_every_chunk_is_back_to_two_good_copies(self, setup):
        d, nodes, oid, data = setup
        victim = nodes[0]
        affected = [c for c in chunks_of(d, oid) if victim.node_id in c.replicas]
        assert affected
        victim.down = True
        r = d.repair()
        assert r["repaired"] == len(affected) and r["copies_created"] == len(affected)
        assert r["unrecoverable"] == 0 and r["partial"] == 0
        for c in chunks_of(d, oid):
            assert len(c.replicas) == 2 and len(set(c.replicas)) == 2 and victim.node_id not in c.replicas
            for node_id in c.replicas:
                assert d._check_copy(node_id, c) == "ok"
        assert d.download(oid) == data
        assert d.replication_report()["summary"]["healthy"] == 6

    def test_the_dead_nodes_stale_copies_are_reported_as_left_behind(self, setup):
        d, nodes, oid, _ = setup
        victim = nodes[0]
        affected = [c.chunk_id for c in chunks_of(d, oid) if victim.node_id in c.replicas]
        victim.down = True
        r = d.repair()
        assert sorted(x["chunk_id"] for x in r["left_behind"]) == sorted(affected)
        assert all(x["node"] == victim.node_id for x in r["left_behind"])

    def test_a_second_repair_changes_nothing(self, setup):
        d, nodes, _, _ = setup
        nodes[0].down = True
        d.repair()
        r = d.repair()
        assert r["healthy"] == 6 and r["copies_created"] == 0

    def test_a_new_copy_never_goes_to_a_node_that_already_holds_a_good_one(self, setup):
        d, nodes, oid, _ = setup
        nodes[0].down = True
        d.repair()
        for c in chunks_of(d, oid):
            assert len(set(c.replicas)) == len(c.replicas) == 2
            assert nodes_holding(nodes[1:], c.chunk_id) == sorted(c.replicas)   # exactly the recorded nodes

    def test_a_damaged_copy_is_rebuilt_from_the_good_one(self, setup):
        d, nodes, oid, data = setup
        c = chunks_of(d, oid)[0]
        bad_node = next(n for n in nodes if n.node_id == c.replicas[0])
        bad_node.damage(c.chunk_id)
        r = d.repair()
        assert r["repaired"] == 1 and r["copies_created"] == 1
        c = chunks_of(d, oid)[0]
        assert all(d._check_copy(n, c) == "ok" for n in c.replicas) and len(set(c.replicas)) == 2
        if bad_node.node_id not in c.replicas:
            assert c.chunk_id not in bad_node.chunks            # the damaged file was removed
        assert d.download(oid) == data

    def test_a_lost_copy_is_recreated(self, setup):
        d, nodes, oid, _ = setup
        c = chunks_of(d, oid)[1]
        next(n for n in nodes if n.node_id == c.replicas[1]).lose(c.chunk_id)
        assert d.repair()["repaired"] == 1
        c = chunks_of(d, oid)[1]
        assert all(d._check_copy(n, c) == "ok" for n in c.replicas) and len(set(c.replicas)) == 2

    def test_when_both_copies_are_bad_nothing_is_invented(self, setup):
        d, nodes, oid, _ = setup
        c = chunks_of(d, oid)[2]
        for n in nodes:
            if n.node_id in c.replicas:
                n.damage(c.chunk_id)
        recorded = list(c.replicas)
        r = d.repair()
        assert r["unrecoverable"] == 1 and r["copies_created"] == 0
        assert chunks_of(d, oid)[2].replicas == recorded           # the record is untouched
        with pytest.raises(RuntimeError, match="No good copy"):
            d.download(oid)

    def test_with_too_few_live_nodes_the_repair_is_partial_and_keeps_the_down_entries(self, make_distributor):
        d, nodes = make_distributor(n=3, chunk_size=100)
        oid = d.upload("f", os.urandom(600))
        survivor = nodes[0]
        nodes[1].down = nodes[2].down = True
        before = {c.chunk_id: list(c.replicas) for c in chunks_of(d, oid)}
        r = d.repair()
        assert r["partial"] >= 1 and r["copies_created"] == 0 and r["left_behind"] == []
        for c in chunks_of(d, oid):
            # the down node is still listed (it may come back with a good copy); only the order may change
            assert sorted(c.replicas) == sorted(before[c.chunk_id])
        survivors = [cid for cid, reps in before.items() if survivor.node_id in reps]
        assert r["partial"] == len(survivors) and r["unrecoverable"] == 6 - len(survivors)

    def test_when_the_nodes_return_there_is_nothing_left_to_copy(self, make_distributor):
        d, nodes = make_distributor(n=3, chunk_size=100)
        d.upload("f", os.urandom(600))
        nodes[1].down = nodes[2].down = True
        d.repair()
        nodes[1].down = nodes[2].down = False
        for n in nodes:
            d.record_heartbeat(n.node_id, 10 ** 9, 0, 10 ** 9, 0)
        r = d.repair()
        assert r["healthy"] == 6 and r["copies_created"] == 0

    def test_a_target_that_turns_out_to_be_dead_is_skipped(self, make_distributor):
        d, nodes = make_distributor(n=4, chunk_size=100)
        oid = d.upload("f", os.urandom(200))
        c = chunks_of(d, oid)[0]
        holders = [n for n in nodes if n.node_id in c.replicas]
        others = [n for n in nodes if n.node_id not in c.replicas]
        holders[0].down = True
        for n in nodes:
            d.record_heartbeat(n.node_id, 10 ** 9, 0, 10 ** 9, 0)   # the cache believes everyone is alive
        others[0].down = True                                        # a likely target is dead
        r = d.repair()
        c = chunks_of(d, oid)[0]
        assert others[0].node_id not in c.replicas
        assert len(c.replicas) == 2 and r["unrecoverable"] == 0

    def test_only_one_repair_runs_at_a_time(self, setup):
        d, *_ = setup
        assert d._repair_lock.acquire(blocking=False)
        try:
            with pytest.raises(RuntimeError, match="already running"):
                d.repair()
        finally:
            d._repair_lock.release()
        d.repair()                                                    # and it works again afterwards

    def test_the_lock_is_released_even_if_the_repair_crashes(self, setup, monkeypatch):
        d, *_ = setup
        monkeypatch.setattr(d, "_repair_chunk", lambda *a, **k: (_ for _ in ()).throw(ValueError("boom")))
        with pytest.raises(ValueError):
            d.repair()
        monkeypatch.undo()
        d.repair()

    def test_an_object_deleted_during_the_repair_leaves_no_new_copies_behind(self, setup):
        d, nodes, oid, _ = setup
        nodes[0].down = True
        fired = []

        def delete_once(chunk_id):
            if not fired:
                fired.append(chunk_id)
                d.delete(oid)

        for n in nodes[1:]:
            n.on_store = delete_once
        d.repair()
        assert fired
        live_chunks = {cid for n in nodes[1:] for cid in n.chunks}
        assert live_chunks == set()
        with pytest.raises(KeyError):
            d.get_object(oid)


# ============================================================ spreading copies (issue #16)
class TestSpread:
    @staticmethod
    def pair_counts(d):
        from collections import Counter
        return Counter(frozenset(c.replicas) for m in d.list_objects() for c in m.chunks)

    def test_with_four_equal_workers_every_pair_is_used_about_equally(self, make_distributor):
        d, _ = make_distributor(4, chunk_size=100)
        for i in range(10):
            d.upload(f"f{i}", os.urandom(600))                  # 60 chunks
        counts = self.pair_counts(d)
        assert len(counts) == 6                                  # all C(4,2) pairs are used
        assert max(counts.values()) - min(counts.values()) <= 3, counts

    def test_losing_any_two_workers_loses_only_a_few_chunks(self, make_distributor):
        import itertools
        d, nodes = make_distributor(4, chunk_size=100)
        for i in range(10):
            d.upload(f"f{i}", os.urandom(600))
        total = sum(len(m.chunks) for m in d.list_objects())
        for a, b in itertools.combinations([n.node_id for n in nodes], 2):
            lost = sum(1 for m in d.list_objects() for c in m.chunks if set(c.replicas) == {a, b})
            assert lost / total <= 0.25, (a, b, lost)            # about 1/6 each; fixed pairs lost 1/2

    def test_six_workers_spread_too(self, make_distributor):
        d, _ = make_distributor(6, chunk_size=100)
        for i in range(15):
            d.upload(f"f{i}", os.urandom(1000))                  # 150 chunks
        counts = self.pair_counts(d)
        assert len(counts) == 15 and max(counts.values()) <= 3 * min(counts.values()), counts

    def test_a_full_worker_is_never_chosen(self, make_distributor):
        d, nodes = make_distributor(4, chunk_size=100, capacity=10 ** 9)
        nodes[0].capacity = 0
        d._status.clear()                                         # let the controller see the new capacity
        oid = d.upload("f", os.urandom(1000))
        assert all("N1" not in c.replicas for c in d.get_object(oid).chunks)

    def test_free_space_still_matters(self, make_distributor):
        d, nodes = make_distributor(4, chunk_size=100, capacity=10 ** 6)
        nodes[3].capacity = 10 ** 9                               # N4 has far more room
        d._status.clear()
        oid = d.upload("f", os.urandom(1000))
        assert all("N4" in c.replicas for c in d.get_object(oid).chunks)
