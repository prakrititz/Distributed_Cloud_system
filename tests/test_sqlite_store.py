"""The SQLite metadata store (controller/metadata_store.py, SqliteStore).

A change writes only the rows it touches, inside one transaction, so its cost
does not grow with the number of objects stored, and a failure part-way leaves
the previous state. The database also keeps the duplicate rule (one object per
client and path) and can answer "which chunks are on worker X" from an index.
"""
import json
import os
import sqlite3
import threading

import pytest

from controller.distributor import Distributor, ObjectMetadata
from controller.metadata_store import JsonFileStore, MemoryStore, SqliteStore, store_from_setting
from tests.fakes import FakeNode


def obj(object_id, chunks=2, path="", client="", custom=None, created="2026-11-19T10:00:00Z", readers=None):
    """A plain-dict object like the distributor saves."""
    return {"object_id": object_id, "name": f"{object_id}.bin", "size": 150, "checksum": "c" * 64,
            "content_type": "application/octet-stream", "created_at": created, "modified_at": created,
            "source_path": path, "client_id": client, "custom": custom or {}, "readers": readers or [],
            "chunks": [{"chunk_id": f"{object_id}-{i}", "index": i, "replicas": ["w1", "w2"],
                        "checksum": f"k{i}", "size": 100 if i == 0 else 50} for i in range(chunks)]}


@pytest.fixture
def db(tmp_path):
    store = SqliteStore(str(tmp_path / "meta" / "metadata.db"))
    yield store
    store.close()


class TestBasics:
    def test_a_new_database_is_empty(self, db):
        assert db.load() == [] and db.persistent is True

    def test_round_trip_of_every_field(self, db):
        a = obj("a", custom={"project": "demo", "n": 3, "ok": True}, path="/f/a", client="alice",
                readers=["bob", "carol"])
        a["chunks"][1]["size"] = None                      # saved before chunk sizes were kept
        db.save([a])
        assert db.load() == [a]

    def test_objects_come_back_oldest_first_with_chunks_in_order(self, db):
        db.save([obj("b", chunks=3, created="2026-11-19T11:00:00Z"), obj("a", created="2026-11-19T09:00:00Z")])
        loaded = db.load()
        assert [o["object_id"] for o in loaded] == ["a", "b"]
        assert [c["index"] for c in loaded[1]["chunks"]] == [0, 1, 2]

    def test_the_setting_picks_the_store(self, tmp_path):
        assert isinstance(store_from_setting(""), MemoryStore)
        assert isinstance(store_from_setting(str(tmp_path / "m.json")), JsonFileStore)
        db = store_from_setting(str(tmp_path / "m.db"))
        assert isinstance(db, SqliteStore)
        db.close()


class TestChanges:
    def test_apply_adds_replaces_and_removes_objects(self, db):
        metadata = {"a": obj("a"), "b": obj("b")}
        db.apply(metadata, objects=["a", "b"])
        metadata["a"] = obj("a", chunks=3, custom={"k": "v"})   # replaced
        db.apply(metadata, objects=["a"], removed=["b"])
        assert db.load() == [metadata["a"]]

    def test_a_chunks_copies_can_change_alone(self, db):
        a = obj("a")
        db.save([a])
        a["chunks"][0]["replicas"] = ["w3", "w4"]
        db.apply({"a": a}, chunks=[a["chunks"][0]])
        assert db.load()[0]["chunks"][0]["replicas"] == ["w3", "w4"]
        assert db.load()[0]["chunks"][1]["replicas"] == ["w1", "w2"]

    def test_deleting_an_object_deletes_its_chunks_and_copies(self, db):
        db.save([obj("a"), obj("b")])
        db.apply({}, removed=["a"])
        rows = db._db.execute("SELECT COUNT(*) FROM chunks").fetchone()[0], \
            db._db.execute("SELECT COUNT(*) FROM replicas").fetchone()[0]
        assert rows == (2, 4)                                  # only b's 2 chunks and 4 copies remain

    def test_one_change_writes_only_its_own_rows_however_much_is_stored(self, db):
        db.save([obj(f"o{i}", chunks=4) for i in range(1000)])   # 1000 objects, 4000 chunks, 8000 copies
        before = db._db.total_changes
        new = obj("new", chunks=4)
        db.apply({"new": new}, objects=["new"])
        # 1 object + 4 chunks + 8 copies; nothing else is rewritten
        assert db._db.total_changes - before == 1 + 4 + 8

    def test_a_failure_part_way_leaves_the_previous_state(self, db, monkeypatch):
        db.save([obj("a")])
        before = db.load()
        real = db._put

        def half(d):
            real(d)
            raise OSError("power cut after the rows were written")
        monkeypatch.setattr(db, "_put", half)
        with pytest.raises(OSError):
            db.apply({"a": obj("a", chunks=5), "b": obj("b")}, objects=["b", "a"], removed=[])
        monkeypatch.undo()
        assert db.load() == before                             # the whole change was rolled back

    def test_the_database_keeps_one_object_per_client_and_path(self, db):
        db.save([obj("a", path="/f/x", client="alice"), obj("b", path="/f/x", client="bob"),
                 obj("c"), obj("d")])                         # no path: never a duplicate
        with pytest.raises(sqlite3.IntegrityError):
            db.apply({"e": obj("e", path="/f/x", client="alice")}, objects=["e"])
        assert len(db.load()) == 4


class TestQueries:
    def test_chunks_on_a_worker_come_from_an_index(self, db):
        a, b = obj("a"), obj("b")
        b["chunks"][1]["replicas"] = ["w3", "w4"]
        db.save([a, b])
        assert db.chunks_on("w3") == [("b", "b-1")]
        assert len(db.chunks_on("w1")) == 3
        plan = " ".join(str(r) for r in db._db.execute(
            "EXPLAIN QUERY PLAN SELECT chunk_id FROM replicas WHERE worker_id = 'w3'"))
        assert "replicas_by_worker" in plan

    def test_the_file_can_be_read_with_plain_sqlite(self, db):
        db.save([obj("a", custom={"project": "demo"})])
        other = sqlite3.connect(db.path)                     # e.g. the sqlite3 command or a viewer
        assert other.execute("SELECT name, json_extract(custom, '$.project') FROM objects").fetchall() == \
            [("a.bin", "demo")]
        other.close()


class TestOpening:
    def test_a_damaged_file_stops_the_controller(self, tmp_path):
        path = tmp_path / "metadata.db"
        path.write_bytes(b"this is not a database" * 100)
        with pytest.raises(ValueError, match="Cannot open"):
            SqliteStore(str(path))

    def test_another_format_version_is_refused(self, tmp_path):
        path = str(tmp_path / "metadata.db")
        SqliteStore(path).close()
        con = sqlite3.connect(path)
        con.execute("UPDATE info SET value = '99' WHERE key = 'version'")
        con.commit()
        con.close()
        with pytest.raises(ValueError, match="format 99"):
            SqliteStore(path)

    def test_an_old_metadata_json_is_imported_once_and_left_alone(self, tmp_path):
        folder = tmp_path / "meta"
        JsonFileStore(str(folder / "metadata.json")).save([obj("a"), obj("b", path="/p", client="c")])
        before = (folder / "metadata.json").read_bytes()
        db = SqliteStore(str(folder / "metadata.db"))
        assert [o["object_id"] for o in db.load()] == ["a", "b"]
        db.apply({}, removed=["a"])
        db.close()
        again = SqliteStore(str(folder / "metadata.db"))      # not imported a second time
        assert [o["object_id"] for o in again.load()] == ["b"]
        again.close()
        assert (folder / "metadata.json").read_bytes() == before


class TestWithTheDistributor:
    def test_uploads_from_many_threads_are_all_saved(self, tmp_path):
        path = str(tmp_path / "metadata.db")
        nodes = [FakeNode(f"N{i}") for i in range(1, 5)]
        d = Distributor(nodes, chunk_size=100, store=SqliteStore(path))
        ids = []

        def go(i):
            ids.append(d.upload(f"f{i}", os.urandom(450 + i), source_path=f"/f/{i}", client_id="c"))
        threads = [threading.Thread(target=go, args=(i,)) for i in range(12)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        d.delete(ids[0])
        d.store.close()
        again = Distributor(nodes, chunk_size=100, store=SqliteStore(path))
        assert sorted(m.object_id for m in again.list_objects()) == sorted(ids[1:])
        for oid in ids[1:]:
            assert again.get_object(oid) == d.get_object(oid)
            assert again.download(oid) == d.download(oid)
        again.store.close()

    def test_saved_objects_load_as_the_same_metadata(self, tmp_path):
        path = str(tmp_path / "metadata.db")
        d = Distributor([FakeNode(f"N{i}") for i in range(1, 5)], chunk_size=100, store=SqliteStore(path))
        oid = d.upload("a.bin", os.urandom(250), custom={"k": "v"}, source_path="/a", client_id="z")
        saved = d.store.load()[0]
        assert ObjectMetadata.from_dict(saved) == d.get_object(oid)
        assert json.loads(json.dumps(saved)) == saved          # plain data only
        d.store.close()
