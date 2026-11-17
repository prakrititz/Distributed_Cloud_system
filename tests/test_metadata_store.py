"""Saving the controller's metadata (controller/metadata_store.py): a restart
keeps every object, and a crash during a save cannot damage the file."""
import json
import os

import pytest
import requests

from controller.distributor import Distributor
from controller.metadata_store import JsonFileStore, MemoryStore, store_from_setting
from controller.remote_node import RemoteNode
from tests.cluster import NODE_IDS
from tests.fakes import FakeNode
from tests.util import MB, TIMEOUT, upload_ok


def nodes4():
    return [FakeNode(f"N{i}") for i in range(1, 5)]


def restart(d, nodes=None, **kwargs):
    """A new controller over the same metadata file (the nodes kept their chunks)."""
    return Distributor(d.nodes if nodes is None else nodes, chunk_size=d.chunk_size, store=JsonFileStore(d.store.path),
                       **kwargs)


@pytest.fixture
def path(tmp_path):
    return str(tmp_path / "meta" / "metadata.json")


class TestJsonFileStore:
    def test_nothing_saved_yet_is_empty_and_creates_nothing(self, path):
        assert JsonFileStore(path).load() == []
        assert not os.path.exists(os.path.dirname(path))     # the folder appears with the first save
        JsonFileStore(path).save([])
        assert os.path.isfile(path)

    def test_round_trip(self, path):
        JsonFileStore(path).save([{"object_id": "a", "x": 1}])
        assert JsonFileStore(path).load() == [{"object_id": "a", "x": 1}]

    def test_a_damaged_file_stops_the_controller_instead_of_starting_empty(self, path):
        store = JsonFileStore(path)
        os.makedirs(os.path.dirname(path))
        with open(path, "w") as f:
            f.write('{"version": 1, "objects": [')               # cut off
        with pytest.raises(ValueError, match="Cannot read"):
            store.load()

    @pytest.mark.parametrize("doc", [[], {"objects": []}, {"version": 99, "objects": []}, {"version": 1}])
    def test_a_file_in_another_format_is_refused(self, path, doc):
        store = JsonFileStore(path)
        os.makedirs(os.path.dirname(path))
        with open(path, "w") as f:
            json.dump(doc, f)
        with pytest.raises(ValueError):
            store.load()

    def test_a_crash_while_saving_leaves_the_previous_file_whole(self, path, monkeypatch):
        store = JsonFileStore(path)
        store.save([{"object_id": "old"}])

        def crash(*a):
            raise OSError("power cut")
        monkeypatch.setattr(os, "replace", crash)
        with pytest.raises(OSError):
            store.save([{"object_id": "new"}])
        monkeypatch.undo()
        assert JsonFileStore(path).load() == [{"object_id": "old"}]
        JsonFileStore(path).save([{"object_id": "after"}])    # a left-over temp file does not get in the way
        assert JsonFileStore(path).load() == [{"object_id": "after"}]

    def test_the_setting(self, path):
        assert isinstance(store_from_setting(""), MemoryStore)
        assert isinstance(store_from_setting(path), JsonFileStore)
        assert MemoryStore().persistent is False and JsonFileStore(path).persistent is True


class TestRestart:
    def test_everything_survives_a_restart(self, path):
        nodes = nodes4()
        d = Distributor(nodes, chunk_size=100, store=JsonFileStore(path))
        data = os.urandom(350)
        oid = d.upload("a.bin", data, content_type="x/y", custom={"k": "v"}, source_path="/f/a.bin", client_id="alice")
        d.update_custom_metadata(oid, {"k2": "v2"})
        before = d.get_object(oid)

        d2 = restart(d)
        after = d2.get_object(oid)
        assert after == before                                   # every field, chunk list and replica
        assert d2.download(oid) == data
        with pytest.raises(FileExistsError):                     # the duplicate rule still knows the path
            d2.upload("a.bin", b"again", source_path="/f/a.bin", client_id="alice")
        d2.upload("a.bin", b"other client", source_path="/f/a.bin", client_id="bob")

    def test_deletes_and_patches_are_saved_too(self, path):
        d = Distributor(nodes4(), chunk_size=100, store=JsonFileStore(path))
        keep = d.upload("keep", b"1" * 50)
        gone = d.upload("gone", b"2" * 50, source_path="/p")
        d.delete(gone)
        d.update_custom_metadata(keep, {"a": "1"})
        d2 = restart(d)
        assert [m.object_id for m in d2.list_objects()] == [keep]
        assert d2.get_object(keep).custom == {"a": "1"}
        d2.upload("again", b"3", source_path="/p")               # the deleted object's path is free again

    def test_repair_changes_are_saved(self, path):
        nodes = nodes4()
        d = Distributor(nodes, chunk_size=100, store=JsonFileStore(path))
        oid = d.upload("f", os.urandom(300))
        victim = d.get_object(oid).chunks[0].replicas[0]
        next(n for n in nodes if n.node_id == victim).down = True
        d.repair()
        d2 = restart(d)
        assert all(victim not in c.replicas for c in d2.get_object(oid).chunks)

    def test_workers_that_have_not_registered_yet_count_as_down(self, path):
        nodes = nodes4()
        d = Distributor(nodes, chunk_size=100, store=JsonFileStore(path))
        data = os.urandom(300)
        oid = d.upload("f", data)
        d2 = restart(d, nodes=[])                                # fresh controller: nobody registered yet
        with pytest.raises(RuntimeError, match="No good copy"):  # a clean "unavailable", not a crash
            d2.download(oid)
        assert d2.replication_report()["summary"]["unavailable"] == 3
        for n in nodes:                                          # the workers register again
            d2.register_node(n, capacity=n.capacity, used=n.used(), free=n.capacity - n.used(), chunks=len(n.chunks))
        assert d2.download(oid) == data

    def test_a_failed_save_fails_the_upload_and_leaves_nothing(self, path, monkeypatch):
        nodes = nodes4()
        store = JsonFileStore(path)
        d = Distributor(nodes, chunk_size=100, store=store)
        monkeypatch.setattr(store, "save", lambda objects: (_ for _ in ()).throw(OSError("disk full")))
        with pytest.raises(OSError):
            d.upload("f", os.urandom(300), source_path="/x")
        assert d.list_objects() == [] and d.paths == {}
        assert all(n.chunks == {} for n in nodes)                # rolled back

    def test_without_a_file_nothing_survives(self):
        d = Distributor(nodes4(), chunk_size=100)
        d.upload("f", b"x")
        assert Distributor(d.nodes, chunk_size=100).list_objects() == []


class TestRestartWithRealWorkers:
    def test_a_new_controller_over_the_same_file_serves_every_object(self, cluster):
        data = {f"f{i}": os.urandom(MB + i * 1000) for i in range(3)}
        ids = {name: upload_ok(cluster, name, blob)["object_id"] for name, blob in data.items()}
        fresh = Distributor([RemoteNode(n, cluster.node_url(n)) for n in NODE_IDS],
                            store=JsonFileStore(cluster.metadata_path))
        for name, oid in ids.items():
            assert fresh.download(oid) == data[name]

    def test_the_file_is_written_on_every_change(self, cluster):
        oid = upload_ok(cluster, "f", b"abc")["object_id"]
        saved = json.load(open(cluster.metadata_path))["objects"]
        assert [o["object_id"] for o in saved] == [oid]
        requests.delete(f"{cluster.api}/objects/{oid}", timeout=TIMEOUT)
        assert json.load(open(cluster.metadata_path))["objects"] == []
