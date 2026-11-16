"""nodes/node.py: the node's file-backed ChunkStore."""
import os
import threading

import pytest

from helpers.checksum import compute
from helpers.errors import ChunkCorrupt
from nodes.node import ChunkStore


@pytest.fixture
def store(tmp_path):
    return ChunkStore(str(tmp_path / "data"), capacity=1000)


def chunk_file(store, chunk_id):
    return os.path.join(store.data_dir, chunk_id)


class TestStoreAndGet:
    def test_round_trip(self, store):
        assert store.store("c-0", b"hello") is True
        assert store.get("c-0") == b"hello"
        assert store.has("c-0")

    def test_a_checksum_file_is_saved_next_to_the_chunk(self, store):
        store.store("c-0", b"hello")
        assert store.stored_checksum("c-0") == compute(b"hello")

    def test_missing_chunk(self, store):
        assert not store.has("nope")
        with pytest.raises(KeyError):
            store.get("nope")
        with pytest.raises(KeyError):
            store.compute_checksum("nope")
        assert store.stored_checksum("nope") is None

    def test_storing_an_existing_id_replaces_it(self, store):
        store.store("c-0", b"old old old")
        store.store("c-0", b"new")
        assert store.get("c-0") == b"new"
        assert store.stored_checksum("c-0") == compute(b"new")
        assert store.chunk_count() == 1

    def test_empty_chunk(self, store):
        assert store.store("c-0", b"")
        assert store.get("c-0") == b""

    def test_survives_a_restart(self, tmp_path):
        a = ChunkStore(str(tmp_path / "d"), 1000)
        a.store("c-0", b"persist me")
        b = ChunkStore(str(tmp_path / "d"), 1000)         # a new object over the same folder
        assert b.get("c-0") == b"persist me"
        assert b.chunk_count() == 1


class TestCapacity:
    def test_space_accounting(self, store):
        assert store.free_space() == 1000
        store.store("a", b"x" * 300)
        assert store.used_space() == 300 and store.free_space() == 700

    def test_refuses_what_does_not_fit_and_stores_nothing(self, store):
        store.store("a", b"x" * 900)
        assert store.store("b", b"y" * 101) is False
        assert not store.has("b") and store.stored_checksum("b") is None

    def test_exact_fit(self, store):
        assert store.store("a", b"x" * 1000) is True
        assert store.free_space() == 0

    def test_replacing_needs_room_only_for_the_difference(self, store):
        store.store("a", b"x" * 900)
        assert store.store("a", b"y" * 1000) is True          # 100 more, 100 free
        assert store.store("a", b"y" * 1001) is False         # one byte too many
        assert store.get("a") == b"y" * 1000                  # the failed attempt changed nothing

    def test_delete_frees_space(self, store):
        store.store("a", b"x" * 400)
        store.delete("a")
        assert store.free_space() == 1000


class TestIntegrity:
    def test_damage_on_disk_is_refused(self, store):
        store.store("c-0", b"important data")
        with open(chunk_file(store, "c-0"), "r+b") as f:
            f.seek(3)
            f.write(b"X")
        with pytest.raises(ChunkCorrupt):
            store.get("c-0")

    def test_compute_checksum_recomputes_from_disk(self, store):
        store.store("c-0", b"important data")
        assert store.compute_checksum("c-0") == compute(b"important data")
        with open(chunk_file(store, "c-0"), "r+b") as f:
            f.write(b"X")
        assert store.compute_checksum("c-0") != store.stored_checksum("c-0")

    def test_a_chunk_without_a_checksum_file_is_returned_unchecked(self, store):
        store.store("c-0", b"data")
        os.remove(os.path.join(store.data_dir, ".checksums", "c-0"))
        assert store.get("c-0") == b"data"

    def test_the_checksum_files_are_not_counted_as_chunks(self, store):
        store.store("a", b"1")
        store.store("b", b"2")
        assert store.chunk_count() == 2


class TestDelete:
    def test_delete_removes_chunk_and_checksum(self, store):
        store.store("c-0", b"data")
        store.delete("c-0")
        assert not store.has("c-0") and store.stored_checksum("c-0") is None

    def test_deleting_twice_is_harmless(self, store):
        store.delete("never-there")
        store.store("c-0", b"data")
        store.delete("c-0")
        store.delete("c-0")


class TestFileInUse:
    """On Windows a file cannot be deleted or replaced while another thread has
    it open (a download reading that chunk). That is brief, so the store waits
    and tries again instead of failing and stranding the chunk."""

    @staticmethod
    def hold_open(path, seconds):
        """Open `path` for reading now and close it after `seconds`, from another thread."""
        handle = open(path, "rb")
        timer = threading.Timer(seconds, handle.close)
        timer.start()
        return timer

    def test_delete_waits_for_a_reader_to_finish(self, store):
        store.store("c-0", b"data" * 100)
        timer = self.hold_open(chunk_file(store, "c-0"), 0.15)
        store.delete("c-0")
        timer.join()
        assert not store.has("c-0") and store.stored_checksum("c-0") is None

    def test_replacing_waits_for_a_reader_to_finish(self, store):
        store.store("c-0", b"old" * 100)
        timer = self.hold_open(chunk_file(store, "c-0"), 0.15)
        assert store.store("c-0", b"new" * 100) is True
        timer.join()
        assert store.get("c-0") == b"new" * 100

    def test_the_checksum_file_being_read_does_not_block_a_delete(self, store):
        store.store("c-0", b"data")
        timer = self.hold_open(os.path.join(store.data_dir, ".checksums", "c-0"), 0.15)
        store.delete("c-0")
        timer.join()
        assert not store.has("c-0") and store.stored_checksum("c-0") is None

    def test_many_readers_and_deleters_at_once_never_fail(self, store):
        errors = []

        def reader(cid):
            for _ in range(40):
                try:
                    store.get(cid)
                except KeyError:
                    pass
                except Exception as e:           # anything else is a bug
                    errors.append(repr(e))

        def deleter(cid):
            try:
                store.delete(cid)
            except Exception as e:
                errors.append(repr(e))

        for round_ in range(10):
            ids = [f"r{round_}-{i}" for i in range(6)]
            for cid in ids:
                store.store(cid, b"x" * 200)
            threads = [threading.Thread(target=reader, args=(cid,)) for cid in ids for _ in range(3)]
            threads += [threading.Thread(target=deleter, args=(cid,)) for cid in ids]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        assert errors == []
        assert store.chunk_count() == 0


class TestSafety:
    @pytest.mark.parametrize("bad", ["../x", "a/b", "a\\b", ".hidden", "", "..", "a/../b", "/abs", "x" + "\x00"])
    def test_unsafe_ids_are_rejected_everywhere(self, store, bad):
        for call in (lambda: store.store(bad, b"x"), lambda: store.get(bad), lambda: store.has(bad),
                     lambda: store.delete(bad), lambda: store.compute_checksum(bad),
                     lambda: store.stored_checksum(bad)):
            with pytest.raises(ValueError):
                call()

    def test_nothing_is_written_outside_the_data_folder(self, store, tmp_path):
        with pytest.raises(ValueError):
            store.store("../escaped", b"x")
        assert not (tmp_path / "escaped").exists()

    def test_non_string_id(self, store):
        with pytest.raises(ValueError):
            store.store(None, b"x")

    def test_leftover_temp_files_are_removed_on_start(self, tmp_path):
        data_dir = tmp_path / "d"
        s = ChunkStore(str(data_dir), 1000)
        (data_dir / ".tmp-half-written").write_bytes(b"junk")
        (data_dir / ".checksums" / ".tmp-half").write_bytes(b"junk")
        ChunkStore(str(data_dir), 1000)
        assert not (data_dir / ".tmp-half-written").exists()
        assert not (data_dir / ".checksums" / ".tmp-half").exists()
        assert s.chunk_count() == 0
