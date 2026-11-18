"""Streaming uploads and downloads (issue #20).

An object passes through the controller one chunk at a time, in both
directions, so neither the controller nor the client ever holds a whole file
in memory. What that must not break:
  - the checksum of the whole object is still checked (at the end now), and
    a bad upload leaves nothing behind;
  - an upload that stops half-way leaves nothing behind;
  - a download that fails half-way is never kept by the client;
  - a delete while a download is running does not cut the download off;
  - the number of transfers at once is bounded (MAX_TRANSFERS).
"""
import os
import socket
import threading
import time
import tracemalloc
from urllib.parse import urlparse

import pytest
import requests

from client import Client
from controller.distributor import ObjectMetadata
from helpers.checksum import compute, compute_file
from helpers.errors import ChecksumMismatch
from tests.cluster import wait_until
from tests.util import MB, TIMEOUT, download, listing, metadata, upload_ok


class Recorder:
    """A file-like stream over some bytes that remembers how far it has been
    read, and gives out at most `most` bytes per read() (like a network
    connection can)."""

    def __init__(self, data: bytes, most: int = None, fail_after: int = None):
        self.data, self.pos, self.most, self.fail_after = data, 0, most, fail_after

    def read(self, n: int) -> bytes:
        if self.fail_after is not None and self.pos >= self.fail_after:
            raise OSError("the client went away")
        if self.most:
            n = min(n, self.most)
        piece = self.data[self.pos:self.pos + n]
        self.pos += len(piece)
        return piece


def peak_memory(fn):
    """Run fn() and return (its result, the most memory Python had allocated
    meanwhile, in bytes), sampled every few milliseconds.

    Left out: Werkzeug's development server, after every request, drains the
    connection into a fresh 10 MB buffer (serving.py, rfile.read(10_000_000)).
    That is a quirk of the test server, the same for every request whatever
    the file size; a production server does not do it."""
    ignore = [tracemalloc.Filter(False, "*werkzeug*serving.py")]
    peak, done = [0], threading.Event()

    def watch():
        while not done.is_set():
            snap = tracemalloc.take_snapshot().filter_traces(ignore)
            peak[0] = max(peak[0], sum(s.size for s in snap.statistics("filename")))
            del snap
            time.sleep(0.005)

    tracemalloc.start()
    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    try:
        result = fn()
    finally:
        done.set()
        watcher.join()
        tracemalloc.stop()
    return result, peak[0]


def stored_anywhere(nodes) -> int:
    return sum(len(n.chunks) for n in nodes)


def count_fetches(nodes) -> list:
    """Record every get_chunk call on these FakeNodes."""
    calls = []
    for node in nodes:
        def counted(chunk_id, real=node.get_chunk):
            calls.append(chunk_id)
            return real(chunk_id)
        node.get_chunk = counted
    return calls


# ---------------------------------------------------------------------------
# the distributor, with in-memory nodes
# ---------------------------------------------------------------------------
class TestStreamingUpload:
    @pytest.mark.parametrize("parallel", [1, 4])
    def test_reading_never_runs_more_than_parallel_chunks_ahead(self, make_distributor, parallel):
        d, nodes = make_distributor(chunk_size=100, parallel_chunks=parallel)
        data = os.urandom(1000)
        stream = Recorder(data)
        read_when_stored = {}
        for n in nodes:
            n.on_store = lambda chunk_id: read_when_stored.setdefault(chunk_id, stream.pos)
        oid = d.upload("f", stream)
        for info in d.get_object(oid).chunks:
            # when a chunk's first copy was stored, at most `parallel` chunks
            # had been read from the stream (this one and the ones in flight);
            # with 1, nothing after it had been read
            assert read_when_stored[info.chunk_id] <= (info.index + parallel) * 100
        assert d.download(oid) == data

    def test_short_reads_still_give_full_chunks(self, make_distributor):
        d, _ = make_distributor(chunk_size=100)
        data = os.urandom(250)
        meta = d.get_object(d.upload("f", Recorder(data, most=7)))
        assert [c.size for c in meta.chunks] == [100, 100, 50]
        assert meta.size == 250 and meta.checksum == compute(data)
        assert d.download(meta.object_id) == data

    @pytest.mark.parametrize("size,sizes", [(0, [0]), (100, [100]), (200, [100, 100]), (201, [100, 100, 1])])
    def test_chunk_boundaries(self, make_distributor, size, sizes):
        d, _ = make_distributor(chunk_size=100)
        data = os.urandom(size)
        for source in (data, Recorder(data)):                     # bytes and a stream: the same chunks
            meta = d.get_object(d.upload("f", source))
            assert [c.size for c in meta.chunks] == sizes and meta.size == size
            assert d.download(meta.object_id) == data

    def test_a_wrong_checksum_leaves_nothing_behind(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100)
        data = os.urandom(450)
        with pytest.raises(ChecksumMismatch, match="damaged in transit"):
            d.upload("f", Recorder(data), checksum=compute(b"something else"), source_path="/a/f")
        assert stored_anywhere(nodes) == 0 and d.list_objects() == [] and not d._in_flight
        # the path was never taken: the same file with the right checksum goes in
        oid = d.upload("f", Recorder(data), checksum=compute(data), source_path="/a/f")
        assert d.download(oid) == data

    def test_the_checksum_is_compared_ignoring_case_and_spaces(self, make_distributor):
        d, _ = make_distributor(chunk_size=100)
        data = os.urandom(300)
        assert d.upload("f", data, checksum=f"  {compute(data).upper()}\n")

    def test_data_that_stops_arriving_leaves_nothing_behind(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100)
        with pytest.raises(OSError, match="went away"):
            d.upload("f", Recorder(os.urandom(1000), fail_after=350), source_path="/a/f")
        assert stored_anywhere(nodes) == 0 and d.list_objects() == [] and not d._in_flight
        assert ("", "/a/f") not in d.paths

    def test_each_copy_is_sent_with_the_chunk_checksum(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100)
        sent = []
        for n in nodes:
            def store(chunk_id, data, checksum=None, real=n.store_chunk):
                sent.append((data, checksum))
                return real(chunk_id, data, checksum=checksum)
            n.store_chunk = store
        d.upload("f", os.urandom(300))
        assert len(sent) == 6 and all(c == compute(data) for data, c in sent)


class TestStreamingDownload:
    @pytest.mark.parametrize("parallel", [1, 3])
    def test_chunks_are_fetched_at_most_parallel_chunks_ahead(self, make_distributor, parallel):
        d, nodes = make_distributor(chunk_size=100, parallel_chunks=parallel)
        data = os.urandom(1000)                                   # 10 chunks
        oid = d.upload("f", data)
        fetched = count_fetches(nodes)
        meta, chunks = d.stream_object(oid)
        assert len(fetched) == 1                                  # only the first chunk, before anything is sent
        got = []
        for i in range(1, 11):
            got.append(next(chunks))
            time.sleep(0.02)                                      # let the fetchers run
            assert len(fetched) <= min(10, i + parallel)          # never more than `parallel` ahead
        with pytest.raises(StopIteration):
            next(chunks)
        assert b"".join(got) == data and len(fetched) == 10 and not d._reading

    def test_a_chunk_with_no_live_copy_fails_before_anything_is_sent(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100)
        oid = d.upload("f", os.urandom(400))
        last = d.get_object(oid).chunks[3]
        for node_id in last.replicas:
            next(n for n in nodes if n.node_id == node_id).down = True
            d._mark_down(node_id)
        fetched = count_fetches(nodes)
        with pytest.raises(RuntimeError, match="No good copy of chunk 3"):
            d.stream_object(oid)
        assert fetched == [] and not d._reading

    def test_a_later_chunk_with_no_good_copy_fails_part_way(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100)
        data = os.urandom(400)
        oid = d.upload("f", data)
        bad = d.get_object(oid).chunks[2]
        for node_id in bad.replicas:
            next(n for n in nodes if n.node_id == node_id).damage(bad.chunk_id)
        _, chunks = d.stream_object(oid)                          # nothing is wrong with chunk 0
        assert next(chunks) + next(chunks) == data[:200]
        with pytest.raises(RuntimeError, match="No good copy of chunk 2"):
            next(chunks)
        assert not d._reading                                     # the download is over

    def test_a_delete_during_a_download_waits_for_it(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100)
        data = os.urandom(400)
        oid = d.upload("f", data, source_path="/a/f")
        _, chunks = d.stream_object(oid)
        first = next(chunks)
        assert d.delete(oid) == []
        with pytest.raises(KeyError):
            d.get_object(oid)                                     # gone for everyone else at once
        assert ("", "/a/f") not in d.paths
        assert stored_anywhere(nodes) == 8                        # but its copies are still there
        assert first + b"".join(chunks) == data                   # so the download completes
        assert stored_anywhere(nodes) == 0                        # and then they go

    def test_an_abandoned_download_lets_the_delete_finish(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100)
        oid = d.upload("f", os.urandom(400))
        _, a = d.stream_object(oid)
        _, b = d.stream_object(oid)
        d.delete(oid)
        a.close()
        assert stored_anywhere(nodes) == 8                        # b is still reading
        b.close()
        b.close()                                                 # closing twice is harmless
        assert stored_anywhere(nodes) == 0 and not d._reading

    def test_chunks_of_a_download_in_progress_are_not_orphans(self, make_distributor):
        d, nodes = make_distributor(chunk_size=100)
        oid = d.upload("f", os.urandom(200))
        _, chunks = d.stream_object(oid)
        d.delete(oid)
        holder = next(n for n in nodes if n.chunks)               # no metadata points to its chunks now
        inventory = d.worker_inventory(holder.node_id)
        assert inventory["orphans"] == [] and inventory["in_progress"]
        chunks.close()

    def test_metadata_saved_before_chunk_sizes_were_kept_still_loads(self):
        meta = ObjectMetadata.from_dict({"object_id": "x", "name": "f", "size": 3, "checksum": "c",
                                         "chunks": [{"chunk_id": "x-0", "index": 0, "replicas": ["N1"],
                                                     "checksum": "k"}]})
        assert meta.chunks[0].size is None


# ---------------------------------------------------------------------------
# the API and the client, against the real cluster
# ---------------------------------------------------------------------------
def raw_upload(cluster, name, data, content_type=None, **params):
    headers = {"Content-Type": content_type} if content_type else {}
    return requests.post(cluster.api + "/objects", params={"name": name, **params}, data=data,
                         headers=headers, timeout=TIMEOUT)


class TestRawUpload:
    def test_round_trip_with_every_field(self, cluster):
        data = os.urandom(3 * MB + 5)
        r = raw_upload(cluster, "clip.mp4", data, "video/mp4", checksum=compute(data), path="/v/clip.mp4",
                       client_id="cam", metadata='{"take": 2}')
        assert r.status_code == 201, r.text
        m = r.json()
        assert (m["name"], m["size"], m["content_type"], m["source_path"], m["client_id"], m["custom"]) == \
            ("clip.mp4", len(data), "video/mp4", "/v/clip.mp4", "cam", {"take": 2})
        assert [c["size"] for c in m["chunks"]] == [MB, MB, MB, 5] and m["checksum"] == compute(data)
        got = download(cluster, m["object_id"])
        assert got.content == data and got.headers["Content-Type"].startswith("video/mp4")
        assert got.headers["Content-Length"] == str(len(data)) and got.headers["X-Checksum"] == compute(data)

    def test_the_content_type_is_guessed_when_not_given(self, cluster):
        r = raw_upload(cluster, "notes.txt", b"hello")
        assert r.status_code == 201 and r.json()["content_type"] == "text/plain"

    def test_a_wrong_checksum_is_refused_and_nothing_is_stored(self, cluster):
        r = raw_upload(cluster, "f", os.urandom(2 * MB + 1), checksum=compute(b"other"))
        assert r.status_code == 400 and "damaged in transit" in r.json()["error"]
        assert cluster.total_chunk_files() == 0 and listing(cluster) == []

    def test_a_refused_upload_shows_its_error_even_with_a_big_body(self, cluster):
        raw_upload(cluster, "f", b"x", path="/a/f", client_id="c")
        big = os.urandom(8 * MB)
        r = raw_upload(cluster, "f", big, path="/a/f", client_id="c")                 # refused at once
        assert r.status_code == 409 and "already exists" in r.json()["error"]
        r = raw_upload(cluster, "f", big, metadata="{oops")
        assert r.status_code == 400 and "metadata" in r.json()["error"]
        assert len(listing(cluster)) == 1

    def test_a_raw_body_without_a_name_is_400(self, cluster):
        r = requests.post(cluster.api + "/objects", data=b"data",
                          headers={"Content-Type": "application/octet-stream"}, timeout=TIMEOUT)
        assert r.status_code == 400 and "name" in r.json()["error"]
        r = raw_upload(cluster, "", b"data")
        assert r.status_code == 400

    def test_an_upload_that_stops_half_way_leaves_nothing(self, cluster):
        url = urlparse(cluster.api)
        promised, sent = 3 * MB, 3 * MB // 2
        with socket.create_connection((url.hostname, url.port), timeout=TIMEOUT) as s:
            s.sendall((f"POST {url.path}/objects?name=cut.bin HTTP/1.1\r\nHost: {url.netloc}\r\n"
                       f"Content-Type: application/octet-stream\r\nContent-Length: {promised}\r\n\r\n").encode())
            s.sendall(os.urandom(sent))
        assert wait_until(lambda: not cluster.distributor._in_flight, 30)
        assert listing(cluster) == [] and cluster.total_chunk_files() == 0


class TestStreamedDownload:
    def test_a_download_that_fails_part_way_is_cut_off(self, cluster, tmp_path):
        data = os.urandom(3 * MB + 1)
        oid = upload_ok(cluster, "f.bin", data)["object_id"]
        bad = metadata(cluster, oid)["chunks"][2]
        for node in bad["replicas"]:
            cluster.damage_chunk(node, bad["chunk_id"])
        r = requests.get(f"{cluster.api}/objects/{oid}", stream=True, timeout=TIMEOUT)
        assert r.status_code == 200                               # chunks 0 and 1 were fine
        with pytest.raises(requests.RequestException):
            r.content                                             # fewer bytes than Content-Length
        # the client never keeps it, and leaves no temporary file behind
        out = tmp_path / "out"
        out.mkdir()
        with pytest.raises(ValueError, match="cut off|incomplete"):
            Client(base_url=cluster.api).retrieve(oid, str(out / "f.bin"))
        assert os.listdir(out) == []

    def test_a_failed_download_does_not_touch_an_existing_file(self, cluster, tmp_path):
        oid = upload_ok(cluster, "f.bin", os.urandom(3 * MB))["object_id"]
        bad = metadata(cluster, oid)["chunks"][1]
        for node in bad["replicas"]:
            cluster.damage_chunk(node, bad["chunk_id"])
        target = tmp_path / "f.bin"
        target.write_bytes(b"precious")
        with pytest.raises(ValueError):
            Client(base_url=cluster.api).retrieve(oid, str(target))
        assert target.read_bytes() == b"precious" and os.listdir(tmp_path) == ["f.bin"]

    def test_a_delete_during_a_download_does_not_cut_it_off(self, cluster):
        data = os.urandom(4 * MB)
        oid = upload_ok(cluster, "f.bin", data)["object_id"]
        r = requests.get(f"{cluster.api}/objects/{oid}", stream=True, timeout=TIMEOUT)
        first = next(r.iter_content(64 * 1024))
        assert requests.delete(f"{cluster.api}/objects/{oid}", timeout=TIMEOUT).status_code == 204
        assert download(cluster, oid).status_code == 404
        assert first + b"".join(r.iter_content(MB)) == data
        assert wait_until(lambda: cluster.total_chunk_files() == 0, 10)


class TestClientStreams:
    def test_an_empty_file(self, cluster, tmp_path):
        src = tmp_path / "empty.txt"
        src.write_bytes(b"")
        c = Client(base_url=cluster.api, client_id="t")
        m = c.upload(str(src))
        assert m["size"] == 0 and m["chunk_count"] == 1
        out = tmp_path / "back.txt"
        c.retrieve(m["object_id"], str(out))
        assert out.read_bytes() == b""

    def test_a_50_mb_file_goes_through_in_a_few_mb_of_memory(self, cluster, tmp_path):
        """Client and controller run in this process (the workers do not), so
        this measures both: with whole files in memory it was over 100 MB."""
        src = tmp_path / "big.bin"
        with open(src, "wb") as f:
            for _ in range(50):
                f.write(os.urandom(MB))
        c = Client(base_url=cluster.api, client_id="mem")
        m, upload_peak = peak_memory(lambda: c.upload(str(src)))
        out, download_peak = peak_memory(lambda: c.retrieve(m["object_id"], str(tmp_path / "back.bin")))
        assert m["size"] == 50 * MB and m["chunk_count"] == 50
        assert compute_file(out) == compute_file(str(src))
        assert upload_peak < 10 * MB and download_peak < 10 * MB, (upload_peak / MB, download_peak / MB)


class TestTransferLimit:
    def test_when_every_slot_is_taken_it_is_503_with_retry_after(self, cluster, monkeypatch, tmp_path):
        app = cluster.app_module
        slots = app.TransferSlots(1)
        monkeypatch.setattr(app, "transfers", slots)
        oid = upload_ok(cluster, "f", os.urandom(2 * MB))["object_id"]   # a slot is given back after an upload
        assert slots.take()                                       # someone else holds the only slot
        r = download(cluster, oid)
        assert r.status_code == 503 and r.headers["Retry-After"] == "1" and "busy" in r.json()["error"]
        assert raw_upload(cluster, "g", b"x").status_code == 503
        assert listing(cluster)[0]["object_id"] == oid and len(listing(cluster)) == 1
        slots.give_back()
        for _ in range(3):                                        # each finished download gives it back
            assert download(cluster, oid).status_code == 200
        assert download(cluster, "no-such-id").status_code == 404     # so does a failed one
        assert download(cluster, oid).status_code == 200

    def test_an_abandoned_download_gives_its_slot_back(self, cluster, monkeypatch):
        app = cluster.app_module
        slots = app.TransferSlots(1)
        monkeypatch.setattr(app, "transfers", slots)
        oid = upload_ok(cluster, "f", os.urandom(8 * MB))["object_id"]
        r = requests.get(f"{cluster.api}/objects/{oid}", stream=True, timeout=TIMEOUT)
        next(r.iter_content(1024))
        r.close()                                                 # the client goes away part-way

        def slot_free():
            if slots.take():
                slots.give_back()
                return True
            return False
        assert wait_until(slot_free, 20)
        assert not cluster.distributor._reading
