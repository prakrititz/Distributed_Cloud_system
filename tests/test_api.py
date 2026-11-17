"""The client-facing REST API (app.py), over real HTTP against the real cluster."""
import os
import time

import requests

from tests.util import (MB, TIMEOUT, compute, download, expected_chunk_files, listing, metadata,
                        upload, upload_ok)

SYSTEM_FIELDS = {"object_id", "name", "size", "content_type", "created_at", "modified_at", "checksum",
                 "source_path", "client_id", "chunk_count", "copies_per_chunk", "chunks", "custom"}


class TestUpload:
    def test_a_spec_style_upload_with_only_the_file(self, cluster):
        data = os.urandom(5000)
        m = upload_ok(cluster, "raw.bin", data, content_type="application/x-custom")
        assert SYSTEM_FIELDS <= set(m)
        assert (m["name"], m["size"], m["content_type"]) == ("raw.bin", 5000, "application/x-custom")
        assert m["checksum"] == compute(data) and m["chunk_count"] == 1 and m["custom"] == {}
        assert m["created_at"] == m["modified_at"]

    def test_the_response_status_is_201(self, cluster):
        assert upload(cluster, "a.txt", b"hi").status_code == 201

    def test_the_content_type_is_guessed_from_the_name_when_not_given(self, cluster):
        assert upload_ok(cluster, "notes.txt", b"hello")["content_type"] == "text/plain"
        assert upload_ok(cluster, "movie.mp4", b"x")["content_type"] == "video/mp4"
        assert upload_ok(cluster, "mystery", b"x")["content_type"] == "application/octet-stream"

    def test_custom_metadata_is_stored(self, cluster):
        m = upload_ok(cluster, "f", b"x", metadata='{"project": "demo", "n": 3, "ok": true}')
        assert m["custom"] == {"project": "demo", "n": 3, "ok": True}

    def test_null_values_in_upload_metadata_are_dropped(self, cluster):
        assert upload_ok(cluster, "f", b"x", metadata='{"a": "1", "b": null}')["custom"] == {"a": "1"}

    def test_a_multi_chunk_file_is_chunked_at_one_megabyte(self, cluster):
        m = upload_ok(cluster, "big", os.urandom(3 * MB + 9))
        assert m["chunk_count"] == 4 and m["size"] == 3 * MB + 9
        assert [c["index"] for c in m["chunks"]] == [0, 1, 2, 3]
        assert all(len(c["replicas"]) == 2 and len(set(c["replicas"])) == 2 for c in m["chunks"])

    def test_the_original_size_can_be_sent_separately(self, cluster):
        assert upload_ok(cluster, "f", b"x" * 100, size="60")["size"] == 60

    def test_same_name_without_a_path_is_never_refused(self, cluster):
        a = upload_ok(cluster, "same.bin", b"1")
        b = upload_ok(cluster, "same.bin", b"2")
        assert a["object_id"] != b["object_id"]

    def test_the_same_path_twice_is_a_conflict_and_stores_nothing(self, cluster):
        first = upload_ok(cluster, "f", b"data", path="/a/f", client_id="c1")
        files_before = cluster.total_chunk_files()
        r = upload(cluster, "f", b"data", path="/a/f", client_id="c1")
        assert r.status_code == 409 and first["object_id"] in r.json()["error"]
        assert cluster.total_chunk_files() == files_before and len(listing(cluster)) == 1

    def test_different_clients_may_use_the_same_path(self, cluster):
        a = upload_ok(cluster, "f", b"A", path="/files/f", client_id="clientA")
        b = upload_ok(cluster, "f", b"B", path="/files/f", client_id="clientB")
        assert a["client_id"] == "clientA" and b["client_id"] == "clientB"
        assert download(cluster, a["object_id"]).content == b"A"
        assert download(cluster, b["object_id"]).content == b"B"

    def test_a_path_without_a_client_id_has_its_own_scope(self, cluster):
        upload_ok(cluster, "f", b"x", path="/p")
        assert upload(cluster, "f", b"x", path="/p").status_code == 409
        assert upload(cluster, "f", b"x", path="/p", client_id="c1").status_code == 201

    def test_deleting_frees_the_path_again(self, cluster):
        m = upload_ok(cluster, "f", b"x", path="/p", client_id="c1")
        requests.delete(f"{cluster.api}/objects/{m['object_id']}", timeout=TIMEOUT)
        assert upload(cluster, "f", b"x", path="/p", client_id="c1").status_code == 201

    def test_bad_fields_are_400(self, cluster):
        f = {"file": ("x.txt", b"data", "text/plain")}
        for data in ({"metadata": "{oops"}, {"metadata": "[1]"}, {"metadata": '{"k": {"nested": 1}}'},
                     {"metadata": '{"": "x"}'}, {"size": "abc"}, {"size": "-5"},
                     {"client_id": "x" * 500}):
            r = requests.post(cluster.api + "/objects", files=f, data=data, timeout=TIMEOUT)
            assert r.status_code == 400 and "error" in r.json(), data
        assert listing(cluster) == []

    def test_a_missing_file_field_is_400(self, cluster):
        for kwargs in ({"data": {"checksum": "x"}}, {"files": {"other": ("x", b"y")}},
                       {"files": {"file": ("", b"y")}}):
            r = requests.post(cluster.api + "/objects", timeout=TIMEOUT, **kwargs)
            assert r.status_code == 400 and "file" in r.json()["error"].lower(), kwargs

    def test_a_wrong_checksum_is_refused_and_nothing_is_stored(self, cluster):
        r = upload(cluster, "f", b"data", checksum="0" * 64)
        assert r.status_code == 400 and "damaged in transit" in r.json()["error"]
        assert cluster.total_chunk_files() == 0 and listing(cluster) == []

    def test_a_correct_checksum_is_accepted(self, cluster):
        assert upload(cluster, "f", b"data", checksum=compute(b"data")).status_code == 201

    def test_a_path_in_the_file_name_is_reduced_to_the_name(self, cluster):
        assert upload_ok(cluster, "..\\..\\evil/../name.txt", b"x")["name"] == "name.txt"

    def test_when_the_nodes_cannot_hold_the_object_it_is_503_and_nothing_is_left(self, cluster):
        for n in ("worker2", "worker3", "worker4"):
            cluster.kill(n)
            assert cluster.wait_down(n)
        r = upload(cluster, "f", os.urandom(2 * MB))
        assert r.status_code == 503 and "error" in r.json()
        assert cluster.chunk_files("worker1") == [] and listing(cluster) == []


class TestDownload:
    def test_returns_the_bytes_with_content_type_and_checksum(self, cluster):
        data = os.urandom(777)
        m = upload_ok(cluster, "pic.png", data, content_type="image/png")
        r = download(cluster, m["object_id"])
        assert r.status_code == 200 and r.content == data
        assert r.headers["Content-Type"].startswith("image/png")
        assert r.headers["X-Checksum"] == compute(data)

    def test_a_multi_chunk_object_is_reassembled_in_order(self, cluster):
        data = os.urandom(3 * MB + 123)
        m = upload_ok(cluster, "big.bin", data)
        assert download(cluster, m["object_id"]).content == data

    def test_an_empty_object(self, cluster):
        m = upload_ok(cluster, "empty", b"")
        assert m["size"] == 0 and m["chunk_count"] == 1
        assert download(cluster, m["object_id"]).content == b""

    def test_unknown_id_is_404_json(self, cluster):
        r = download(cluster, "no-such-id")
        assert r.status_code == 404 and "error" in r.json()

    def test_the_client_does_not_need_to_know_where_the_chunks_are(self, cluster):
        data = os.urandom(2 * MB)
        m = upload_ok(cluster, "f", data)
        assert download(cluster, m["object_id"]).content == data      # only the id was used


class TestMetadataEndpoints:
    def test_get_returns_system_and_custom_metadata_without_downloading(self, cluster):
        m = upload_ok(cluster, "f", os.urandom(2 * MB + 1), metadata='{"k": "v"}')
        g = metadata(cluster, m["object_id"])
        assert SYSTEM_FIELDS <= set(g) and g["custom"] == {"k": "v"} and g["chunk_count"] == 3
        assert g == m

    def test_patch_adds_changes_and_removes_keys(self, cluster):
        m = upload_ok(cluster, "f", b"x", metadata='{"project": "demo", "owner": "ansh"}')
        r = requests.patch(f"{cluster.api}/objects/{m['object_id']}/metadata", timeout=TIMEOUT,
                           json={"project": "demo2", "stage": "2", "owner": None})
        assert r.status_code == 200 and r.json()["custom"] == {"project": "demo2", "stage": "2"}
        assert metadata(cluster, m["object_id"])["custom"] == {"project": "demo2", "stage": "2"}

    def test_patch_updates_modified_at_but_not_created_at_chunks_or_data(self, cluster):
        data = os.urandom(MB + 5)
        m = upload_ok(cluster, "f", data)
        files_before = {n: cluster.chunk_files(n) for n in ("worker1", "worker2", "worker3", "worker4")}
        time.sleep(1.1)                                   # timestamps have one-second resolution
        u = requests.patch(f"{cluster.api}/objects/{m['object_id']}/metadata", json={"a": "b"}, timeout=TIMEOUT).json()
        assert u["modified_at"] > m["modified_at"] and u["created_at"] == m["created_at"]
        assert u["chunks"] == m["chunks"] and u["checksum"] == m["checksum"]
        assert {n: cluster.chunk_files(n) for n in files_before} == files_before
        assert download(cluster, m["object_id"]).content == data

    def test_patch_with_a_bad_body_is_400(self, cluster):
        m = upload_ok(cluster, "f", b"x")
        url = f"{cluster.api}/objects/{m['object_id']}/metadata"
        for kwargs in ({"json": ["x"]}, {"json": {"k": {"nested": 1}}}, {"json": {"k": [1]}}, {"json": {"": "x"}},
                       {"data": "not json", "headers": {"Content-Type": "application/json"}}):
            r = requests.patch(url, timeout=TIMEOUT, **kwargs)
            assert r.status_code == 400 and "error" in r.json(), kwargs

    def test_unknown_object_is_404_for_get_and_patch(self, cluster):
        assert requests.get(cluster.api + "/objects/nope/metadata", timeout=TIMEOUT).status_code == 404
        assert requests.patch(cluster.api + "/objects/nope/metadata", json={"a": 1}, timeout=TIMEOUT).status_code == 404


class TestList:
    def test_lists_every_object_with_the_required_fields_oldest_first(self, cluster):
        for i in range(4):
            upload_ok(cluster, f"f{i}", bytes([i]) * 10, content_type="text/plain")
        items = listing(cluster)
        assert len(items) == 4
        assert all({"object_id", "name", "size", "content_type", "created_at", "modified_at"} <= set(o) for o in items)
        assert [o["created_at"] for o in items] == sorted(o["created_at"] for o in items)

    def test_an_empty_system(self, cluster):
        assert listing(cluster) == []

    def test_pagination_and_the_total_count_header(self, cluster):
        for i in range(5):
            upload_ok(cluster, f"f{i}", b"x")
        everything = listing(cluster)
        r = requests.get(cluster.api + "/objects", params={"limit": 2, "offset": 1}, timeout=TIMEOUT)
        assert [o["object_id"] for o in r.json()] == [o["object_id"] for o in everything[1:3]]
        assert r.headers["X-Total-Count"] == "5"
        assert listing(cluster, offset=4) == everything[4:]
        assert listing(cluster, offset=50) == []
        assert listing(cluster, limit=0) == []

    def test_bad_pagination_is_400(self, cluster):
        for params in ({"limit": "x"}, {"offset": "x"}, {"offset": -1}, {"limit": -1}):
            assert requests.get(cluster.api + "/objects", params=params, timeout=TIMEOUT).status_code == 400


class TestDelete:
    def test_removes_the_object_and_every_copy_from_the_nodes(self, cluster):
        keep = upload_ok(cluster, "keep", os.urandom(MB + 1))
        gone = upload_ok(cluster, "gone", os.urandom(3 * MB + 9))
        assert cluster.total_chunk_files() == 2 * (keep["chunk_count"] + gone["chunk_count"])
        r = requests.delete(f"{cluster.api}/objects/{gone['object_id']}", timeout=TIMEOUT)
        assert r.status_code == 204 and r.content == b""
        assert cluster.total_chunk_files() == 2 * keep["chunk_count"] == expected_chunk_files(cluster)
        assert download(cluster, keep["object_id"]).status_code == 200

    def test_everything_about_a_deleted_object_is_gone(self, cluster):
        m = upload_ok(cluster, "f", b"x")
        oid = m["object_id"]
        requests.delete(f"{cluster.api}/objects/{oid}", timeout=TIMEOUT)
        assert download(cluster, oid).status_code == 404
        assert requests.get(f"{cluster.api}/objects/{oid}/metadata", timeout=TIMEOUT).status_code == 404
        assert requests.patch(f"{cluster.api}/objects/{oid}/metadata", json={"a": 1}, timeout=TIMEOUT).status_code == 404
        assert requests.delete(f"{cluster.api}/objects/{oid}", timeout=TIMEOUT).status_code == 404
        assert oid not in [o["object_id"] for o in listing(cluster)]


class TestErrorsAreJson:
    def test_unknown_url(self, cluster):
        r = requests.get(cluster.api + "/nothing-here", timeout=TIMEOUT)
        assert r.status_code == 404 and "error" in r.json()

    def test_wrong_method(self, cluster):
        r = requests.put(cluster.api + "/objects", timeout=TIMEOUT)
        assert r.status_code == 405 and "error" in r.json()

    def test_an_unexpected_failure_is_a_500_without_a_stack_trace(self, cluster, monkeypatch):
        monkeypatch.setattr(cluster.distributor, "list_objects",
                            lambda: (_ for _ in ()).throw(RuntimeError("secret internal detail")))
        r = requests.get(cluster.api + "/objects", timeout=TIMEOUT)
        assert r.status_code == 500 and r.json() == {"error": "Internal server error"}
        assert "secret" not in r.text and "Traceback" not in r.text


class TestDebugEndpoints:
    def test_nodes_lists_all_four_with_heartbeat_ages(self, cluster):
        nodes = requests.get(cluster.api + "/nodes", timeout=TIMEOUT).json()
        assert sorted(n["node"] for n in nodes) == ["worker1", "worker2", "worker3", "worker4"]
        assert all(n["alive"] and n["capacity"] == cluster.capacity and n["heartbeat_age"] is not None for n in nodes)

    def test_replication_report_summarises_every_chunk(self, cluster):
        m = upload_ok(cluster, "f", os.urandom(2 * MB + 1))
        r = requests.get(cluster.api + "/replication", timeout=TIMEOUT).json()
        assert r["replication_factor"] == 2
        assert r["summary"] == {"total_chunks": 3, "healthy": 3, "under_replicated": 0, "unavailable": 0}
        assert all(c["object_id"] == m["object_id"] for c in r["chunks"])
