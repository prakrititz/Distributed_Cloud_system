"""Data integrity (spec section 8): corruption is detected and never returned
while a good copy exists, and the failure is clear when none does."""
import os

import requests

from tests.util import MB, TIMEOUT, download, replicas_of, upload, upload_ok

NODES = ("worker1", "worker2", "worker3", "worker4")


def first_chunk(cluster, object_id):
    chunk_id, replicas = replicas_of(cluster, object_id)[0]
    return chunk_id, replicas


class TestDamageOnDisk:
    def test_one_damaged_copy_the_download_is_still_correct(self, cluster):
        data = os.urandom(2 * MB + 5)
        oid = upload_ok(cluster, "f", data)["object_id"]
        chunk_id, replicas = first_chunk(cluster, oid)
        cluster.damage_chunk(replicas[0], chunk_id)
        r = download(cluster, oid)
        assert r.status_code == 200 and r.content == data

    def test_each_copy_can_be_the_damaged_one(self, cluster):
        data = os.urandom(MB + 5)
        oid = upload_ok(cluster, "f", data)["object_id"]
        for chunk_id, replicas in replicas_of(cluster, oid):
            for node in replicas:
                cluster.damage_chunk(node, chunk_id)
                assert download(cluster, oid).content == data
                # put a good copy back for the next round
                requests.put(f"{cluster.node_url(node)}/internal/chunks/{chunk_id}",
                             data=requests.get(
                                 f"{cluster.node_url([r for r in replicas if r != node][0])}/internal/chunks/{chunk_id}",
                                 timeout=TIMEOUT).content,
                             headers={"X-Checksum": requests.get(
                                 f"{cluster.node_url([r for r in replicas if r != node][0])}/internal/chunks/{chunk_id}",
                                 timeout=TIMEOUT).headers["X-Checksum"]}, timeout=TIMEOUT)

    def test_both_copies_damaged_is_a_clear_503_and_never_bad_data(self, cluster):
        data = os.urandom(2 * MB)
        oid = upload_ok(cluster, "f", data)["object_id"]
        chunk_id, replicas = first_chunk(cluster, oid)
        for node in replicas:
            cluster.damage_chunk(node, chunk_id)
        r = download(cluster, oid)
        assert r.status_code == 503 and "No good copy of chunk 0" in r.json()["error"]
        assert "corrupt" in r.json()["error"] and r.content != data

    def test_one_damaged_and_the_other_node_down_is_a_clear_503(self, cluster):
        oid = upload_ok(cluster, "f", os.urandom(MB + 1))["object_id"]
        chunk_id, replicas = first_chunk(cluster, oid)
        cluster.damage_chunk(replicas[0], chunk_id)
        cluster.kill(replicas[1])
        r = download(cluster, oid)
        assert r.status_code == 503 and "No good copy" in r.json()["error"]

    def test_the_other_objects_are_not_affected(self, cluster):
        a = os.urandom(MB + 1)
        b = os.urandom(MB + 2)
        oa = upload_ok(cluster, "a", a)["object_id"]
        ob = upload_ok(cluster, "b", b)["object_id"]
        chunk_id, replicas = first_chunk(cluster, oa)
        for node in replicas:
            cluster.damage_chunk(node, chunk_id)
        assert download(cluster, oa).status_code == 503
        assert download(cluster, ob).content == b

    def test_a_deleted_chunk_file_is_survivable_too(self, cluster):
        data = os.urandom(MB + 1)
        oid = upload_ok(cluster, "f", data)["object_id"]
        chunk_id, replicas = first_chunk(cluster, oid)
        os.remove(cluster.chunk_path(replicas[0], chunk_id))
        assert download(cluster, oid).content == data

    def test_the_replication_report_flags_the_corrupt_copy(self, cluster):
        oid = upload_ok(cluster, "f", os.urandom(MB + 1))["object_id"]
        chunk_id, replicas = first_chunk(cluster, oid)
        cluster.damage_chunk(replicas[0], chunk_id)
        report = requests.get(cluster.api + "/replication", timeout=TIMEOUT).json()
        chunk = next(c for c in report["chunks"] if c["chunk_id"] == chunk_id)
        assert {r["node"]: r["state"] for r in chunk["replicas"]} == {replicas[0]: "corrupt", replicas[1]: "ok"}
        assert chunk["status"] == "under_replicated" and report["summary"]["under_replicated"] == 1


class TestDamageInTransit:
    def test_an_upload_with_a_wrong_checksum_is_rejected_before_anything_is_stored(self, cluster):
        r = upload(cluster, "f", os.urandom(MB), checksum="a" * 64)
        assert r.status_code == 400
        assert cluster.total_chunk_files() == 0

    def test_a_truncated_upload_is_caught_by_the_checksum(self, cluster):
        from helpers.checksum import compute
        data = os.urandom(5000)
        r = upload(cluster, "f", data[:4000], checksum=compute(data))     # the tail was lost on the way
        assert r.status_code == 400 and cluster.total_chunk_files() == 0
