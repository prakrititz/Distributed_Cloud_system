"""Worker failure and repair (spec sections 7 and 10), end to end: before,
during and after a failure, with real node processes and `POST /repair`."""
import os

import requests

from tests.util import MB, TIMEOUT, download, metadata, replicas_of, upload_ok


def report(cluster):
    return requests.get(cluster.api + "/replication", timeout=TIMEOUT).json()


def repair(cluster):
    r = requests.post(cluster.api + "/repair", timeout=120)
    assert r.status_code == 200, r.text
    return r.json()


def every_chunk_on_two_distinct_live_nodes(cluster, object_id, dead=()):
    for chunk_id, replicas in replicas_of(cluster, object_id):
        assert len(replicas) == 2 and len(set(replicas)) == 2, (chunk_id, replicas)
        assert not set(replicas) & set(dead), (chunk_id, replicas)
        for node in replicas:
            assert chunk_id in cluster.chunk_files(node)


class TestWorkerFailure:
    def test_the_full_story_before_during_and_after(self, cluster):
        data = os.urandom(5 * MB + 17)
        oid = upload_ok(cluster, "movie.bin", data)["object_id"]

        # before: every chunk has two copies on different workers
        assert report(cluster)["summary"] == {"total_chunks": 6, "healthy": 6, "under_replicated": 0, "unavailable": 0}
        every_chunk_on_two_distinct_live_nodes(cluster, oid)

        # during: stop a worker that holds copies
        victim = replicas_of(cluster, oid)[0][1][0]
        held = sum(victim in reps for _, reps in replicas_of(cluster, oid))
        cluster.kill(victim)
        assert cluster.wait_down(victim)
        during = report(cluster)
        assert during["summary"]["under_replicated"] == held and during["summary"]["unavailable"] == 0
        assert download(cluster, oid).content == data                    # still served from the other copies

        # repair restores the factor on the remaining workers
        result = repair(cluster)
        assert result["repaired"] == held and result["copies_created"] == held and result["unrecoverable"] == 0
        every_chunk_on_two_distinct_live_nodes(cluster, oid, dead=[victim])
        assert report(cluster)["summary"]["healthy"] == 6
        assert download(cluster, oid).content == data

        # after: the worker comes back, with its old (now surplus) copies on disk
        cluster.restart(victim)
        assert cluster.wait_alive(victim)
        assert report(cluster)["summary"]["healthy"] == 6
        assert repair(cluster)["copies_created"] == 0
        assert download(cluster, oid).content == data

    def test_a_worker_that_returns_before_repair_needs_no_copying(self, cluster):
        data = os.urandom(3 * MB)
        oid = upload_ok(cluster, "f", data)["object_id"]
        victim = replicas_of(cluster, oid)[0][1][0]
        cluster.kill(victim)
        assert cluster.wait_down(victim)
        cluster.restart(victim)
        assert cluster.wait_alive(victim)
        result = repair(cluster)
        assert result["copies_created"] == 0 and result["healthy"] == result["chunks_checked"]
        assert download(cluster, oid).content == data

    def test_two_workers_down_one_copy_left_is_still_served_and_repaired_as_far_as_possible(self, cluster):
        data = os.urandom(3 * MB)
        oid = upload_ok(cluster, "f", data)["object_id"]
        cluster.kill("Node1")
        assert cluster.wait_down("Node1")
        repair(cluster)
        cluster.kill("Node2")
        assert cluster.wait_down("Node2")
        assert download(cluster, oid).content == data               # every chunk was back to two copies first
        result = repair(cluster)
        assert result["unrecoverable"] == 0
        every_chunk_on_two_distinct_live_nodes(cluster, oid, dead=["Node1", "Node2"])


class TestCorruptionRepair:
    def test_a_damaged_copy_is_found_and_rebuilt(self, cluster):
        data = os.urandom(2 * MB + 3)
        oid = upload_ok(cluster, "f", data)["object_id"]
        chunk_id, replicas = replicas_of(cluster, oid)[0]
        cluster.damage_chunk(replicas[0], chunk_id)
        assert report(cluster)["summary"]["under_replicated"] == 1
        result = repair(cluster)
        assert result["repaired"] == 1 and result["copies_created"] == 1
        assert report(cluster)["summary"]["healthy"] == 3
        every_chunk_on_two_distinct_live_nodes(cluster, oid)
        assert download(cluster, oid).content == data

    def test_after_the_repair_the_damaged_node_can_be_the_only_one_left_and_data_is_still_right(self, cluster):
        data = os.urandom(MB + 1)
        oid = upload_ok(cluster, "f", data)["object_id"]
        chunk_id, replicas = replicas_of(cluster, oid)[0]
        cluster.damage_chunk(replicas[0], chunk_id)
        repair(cluster)
        _, new_replicas = replicas_of(cluster, oid)[0]
        cluster.kill(new_replicas[0])                               # lose either of the two good copies...
        assert cluster.wait_down(new_replicas[0])
        assert download(cluster, oid).content == data               # ...and the data is still right

    def test_when_both_copies_are_damaged_nothing_is_invented(self, cluster):
        oid = upload_ok(cluster, "f", os.urandom(2 * MB))["object_id"]
        chunk_id, replicas = replicas_of(cluster, oid)[0]
        for node in replicas:
            cluster.damage_chunk(node, chunk_id)
        result = repair(cluster)
        assert result["unrecoverable"] == 1 and result["copies_created"] == 0
        assert replicas_of(cluster, oid)[0][1] == replicas
        assert download(cluster, oid).status_code == 503

    def test_a_lost_chunk_file_is_recreated(self, cluster):
        data = os.urandom(MB + 1)
        oid = upload_ok(cluster, "f", data)["object_id"]
        chunk_id, replicas = replicas_of(cluster, oid)[0]
        os.remove(cluster.chunk_path(replicas[1], chunk_id))
        assert repair(cluster)["repaired"] == 1
        every_chunk_on_two_distinct_live_nodes(cluster, oid)


class TestRepairEndpoint:
    def test_on_a_healthy_system_it_does_nothing(self, cluster):
        upload_ok(cluster, "f", os.urandom(2 * MB + 1))                  # 3 chunks
        files = cluster.total_chunk_files()
        result = repair(cluster)
        assert result["healthy"] == result["chunks_checked"] == 3 and result["copies_created"] == 0
        assert cluster.total_chunk_files() == files

    def test_on_an_empty_system(self, cluster):
        assert repair(cluster)["chunks_checked"] == 0

    def test_get_is_not_allowed(self, cluster):
        assert requests.get(cluster.api + "/repair", timeout=TIMEOUT).status_code == 405

    def test_a_second_repair_while_one_runs_is_a_409(self, cluster):
        lock = cluster.distributor._repair_lock
        assert lock.acquire(blocking=False)
        try:
            r = requests.post(cluster.api + "/repair", timeout=TIMEOUT)
            assert r.status_code == 409 and "already running" in r.json()["error"]
        finally:
            lock.release()
        assert requests.post(cluster.api + "/repair", timeout=TIMEOUT).status_code == 200

    def test_the_metadata_of_repaired_objects_stays_consistent(self, cluster):
        oid = upload_ok(cluster, "f", os.urandom(2 * MB))["object_id"]
        victim = replicas_of(cluster, oid)[0][1][0]
        before = metadata(cluster, oid)
        cluster.kill(victim)
        assert cluster.wait_down(victim)
        repair(cluster)
        after = metadata(cluster, oid)
        for key in ("object_id", "name", "size", "checksum", "created_at", "custom", "chunk_count"):
            assert after[key] == before[key]
        assert [c["checksum"] for c in after["chunks"]] == [c["checksum"] for c in before["chunks"]]
