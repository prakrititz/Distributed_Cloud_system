"""Several clients at once: different folders, the same files, concurrent
uploads, downloads, patches and deletes. After everything, the system must be
consistent: no lost updates, no orphaned chunks, every chunk on two nodes."""
import os
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests

from client import Client
from tests.util import MB, TIMEOUT, expected_chunk_files, listing, metadata, replicas_of

WORKERS = 12          # threads used to hammer the controller at once


def make_client(cluster, name):
    return Client(base_url=cluster.api, client_id=name)


def write(tmp_path, relative, content):
    path = tmp_path / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return str(path)


def assert_consistent(cluster):
    """No orphans, every chunk on two distinct nodes holding intact data, report healthy."""
    assert cluster.total_chunk_files() == expected_chunk_files(cluster), "orphaned or missing chunk files"
    for o in listing(cluster):
        for chunk_id, replicas in replicas_of(cluster, o["object_id"]):
            assert len(replicas) == 2 and len(set(replicas)) == 2
            for node in replicas:
                assert chunk_id in cluster.chunk_files(node)
    r = requests.get(cluster.api + "/replication", timeout=TIMEOUT).json()
    assert r["summary"]["healthy"] == r["summary"]["total_chunks"], r["summary"]


# ============================================================ identity rules
class TestClientIdentity:
    def test_the_same_file_path_from_two_clients_gives_two_objects(self, cluster, tmp_path):
        path = write(tmp_path, "shared/report.txt", b"the same file on a shared mount")
        a = make_client(cluster, "clientA").upload(path)
        b = make_client(cluster, "clientB").upload(path)
        assert a["object_id"] != b["object_id"]
        assert (a["client_id"], b["client_id"]) == ("clientA", "clientB")
        assert a["source_path"] == b["source_path"] == os.path.abspath(path)

    def test_each_client_still_cannot_upload_its_own_path_twice(self, cluster, tmp_path):
        path = write(tmp_path, "f.txt", b"x")
        for name in ("clientA", "clientB"):
            c = make_client(cluster, name)
            first = c.upload(path)
            with pytest.raises(ValueError) as e:
                c.upload(path)
            assert "409" in str(e.value) and first["object_id"] in str(e.value) and name in str(e.value)

    def test_same_file_name_in_different_folders_is_allowed_for_one_client(self, cluster, tmp_path):
        c = make_client(cluster, "clientA")
        a = c.upload(write(tmp_path, "a/report.txt", b"A"))
        b = c.upload(write(tmp_path, "b/report.txt", b"B"))
        assert a["name"] == b["name"] == "report.txt" and a["object_id"] != b["object_id"]

    def test_clients_see_each_others_objects(self, cluster, tmp_path):
        a, b = make_client(cluster, "clientA"), make_client(cluster, "clientB")
        data = os.urandom(MB + 5)
        m = a.upload(write(tmp_path, "up/data.bin", data))
        out = tmp_path / "down"
        out.mkdir()
        saved = b.retrieve(m["object_id"], str(out / "copy.bin"))
        assert open(saved, "rb").read() == data
        assert m["object_id"] in [o["object_id"] for o in b.list_objects()]

    def test_one_client_can_change_and_delete_what_another_uploaded(self, cluster, tmp_path):
        a, b = make_client(cluster, "clientA"), make_client(cluster, "clientB")
        m = a.upload(write(tmp_path, "f.txt", b"x"))
        assert b.update_metadata(m["object_id"], {"seen_by": "B"})["custom"] == {"seen_by": "B"}
        b.delete(m["object_id"])
        assert a.list_objects() == []

    def test_the_default_client_id_comes_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("CLIENT_ID", "from-env")
        assert Client(base_url="http://x").client_id == "from-env"
        monkeypatch.delenv("CLIENT_ID")
        assert Client(base_url="http://x").client_id          # falls back to the machine's name
        assert Client(base_url="http://x", client_id="explicit").client_id == "explicit"


# ========================================================== concurrent uploads
class TestConcurrentUploads:
    def test_many_clients_upload_different_folders_at_the_same_time(self, cluster, tmp_path):
        clients = [f"client{i}" for i in range(6)]
        jobs = []                                       # (client name, path, content)
        for i, name in enumerate(clients):
            for j, size in enumerate([0, 700, MB + 11 + i, 2 * MB + i]):
                jobs.append((name, write(tmp_path, f"{name}/folder{j}/report.txt", os.urandom(size)), None))
        jobs = [(n, p, open(p, "rb").read()) for n, p, _ in jobs]
        start = threading.Barrier(len(jobs))

        def go(job):
            name, path, _ = job
            start.wait()                                # everyone starts at the same instant
            return make_client(cluster, name).upload(path)

        with ThreadPoolExecutor(len(jobs)) as pool:
            results = list(pool.map(go, jobs))

        assert len({m["object_id"] for m in results}) == len(jobs)          # 24 distinct objects
        assert len(listing(cluster)) == len(jobs)
        for (name, path, content), m in zip(jobs, results):
            assert m["client_id"] == name and m["size"] == len(content) and m["name"] == "report.txt"

        # and all of them come back byte for byte, downloaded concurrently too
        out = tmp_path / "out"
        out.mkdir()

        def fetch(args):
            (name, path, content), m = args
            saved = make_client(cluster, name).retrieve(m["object_id"], str(out / m["object_id"]))
            return open(saved, "rb").read() == content

        with ThreadPoolExecutor(WORKERS) as pool:
            assert all(pool.map(fetch, zip(jobs, results)))
        assert_consistent(cluster)

    def test_the_very_same_file_uploaded_concurrently_by_different_clients(self, cluster, tmp_path):
        data = os.urandom(2 * MB + 3)
        path = write(tmp_path, "shared/the-file.bin", data)
        names = [f"client{i}" for i in range(8)]
        start = threading.Barrier(len(names))

        def go(name):
            start.wait()
            return make_client(cluster, name).upload(path)

        with ThreadPoolExecutor(len(names)) as pool:
            results = list(pool.map(go, names))

        assert len({m["object_id"] for m in results}) == 8
        assert {m["client_id"] for m in results} == set(names)
        assert {m["checksum"] for m in results} == {results[0]["checksum"]}
        assert_consistent(cluster)

    def test_one_client_racing_itself_on_one_path_gets_exactly_one_object_and_no_orphans(self, cluster, tmp_path):
        path = write(tmp_path, "f/big.bin", os.urandom(3 * MB))              # several chunks: a real race window
        racers = 8
        start = threading.Barrier(racers)
        outcomes = []

        def go(_):
            start.wait()
            try:
                outcomes.append(("ok", make_client(cluster, "solo").upload(path)))
            except ValueError as e:
                outcomes.append(("conflict", str(e)))

        with ThreadPoolExecutor(racers) as pool:
            list(pool.map(go, range(racers)))

        winners = [o for kind, o in outcomes if kind == "ok"]
        losers = [o for kind, o in outcomes if kind == "conflict"]
        assert len(winners) == 1 and len(losers) == racers - 1
        assert all("409" in text for text in losers)
        assert len(listing(cluster)) == 1
        assert_consistent(cluster)               # the losers' partial chunks were rolled back, none left over

    def test_uploading_many_files_from_one_client_in_parallel(self, cluster, tmp_path):
        c = make_client(cluster, "solo")
        paths = [write(tmp_path, f"d{i % 3}/file{i}.bin", os.urandom(MB // 2 + i)) for i in range(15)]
        with ThreadPoolExecutor(WORKERS) as pool:
            results = list(pool.map(c.upload, paths))
        assert len({m["object_id"] for m in results}) == 15
        assert_consistent(cluster)


# ============================================================ concurrent changes
class TestConcurrentChanges:
    def test_parallel_patches_lose_no_updates(self, cluster, tmp_path):
        m = make_client(cluster, "a").upload(write(tmp_path, "f.txt", b"x"))
        oid = m["object_id"]
        n = 40

        def patch(i):
            r = requests.patch(f"{cluster.api}/objects/{oid}/metadata", json={f"key{i}": i}, timeout=TIMEOUT)
            return r.status_code

        with ThreadPoolExecutor(WORKERS) as pool:
            assert set(pool.map(patch, range(n))) == {200}
        assert metadata(cluster, oid)["custom"] == {f"key{i}": i for i in range(n)}

    def test_parallel_patches_of_the_same_key_leave_one_of_the_values(self, cluster, tmp_path):
        oid = make_client(cluster, "a").upload(write(tmp_path, "f.txt", b"x"))["object_id"]
        with ThreadPoolExecutor(WORKERS) as pool:
            list(pool.map(lambda i: requests.patch(f"{cluster.api}/objects/{oid}/metadata",
                                                   json={"winner": i}, timeout=TIMEOUT), range(30)))
        assert metadata(cluster, oid)["custom"]["winner"] in range(30)

    def test_two_clients_deleting_the_same_object_exactly_one_succeeds(self, cluster, tmp_path):
        oid = make_client(cluster, "a").upload(write(tmp_path, "f.bin", os.urandom(2 * MB)))["object_id"]
        start = threading.Barrier(6)

        def delete(_):
            start.wait()
            return requests.delete(f"{cluster.api}/objects/{oid}", timeout=TIMEOUT).status_code

        with ThreadPoolExecutor(6) as pool:
            codes = sorted(pool.map(delete, range(6)))
        assert codes == [204] + [404] * 5
        assert cluster.total_chunk_files() == 0

    def test_a_mixed_workload_never_causes_a_server_error_and_ends_consistent(self, cluster, tmp_path):
        """Uploads, downloads, lists, patches and deletes all at once, from several clients."""
        seed = [make_client(cluster, "seed").upload(write(tmp_path, f"seed/f{i}.bin", os.urandom(MB + i)))
                for i in range(6)]
        seed_ids = [m["object_id"] for m in seed]
        statuses = []
        lock = threading.Lock()

        def record(code, allowed):
            with lock:
                statuses.append((code, allowed))

        def uploader(i):
            c = make_client(cluster, f"up{i}")
            for j in range(3):
                path = write(tmp_path, f"up{i}/f{j}.bin", os.urandom(MB // 2 + j))
                try:
                    c.upload(path)
                    record(201, {201})
                except ValueError as e:
                    record(int(str(e).split()[2].rstrip(":")), {201})

        def downloader(i):
            for oid in seed_ids * 2:
                r = requests.get(f"{cluster.api}/objects/{oid}", timeout=TIMEOUT)
                record(r.status_code, {200, 404, 503})            # 404/503: it was deleted meanwhile

        def lister(i):
            for _ in range(10):
                record(requests.get(cluster.api + "/objects", timeout=TIMEOUT).status_code, {200})

        def patcher(i):
            for oid in seed_ids:
                r = requests.patch(f"{cluster.api}/objects/{oid}/metadata", json={f"p{i}": i}, timeout=TIMEOUT)
                record(r.status_code, {200, 404})

        def deleter(i):
            for oid in seed_ids[i::2]:
                record(requests.delete(f"{cluster.api}/objects/{oid}", timeout=TIMEOUT).status_code, {204, 404})

        tasks = ([(uploader, i) for i in range(4)] + [(downloader, i) for i in range(3)] +
                 [(lister, i) for i in range(2)] + [(patcher, i) for i in range(3)] + [(deleter, i) for i in range(2)])
        start = threading.Barrier(len(tasks))

        def run(task):
            fn, i = task
            start.wait()
            fn(i)

        with ThreadPoolExecutor(len(tasks)) as pool:
            list(pool.map(run, tasks))

        assert statuses, "nothing ran"
        unexpected = [(code, allowed) for code, allowed in statuses if code not in allowed]
        assert not unexpected, unexpected[:5]
        assert not [c for c, _ in statuses if c >= 500 and c != 503]
        # everything the deleters touched is gone; what remains is intact
        remaining = {o["object_id"] for o in listing(cluster)}
        assert not remaining & set(seed_ids)
        assert_consistent(cluster)
