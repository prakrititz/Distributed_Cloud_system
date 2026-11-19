"""Authentication, per-client access, and a fair share of capacity (issue #27).

With API_KEYS set, every client proves who it is with its key, sees and
changes only its own objects (and reads the ones shared with it), and gets
its own share of the transfer slots, its own request rate and its own storage
quota. Admins may do everything. Without API_KEYS nothing changes (the rest of
the test suite runs that way).
"""
import os
import threading

import pytest
import requests

from client import Client
from controller.access import ClientSlots, KeyRing, RateLimiter, parse_keys
from tests.util import MB, TIMEOUT, post_heartbeat

KEYS = {"alice": "alice-key-0001", "bob": "bob-key-00002", "admin": "admin-key-0003"}


def auth(who):
    return {"Authorization": f"Bearer {KEYS[who]}"}


@pytest.fixture
def secured(cluster, monkeypatch):
    """The cluster with authentication on (no rate limit, no quota unless a test sets them)."""
    app = cluster.app_module
    monkeypatch.setattr(app, "keyring", KeyRing(KEYS, ["admin"]))
    monkeypatch.setattr(app, "rate_limiter", RateLimiter(0, 1))
    monkeypatch.setattr(app, "client_slots", ClientSlots(8))
    monkeypatch.setattr(app, "quota_per_client", 0)
    return cluster


def upload(cluster, who, data=b"hello", name="f.txt", headers=None, **params):
    return requests.post(cluster.api + "/objects", params={"name": name, **params}, data=data,
                         headers=headers if headers is not None else auth(who), timeout=TIMEOUT)


def upload_ok(cluster, who, data=b"hello", **kwargs) -> str:
    r = upload(cluster, who, data, **kwargs)
    assert r.status_code == 201, r.text
    return r.json()["object_id"]


def call(cluster, method, path, who=None, **kwargs):
    return requests.request(method, cluster.api + path, headers=auth(who) if who else {}, timeout=TIMEOUT, **kwargs)


# ---------------------------------------------------------------------------
# the building blocks
# ---------------------------------------------------------------------------
class TestKeys:
    def test_parsing(self):
        assert parse_keys(" a=key-aaaa1 , b=key-bbbb2 ") == {"a": "key-aaaa1", "b": "key-bbbb2"}
        assert parse_keys("") == {}
        for bad in ("a", "a=", "=key-aaaa1", "a=short", "a=key-aaaa1,a=key-bbbb2", "a=key-aaaa1,b=key-aaaa1"):
            with pytest.raises(ValueError):
                parse_keys(bad)

    def test_identifying_a_caller(self):
        ring = KeyRing(KEYS, ["admin"])
        assert ring.enabled and not KeyRing().enabled
        assert ring.identify("Bearer alice-key-0001") == "alice"
        assert ring.identify("bearer   alice-key-0001 ") == "alice"
        for bad in (None, "", "alice-key-0001", "Basic alice-key-0001", "Bearer ", "Bearer wrong-key-000"):
            assert ring.identify(bad) is None
        assert ring.is_admin("admin") and not ring.is_admin("alice")


class TestRateLimiter:
    def test_a_burst_then_the_steady_rate(self):
        now = [100.0]
        limiter = RateLimiter(rate=2, burst=3, clock=lambda: now[0])
        assert [limiter.allow("a")[0] for _ in range(4)] == [True, True, True, False]
        allowed, wait = limiter.allow("a")
        assert not allowed and wait == pytest.approx(0.5)
        assert limiter.allow("b")[0]                      # every client has its own bucket
        now[0] += 0.5                                      # one token back
        assert limiter.allow("a")[0] and not limiter.allow("a")[0]
        now[0] += 60                                       # full again, but never more than the burst
        assert [limiter.allow("a")[0] for _ in range(4)] == [True, True, True, False]

    def test_rate_zero_means_no_limit(self):
        limiter = RateLimiter(rate=0, burst=1)
        assert not limiter.enabled and all(limiter.allow("a")[0] for _ in range(1000))


class TestClientSlots:
    def test_each_client_has_its_own_slots(self):
        slots = ClientSlots(2)
        assert slots.take("a") and slots.take("a") and not slots.take("a")
        assert slots.take("b")
        slots.give_back("a")
        assert slots.running("a") == 1 and slots.take("a")


# ---------------------------------------------------------------------------
# authentication and access, against the real cluster
# ---------------------------------------------------------------------------
class TestAuthentication:
    def test_no_key_or_a_wrong_key_is_401(self, secured):
        for headers in ({}, {"Authorization": "Bearer nope-nope-nope"}, {"Authorization": "alice-key-0001"}):
            r = requests.get(secured.api + "/objects", headers=headers, timeout=TIMEOUT)
            assert r.status_code == 401 and "API key" in r.json()["error"]
            assert r.headers["WWW-Authenticate"].startswith("Bearer")

    def test_a_refused_upload_with_a_big_body_still_shows_its_401(self, secured):
        r = upload(secured, None, os.urandom(4 * MB), headers={})
        assert r.status_code == 401
        assert secured.total_chunk_files() == 0

    def test_the_owner_is_who_the_key_says(self, secured):
        r = upload(secured, "alice")
        assert r.status_code == 201 and r.json()["client_id"] == "alice"
        r = upload(secured, "alice", name="g.txt", client_id="bob")      # claiming to be bob
        assert r.status_code == 403 and "alice" in r.json()["error"]
        assert upload(secured, "alice", name="h.txt", client_id="alice").status_code == 201

    def test_me(self, secured):
        me = call(secured, "GET", "/me", "bob").json()
        assert me["client_id"] == "bob" and me["authenticated"] and not me["admin"]
        assert call(secured, "GET", "/me", "admin").json()["admin"] is True

    def test_the_workers_still_report_without_a_key(self, secured):
        body = {"worker_id": "worker1", "capacity": 1, "used": 0, "free": 1, "chunks": 0}
        assert post_heartbeat(secured, body).status_code == 200


class TestOwnObjectsOnly:
    def test_another_client_cannot_see_download_change_or_delete(self, secured):
        oid = upload_ok(secured, "alice", b"alice's secret")
        for method, path, kwargs in [("GET", f"/objects/{oid}", {}), ("GET", f"/objects/{oid}/metadata", {}),
                                     ("PATCH", f"/objects/{oid}/metadata", {"json": {"k": "v"}}),
                                     ("DELETE", f"/objects/{oid}", {}), ("GET", f"/objects/{oid}/readers", {}),
                                     ("PUT", f"/objects/{oid}/readers", {"json": {"readers": ["bob"]}})]:
            r = call(secured, method, path, "bob", **kwargs)
            assert r.status_code == 404, (method, path)            # exactly like an object that does not exist
        assert call(secured, "GET", f"/objects/{oid}", "alice").content == b"alice's secret"

    def test_each_client_lists_only_its_own_objects(self, secured):
        a = upload_ok(secured, "alice")
        b = upload_ok(secured, "bob")
        assert [o["object_id"] for o in call(secured, "GET", "/objects", "alice").json()] == [a]
        listed = call(secured, "GET", "/objects", "bob")
        assert [o["object_id"] for o in listed.json()] == [b] and listed.headers["X-Total-Count"] == "1"
        everything = call(secured, "GET", "/objects", "admin").json()
        assert {o["object_id"]: o["owner"] for o in everything} == {a: "alice", b: "bob"}

    def test_the_path_rule_is_per_client(self, secured):
        assert upload(secured, "alice", path="/files/x").status_code == 201
        assert upload(secured, "alice", path="/files/x").status_code == 409
        assert upload(secured, "bob", path="/files/x").status_code == 201


class TestSharing:
    def test_a_reader_may_read_but_not_change(self, secured):
        oid = upload_ok(secured, "alice", b"shared data")
        r = call(secured, "PUT", f"/objects/{oid}/readers", "alice", json={"readers": ["bob", "alice"]})
        assert r.status_code == 200 and r.json() == {"object_id": oid, "owner": "alice", "readers": ["bob"]}
        assert call(secured, "GET", f"/objects/{oid}", "bob").content == b"shared data"
        meta = call(secured, "GET", f"/objects/{oid}/metadata", "bob").json()
        assert meta["client_id"] == "alice" and "readers" not in meta      # who else may read: owner only
        assert oid in [o["object_id"] for o in call(secured, "GET", "/objects", "bob").json()]
        for method, path, kwargs in [("PATCH", f"/objects/{oid}/metadata", {"json": {"k": "v"}}),
                                     ("DELETE", f"/objects/{oid}", {}), ("GET", f"/objects/{oid}/readers", {}),
                                     ("PUT", f"/objects/{oid}/readers", {"json": {"readers": []}})]:
            r = call(secured, method, path, "bob", **kwargs)
            assert r.status_code == 403 and "owner" in r.json()["error"], (method, path)
        assert call(secured, "GET", f"/objects/{oid}/readers", "alice").json()["readers"] == ["bob"]

    def test_stopping_sharing(self, secured):
        oid = upload_ok(secured, "alice")
        call(secured, "PUT", f"/objects/{oid}/readers", "alice", json={"readers": ["bob"]})
        call(secured, "PUT", f"/objects/{oid}/readers", "alice", json={"readers": []})
        assert call(secured, "GET", f"/objects/{oid}", "bob").status_code == 404

    def test_a_bad_readers_body_is_400(self, secured):
        oid = upload_ok(secured, "alice")
        for body in ({}, {"readers": "bob"}, {"readers": [1]}, {"readers": [""]}, {"readers": ["x"] * 101}):
            assert call(secured, "PUT", f"/objects/{oid}/readers", "alice", json=body).status_code == 400, body

    def test_sharing_survives_a_restart_of_the_controller(self, secured):
        from controller.distributor import Distributor
        from controller.metadata_store import store_from_setting
        oid = upload_ok(secured, "alice")
        call(secured, "PUT", f"/objects/{oid}/readers", "alice", json={"readers": ["bob"]})
        store = store_from_setting(secured.metadata_path)
        try:
            assert Distributor([], store=store).get_object(oid).readers == ["bob"]
        finally:
            store.close()


class TestAdmins:
    def test_admins_may_do_everything(self, secured):
        oid = upload_ok(secured, "alice")
        assert call(secured, "GET", f"/objects/{oid}/metadata", "admin").status_code == 200
        assert call(secured, "DELETE", f"/objects/{oid}", "admin").status_code == 204

    @pytest.mark.parametrize("method,path", [("GET", "/nodes"), ("GET", "/replication"), ("POST", "/repair"),
                                             ("POST", "/cleanup?min_age=0")])
    def test_operator_endpoints_are_for_admins(self, secured, method, path):
        assert call(secured, method, path).status_code == 401
        r = call(secured, method, path, "alice")
        assert r.status_code == 403 and "admin" in r.json()["error"]
        assert call(secured, method, path, "admin").status_code == 200

    def test_a_workers_chunk_inventory_is_for_admins(self, secured):
        url = f"{secured.root}/internal/workers/worker1/chunks"
        assert requests.get(url, timeout=TIMEOUT).status_code == 401
        assert requests.get(url, headers=auth("alice"), timeout=TIMEOUT).status_code == 403
        assert requests.get(url, headers=auth("admin"), timeout=TIMEOUT).status_code == 200


class TestTheClientProgram:
    def test_a_client_with_its_key(self, secured, tmp_path):
        src = tmp_path / "notes.txt"
        src.write_bytes(b"my notes")
        alice = Client(base_url=secured.api, api_key=KEYS["alice"])
        bob = Client(base_url=secured.api, api_key=KEYS["bob"])
        m = alice.upload(str(src))
        assert m["client_id"] == "alice"
        with pytest.raises(ValueError, match="404"):
            bob.retrieve(m["object_id"], str(tmp_path / "stolen.txt"))
        alice.share(m["object_id"], ["bob"])
        out = bob.retrieve(m["object_id"], str(tmp_path / "copy.txt"))
        assert open(out, "rb").read() == b"my notes"
        assert bob.me()["client_id"] == "bob"

    def test_a_client_without_a_key_is_refused(self, secured):
        with pytest.raises(ValueError, match="401"):
            Client(base_url=secured.api, api_key="").list_objects()


# ---------------------------------------------------------------------------
# a fair share of capacity
# ---------------------------------------------------------------------------
class TestFairShare:
    def test_one_client_at_its_transfer_limit_does_not_block_another(self, secured, monkeypatch):
        slots = ClientSlots(1)
        monkeypatch.setattr(secured.app_module, "client_slots", slots)
        a = upload_ok(secured, "alice")
        b = upload_ok(secured, "bob")
        assert slots.take("alice")                                   # alice's only transfer is running
        r = call(secured, "GET", f"/objects/{a}", "alice")
        assert r.status_code == 503 and r.headers["Retry-After"] == "1" and "alice" in r.json()["error"]
        assert upload(secured, "alice", name="more.txt").status_code == 503
        assert call(secured, "GET", f"/objects/{b}", "bob").status_code == 200   # bob is not held up
        slots.give_back("alice")
        assert call(secured, "GET", f"/objects/{a}", "alice").status_code == 200
        assert slots.running("alice") == 0 and slots.running("bob") == 0   # every finished transfer gave its slot back

    def test_too_many_requests_from_one_client_is_429_and_others_go_on(self, secured, monkeypatch):
        monkeypatch.setattr(secured.app_module, "rate_limiter", RateLimiter(rate=0.5, burst=3))
        codes = [call(secured, "GET", "/objects", "alice").status_code for _ in range(4)]
        assert codes == [200, 200, 200, 429]
        r = call(secured, "GET", "/objects", "alice")
        assert r.status_code == 429 and int(r.headers["Retry-After"]) >= 1
        assert call(secured, "GET", "/objects", "bob").status_code == 200

    def test_the_quota_refuses_an_upload_known_to_be_too_big_at_once(self, secured, monkeypatch):
        monkeypatch.setattr(secured.app_module, "quota_per_client", 3 * MB)
        upload_ok(secured, "alice", os.urandom(2 * MB))
        files = secured.total_chunk_files()
        r = upload(secured, "alice", os.urandom(2 * MB), name="second.bin")
        assert r.status_code == 413 and "quota" in r.json()["error"]
        assert secured.total_chunk_files() == files                  # nothing of it was stored
        assert upload(secured, "bob", os.urandom(2 * MB), name="bobs.bin").status_code == 201   # bob has his own

    def test_the_quota_stops_an_upload_of_unknown_size_part_way(self, secured, monkeypatch):
        monkeypatch.setattr(secured.app_module, "quota_per_client", 3 * MB)
        upload_ok(secured, "alice", os.urandom(2 * MB))
        files = secured.total_chunk_files()
        r = requests.post(secured.api + "/objects", files={"file": ("big.bin", os.urandom(2 * MB))},
                          headers=auth("alice"), timeout=TIMEOUT)          # a form upload: size not checked up front
        assert r.status_code == 413
        assert secured.total_chunk_files() == files and not secured.distributor._in_flight
        assert secured.distributor.used_bytes("alice") == 2 * MB      # the refused upload left no usage behind

    def test_deleting_frees_quota_and_admins_have_none(self, secured, monkeypatch):
        monkeypatch.setattr(secured.app_module, "quota_per_client", 3 * MB)
        oid = upload_ok(secured, "alice", os.urandom(2 * MB))
        assert call(secured, "GET", "/me", "alice").json()["stored_bytes"] == 2 * MB
        call(secured, "DELETE", f"/objects/{oid}", "alice")
        assert call(secured, "GET", "/me", "alice").json()["stored_bytes"] == 0
        upload_ok(secured, "alice", os.urandom(2 * MB), name="again.bin")
        upload_ok(secured, "admin", os.urandom(5 * MB), name="admin.bin")
        assert call(secured, "GET", "/me", "admin").json()["quota_bytes"] is None

    def test_uploads_at_the_same_time_cannot_get_around_the_quota(self, secured, monkeypatch):
        monkeypatch.setattr(secured.app_module, "quota_per_client", 3 * MB)
        codes = []

        def go(i):
            codes.append(upload(secured, "alice", os.urandom(2 * MB), name=f"c{i}.bin").status_code)
        threads = [threading.Thread(target=go, args=(i,)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert codes.count(201) <= 1 and 413 in codes
        assert secured.distributor.used_bytes("alice") <= 3 * MB
