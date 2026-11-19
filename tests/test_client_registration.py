"""Clients that register themselves (POST /api/v1/clients).

A new client asks the controller for an account; the controller makes up a
random key, gives it to the client once, and keeps only its hash. From then on
the client is like any other: it sees only its own objects and has its own
fair share. A name that already means someone (a fixed client, an admin, a
registered client, the owner of stored objects) can never be registered.
"""
import hashlib
import json
import threading

import pytest
import requests

import client as client_module
from client import Client, load_credentials
from controller.access import ClientRegistry, ClientSlots, KeyRing, RateLimiter
from controller.metadata_store import JsonFileStore, MemoryStore, SqliteStore
from tests.util import TIMEOUT

KEYS = {"alice": "alice-key-0001", "admin": "admin-key-0003"}


def clear_registrations(store):
    with store._lock:
        store._db.execute("DELETE FROM clients")


@pytest.fixture
def open_door(cluster, monkeypatch):
    """The cluster with authentication on and self-registration allowed."""
    app = cluster.app_module
    store = cluster.distributor.store
    clear_registrations(store)
    monkeypatch.setattr(app, "keyring", KeyRing(KEYS, ["admin"], registry=ClientRegistry(store)))
    monkeypatch.setattr(app, "allow_registration", True)
    monkeypatch.setattr(app, "registration_limiter", RateLimiter(0, 1))      # no limit unless a test sets one
    monkeypatch.setattr(app, "rate_limiter", RateLimiter(0, 1))
    monkeypatch.setattr(app, "client_slots", ClientSlots(8))
    monkeypatch.setattr(app, "quota_per_client", 0)
    yield cluster
    clear_registrations(store)


def register(cluster, body=None, raw=None):
    if raw is not None:
        return requests.post(cluster.api + "/clients", data=raw, headers={"Content-Type": "application/json"},
                             timeout=TIMEOUT)
    return requests.post(cluster.api + "/clients", json=body if body is not None else {}, timeout=TIMEOUT)


def bearer(key):
    return {"Authorization": f"Bearer {key}"}


class TestRegistering:
    def test_a_new_client_gets_a_key_that_works(self, open_door):
        r = register(open_door, {"client_id": "client7"})
        assert r.status_code == 201
        answer = r.json()
        assert answer["client_id"] == "client7" and answer["api_key"].startswith("sk_")
        assert "only this once" in answer["message"]
        me = requests.get(open_door.api + "/me", headers=bearer(answer["api_key"]), timeout=TIMEOUT).json()
        assert me["client_id"] == "client7" and me["registered"] and not me["admin"]
        up = requests.post(open_door.api + "/objects", params={"name": "a.txt"}, data=b"mine",
                           headers=bearer(answer["api_key"]), timeout=TIMEOUT)
        assert up.status_code == 201 and up.json()["client_id"] == "client7"

    def test_without_a_name_one_is_made_up(self, open_door):
        a, b = register(open_door).json(), register(open_door).json()
        assert a["client_id"].startswith("client-") and a["client_id"] != b["client_id"]
        assert a["api_key"] != b["api_key"]

    def test_the_controller_keeps_only_a_hash_of_the_key(self, open_door):
        key = register(open_door, {"client_id": "client8"}).json()["api_key"]
        store = open_door.distributor.store
        with store._lock:
            rows = store._db.execute("SELECT client_id, key_hash FROM clients").fetchall()
        assert rows == [("client8", hashlib.sha256(key.encode()).hexdigest())]
        dump = "\n".join(store._db.iterdump())
        assert key not in dump                                  # the key itself is nowhere in the database

    def test_registrations_survive_a_controller_restart(self, open_door):
        key = register(open_door, {"client_id": "client9"}).json()["api_key"]
        fresh = SqliteStore(open_door.metadata_path)
        try:
            assert ClientRegistry(fresh).lookup(key) == "client9"
        finally:
            fresh.close()


class TestNamesThatAreTaken:
    @pytest.mark.parametrize("name", ["alice", "admin"])
    def test_a_fixed_client_or_an_admin_cannot_be_registered(self, open_door, name):
        r = register(open_door, {"client_id": name})
        assert r.status_code == 409 and "taken" in r.json()["error"]

    def test_a_registered_name_cannot_be_registered_again(self, open_door):
        assert register(open_door, {"client_id": "dup"}).status_code == 201
        assert register(open_door, {"client_id": "dup"}).status_code == 409

    def test_the_owner_of_stored_objects_cannot_be_taken_over(self, open_door):
        open_door.distributor.upload("old.txt", b"from before authentication", client_id="carol")
        assert register(open_door, {"client_id": "carol"}).status_code == 409

    def test_two_registrations_of_one_name_at_once_give_it_to_one(self, open_door):
        codes = []
        threads = [threading.Thread(target=lambda: codes.append(register(open_door, {"client_id": "race"}).status_code))
                   for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sorted(codes) == [201] + [409] * 5


class TestRules:
    @pytest.mark.parametrize("body", [{"client_id": ""}, {"client_id": "has space"}, {"client_id": ".hidden"},
                                      {"client_id": "x" * 65}, {"client_id": 7}])
    def test_bad_names_are_400(self, open_door, body):
        assert register(open_door, body).status_code == 400

    def test_a_body_that_is_not_an_object_is_400(self, open_door):
        assert register(open_door, raw="[1, 2]").status_code == 400

    def test_registration_is_off_without_authentication_or_when_disabled(self, open_door, monkeypatch):
        monkeypatch.setattr(open_door.app_module, "allow_registration", False)
        assert register(open_door, {"client_id": "x1"}).status_code == 404
        monkeypatch.setattr(open_door.app_module, "allow_registration", True)
        monkeypatch.setattr(open_door.app_module, "keyring", KeyRing())          # authentication off
        assert register(open_door, {"client_id": "x2"}).status_code == 404

    def test_too_many_registrations_from_one_address_is_429(self, open_door, monkeypatch):
        monkeypatch.setattr(open_door.app_module, "registration_limiter", RateLimiter(rate=0.01, burst=2))
        codes = [register(open_door).status_code for _ in range(3)]
        assert codes == [201, 201, 429]

    def test_registered_clients_see_only_their_own_objects(self, open_door):
        k1 = register(open_door, {"client_id": "r1"}).json()["api_key"]
        k2 = register(open_door, {"client_id": "r2"}).json()["api_key"]
        oid = requests.post(open_door.api + "/objects", params={"name": "s.txt"}, data=b"secret",
                            headers=bearer(k1), timeout=TIMEOUT).json()["object_id"]
        assert requests.get(f"{open_door.api}/objects/{oid}", headers=bearer(k2), timeout=TIMEOUT).status_code == 404
        assert requests.get(f"{open_door.api}/objects/{oid}", headers=bearer(KEYS["alice"]),
                            timeout=TIMEOUT).status_code == 404
        assert requests.get(f"{open_door.api}/objects/{oid}", headers=bearer(k1), timeout=TIMEOUT).content == b"secret"

    def test_a_made_up_key_is_still_401(self, open_door):
        r = requests.get(open_door.api + "/objects", headers=bearer("sk_" + "x" * 43), timeout=TIMEOUT)
        assert r.status_code == 401


class TestTheClientProgram:
    def test_register_saves_the_key_and_later_runs_use_it(self, open_door, tmp_path, monkeypatch):
        creds = str(tmp_path / "credentials.json")
        monkeypatch.setattr(client_module, "CREDENTIALS", creds)
        monkeypatch.delenv("API_KEY", raising=False)
        answer = Client(base_url=open_door.api, api_key="").register("client10")
        assert answer["saved_to"] == creds
        assert load_credentials(open_door.api, creds) == ("client10", answer["api_key"])
        later = Client(base_url=open_door.api)                     # no key given: read from the file
        assert later.api_key == answer["api_key"] and later.me()["client_id"] == "client10"

    def test_the_register_command(self, open_door, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr(client_module, "CREDENTIALS", str(tmp_path / "c.json"))
        monkeypatch.delenv("API_KEY", raising=False)
        monkeypatch.setattr(client_module, "BASE_URL", open_door.api)
        assert client_module.run_command(Client(base_url=open_door.api, api_key=""), ["register", "client11"]) == 0
        out = capsys.readouterr().out
        assert "Registered as 'client11'" in out and "sk_" in out
        assert json.load(open(tmp_path / "c.json"))[open_door.api]["client_id"] == "client11"


class TestTheRegistry:
    @pytest.mark.parametrize("make", [lambda tmp: MemoryStore(),
                                      lambda tmp: JsonFileStore(str(tmp / "m.json")),
                                      lambda tmp: SqliteStore(str(tmp / "m.db"))], ids=["memory", "json", "sqlite"])
    def test_every_store_keeps_registrations(self, tmp_path, make):
        store = make(tmp_path)
        registry = ClientRegistry(store)
        key = registry.register("c1", "2026-11-19T10:00:00Z")
        assert registry.lookup(key) == "c1" and registry.known("c1") and len(registry) == 1
        assert registry.lookup(key + "x") is None
        with pytest.raises(ValueError):
            registry.register("c1", "2026-11-19T10:00:01Z")
        if not isinstance(store, MemoryStore):                    # saved: a new registry over the same file knows it
            again = make(tmp_path)
            assert ClientRegistry(again).lookup(key) == "c1"
            again.close()
        store.close()

    def test_the_json_store_keeps_objects_and_clients_together(self, tmp_path):
        path = str(tmp_path / "m.json")
        store = JsonFileStore(path)
        store.save([{"object_id": "a", "x": 1}])
        ClientRegistry(store).register("c1", "2026-11-19T10:00:00Z")
        store.save([{"object_id": "a", "x": 1}, {"object_id": "b", "x": 2}])   # a later metadata save keeps the client
        fresh = JsonFileStore(path)
        assert [o["object_id"] for o in fresh.load()] == ["a", "b"]
        assert [c for c, _ in fresh.load_clients()] == ["c1"]

    def test_what_counts_as_taken(self):
        ring = KeyRing(KEYS, ["admin", "root"], registry=ClientRegistry(MemoryStore()))
        ring.registry.register("reg", "2026-11-19T10:00:00Z")
        assert all(ring.taken(n) for n in ("alice", "admin", "root", "reg"))
        assert not ring.taken("newcomer")

    def test_authentication_can_be_required_with_no_fixed_keys(self):
        assert not KeyRing().enabled
        ring = KeyRing(required=True, registry=ClientRegistry(MemoryStore()))
        assert ring.enabled
        key = ring.registry.register("only", "2026-11-19T10:00:00Z")
        assert ring.identify(f"Bearer {key}") == "only"
