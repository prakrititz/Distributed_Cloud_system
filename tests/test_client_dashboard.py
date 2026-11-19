"""Different dashboards for different callers.

- each client's activity (GET /api/v1/me/activity): its uploads, downloads,
  deletes, metadata changes and shares, seen by that client only
- the sign-in portal: registering gives a key, shown once and saved
- the client dashboard: only the client's own files, the files shared with it
  and its history, never the workers; driven by keyboard against the cluster
- the admin dashboard with authentication on
- signing in picks the dashboard from the key
"""
import asyncio
import os
from types import SimpleNamespace

import pytest
import requests

import client as client_module
from controller.access import ClientRegistry, ClientSlots, KeyRing, RateLimiter
from controller.activity import ActivityLog
from controller.metadata_store import MemoryStore, SqliteStore
from dashboard import credentials
from dashboard.api import Api, ApiError
from tests.cluster import wait_until
from tests.util import TIMEOUT

KEYS = {"alice": "alice-key-0001", "bob": "bob-key-00002", "admin": "admin-key-0003"}


def bearer(who):
    return {"Authorization": f"Bearer {KEYS[who]}"}


def clear(store):
    with store._lock:
        store._db.execute("DELETE FROM clients")
        store._db.execute("DELETE FROM activity")


@pytest.fixture
def secured(cluster, monkeypatch, tmp_path):
    """The cluster with authentication on, self-registration allowed, and a
    credentials file of this test's own."""
    app = cluster.app_module
    store = cluster.distributor.store
    clear(store)
    monkeypatch.setattr(app, "keyring", KeyRing(KEYS, ["admin"], registry=ClientRegistry(store)))
    monkeypatch.setattr(app, "allow_registration", True)
    monkeypatch.setattr(app, "registration_limiter", RateLimiter(0, 1))
    monkeypatch.setattr(app, "rate_limiter", RateLimiter(0, 1))
    monkeypatch.setattr(app, "client_slots", ClientSlots(8))
    monkeypatch.setattr(app, "quota_per_client", 0)
    monkeypatch.setenv("OBJECTSTORE_CREDENTIALS", str(tmp_path / "credentials.json"))
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.chdir(tmp_path)
    yield cluster
    clear(store)


def put(cluster, who, name, data=b"hello"):
    r = requests.post(cluster.api + "/objects", params={"name": name}, data=data, headers=bearer(who),
                      timeout=TIMEOUT)
    assert r.status_code == 201, r.text
    return r.json()


def downloaded(cluster, who, object_id):
    """Download, then wait until the controller has noted it (it does so just
    after the last byte is sent, so a moment after the client has it all)."""
    data = requests.get(f"{cluster.api}/objects/{object_id}", headers=bearer(who), timeout=TIMEOUT).content
    assert wait_until(lambda: any(e["action"] == "download" and e["object_id"] == object_id
                                  for e in activity(cluster, who, limit=5)), 5)
    return data


def activity(cluster, who, **params):
    r = requests.get(cluster.api + "/me/activity", headers=bearer(who), params=params, timeout=TIMEOUT)
    assert r.status_code == 200, r.text
    return r.json()


# ======================================================================= activity
class TestActivity:
    def test_everything_a_client_does_newest_first(self, secured):
        meta = put(secured, "alice", "a.txt", b"x" * 3000)
        oid = meta["object_id"]
        h = bearer("alice")
        assert downloaded(secured, "alice", oid) == b"x" * 3000
        requests.patch(f"{secured.api}/objects/{oid}/metadata", json={"k": "v", "old": None}, headers=h,
                       timeout=TIMEOUT)
        requests.put(f"{secured.api}/objects/{oid}/readers", json={"readers": ["bob"]}, headers=h, timeout=TIMEOUT)
        requests.delete(f"{secured.api}/objects/{oid}", headers=h, timeout=TIMEOUT)
        got = activity(secured, "alice")
        assert [e["action"] for e in got] == ["delete", "share", "metadata", "download", "upload"]
        assert all(e["object_id"] == oid and e["name"] == "a.txt" for e in got)
        assert got[-1]["size"] == 3000 and "readers: bob" in got[1]["detail"] and "old=(removed)" in got[2]["detail"]

    def test_a_client_sees_only_its_own(self, secured):
        shared = put(secured, "bob", "b.txt")
        requests.put(f"{secured.api}/objects/{shared['object_id']}/readers", json={"readers": ["alice"]},
                     headers=bearer("bob"), timeout=TIMEOUT)
        downloaded(secured, "alice", shared["object_id"])
        assert [e["action"] for e in activity(secured, "alice")] == ["download"]        # what alice did
        assert [e["action"] for e in activity(secured, "bob")] == ["share", "upload"]    # not alice's download

    def test_refused_requests_are_not_activity(self, secured):
        oid = put(secured, "bob", "b.txt")["object_id"]
        assert requests.get(f"{secured.api}/objects/{oid}", headers=bearer("alice"), timeout=TIMEOUT).status_code == 404
        assert requests.delete(f"{secured.api}/objects/{oid}", headers=bearer("alice"),
                               timeout=TIMEOUT).status_code == 404
        assert activity(secured, "alice") == []

    def test_registering_is_the_first_entry(self, secured):
        key = requests.post(secured.api + "/clients", json={"client_id": "carol"}, timeout=TIMEOUT).json()["api_key"]
        got = requests.get(secured.api + "/me/activity", headers={"Authorization": f"Bearer {key}"},
                           timeout=TIMEOUT).json()
        assert [e["action"] for e in got] == ["register"]

    def test_limit(self, secured):
        for i in range(3):
            put(secured, "alice", f"{i}.txt")
        assert [e["name"] for e in activity(secured, "alice", limit=2)] == ["2.txt", "1.txt"]
        for bad in ("-1", "501", "x"):
            r = requests.get(secured.api + "/me/activity", params={"limit": bad}, headers=bearer("alice"),
                             timeout=TIMEOUT)
            assert r.status_code == 400 and "limit" in r.json()["error"]

    def test_needs_a_key(self, secured):
        assert requests.get(secured.api + "/me/activity", timeout=TIMEOUT).status_code == 401


def test_the_healthcheck_needs_no_key(secured):
    # Docker's healthcheck asks /health; /api/v1/objects needs a key (401)
    r = requests.get(secured.root + "/health", timeout=TIMEOUT)
    assert r.status_code == 200 and r.json() == {"status": "ok"}
    dockerfile = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Dockerfile.controller")
    with open(dockerfile, encoding="utf-8") as f:
        assert "/health'" in f.read()


def test_sqlite_keeps_the_latest_entries_per_client_across_a_restart(tmp_path):
    path = str(tmp_path / "m.db")
    log = ActivityLog(SqliteStore(path), keep=3)
    for i in range(5):
        log.record("alice", "upload", f"o{i}", f"{i}.txt", i)
    log.record("bob", "upload", "ob", "b.txt", 1)
    log.store.close()
    again = ActivityLog(SqliteStore(path), keep=3)
    assert [e["name"] for e in again.recent("alice")] == ["4.txt", "3.txt", "2.txt"]
    assert [e["name"] for e in again.recent("bob")] == ["b.txt"]
    again.store.close()


def test_without_sqlite_the_activity_is_kept_in_memory():
    log = ActivityLog(MemoryStore(), keep=2)
    for i in range(3):
        log.record("alice", "download", f"o{i}", f"{i}.txt")
    log.record(None, "upload")                                 # nobody known: nothing kept
    assert [e["name"] for e in log.recent("alice")] == ["2.txt", "1.txt"] and log.recent(None) == []


def test_a_store_that_fails_does_not_fail_the_request():
    class Broken(MemoryStore):
        def add_activity(self, *a):
            raise OSError("disk full")
    ActivityLog(Broken()).record("alice", "upload")            # no exception


# =================================================================== credentials
def test_the_dashboard_and_client_py_share_saved_keys(tmp_path):
    file = str(tmp_path / "c.json")
    api = "http://127.0.0.1:5000/api/v1"
    credentials.save(api, "alice", "k1", file)
    assert client_module.load_credentials(api, file) == ("alice", "k1")
    client_module.save_credentials("http://other/api/v1", "bob", "k2", file)
    assert credentials.load("http://other/api/v1", file) == ("bob", "k2")
    assert credentials.load(api, file) == ("alice", "k1")
    assert credentials.forget(api, file) and credentials.load(api, file) == (None, None)
    assert not credentials.forget(api, file)


# ======================================================================= the apps
textual = pytest.importorskip("textual")


async def until(pilot, condition, seconds=15.0):
    for _ in range(int(seconds / 0.1)):
        if condition():
            return True
        await pilot.pause(0.1)
    return False


def screen_text(app) -> str:
    """Everything the panes show right now."""
    parts = []
    for pane in app.query(".pane"):
        if hasattr(pane, "lines"):                       # the output log
            parts += [line.text for line in pane.lines]
        else:
            parts.append(pane.render().plain)
        parts.append(str(pane.border_title))
    return "\n".join(parts)


class TestPortal:
    def test_registering_shows_the_key_once_and_saves_it(self, secured):
        asyncio.run(self._register(secured))

    def test_a_wrong_key_is_refused_and_a_right_one_signs_in(self, secured):
        asyncio.run(self._sign_in(secured))

    def test_registration_off_says_so(self, secured, monkeypatch):
        monkeypatch.setattr(secured.app_module, "allow_registration", False)
        asyncio.run(self._registration_off(secured))

    async def _register(self, cluster):
        from dashboard.portal import Portal
        app = Portal(cluster.root)
        async with app.run_test(size=(120, 40)) as pilot:
            app.query_one("#name").focus()
            await pilot.press(*"dave")
            await pilot.press("enter")
            assert await until(pilot, lambda: app.session is not None)
            key = app.session.api_key
            assert key.startswith("sk_") and str(app.query_one("#issued-key").content) == key
            assert app.session.me["client_id"] == "dave" and not app.session.admin
            assert credentials.load(cluster.api) == ("dave", key)        # where client.py looks too
            await pilot.press("enter")
        assert app.return_value.api_key == key

    async def _sign_in(self, cluster):
        from dashboard.portal import Portal
        app = Portal(cluster.root)
        async with app.run_test(size=(120, 40)) as pilot:
            app.query_one("#key").focus()
            await pilot.press(*"not-a-key")
            await pilot.press("enter")
            assert await until(pilot, lambda: "not valid" in str(app.query_one("#status").content))
            app.query_one("#key").value = KEYS["alice"]
            await pilot.press("enter")
            assert await until(pilot, lambda: app.return_value is not None)
        assert app.return_value.me["client_id"] == "alice"

    def test_an_older_controller_without_registration_says_so(self, secured, monkeypatch):
        # a controller built before registration: Flask's own 404 for an unknown URL
        def missing(self, *a, **kw):
            raise ApiError("404: The requested URL was not found on the server.", 404)
        monkeypatch.setattr(Api, "register", missing)
        asyncio.run(self._older(secured))

    async def _older(self, cluster):
        from dashboard.portal import Portal
        app = Portal(cluster.root)
        async with app.run_test(size=(120, 40)) as pilot:
            app.query_one("#name").focus()
            await pilot.press("enter")
            assert await until(pilot, lambda: "older version" in str(app.query_one("#status").content))
            assert "Registration is off" not in str(app.query_one("#status").content)
            await pilot.press("escape")

    async def _registration_off(self, cluster):
        from dashboard.portal import Portal
        app = Portal(cluster.root)
        async with app.run_test(size=(120, 40)) as pilot:
            app.query_one("#name").focus()
            await pilot.press("enter")
            assert await until(pilot, lambda: "Registration is off" in str(app.query_one("#status").content))
            await pilot.press("escape")


class TestClientDashboard:
    def test_my_files_shared_files_my_history_and_nothing_else(self, secured, tmp_path):
        asyncio.run(self._run(secured, tmp_path))

    async def _run(self, cluster, tmp_path):
        from dashboard.client_app import ClientDashboard
        theirs = put(cluster, "bob", "bobs-notes.txt", b"from bob")
        put(cluster, "bob", "bobs-secret.txt", b"private")
        requests.put(f"{cluster.api}/objects/{theirs['object_id']}/readers", json={"readers": ["alice"]},
                     headers=bearer("bob"), timeout=TIMEOUT)
        local = tmp_path / "report.pdf"
        local.write_bytes(os.urandom(1500 * 1024))                    # 2 chunks

        me = Api(cluster.root, KEYS["alice"]).me()
        app = ClientDashboard(api_url=cluster.root, api_key=KEYS["alice"], me=me)
        async with app.run_test(size=(160, 45)) as pilot:
            assert await until(pilot, lambda: len(app.files("shared")) == 1)
            assert app.files("mine") == []

            await pilot.press("u")                                     # u opens the command line with "upload "
            assert app.cmd_input.value == "upload "
            app.cmd_input.value = f"upload {local} project=demo"
            await pilot.press("enter")
            assert await until(pilot, lambda: len(app.files("mine")) == 1)
            mine = app.files("mine")[0]
            assert mine["name"] == "report.pdf"
            assert await until(pilot, lambda: (app.details.get(mine["object_id"]) or {}).get("custom")
                               == {"project": "demo"})

            await pilot.press("colon")                                 # metadata, then share
            app.cmd_input.value = "meta status=final"
            await pilot.press("enter")
            assert await until(pilot, lambda: app.details[mine["object_id"]]["custom"].get("status") == "final")
            await pilot.press("colon")
            app.cmd_input.value = "share bob"
            await pilot.press("enter")
            assert await until(pilot, lambda: app.details[mine["object_id"]].get("readers") == ["bob"])

            await pilot.press("g")                                     # download the selected (mine)
            assert await until(pilot, lambda: (tmp_path / "report (1).pdf").exists())
            assert (tmp_path / "report (1).pdf").read_bytes() == local.read_bytes()

            await pilot.press("tab")                                   # the shared list
            assert await until(pilot, lambda: app.active == "shared")
            await pilot.press("g")
            assert await until(pilot, lambda: (tmp_path / "bobs-notes.txt").exists())
            await pilot.press("d")                                     # not mine: refused, no question asked
            await pilot.pause(0.2)
            assert app.screen is app.screen_stack[0]

            assert await until(pilot, lambda: [e["action"] for e in app.activity[:5]]
                               == ["download", "download", "share", "metadata", "upload"])
            text = screen_text(app)
            assert "report.pdf" in text and "bobs-notes.txt" in text and "from bob" in text
            assert "only its owner may delete it" in text
            assert "bobs-secret.txt" not in text                      # not shared with alice
            for internal in ("worker", "replica", "heartbeat", "orphan"):
                assert internal not in text.lower()

            await pilot.press("a")                                     # the account window hides the key
            assert KEYS["alice"] not in app.screen.query_one("Static").content.plain
            await pilot.press("v")
            assert KEYS["alice"] in app.screen.query_one("Static").content.plain
            await pilot.press("escape")

            await pilot.press("tab", "tab")                            # back to my files, delete mine
            assert await until(pilot, lambda: app.active == "mine")
            await pilot.press("d")
            await pilot.press("y")
            assert await until(pilot, lambda: app.files("mine") == [])
            await pilot.press("q")

    def test_the_controller_going_away(self):
        asyncio.run(self._absent())

    async def _absent(self):
        from dashboard.client_app import ClientDashboard
        app = ClientDashboard(api_url="http://127.0.0.1:9", api_key="k", me={"client_id": "alice"})
        async with app.run_test(size=(120, 40)) as pilot:
            assert await until(pilot, lambda: not app.controller_ok)
            assert "UNREACHABLE" in app.topbar().plain
            await pilot.press("q")


class TestAdminDashboardWithAuthentication:
    def test_the_admin_sees_everything_and_its_uploads_work(self, secured, tmp_path):
        asyncio.run(self._run(secured))

    async def _run(self, cluster):
        from dashboard.app import OnStore
        from dashboard.docker_ctl import Docker
        put(cluster, "alice", "alices.txt")
        docker = Docker()
        docker.available = False
        me = Api(cluster.root, KEYS["admin"]).me()
        app = OnStore(api_url=cluster.root, api_key=KEYS["admin"], me=me, docker=docker)
        async with app.run_test(size=(160, 45)) as pilot:
            assert await until(pilot, lambda: app.state.workers_up() == 4 and len(app.state.objects) == 1)
            assert "ADMIN" in app.ov("#topbar").render().plain
            await pilot.press("colon")
            app.cmd_input.value = "upload gen.bin 1.5"
            await pilot.press("enter")
            assert await until(pilot, lambda: len(app.state.objects) == 2)
            assert next(m for m in app.state.objects.values() if m["name"] == "gen.bin")["client_id"] == "admin"
            await pilot.press("q")

    def test_a_client_key_cannot_read_the_admin_views(self, secured):
        api = Api(secured.root, KEYS["alice"])
        for call in (api.nodes, api.replication, lambda: api.inventory("worker1")):
            with pytest.raises(ApiError) as e:
                call()
            assert e.value.status == 403


# ===================================================================== signing in
class TestChoosingTheDashboard:
    def args(self, api, **kw):
        return SimpleNamespace(api=api, key=kw.get("key"), sign_in=kw.get("sign_in", False))

    def test_where_the_key_comes_from(self, secured, monkeypatch):
        from dashboard.__main__ import find_key
        api = Api(secured.root)
        assert find_key(self.args(secured.root), api) == (None, None)
        credentials.save(api.api_url, "alice", KEYS["alice"])
        assert find_key(self.args(secured.root), api)[0] == KEYS["alice"]
        assert find_key(self.args(secured.root, sign_in=True), api) == (None, None)   # --sign-in: ignore it
        monkeypatch.setenv("API_KEY", "from-env")
        assert find_key(self.args(secured.root), api) == ("from-env", "API_KEY")
        assert find_key(self.args(secured.root, key="flag"), api) == ("flag", "--key")

    def test_the_key_decides_admin_or_client(self, secured):
        from dashboard.__main__ import sign_in
        assert sign_in(self.args(secured.root, key=KEYS["admin"])).admin
        session = sign_in(self.args(secured.root, key=KEYS["alice"]))
        assert not session.admin and session.me["client_id"] == "alice"

    def test_without_authentication_it_is_the_admin_dashboard(self, cluster, monkeypatch, tmp_path):
        from dashboard.__main__ import sign_in
        monkeypatch.setenv("OBJECTSTORE_CREDENTIALS", str(tmp_path / "none.json"))
        monkeypatch.delenv("API_KEY", raising=False)
        session = sign_in(self.args(cluster.root))
        assert session.admin and session.api_key is None
