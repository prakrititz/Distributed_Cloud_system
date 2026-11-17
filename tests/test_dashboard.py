"""dashboard/ (On Store, the terminal dashboard): its model on its own, and the
real app driven with the keyboard against the real test cluster."""
import asyncio

import pytest

from dashboard.model import (DOWN, LATE, SILENT, UP, HeartbeatTracker, State, Worker, auto_repair_events,
                             build_health, build_workers, health_events, object_events, repair_steps,
                             worker_events, worker_sort_key, worker_state)


def obj(oid, name="f", chunks=(("w1", "w2"),), client="c1", custom=None):
    return {"object_id": oid, "name": name, "size": 10, "client_id": client, "custom": custom or {},
            "chunk_count": len(chunks), "copies_per_chunk": 2,
            "chunks": [{"index": i, "chunk_id": f"{oid}-{i}", "replicas": list(r)} for i, r in enumerate(chunks)]}


def state(workers=(), objects=(), health=None, **kw):
    return State(controller_ok=True, workers={w.id: w for w in workers},
                 objects={o["object_id"]: o for o in objects}, health=health or {}, **kw)


class TestWorkerState:
    @pytest.mark.parametrize("w, expected", [
        (Worker("w1", reachable=True, heartbeat_age=0.5), UP),
        (Worker("w1", reachable=True, heartbeat_age=4.0), LATE),
        (Worker("w1", reachable=False, heartbeat_age=3.0), SILENT),     # dead, but the controller has not noticed
        (Worker("w1", reachable=False, heartbeat_age=6.5), DOWN),
        (Worker("w1", reachable=True, heartbeat_age=None), DOWN),
        (Worker("w1", reachable=False, registered=False), DOWN),
    ])
    def test_states(self, w, expected):
        assert worker_state(w) == expected

    def test_workers_sort_by_number(self):
        assert sorted(["worker10", "worker2", "worker1"], key=worker_sort_key) == ["worker1", "worker2", "worker10"]

    def test_workers_named_in_the_metadata_but_not_registered_are_listed(self):
        ws = build_workers([{"node": "w1", "alive": True, "heartbeat_age": 1, "capacity": 5, "used": 0,
                             "free": 5, "chunks": 0}], {"o": obj("o", chunks=[("w1", "w9")])})
        assert list(ws) == ["w1", "w9"] and ws["w9"].registered is False


class TestChunkHealth:
    def test_from_the_report_or_from_the_workers(self):
        w1, w2 = Worker("w1", True, 0.5), Worker("w2", False, 9.0)
        o = obj("o", chunks=[("w1", "w2"), ("w1", "w2")])
        health = build_health({"chunks": [{"object_id": "o", "index": 0, "status": "under_replicated",
                                           "replicas": [{"node": "w1", "state": "corrupt"},
                                                        {"node": "w2", "state": "down"}]}]})
        st = state([w1, w2], [o], health)
        assert st.copy_state("o", 0, "w1") == "corrupt"          # from the report
        assert st.copy_state("o", 1, "w2") == "down"             # not in the report: from the worker
        assert st.chunk_status("o", o["chunks"][0]) == "unavailable"
        assert st.chunk_status("o", o["chunks"][1]) == "under_replicated"

    def test_a_worker_back_up_is_not_shown_down_by_an_old_report(self):
        st = state([Worker("w1", True, 0.5), Worker("w2", True, 0.5)], [obj("o")],
                   build_health({"chunks": [{"object_id": "o", "index": 0, "status": "under_replicated",
                                             "replicas": [{"node": "w1", "state": "ok"},
                                                          {"node": "w2", "state": "down"}]}]}))
        assert st.copy_state("o", 0, "w2") == "ok"

    def test_pair_counts(self):
        st = state(objects=[obj("o", chunks=[("w1", "w2"), ("w2", "w1"), ("w1", "w3")])])
        assert st.pair_counts() == {("w1", "w2"): 2, ("w1", "w3"): 1}


class TestEvents:
    def test_a_worker_dying_and_coming_back(self):
        up = state([Worker("w1", True, 0.5)])
        silent = state([Worker("w1", False, 3.0)])
        down = state([Worker("w1", False, 7.0)])
        assert worker_events(up, silent)[0][0] == "warn"
        assert worker_events(silent, down) == [("bad", "w1: no heartbeat for 6s -> DOWN")]
        assert worker_events(down, up)[0] == ("good", "w1 is back: heartbeat received -> UP")
        assert worker_events(down, silent) == []                # a restarted worker still starting up
        assert worker_events(up, up) == []

    def test_uploads_deletes_and_moved_copies(self):
        before = state(objects=[obj("a", "a.txt"), obj("b", "b.txt")])
        after = state(objects=[obj("a", "a.txt", chunks=[("w1", "w3")]), obj("c", "c.txt", client="client2")])
        events, moves = object_events(before, after)
        text = " | ".join(t for _, t in events)
        assert "client2 uploaded c.txt" in text and "b.txt was deleted" in text and "w1+w2 -> w1+w3" in text
        assert moves == [("a", 0, ["w2"], ["w3"])]
        assert object_events(before, after, own={"b", "c"})[0] == [events[0]]   # our own: no duplicate event

    def test_health_changes_are_grouped(self):
        def report(status, w2):
            return build_health({"chunks": [{"object_id": "o", "index": i, "status": status,
                                             "replicas": [{"node": "w1", "state": "ok"}, {"node": "w2", "state": w2}]}
                                            for i in range(6)]})
        o = obj("o", chunks=[("w1", "w2")] * 6)
        events = health_events(state(objects=[o], health=report("healthy", "ok")),
                               state(objects=[o], health=report("under_replicated", "corrupt")))
        kinds = sorted(k for k, _ in events)
        assert kinds == ["bad", "warn"] and all("and 2 more" in t for _, t in events)

    def test_only_a_new_auto_repair_is_an_event(self):
        last = {"at": "2026-11-18T10:00:00Z", "because_down": ["w2"], "repaired": 3, "copies_created": 3,
                "partial": 0, "unrecoverable": 0, "chunks_checked": 9}
        none_yet = state(auto_repair={"enabled": True, "grace_seconds": 30, "last": None})
        done = state(auto_repair={"enabled": True, "grace_seconds": 30, "last": last})
        assert auto_repair_events(state(), done) == []           # the dashboard just started: old news
        assert "3 chunk(s) repaired" in auto_repair_events(none_yet, done)[0][1]
        assert auto_repair_events(done, done) == []


class TestHeartbeats:
    def test_each_heartbeat_is_found_once(self):
        tr = HeartbeatTracker(window=20)
        assert tr.observe(state([Worker("w1", True, 0.3)]), now=100.0) == ["w1"]
        assert tr.observe(state([Worker("w1", True, 1.3)]), now=101.0) == []     # same heartbeat, older
        assert tr.observe(state([Worker("w1", True, 0.2)]), now=102.1) == ["w1"]  # a new one

    def test_a_dead_worker_sends_no_ghost_heartbeat_later(self):
        tr = HeartbeatTracker(window=20)
        tr.observe(state([Worker("w1", True, 0.0)]), now=100.0)
        for t in range(101, 160):                                # long after the strip forgot the last one
            assert tr.observe(state([Worker("w1", False, t - 100.0)]), now=float(t)) == []
        strip = tr.strip("w1", 20, now=159.0)
        assert "beat" not in strip and strip[-1] == "down"


def test_repair_steps_tell_the_story():
    steps = repair_steps({"chunks_checked": 2, "details": [
        {"name": "f", "index": 0, "before": {"w1": "ok", "w2": "down"}, "added": ["w3"], "outcome": "repaired"},
        {"name": "f", "index": 1, "before": {"w1": "corrupt", "w2": "down"}, "added": [], "outcome": "unrecoverable"}]})
    texts = [t for _, t, _ in steps]
    assert any("reading good copy from w1" in t for t in texts)
    assert [m for _, _, m in steps if m] == [("w1", "w3")]
    assert any("cannot rebuild" in t for t in texts)


# ------------------------------------------------------------- the real app
textual = pytest.importorskip("textual")


def no_docker():
    from dashboard.docker_ctl import Docker
    d = Docker()
    d.available = False                  # as on a machine without docker
    return d


async def until(pilot, condition, seconds=15.0):
    for _ in range(int(seconds / 0.1)):
        if condition():
            return True
        await pilot.pause(0.1)
    return False


class TestApp:
    def test_live_view_upload_flow_screen_and_failure(self, cluster, tmp_path, monkeypatch):
        asyncio.run(self._live(cluster, tmp_path, monkeypatch))

    def test_a_controller_that_is_not_there(self):
        asyncio.run(self._absent())

    async def _live(self, cluster, tmp_path, monkeypatch):
        from dashboard.app import OnStore
        from dashboard.flow import FlowScreen
        monkeypatch.chdir(tmp_path)
        app = OnStore(api_url=cluster.api, docker=no_docker())
        async with app.run_test(size=(160, 45)) as pilot:
            assert await until(pilot, lambda: len(app.state.workers) == 4 and app.state.workers_up() == 4)

            await pilot.press("colon")                           # : upload NAME MB
            app.cmd_input.value = "upload big.bin 2.5 project=demo"
            await pilot.press("enter")
            assert await until(pilot, lambda: any(m["name"] == "big.bin" for m in app.state.objects.values()))
            meta = next(m for m in app.state.objects.values() if m["name"] == "big.bin")
            assert meta["chunk_count"] == 3 and meta["custom"] == {"project": "demo"}
            assert app.state.counts()["healthy"] == 3

            await pilot.press("colon")                           # : download (the selected object)
            app.cmd_input.value = "download"
            await pilot.press("enter")
            assert await until(pilot, lambda: (tmp_path / "big.bin").exists())
            assert (tmp_path / "big.bin").stat().st_size == int(2.5 * 1024 * 1024)

            await pilot.press("2")                               # the flow screen, then back
            assert isinstance(app.screen, FlowScreen)
            assert await until(pilot, lambda: app.flow.heartbeats > 4)
            await pilot.press("1")
            assert not isinstance(app.screen, FlowScreen)

            victim = meta["chunks"][0]["replicas"][0]            # a worker dies: SILENT, then DOWN
            cluster.kill(victim)
            assert await until(pilot, lambda: app.state.state_of(victim) == DOWN, seconds=15)
            assert await until(pilot, lambda: app.state.counts()["under_replicated"] >= 1)
            log = "\n".join(line.text for line in app.events_log.lines)
            assert f"{victim}: no heartbeat for 6s -> DOWN" in log

            await pilot.press("r")                               # repair from the keyboard
            assert await until(pilot, lambda: app.state.counts()["under_replicated"] == 0
                               and app.state.counts()["healthy"] == 3, seconds=20)
            assert any("writing new copy" in text for _, _, text in app.flow.steps)

            await pilot.press("k")                               # docker actions say why they cannot run
            await pilot.pause(0.3)
            assert "docker is not available" in "\n".join(line.text for line in app.events_log.lines)
            await pilot.press("q")

    async def _absent(self):
        from dashboard.app import OnStore
        app = OnStore(api_url="http://127.0.0.1:9", docker=no_docker())
        async with app.run_test(size=(120, 40)) as pilot:
            assert await until(pilot, lambda: bool(app.state.error))
            assert not app.state.controller_ok
            await pilot.press("question_mark")                   # still usable: help opens and closes
            await pilot.press("escape")
            await pilot.press("q")
