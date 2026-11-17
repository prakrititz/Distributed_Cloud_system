"""On Store: the terminal dashboard app.

Three loops poll the controller (GET /nodes every second, the objects and their
metadata every 2 s, the replication report and each worker's inventory every
3 s). Each answer gives a new model.State; comparing it with the previous one
gives the events. The panes draw themselves from app.state.
"""
import asyncio
import os
import random
import shlex
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Grid, Horizontal
from textual.screen import ModalScreen
from textual.widgets import Footer, Input, RichLog, Static

from .api import Api, ApiError
from .docker_ctl import Docker, DockerError, run_shell
from .flow import FlowScreen, FlowState
from .model import (DOWN, HEARTBEAT_TIMEOUT, LATE, SILENT, UP, HeartbeatTracker, State, auto_repair_events,
                    build_health, build_workers, health_events, human_size, label, object_events, repair_steps,
                    worker_events, worker_state)
from .screens import HELP, ConfirmScreen, TextScreen
from .widgets import (DIM, FG, GREEN, KIND_COLORS, PINK, RED, STATE_COLORS, STATUS_COLORS, YELLOW,
                      ChunkMapPane, ObjectPane, SpreadPane, WorkersPane, local_time, topbar, worker_color)

MB = 1024 * 1024

# How often each part is asked for (seconds). Workers change fastest and
# GET /nodes is cheap; the replication report checks every copy, so it is the
# slow one. After an action, the parts it changed are asked for at once.
NODES_EVERY, OBJECTS_EVERY, HEALTH_EVERY = 0.5, 1.0, 2.0
PARALLEL = 8          # requests at a time when fetching many objects / workers


def fetch_many(fn, keys) -> dict:
    """{key: fn(key)} with several requests at a time; keys whose request
    fails (ApiError) are left out."""
    def one(key):
        try:
            return key, fn(key)
        except ApiError:
            return key, None
    keys = list(keys)
    if not keys:
        return {}
    with ThreadPoolExecutor(max_workers=min(PARALLEL, len(keys))) as pool:
        return {k: v for k, v in pool.map(one, keys) if v is not None}


def parse_pairs(text: str) -> dict:
    """'a=1, b=two, c=null' -> {'a': '1', 'b': 'two', 'c': None}"""
    out = {}
    for piece in text.split(","):
        piece = piece.strip()
        if not piece:
            continue
        if "=" not in piece:
            raise ValueError(f"'{piece}' is not key=value")
        k, v = piece.split("=", 1)
        if not k.strip():
            raise ValueError(f"'{piece}' has no key")
        v = v.strip()
        out[k.strip()] = None if v == "null" else v
    return out


class CommandInput(Input):
    """The : command line and the ! shell line, with history on ↑ ↓."""
    BINDINGS = [Binding("up", "history(-1)", show=False), Binding("down", "history(1)", show=False),
                Binding("escape", "cancel", show=False)]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mode = "cmd"
        self.history = {"cmd": [], "shell": []}
        self._pos = 0

    def start(self, mode: str, text: str = "") -> None:
        self.mode = mode
        self._pos = len(self.history[mode])
        self.value = text
        self.cursor_position = len(text)
        self.focus()

    def action_history(self, step: int) -> None:
        h = self.history[self.mode]
        if not h:
            return
        self._pos = max(0, min(len(h), self._pos + step))
        self.value = h[self._pos] if self._pos < len(h) else ""
        self.cursor_position = len(self.value)

    def action_cancel(self) -> None:
        self.app.close_command_line()


class OnStore(App):
    CSS_PATH = "onstore.tcss"
    TITLE = "On Store"
    ENABLE_COMMAND_PALETTE = False
    BINDINGS = [
        Binding("1", "overview", "Overview"),
        Binding("2", "flow", "Flow"),
        Binding("u", "upload", "Upload"),
        Binding("d", "delete", "Delete"),
        Binding("r", "repair", "Repair"),
        Binding("x", "cleanup", "Cleanup"),
        Binding("k", "kill", "Kill"),
        Binding("s", "start", "Start"),
        Binding("plus", "add_worker", "+Worker"),
        Binding("c", "corrupt", "Corrupt"),
        Binding("t", "traffic", "Traffic"),
        Binding("colon", "command", ":Cmd"),
        Binding("exclamation_mark", "shell", "!Shell"),
        Binding("question_mark", "help", "?Help"),
        Binding("q", "quit", "Quit"),
        Binding("ctrl+c", "stop_shell", "Stop command", show=False, priority=True),
    ]

    def __init__(self, api_url: str = "http://127.0.0.1:5000", docker: Docker = None,
                 heartbeat_timeout: float = HEARTBEAT_TIMEOUT, start_on_flow: bool = False, poll: bool = True):
        super().__init__()
        self.api = Api(api_url)
        self.docker = docker or Docker()
        self.state = State(timeout=heartbeat_timeout)
        self.tracker = HeartbeatTracker()
        self.flow = FlowState()
        self.start_on_flow = start_on_flow
        self.poll = poll
        # the latest answers of each poll
        self._raw_nodes, self._raw_objects, self._raw_health, self._raw_inventories = [], {}, {}, {}
        self._rf, self._auto_repair = 2, None
        self._loaded = {"nodes": False, "objects": False}
        self._skip_moves = False          # our own repair already animated its copies
        self.own = set()                  # objects the dashboard uploaded or deleted itself
        # what is selected
        self.sel_worker = None
        self.sel_object = None
        self.chunk_index = 0
        self.shell_proc = None
        self.traffic_on = False
        self._last_focus = None

    # ================================================================ layout
    def compose(self) -> ComposeResult:
        yield Static(id="topbar")
        with Grid(id="overview"):
            yield WorkersPane(id="workers")
            yield ChunkMapPane(id="chunkmap")
            yield ObjectPane(id="object")
            yield SpreadPane(id="spread")
            yield RichLog(id="events", classes="pane", wrap=True, markup=False, highlight=False, max_lines=3000)
        with Horizontal(id="cmdbar"):
            yield Static("", id="prompt")
            yield CommandInput(id="cmd")
        yield Footer()

    def on_mount(self) -> None:
        self.events_log = self.ov("#events", RichLog)
        self.events_log.border_title = "EVENTS & OUTPUT"
        self.cmdbar = self.ov("#cmdbar")
        self.cmd_input = self.ov("#cmd", CommandInput)
        self.prompt = self.ov("#prompt", Static)
        self.ov("#workers").focus()
        self.log_event("info", f"On Store: watching the controller at {self.api.base}   (? help, 2 flow screen)")
        if not self.docker.available:
            self.log_event("warn", "docker not found: kill / start / + / corrupt are off (everything else works)")
        if self.poll:
            self.run_worker(self._poll_forever(self.poll_nodes, NODES_EVERY), group="poll-nodes")
            self.run_worker(self._poll_forever(self.poll_objects, OBJECTS_EVERY), group="poll-objects")
            self.run_worker(self._poll_forever(self.poll_health, HEALTH_EVERY), group="poll-health")
        self.set_interval(0.5, self.redraw)
        self.redraw()
        if self.start_on_flow:
            self.action_flow()

    def redraw(self) -> None:
        self.ov("#topbar", Static).update(topbar(self))
        for pane in ("#workers", "#chunkmap", "#object", "#spread"):
            self.ov(pane).refresh()

    def ov(self, selector, expect_type=None):
        """An overview widget, also while the flow screen is on top."""
        screen = self.screen_stack[0] if self.screen_stack else self.screen
        return screen.query_one(selector, expect_type) if expect_type else screen.query_one(selector)

    def focused_pane(self):
        f = self.screen_stack[0].focused if self.screen_stack else None
        return f.id if f is not None else None

    # ================================================================ events
    def log_event(self, kind: str, text: str) -> None:
        line = Text(datetime.now().strftime("%H:%M:%S "), style=DIM)
        line.append(text, style=KIND_COLORS.get(kind, FG))
        self.events_log.write(line)
        if kind != "out":
            self.flow.note(kind, text)

    # ================================================================ polling
    async def _poll_forever(self, fn, interval: float) -> None:
        while True:
            try:
                await fn()
            except ApiError as e:
                self._lost(str(e))
            except Exception as e:                    # never let a poll loop die
                self.log_event("bad", f"dashboard error: {type(e).__name__}: {e}")
            await asyncio.sleep(interval)

    def _lost(self, error: str) -> None:
        if self.state.controller_ok:
            self.log_event("bad", f"controller unreachable: {error}")
        self.state.controller_ok = False
        self.state.error = error

    async def poll_nodes(self) -> None:
        self._raw_nodes = await asyncio.to_thread(self.api.nodes)
        self._rebuild("nodes")
        for wid in self.tracker.observe(self.state):
            self.flow.heartbeat(wid)

    async def poll_objects(self) -> None:
        listing = await asyncio.to_thread(self.api.objects)
        ids = [o["object_id"] for o in listing]
        found = await asyncio.to_thread(fetch_many, self.api.metadata, ids)   # (deleted meanwhile: left out)
        self._raw_objects = {oid: found[oid] for oid in ids if oid in found}
        self._rebuild("objects")

    async def poll_health(self) -> None:
        report = await asyncio.to_thread(self.api.replication)
        self._raw_health = build_health(report)
        self._rf = report.get("replication_factor", self._rf)
        self._auto_repair = report.get("auto_repair")
        live = [wid for wid in self.state.workers if self.state.state_of(wid) in (UP, LATE)]
        self._raw_inventories = await asyncio.to_thread(fetch_many, self.api.inventory, live)
        self._rebuild("health")

    async def refresh_now(self) -> None:
        """Right after an action: the quick parts (workers, objects) together,
        then the slower replication report."""
        async def safe(fn):
            try:
                await fn()
            except ApiError as e:
                self._lost(str(e))
        await asyncio.gather(safe(self.poll_nodes), safe(self.poll_objects))
        await safe(self.poll_health)

    async def watch_workers(self, seconds: float = 8.0) -> None:
        """After kill / start / scale: ask for the workers 4 times a second for
        a while, so the change shows as soon as the controller sees it."""
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            try:
                await self.poll_nodes()
            except ApiError as e:
                self._lost(str(e))
            await asyncio.sleep(0.25)

    def _rebuild(self, source: str) -> None:
        prev = self.state
        cur = State(controller_ok=True, workers=build_workers(self._raw_nodes, self._raw_objects, self._raw_inventories),
                    objects=self._raw_objects, health=self._raw_health, replication_factor=self._rf,
                    auto_repair=self._auto_repair, timeout=prev.timeout)
        self.state = cur
        if not prev.controller_ok and prev.error:
            self.log_event("good", "controller reachable again")
        first = not self._loaded.get(source, True)
        if source in self._loaded:
            self._loaded[source] = True
        if first and source == "nodes":
            self.log_event("info", f"{len(cur.workers)} worker(s): " + ", ".join(cur.workers))
        elif source == "nodes":
            for kind, text in worker_events(prev, cur):
                self.log_event(kind, text)
        if first and source == "objects":
            self.log_event("info", f"{len(cur.objects)} object(s), {cur.chunk_total()} chunk(s) stored")
        elif source == "objects":
            events, moves = object_events(prev, cur, self.own)
            for kind, text in events:
                self.log_event(kind, text)
            for oid in cur.objects.keys() - prev.objects.keys() - self.own:
                self.flow.upload(cur.objects[oid].get("client_id") or "client", cur.objects[oid])
            for oid in prev.objects.keys() - cur.objects.keys() - self.own:
                self.flow.delete(prev.objects[oid])
            if self._skip_moves:
                self._skip_moves = False
            else:
                self._animate_moves(cur, moves)
        if source == "health":
            for kind, text in health_events(prev, cur) + auto_repair_events(prev, cur):
                self.log_event(kind, text)
        self.redraw()

    def _animate_moves(self, st: State, moves) -> None:
        """Copies that moved without our own repair (auto-repair, adoption)."""
        steps = []
        for oid, index, removed, added in moves:
            meta = st.objects[oid]
            replicas = next(c["replicas"] for c in meta["chunks"] if c["index"] == index)
            src = next((n for n in replicas if n not in added), None)
            name = label(meta, index)
            if removed:
                steps.append(("bad", f"{name}  copy on {', '.join(removed)} dropped", None))
            for a in added:
                steps.append(("repair", f"{name}  > copied from {src} to {a}", (src, a)))
        if steps:
            self.flow.add_steps(steps)

    # ================================================================ selection
    def selected_worker(self):
        ids = list(self.state.workers)
        if not ids:
            return None
        if self.sel_worker not in ids:
            self.sel_worker = ids[0]
        return self.sel_worker

    def select_worker(self, step: int) -> None:
        ids = list(self.state.workers)
        if ids:
            i = ids.index(self.selected_worker())
            self.sel_worker = ids[(i + step) % len(ids)]
            self.redraw()

    def selected_object(self):
        ids = list(self.state.objects)
        if not ids:
            return None
        if self.sel_object not in ids:
            self.sel_object = ids[-1]
            self.chunk_index = 0
        return self.sel_object

    def select_object(self, step: int) -> None:
        ids = list(self.state.objects)
        if ids:
            i = ids.index(self.selected_object())
            self.sel_object = ids[(i + step) % len(ids)]
            self.chunk_index = 0
            self.redraw()

    def select_chunk(self, step: int) -> None:
        oid = self.selected_object()
        if oid:
            n = len(self.state.objects[oid]["chunks"]) or 1
            self.chunk_index = (self.chunk_index + step) % n
            self.redraw()

    def selected_chunk(self):
        oid = self.selected_object()
        if oid is None or not self.state.objects[oid]["chunks"]:
            return None, None
        chunks = self.state.objects[oid]["chunks"]
        return self.state.objects[oid], chunks[min(self.chunk_index, len(chunks) - 1)]

    def resolve(self, ref: str = None) -> str:
        """An object from its id, its name, or the start of its id; default: the selected one."""
        objs = self.state.objects
        if ref is None:
            oid = self.selected_object()
            if oid is None:
                raise ValueError("no object stored")
            return oid
        if ref in objs:
            return ref
        by_name = [oid for oid, m in objs.items() if m["name"] == ref]
        if by_name:
            return by_name[-1]
        prefix = [oid for oid in objs if oid.startswith(ref)]
        if len(prefix) == 1:
            return prefix[0]
        raise ValueError(f"no object '{ref}'" if not prefix else f"'{ref}' matches several objects")

    # ================================================================ actions
    def check_action(self, action: str, parameters) -> bool:
        if isinstance(self.screen, ModalScreen) and action not in ("stop_shell",):
            return False
        return True

    def action_overview(self) -> None:
        if isinstance(self.screen, FlowScreen):
            self.pop_screen()

    def action_flow(self) -> None:
        if not isinstance(self.screen, FlowScreen):
            self.push_screen(FlowScreen())

    def action_help(self) -> None:
        self.push_screen(TextScreen("ON STORE  HELP", Text.from_markup(HELP)))

    def action_command(self) -> None:
        self.open_command_line("cmd")

    def action_shell(self) -> None:
        self.open_command_line("shell")

    def action_upload(self) -> None:
        self.open_command_line("cmd", "upload ")

    def open_command_line(self, mode: str, text: str = "") -> None:
        if isinstance(self.screen, FlowScreen):
            self.pop_screen()
        self._last_focus = self.screen_stack[0].focused if self.screen_stack[0].focused is not self.cmd_input else self._last_focus
        self.cmdbar.display = True
        self.prompt.update(Text(" onstore › " if mode == "cmd" else " shell $ ",
                                style="bold #000000 on " + ("#00e5ff" if mode == "cmd" else "#ffd700")))
        self.cmd_input.placeholder = ("upload PATH | upload NAME MB | download | delete | repair | cleanup [age] | "
                                      "kill W | start W | scale N | help" if mode == "cmd" else
                                      "any shell command, e.g. docker ps   (Ctrl+C stops it)")
        self.cmd_input.start(mode, text)

    def close_command_line(self) -> None:
        self.cmdbar.display = False
        self.cmd_input.value = ""
        (self._last_focus or self.ov("#workers")).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        text = event.value.strip()
        mode = self.cmd_input.mode
        self.close_command_line()
        if not text:
            return
        history = self.cmd_input.history[mode]
        if not history or history[-1] != text:
            history.append(text)
        if mode == "cmd":
            self.run_worker(self.run_command(text), group="command")
        else:
            self.run_worker(self.run_shell(text), group="shell")

    def action_delete(self) -> None:
        oid = self.selected_object()
        if oid is None:
            self.log_event("warn", "nothing to delete")
            return
        meta = self.state.objects[oid]

        def answer(yes):
            if yes:
                self.run_worker(self.do_delete(oid), group="command")
        self.push_screen(ConfirmScreen(f"Delete {meta['name']} ({human_size(meta['size'])}) and every copy "
                                       f"of its {meta['chunk_count']} chunk(s)?"), answer)

    def action_repair(self) -> None:
        self.run_worker(self.do_repair(), group="command")

    def action_cleanup(self) -> None:
        self.run_worker(self.do_cleanup(None), group="command")

    def action_kill(self) -> None:
        wid = self.selected_worker()
        if wid:
            self.run_worker(self.do_docker("kill", wid), group="docker")

    def action_start(self) -> None:
        wid = self.selected_worker()
        if wid:
            self.run_worker(self.do_docker("start", wid), group="docker")

    def action_add_worker(self) -> None:
        self.run_worker(self.do_scale(len(self.state.workers) + 1), group="docker")

    def action_corrupt(self) -> None:
        self.run_worker(self.do_corrupt(None), group="docker")

    def action_traffic(self) -> None:
        self.traffic_on = not self.traffic_on
        self.log_event("info", "client traffic on: the dashboard downloads a random object every 3 s"
                       if self.traffic_on else "client traffic off")
        if self.traffic_on:
            self.run_worker(self.traffic(), group="traffic", exclusive=True)

    def action_stop_shell(self) -> None:
        proc = self.shell_proc
        if proc is None:
            self.notify("No command running. q quits.", timeout=2)
            return
        if os.name == "nt":                       # kill the whole tree (cmd.exe and what it started)
            subprocess.run(["taskkill", "/T", "/F", "/PID", str(proc.pid)], capture_output=True)
        else:
            proc.kill()
        self.log_event("warn", "^C: command stopped")

    # ================================================================ details
    def show_worker_details(self) -> None:
        wid = self.selected_worker()
        if wid:
            self.run_worker(self._worker_details(wid), group="details")

    async def _worker_details(self, wid: str) -> None:
        st = self.state
        w = st.workers[wid]
        state = worker_state(w, st.timeout)
        t = Text()
        t.append(f"{wid}  ", style=f"bold {worker_color(wid)}")
        t.append(f"{state}\n", style=f"bold {STATE_COLORS[state]}")
        if w.registered:
            t.append(f"capacity {human_size(w.capacity)} · used {human_size(w.used)} · free {human_size(w.free)}"
                     f" · heartbeat {w.heartbeat_age}s ago\n", style=FG)
            try:
                container = self.docker.container(wid)
                t.append(f"container {container}\n", style=DIM)
            except DockerError:
                pass
        try:
            inv = await asyncio.to_thread(self.api.inventory, wid)
        except ApiError as e:
            inv = None
            t.append(f"\ncannot read what it holds: {e}\n", style=RED)
        if inv:
            t.append(f"\nholds {inv['held']} chunk file(s) · the metadata puts {inv['expected']} here\n", style=FG)
            t.append(f"\norphans ({len(inv['orphans'])})", style=f"bold {YELLOW}")
            t.append("  held here, but no object points to this copy\n", style=DIM)
            for c in inv["orphans"]:
                t.append(f"  {c['chunk_id']}  {human_size(c['size'])}  written {c['age']:.0f}s ago\n", style=YELLOW)
            if inv["orphans"]:
                t.append("  x cleans up the ones older than 60 s (or :cleanup 0)\n", style=DIM)
            if inv["in_progress"]:
                t.append(f"\nupload in progress ({len(inv['in_progress'])})\n", style=FG)
            t.append(f"\nmissing ({len(inv['missing'])})", style=f"bold {RED if inv['missing'] else FG}")
            t.append("  the metadata says it is here, but it is not\n", style=DIM)
            for cid in inv["missing"]:
                t.append(f"  {cid}\n", style=RED)
        t.append("\nchunks the metadata puts on this worker\n", style=f"bold {FG}")
        for oid, meta in st.objects.items():
            for c in meta["chunks"]:
                if wid in c["replicas"]:
                    others = [n for n in c["replicas"] if n != wid]
                    s = st.copy_state(oid, c["index"], wid)
                    t.append(f"  {label(meta, c['index']):<28}", style=FG)
                    t.append(f"{s:<8}", style={"ok": GREEN, "corrupt": PINK, "down": RED}.get(s, YELLOW))
                    t.append(f"other copy: {', '.join(others) or '-'}\n", style=DIM)
        self.push_screen(TextScreen(f"WORKER {wid}", t))

    def show_chunk_details(self) -> None:
        meta, chunk = self.selected_chunk()
        if chunk is None:
            return
        st = self.state
        oid = meta["object_id"]
        status = st.chunk_status(oid, chunk)
        t = Text()
        t.append(f"{label(meta, chunk['index'])}  ", style=f"bold {FG}")
        t.append(status.replace("_", "-").upper() + "\n", style=f"bold {STATUS_COLORS[status]}")
        t.append(f"chunk id  {chunk['chunk_id']}\nchecksum  {chunk['checksum']}\n\n", style=DIM)
        reader = None
        for n in chunk["replicas"]:
            s = st.copy_state(oid, chunk["index"], n)
            if s == "ok" and reader is None:
                reader = n
            t.append("  ● ", style=worker_color(n))
            t.append(f"{n:<12}", style=f"bold {worker_color(n)}")
            t.append(f"{s.upper():<9}", style={"ok": GREEN, "corrupt": PINK, "down": RED}.get(s, YELLOW))
            t.append({"ok": "matches the checksum recorded at upload", "down": "worker not reachable",
                      "corrupt": "bytes changed on disk: never served, repair replaces it",
                      "missing": "worker is up but has no such chunk"}.get(s, "") + "\n", style=DIM)
        t.append("\n")
        if reader:
            t.append(f"a download reads this chunk from {reader} (the first good copy)\n", style=FG)
        else:
            t.append("no good copy: downloading this object fails (503)\n", style=RED)
        short = meta["copies_per_chunk"] - sum(st.copy_state(oid, chunk["index"], n) == "ok" for n in chunk["replicas"])
        if short > 0 and reader:
            t.append(f"{short} copy/copies short: r (repair) makes new ones from {reader}\n", style=YELLOW)
        self.push_screen(TextScreen("CHUNK", t))

    # ================================================================ doing things
    async def run_command(self, text: str) -> None:
        self.log_event("cmd", f"› {text}")
        try:
            parts = [p.strip('"\'') for p in shlex.split(text, posix=False)]
        except ValueError as e:
            self.log_event("bad", f"cannot read that command: {e}")
            return
        cmd, args = parts[0].lower(), parts[1:]
        try:
            if cmd in ("help", "?"):
                self.action_help()
            elif cmd == "upload":
                await self.do_upload(args)
            elif cmd in ("download", "get"):
                await self.do_download(self.resolve(args[0] if args else None), args[1] if len(args) > 1 else None)
            elif cmd in ("delete", "rm"):
                await self.do_delete(self.resolve(args[0] if args else None))
            elif cmd == "meta":
                if not args:
                    raise ValueError("meta [ID|NAME] key=value,...")
                ref, pairs = (None, args) if "=" in args[0] else (args[0], args[1:])
                oid = self.resolve(ref)
                meta = await asyncio.to_thread(self.api.update_metadata, oid, parse_pairs(" ".join(pairs)))
                self.log_event("good", f"{meta['name']}: custom metadata now {meta['custom'] or '{}'}")
                await self.refresh_now()
            elif cmd == "repair":
                await self.do_repair()
            elif cmd == "cleanup":
                await self.do_cleanup(float(args[0]) if args else None)
            elif cmd in ("kill", "start"):
                if not args:
                    raise ValueError(f"{cmd} WORKER, e.g. {cmd} worker2")
                await self.do_docker(cmd, args[0])
            elif cmd == "scale":
                if not args or not args[0].isdigit():
                    raise ValueError("scale N, e.g. scale 5")
                await self.do_scale(int(args[0]))
            elif cmd == "corrupt":
                await self.do_corrupt(args[0] if args else None)
            elif cmd == "clear":
                self.events_log.clear()
            else:
                self.log_event("bad", f"unknown command '{cmd}' (help lists them)")
        except (ValueError, ApiError, DockerError) as e:
            self.log_event("bad", str(e))

    async def do_upload(self, args) -> None:
        if not args:
            raise ValueError("upload PATH [key=value,...]   or   upload NAME MB")
        # a path with spaces may come unquoted: take the longest start that is a file
        for n in range(len(args), 0, -1):
            if os.path.isfile(os.path.expanduser(" ".join(args[:n]))):
                args = [" ".join(args[:n])] + args[n:]
                break
        path = os.path.expanduser(args[0])
        if os.path.isfile(path):
            with open(path, "rb") as f:
                data = f.read()
            name, source = os.path.basename(path), os.path.abspath(path)
            custom = parse_pairs(" ".join(args[1:])) if len(args) > 1 else {}
        elif len(args) > 1 and args[1].replace(".", "", 1).isdigit():
            data = os.urandom(int(float(args[1]) * MB))
            name, source = args[0], None
            custom = parse_pairs(" ".join(args[2:])) if len(args) > 2 else {}
        else:
            raise ValueError(f"no file '{args[0]}' (to make one: upload NAME MB)")
        custom = {k: v for k, v in custom.items() if v is not None}
        self.log_event("info", f"uploading {name} ({human_size(len(data))}) ...")
        meta = await asyncio.to_thread(self.api.upload, name, data, custom, source)
        self.own.add(meta["object_id"])
        self._raw_objects = {**self._raw_objects, meta["object_id"]: meta}     # show it now
        self._rebuild("objects")
        self.flow.upload("dashboard", meta)
        where = ", ".join(f"#{c['index']}->{'+'.join(c['replicas'])}" for c in meta["chunks"][:6])
        self.log_event("good", f"uploaded {name}: {meta['chunk_count']} chunk(s) x{meta['copies_per_chunk']}  "
                               f"{where}{' ...' if meta['chunk_count'] > 6 else ''}   id {meta['object_id']}")
        self.sel_object, self.chunk_index = meta["object_id"], 0
        await self.refresh_now()

    async def do_download(self, oid: str, dest: str = None) -> None:
        meta = self.state.objects[oid]
        self.flow.download(meta, self.state, True)
        data = await asyncio.to_thread(self.api.download, oid)
        if dest is None:
            base, ext = os.path.splitext(meta["name"])
            dest, n = meta["name"], 1
            while os.path.exists(dest):
                dest, n = f"{base} ({n}){ext}", n + 1
        elif os.path.exists(dest):
            raise ValueError(f"{dest} already exists")
        with open(dest, "wb") as f:
            f.write(data)
        self.log_event("good", f"downloaded {meta['name']} ({human_size(len(data))}, checksum ok) -> "
                               f"{os.path.abspath(dest)}")

    async def do_delete(self, oid: str) -> None:
        meta = self.state.objects.get(oid)
        await asyncio.to_thread(self.api.delete, oid)
        self.own.add(oid)
        self._raw_objects = {k: v for k, v in self._raw_objects.items() if k != oid}   # gone now
        self._rebuild("objects")
        if meta:
            self.flow.delete(meta)
        self.log_event("good", f"deleted {meta['name'] if meta else oid} and its copies")
        await self.refresh_now()

    async def do_repair(self) -> None:
        self.log_event("repair", "repair: checking every copy of every chunk ...")
        try:
            result = await asyncio.to_thread(self.api.repair)
        except ApiError as e:
            self.log_event("bad", f"repair: {e}")
            return
        self._skip_moves = True
        self.flow.add_steps(repair_steps(result))
        self.log_event("repair", f"repair: {result['chunks_checked']} chunk(s) checked, {result['repaired']} repaired, "
                                 f"{result['partial']} partial, {result['unrecoverable']} unrecoverable, "
                                 f"{result['copies_created']} copy/copies created")
        for d in result["details"]:
            self.log_event("repair", f"  {d['name']}#{d['index']}: {d['outcome']}"
                                     + (f", new copy on {', '.join(d['added'])}" if d.get("added") else ""))
        if result["left_behind"]:
            self.log_event("warn", f"  {len(result['left_behind'])} copy/copies left on down workers: they become "
                                   "orphans when those workers return (x cleans them up)")
        await self.refresh_now()

    async def do_cleanup(self, min_age) -> None:
        try:
            r = await asyncio.to_thread(self.api.cleanup, min_age)
        except ApiError as e:
            self.log_event("bad", f"cleanup: {e}")
            return
        self.log_event("repair", f"cleanup: {len(r['deleted'])} orphan(s) deleted, {len(r['adopted'])} adopted back, "
                                 f"{len(r['kept_young'])} kept (too recent), "
                                 f"{len(r['unreachable'])} worker(s) unreachable")
        for e in r["deleted"]:
            self.flow.send(("c2w", e["node"]), "✕", DIM)
            self.log_event("info", f"  deleted {e['chunk_id']} on {e['node']}")
        for e in r["adopted"]:
            self.log_event("good", f"  adopted {e['chunk_id']} on {e['node']}: its chunk was short of good copies")
        await self.refresh_now()

    async def do_docker(self, what: str, wid: str) -> None:
        try:
            container = self.docker.container(wid)
            self.log_event("cmd", f"docker {what} {container}")
            await (self.docker.kill(wid) if what == "kill" else self.docker.start(wid))
        except DockerError as e:
            self.log_event("bad", f"{what} {wid}: {e}")
            return
        if what == "kill":
            # it no longer answers (SILENT) from this moment; the controller
            # still counts it alive until its heartbeat is HEARTBEAT_TIMEOUT old
            self._raw_nodes = [dict(n, alive=False) if n["node"] == wid else n for n in self._raw_nodes]
            self._rebuild("nodes")
        self.run_worker(self.watch_workers(), group="watch", exclusive=True)
        if what == "kill":
            self.log_event("warn", f"{wid} killed. The controller marks it DOWN after {self.state.timeout:g}s without a "
                                   "heartbeat" + (f", auto-repair after {self._auto_repair['grace_seconds']:g}s more"
                                                  if (self._auto_repair or {}).get("enabled") else ""))
        else:
            self.log_event("good", f"{wid} started: it registers and sends heartbeats within ~2 s")

    async def do_scale(self, count: int) -> None:
        self.log_event("cmd", f"docker compose up -d --scale worker={count}")
        try:
            await self.docker.scale(count)
        except DockerError as e:
            self.log_event("bad", f"scale: {e}")
            return
        self.log_event("good", f"{count} worker container(s); new ones register themselves")
        self.run_worker(self.watch_workers(), group="watch", exclusive=True)

    async def do_corrupt(self, wid) -> None:
        meta, chunk = self.selected_chunk()
        if chunk is None:
            raise ValueError("select a chunk first (chunk map, ← →)")
        oid = meta["object_id"]
        if wid is None:
            wid = next((n for n in chunk["replicas"] if self.state.copy_state(oid, chunk["index"], n) == "ok"), None)
        if wid is None or wid not in chunk["replicas"]:
            raise ValueError(f"{label(meta, chunk['index'])} has no good copy on {wid or 'any worker'}")
        try:
            await self.docker.corrupt(wid, chunk["chunk_id"])
        except DockerError as e:
            self.log_event("bad", f"corrupt: {e}")
            return
        key = (oid, chunk["index"])                      # show it now; the next report confirms it
        old = self._raw_health.get(key) or {"status": "", "copies": {n: "ok" for n in chunk["replicas"]}}
        self._raw_health = {**self._raw_health, key: {**old, "copies": {**old["copies"], wid: "corrupt"}}}
        self._rebuild("health")
        self.log_event("warn", f"damaged the copy of {label(meta, chunk['index'])} on {wid} (first byte flipped). "
                               "The next check shows it CORRUPT; downloads skip it; r repairs it")
        await self.refresh_now()

    async def traffic(self) -> None:
        while self.traffic_on:
            objs = list(self.state.objects.values())
            if objs and self.state.controller_ok:
                meta = random.choice(objs)
                self.flow.download(meta, self.state, True)
                try:
                    data = await asyncio.to_thread(self.api.download, meta["object_id"])
                    self.flow.note("info", f"dashboard GET {meta['name']}: {human_size(len(data))}, checksum ok")
                except ApiError as e:
                    self.flow.note("bad", f"dashboard GET {meta['name']}: {e}")
            await asyncio.sleep(3)

    async def run_shell(self, command: str) -> None:
        if self.shell_proc is not None:
            self.log_event("warn", "a command is still running (Ctrl+C stops it)")
            return
        self.log_event("cmd", f"$ {command}")

        def started(proc):
            self.shell_proc = proc
        try:
            code = await run_shell(command, lambda line: self.log_event("out", line), started)
            self.log_event("good" if code == 0 else "warn", f"exit code {code}")
        except Exception as e:
            self.log_event("bad", f"{type(e).__name__}: {e}")
        finally:
            self.shell_proc = None
