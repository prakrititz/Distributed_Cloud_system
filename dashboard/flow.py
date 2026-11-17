"""Screen 2, Flow: what is moving through the system, animated.

Everything shown comes from the real system:
  ♥  a heartbeat. GET /nodes reports each worker's heartbeat age; when the age
     drops, a new heartbeat has arrived, and a ♥ travels to the controller.
  ■  data: an upload seen in the metadata (from any client) or a download the
     dashboard makes (t turns on a download every 3 s).
  ?  a request from the controller to a worker; × a request to a worker that
     is down (the controller then uses the next copy).
  ▣  a repair copy: read from a good copy, written to a new worker. From the
     repair report (r) or from copies that moved in the metadata (auto-repair).
"""
import time
from collections import deque

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Grid
from textual.screen import Screen
from textual.widget import Widget
from textual.widgets import Footer, Static

from .model import DOWN, LATE, SILENT, UP, repair_countdown, worker_state
from .widgets import (DIM, FAINT, FG, GREEN, KIND_COLORS, ORANGE, RED, STATE_COLORS, YELLOW, bar, fit,
                      local_time, topbar, worker_color)

DATA = "#ffffff"
REPAIR = "#d787ff"

# line pieces: which directions a cell connects to
U, D, L, R = 1, 2, 4, 8
GLYPH = {L | R: "─", L: "─", R: "─", U | D: "│", U: "│", D: "│", D | R: "┌", D | L: "┐", U | R: "└",
         U | L: "┘", U | D | R: "├", U | D | L: "┤", L | R | D: "┬", L | R | U: "┴", U | D | L | R: "┼"}


class Packet:
    __slots__ = ("route", "glyph", "style", "start", "dur", "fail")

    def __init__(self, route, glyph, style, start, dur, fail):
        self.route, self.glyph, self.style, self.start, self.dur, self.fail = route, glyph, style, start, dur, fail


class FlowState:
    """What the flow screen animates. Kept by the app, so nothing is lost while
    the overview is on screen."""

    def __init__(self):
        self.packets = []
        self.flashes = deque(maxlen=50)          # times the controller received something
        self.steps = deque(maxlen=200)           # (reveal_at, kind, text)
        self.activity = deque(maxlen=200)        # (time, kind, text)
        self.clients = ["dashboard"]
        self.heartbeats = 0
        self.requests = 0
        self._next_step = 0.0

    # ---------------------------------------------------------------- packets
    def send(self, route, glyph, style, delay=0.0, dur=0.7, fail=False) -> float:
        """route: ("w2c", worker) ("c2w", worker) ("k2c", client) ("c2k", client).
        Returns the time it arrives."""
        start = time.monotonic() + delay
        self.packets.append(Packet(route, glyph, style, start, dur, fail))
        if route[0] in ("w2c", "k2c") and not fail:
            self.flashes.append(start + dur)
        return delay + dur

    def heartbeat(self, wid: str) -> None:
        self.heartbeats += 1
        self.send(("w2c", wid), "♥", f"bold {worker_color(wid)}", dur=0.8)

    def client(self, name: str) -> str:
        name = name or "client"
        if name not in self.clients:
            self.clients.insert(1, name)
            del self.clients[4:]
        return name

    def upload(self, client: str, meta: dict) -> None:
        client = self.client(client)
        self.requests += 1
        t = self.send(("k2c", client), "■", f"bold {DATA}", dur=0.7)
        for i, chunk in enumerate(meta["chunks"]):
            for j, node in enumerate(chunk["replicas"]):
                self.send(("c2w", node), "■", f"bold {worker_color(node)}", delay=t + i * 0.35 + j * 0.1)
        self.note("info", f"{client} PUT {meta['name']}: {meta['chunk_count']} chunk(s) -> "
                          + ", ".join(sorted({n for c in meta['chunks'] for n in c['replicas']})))

    def download(self, meta: dict, state, ok: bool) -> None:
        self.requests += 1
        t = self.send(("k2c", "dashboard"), "?", f"bold {DATA}", dur=0.5)
        for i, chunk in enumerate(meta["chunks"]):
            d = t + i * 0.5
            for node in chunk["replicas"]:
                if state.copy_state(meta["object_id"], chunk["index"], node) == "ok":
                    self.send(("c2w", node), "?", f"bold {worker_color(node)}", delay=d, dur=0.5)
                    self.send(("w2c", node), "■", f"bold {worker_color(node)}", delay=d + 0.5, dur=0.5)
                    break
                self.send(("c2w", node), "×", f"bold {RED}", delay=d, dur=0.5, fail=True)
                d += 0.5
        if ok:
            self.send(("c2k", "dashboard"), "■", f"bold {DATA}", delay=t + len(meta["chunks"]) * 0.5 + 0.6)

    def delete(self, meta: dict) -> None:
        for node in sorted({n for c in meta["chunks"] for n in c["replicas"]}):
            self.send(("c2w", node), "✕", DIM, dur=0.6)

    def copy(self, src: str, dst: str, label: str, delay: float = 0.0) -> None:
        """A repair copy: read from src, write to dst (both go through the controller)."""
        if src:
            delay += self.send(("w2c", src), "▣", f"bold {REPAIR}", delay=delay, dur=0.6)
        self.send(("c2w", dst), "▣", f"bold {REPAIR}", delay=delay, dur=0.6)

    # ---------------------------------------------------------------- text
    def note(self, kind: str, text: str) -> None:
        self.activity.append((time.time(), kind, text))

    def add_steps(self, steps, spacing: float = 0.45) -> None:
        """Show the steps one after another; animate each copy as its step appears."""
        now = time.monotonic()
        at = max(now, self._next_step)
        for kind, text, move in steps:
            self.steps.append((at, kind, text))
            if move:
                self.copy(move[0], move[1], text, delay=at - now)
            at += spacing
        self._next_step = at

    def prune(self) -> None:
        now = time.monotonic()
        self.packets = [p for p in self.packets if p.start + p.dur + (0.6 if p.fail else 0) > now]


# ================================================================== widgets
class FlowPane(Widget):
    DEFAULT_CLASSES = "pane"
    TITLE = ""

    def on_mount(self) -> None:
        self.border_title = self.TITLE


class Traffic(FlowPane):
    TITLE = "TRAFFIC   ♥ heartbeat   ? request   ■ data   × failed   ▣ repair"

    def render(self) -> Text:
        app, st, flow = self.app, self.app.state, self.app.flow
        W = max(40, self.content_size.width or 80)
        H = max(8, self.content_size.height or 16)
        now = time.monotonic()
        chars = [[" "] * W for _ in range(H)]
        styles = [[None] * W for _ in range(H)]
        bits = {}
        line_style = {}

        def put(x, y, s, style=None):
            for i, ch in enumerate(s):
                if 0 <= x + i < W and 0 <= y < H:
                    chars[y][x + i] = ch
                    styles[y][x + i] = style

        def link(points, style):
            """Lines through the points; returns every cell on the way (for packets)."""
            cells = [points[0]]
            for (x0, y0), (x1, y1) in zip(points, points[1:]):     # straight segments only
                while (x0, y0) != (x1, y1):
                    x0 += (x1 > x0) - (x1 < x0)
                    y0 += (y1 > y0) - (y1 < y0)
                    cells.append((x0, y0))
            for a, b in zip(cells, cells[1:]):
                dx, dy = b[0] - a[0], b[1] - a[1]
                way = R if dx > 0 else L if dx < 0 else D if dy > 0 else U
                bits[a] = bits.get(a, 0) | way
                bits[b] = bits.get(b, 0) | {R: L, L: R, U: D, D: U}[way]
            for c in cells:
                line_style.setdefault(c, style)
            return cells

        # --------------------------------------------------------- layout
        ids = list(st.workers)
        shown = ids if len(ids) <= H else ids[: H - 1]
        cw, ch_w, ch_h = 13, 20, 6
        cx = max(cw + 6, int(W * 0.34))
        cy = max(0, (H - ch_h) // 2)
        cmid = cy + 2
        wx = W - 18
        bx = (cx + ch_w + wx) // 2
        kx = (cw + cx) // 2

        routes = {}
        if shown:
            gap = H / len(shown)
            for i, wid in enumerate(shown):
                wy = min(H - 1, int(i * gap + gap / 2))
                state = worker_state(st.workers[wid], st.timeout)
                color = "#5f0000" if state in (DOWN, SILENT) else FAINT
                cells = link([(cx + ch_w, cmid), (bx, cmid), (bx, wy), (wx - 2, wy)], color)
                routes[("c2w", wid)] = cells
                routes[("w2c", wid)] = cells[::-1]
                put(wx, wy, "● ", STATE_COLORS[state])
                sel = wid == app.selected_worker()
                put(wx + 2, wy, f"{fit(wid, 9):<9}", f"bold {worker_color(wid)}" + (" reverse" if sel else ""))
                put(wx + 12, wy, f"{state:<6}", STATE_COLORS[state])
        if len(shown) < len(ids):
            put(wx, H - 1, f"+{len(ids) - len(shown)} more", DIM)
        clients = flow.clients[: max(1, (H + 1) // 4)]
        top = max(0, (H - (len(clients) * 4 - 1)) // 2)
        for j, name in enumerate(clients):
            ky = top + j * 4
            put(0, ky, "┌" + "─" * (cw - 2) + "┐", DIM)
            put(0, ky + 1, "│", DIM)
            put(1, ky + 1, f"{fit(name, cw - 2):^{cw - 2}}", f"bold {FG}")
            put(cw - 1, ky + 1, "│", DIM)
            put(0, ky + 2, "└" + "─" * (cw - 2) + "┘", DIM)
            cells = link([(cw, ky + 1), (kx, ky + 1), (kx, cmid), (cx - 1, cmid)], FAINT)
            routes[("k2c", name)] = cells
            routes[("c2k", name)] = cells[::-1]

        for (x, y), b in bits.items():
            if 0 <= x < W and 0 <= y < H and chars[y][x] == " ":
                chars[y][x] = GLYPH.get(b, "─")
                styles[y][x] = line_style[(x, y)]

        # --------------------------------------------------------- controller
        flash = any(t <= now <= t + 0.3 for t in flow.flashes)
        border = "bold #00e5ff" if flash else "#bcbcbc"
        ar = st.auto_repair or {}
        put(cx, cy, "┌ CONTROLLER " + "─" * (ch_w - 14) + "┐", border)
        body = [(app.api.base.replace("http://", ""), FG),
                (f"♥ received {flow.heartbeats}", "#ff6f91" if flash else DIM),
                (f"requests {flow.requests}", DIM),
                (f"auto-repair {ar['grace_seconds']:g}s" if ar.get("enabled") else "auto-repair off", DIM)]
        for i, (s, style) in enumerate(body):
            put(cx, cy + 1 + i, "│", border)
            put(cx + 1, cy + 1 + i, f" {fit(s, ch_w - 3):<{ch_w - 3}}", style)
            put(cx + ch_w - 1, cy + 1 + i, "│", border)
        put(cx, cy + ch_h - 1, "└" + "─" * (ch_w - 2) + "┘", border)
        if not st.controller_ok:
            put(cx + 1, cy + ch_h, " UNREACHABLE ", f"bold {RED}")

        # --------------------------------------------------------- packets
        flow.prune()
        for p in flow.packets:
            cells = routes.get(p.route)
            if not cells or now < p.start:
                continue
            pos = (now - p.start) / p.dur
            if p.fail:
                half = cells[: max(1, len(cells) * 2 // 3)]
                if pos <= 1:
                    x, y = half[min(len(half) - 1, int(pos * (len(half) - 1)))]
                    put(x, y, "?", f"bold {RED}")
                else:
                    x, y = half[-1]
                    put(x, y, "×", f"bold {RED}")
                continue
            if pos > 1:
                continue
            x, y = cells[min(len(cells) - 1, int(pos * (len(cells) - 1)))]
            put(x, y, p.glyph, p.style)

        # --------------------------------------------------------- to Text
        out = Text(no_wrap=True, overflow="crop")
        for y in range(H):
            run, run_style = "", None
            for x in range(W):
                if styles[y][x] != run_style and run:
                    out.append(run, style=run_style)
                    run = ""
                run_style = styles[y][x]
                run += chars[y][x]
            out.append(run, style=run_style)
            if y < H - 1:
                out.append("\n")
        return out


class Heartbeats(FlowPane):
    TITLE = "HEARTBEATS   last 20 s"

    def render(self) -> Text:
        st, tracker = self.app.state, self.app.tracker
        width = max(20, self.content_size.width or 40)
        t = Text(no_wrap=True, overflow="crop")
        cells = max(10, width - 19)
        t.append(f"{'':11}{'20s ago':<{cells - 3}}now\n", style=DIM)
        now = time.monotonic()
        for wid, w in st.workers.items():
            state = worker_state(w, st.timeout)
            t.append("● ", style=STATE_COLORS[state])
            t.append(f"{fit(wid, 8):<9}", style=f"bold {worker_color(wid)}")
            for c in tracker.strip(wid, cells, now):
                if c == "beat":
                    t.append("♥", style=f"bold {worker_color(wid)}")
                elif c == "down":
                    t.append("·", style=RED)
                elif c == "silent":
                    t.append("·", style=ORANGE)
                elif c == "gap":
                    t.append("─", style=FAINT)
                else:
                    t.append(" ")
            age = w.heartbeat_age
            t.append(f" {age:>5.1f}s\n" if age is not None else "     -\n",
                     style=DIM if state == UP else STATE_COLORS[state])
        t.append("\n♥ every 2 s · ", style=DIM)
        t.append(f"no heartbeat for {st.timeout:g} s -> DOWN", style=DIM)
        return t


class Failures(FlowPane):
    TITLE = "FAILURE DETECTION -> AUTO-REPAIR"

    def render(self) -> Text:
        st = self.app.state
        width = max(20, self.content_size.width or 40)
        t = Text(no_wrap=True, overflow="ellipsis")
        ar = st.auto_repair or {}
        grace = ar.get("grace_seconds", 30)
        trouble = [w for w in st.workers if worker_state(st.workers[w], st.timeout) != UP]
        if not st.workers:
            t.append("no workers yet", style=DIM)
        elif not trouble:
            t.append("✓ every worker sent a heartbeat in the last 2 s\n", style=GREEN)
        for wid in trouble:
            w = st.workers[wid]
            state = worker_state(w, st.timeout)
            t.append(f"{wid}", style=f"bold {worker_color(wid)}")
            if not w.registered:
                t.append(": not registered (named in the metadata)\n", style=RED)
                continue
            age = w.heartbeat_age or 0
            if state == LATE:
                t.append(f": heartbeat late, {age:.1f}s\n", style=YELLOW)
            elif state == SILENT:
                left = max(0.0, st.timeout - age)
                t.append(f": not answering, last heartbeat {age:.1f}s ago\n", style=ORANGE)
                t.append(f"   DOWN in {left:4.1f}s  ", style=ORANGE)
                t.append_text(bar(age / st.timeout, max(5, width - 20), ORANGE))
                t.append("\n")
            else:
                down_for, left = repair_countdown(st, wid)
                t.append(f": no heartbeat for {age:.0f}s -> DOWN\n", style=RED)
                last = ar.get("last") or {}
                if left is None:
                    t.append("   auto-repair off: press r to repair\n", style=DIM)
                elif wid in last.get("because_down", []) and down_for >= grace:
                    t.append(f"   ✓ auto-repaired at {local_time(last['at'])[11:]}: {last['repaired']} chunk(s), "
                             f"{last['copies_created']} copies\n", style=GREEN)
                elif left > 0:
                    t.append(f"   auto-repair in ≈{left:4.1f}s ", style=YELLOW)
                    t.append_text(bar(down_for / grace if grace else 1, max(5, width - 26), YELLOW))
                    t.append("\n")
                else:
                    t.append("   auto-repair running / due now\n", style=REPAIR)
        t.append("\n")
        rules = [("♥", f"every 2 s; none for {st.timeout:g} s -> DOWN"),
                 ("⏱", f"DOWN for {grace:g} s -> automatic repair" if ar.get("enabled") else "automatic repair is off"),
                 ("↺", "back before that -> nothing is copied")]
        for icon, text in rules:
            t.append(f"{icon} ", style=FG)
            t.append(text + "\n", style=DIM)
        return t


class Steps(FlowPane):
    TITLE = "REPAIR STEPS & ACTIVITY"

    def render(self) -> Text:
        flow = self.app.flow
        height = max(4, self.content_size.height or 10)
        now = time.monotonic()
        t = Text(no_wrap=True, overflow="ellipsis")
        steps = [s for s in flow.steps if s[0] <= now]
        room_steps = max(2, height // 2)
        for at, kind, text in steps[-room_steps:]:
            t.append(text + "\n", style=KIND_COLORS.get(kind, FG))
        if not steps:
            t.append("no repair yet (r repairs; auto-repair shows here too)\n", style=DIM)
        t.append("─" * max(10, (self.content_size.width or 40)) + "\n", style=FAINT)
        room = height - min(len(steps), room_steps) - 2
        for when, kind, text in list(flow.activity)[-max(1, room):]:
            t.append(time.strftime("%H:%M:%S ", time.localtime(when)), style=DIM)
            t.append(text + "\n", style=KIND_COLORS.get(kind, FG))
        return t


class FlowScreen(Screen):
    BINDINGS = [Binding("up", "move(-1)", show=False), Binding("down", "move(1)", show=False),
                Binding("escape", "app.overview", show=False)]

    def compose(self) -> ComposeResult:
        yield Static(id="topbar")
        with Grid(id="flow"):
            yield Traffic(id="traffic")
            yield Heartbeats(id="heartbeats")
            yield Failures(id="failures")
            yield Steps(id="steps")
        yield Footer()

    def on_mount(self) -> None:
        self.set_interval(0.08, self.tick)

    def tick(self) -> None:
        self.query_one("#topbar", Static).update(topbar(self.app))
        for w in self.query(FlowPane):
            w.refresh()

    def action_move(self, step: int) -> None:
        self.app.select_worker(step)
