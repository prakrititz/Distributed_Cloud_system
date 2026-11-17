"""The panes of the overview screen. Each pane draws itself from app.state
(see model.py) every time the app refreshes it."""
import re
from datetime import datetime, timezone

from rich.text import Text
from textual.binding import Binding
from textual.widget import Widget

from .model import DOWN, LATE, SILENT, UP, human_size, worker_state

# ---------------------------------------------------------------- colours
DIM = "#6c6c6c"
FAINT = "#3a3a3a"
FG = "#e8e8e8"
GREEN, YELLOW, ORANGE, RED, PINK = "#00ff5f", "#ffd700", "#ff8700", "#ff3b3b", "#ff00af"
WORKER_COLORS = ["#00e5ff", "#ff4fd8", "#ffd400", "#7c8cff", "#ff8a00", "#00ffa3", "#c792ff", "#ff6f91",
                 "#5fd7ff", "#d7ff5f"]
STATE_COLORS = {UP: GREEN, LATE: YELLOW, SILENT: ORANGE, DOWN: RED}
STATUS_COLORS = {"healthy": GREEN, "under_replicated": YELLOW, "unavailable": RED}
KIND_COLORS = {"good": GREEN, "bad": RED, "warn": YELLOW, "info": "#87d7ff", "repair": "#d787ff",
               "cmd": "#ffffff", "out": "#bcbcbc"}


def worker_color(worker_id: str) -> str:
    """The same colour for a worker everywhere, from its number."""
    m = re.search(r"(\d+)$", worker_id)
    n = int(m.group(1)) - 1 if m else sum(map(ord, worker_id))
    return WORKER_COLORS[n % len(WORKER_COLORS)]


def fit(s: str, n: int) -> str:
    s = str(s)
    return s if len(s) <= n else s[: max(0, n - 1)] + "…"


def bar(frac: float, width: int, color: str, empty: str = FAINT) -> Text:
    k = max(0, min(width, round(frac * width)))
    t = Text("█" * k, style=color)
    t.append("░" * (width - k), style=empty)
    return t


def local_time(iso: str) -> str:
    try:
        return (datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                .astimezone().strftime("%Y-%m-%d %H:%M:%S"))
    except (TypeError, ValueError):
        return str(iso)


def topbar(app) -> Text:
    """ ◆ ON STORE │ controller │ ● 4/4 workers │ 4 objects · 9 chunks · x2 │ ● ALL HEALTHY │ auto-repair │ clock"""
    st = app.state
    t = Text()
    t.append(" ◆ ON STORE ", style="bold #000000 on #00e5ff")
    sep = Text(" │ ", style=DIM)
    t.append(" ")
    t.append(app.api.base.replace("http://", ""), style=FG)
    t.append_text(sep)
    if not st.controller_ok:
        t.append("● CONTROLLER UNREACHABLE", style=f"bold {RED}")
        t.append(f"  {fit(st.error, 60)}", style=DIM)
        return t
    up, total = st.workers_up(), len(st.workers)
    t.append("● ", style=GREEN if up == total and total else (YELLOW if up else RED))
    t.append(f"{up}/{total} workers", style=FG)
    t.append_text(sep)
    t.append(f"{len(st.objects)} objects · {st.chunk_total()} chunks · x{st.replication_factor}", style=FG)
    t.append_text(sep)
    overall = st.overall()
    t.append("● ", style=STATUS_COLORS[overall])
    t.append({"healthy": "ALL HEALTHY", "under_replicated": "UNDER-REPLICATED",
              "unavailable": "DATA UNAVAILABLE"}[overall], style=f"bold {STATUS_COLORS[overall]}")
    ar = st.auto_repair or {}
    t.append_text(sep)
    if ar.get("enabled"):
        t.append(f"auto-repair {ar['grace_seconds']:g}s", style=FG)
        if ar.get("last"):
            t.append(f" (last {local_time(ar['last']['at'])[11:]})", style=DIM)
    else:
        t.append("auto-repair off", style=DIM)
    if app.shell_proc is not None:
        t.append_text(sep)
        t.append("! running", style=f"bold {YELLOW}")
    if app.traffic_on:
        t.append_text(sep)
        t.append("traffic on", style=f"bold {KIND_COLORS['info']}")
    t.append_text(sep)
    t.append(datetime.now().strftime("%H:%M:%S"), style=DIM)
    return t


class Pane(Widget):
    """A bordered pane that draws itself from the app's state."""
    can_focus = True
    DEFAULT_CLASSES = "pane"
    TITLE = ""

    def on_mount(self) -> None:
        self.border_title = self.TITLE


# ===================================================================== workers
class WorkersPane(Pane):
    TITLE = "WORKERS"
    BINDINGS = [Binding("up", "move(-1)", "Up", show=False), Binding("down", "move(1)", "Down", show=False),
                Binding("enter", "details", "Details", show=False)]

    def action_move(self, step: int) -> None:
        self.app.select_worker(step)

    def action_details(self) -> None:
        self.app.show_worker_details()

    def render(self) -> Text:
        app, st = self.app, self.app.state
        ids = list(st.workers)
        self.border_title = f"WORKERS {st.workers_up()}/{len(ids)}"
        t = Text(no_wrap=True, overflow="ellipsis")
        if not ids:
            t.append("no worker has registered yet\n", style=DIM)
            t.append("start the system:\n  docker compose up -d", style=DIM)
            return t
        sel = app.selected_worker()
        height = self.content_size.height or 20
        if len(ids) * 2 <= height:
            mode = "full"
        elif len(ids) <= height:
            mode = "line"
        else:
            mode = "tiles"
        if mode == "tiles":
            return self._tiles(ids, sel)
        for wid in ids:
            w = st.workers[wid]
            state = worker_state(w, st.timeout)
            mark = "▸ " if wid == sel else "  "
            t.append(mark, style=f"bold {FG}")
            t.append("● ", style=STATE_COLORS[state])
            t.append(f"{fit(wid, 9):<9} ", style=f"bold {worker_color(wid)}" + (" reverse" if wid == sel and self.has_focus else ""))
            t.append(f"{state:<6}", style=STATE_COLORS[state])
            if w.registered:
                t.append(f"{w.chunks:>3} ch", style=FG if state in (UP, LATE) else DIM)
                if w.heartbeat_age is not None:
                    t.append(f" ♥{w.heartbeat_age:>4.1f}s", style=DIM if state == UP else STATE_COLORS[state])
            t.append("\n")
            if mode == "full":
                t.append("    ")
                if not w.registered:
                    t.append("not registered (named in the metadata)", style=DIM)
                elif state in (DOWN, SILENT):
                    t.append(f"no heartbeat for {w.heartbeat_age:.0f}s", style=STATE_COLORS[state])
                else:
                    color = GREEN if w.usage < .7 else (YELLOW if w.usage < .9 else RED)
                    t.append_text(bar(w.usage, 10, color))
                    t.append(f" {w.usage * 100:3.0f}% {human_size(w.used)}/{human_size(w.capacity)}", style=DIM)
                if w.orphans:
                    t.append(f" ◌{w.orphans}", style=YELLOW)
                t.append("\n")
        return t

    def _tiles(self, ids, sel) -> Text:
        st = self.app.state
        t = Text()
        per_row = max(1, (self.content_size.width or 30) // 6)
        for i, wid in enumerate(ids):
            state = worker_state(st.workers[wid], st.timeout)
            m = re.search(r"(\d+)$", wid)
            num = m.group(1) if m else wid[:3]
            style = STATE_COLORS[state] + (" reverse" if wid == sel else "")
            t.append(f"●{num:<4}", style=style)
            t.append(" ")
            if (i + 1) % per_row == 0:
                t.append("\n")
        t.append(f"\n\n▸ {sel}  ", style=f"bold {worker_color(sel)}")
        w = st.workers[sel]
        t.append(f"{worker_state(w, st.timeout)} · {w.chunks} ch · {w.usage * 100:.0f}%", style=FG)
        return t


# =================================================================== chunk map
class ChunkMapPane(Pane):
    TITLE = "CHUNK MAP"
    BINDINGS = [Binding("up", "obj(-1)", show=False), Binding("down", "obj(1)", show=False),
                Binding("left", "chunk(-1)", show=False), Binding("right", "chunk(1)", show=False),
                Binding("enter", "details", "Details", show=False)]

    def action_obj(self, step: int) -> None:
        self.app.select_object(step)

    def action_chunk(self, step: int) -> None:
        self.app.select_chunk(step)

    def action_details(self) -> None:
        self.app.show_chunk_details()

    def _cell(self, oid: str, chunk: dict, rf: int, selected: bool, focus_worker: str) -> Text:
        st = self.app.state
        status = st.chunk_status(oid, chunk)
        bracket = STATUS_COLORS[status]
        faded = focus_worker is not None and focus_worker not in chunk["replicas"]
        if selected:
            bracket = f"bold #000000 on {bracket}"
        t = Text("[", style=bracket)
        for node in chunk["replicas"]:
            state = st.copy_state(oid, chunk["index"], node)
            if faded and state == "ok":         # dimmed, but a damaged copy still shows
                t.append("█", style=FAINT)
            elif state == "ok":
                t.append("█", style=worker_color(node))
            elif state == "down":
                t.append("░", style=worker_color(node) + " dim")
            elif state == "corrupt":
                t.append("✖", style=f"bold {PINK}")
            else:
                t.append("?", style=f"bold {YELLOW}")
        t.append(" " * max(0, rf - len(chunk["replicas"])))
        t.append("]", style=bracket)
        return t

    def render(self) -> Text:
        app, st = self.app, self.app.state
        width = max(10, self.content_size.width or 60)
        height = max(3, self.content_size.height or 20)
        out = Text(no_wrap=True, overflow="crop")
        sel_oid, sel_idx = app.selected_object(), app.chunk_index
        focus_worker = app.selected_worker() if app.focused_pane() == "workers" else None
        self.border_title = f"CHUNK MAP  {len(st.objects)} objects" + (
            f"  ·  lit: chunks on {focus_worker}" if focus_worker and st.objects else "")
        if not st.objects:
            out.append("nothing stored yet\n\n", style=DIM)
            out.append("press u to upload, or\n  docker compose run --rm client1 upload /files/hello.txt", style=DIM)
            return out
        lines, starts = [], {}
        for oid, meta in st.objects.items():
            starts[oid] = len(lines)
            head = Text(no_wrap=True)
            is_sel = oid == sel_oid
            head.append("▸ " if is_sel else "  ", style=f"bold {FG}")
            size = human_size(meta["size"])
            head.append(f"{fit(meta['name'], width - len(size) - 4):<{width - len(size) - 3}}",
                        style=("bold " if is_sel else "") + FG)
            head.append(size, style=DIM)
            lines.append(head)
            rf = max([meta["copies_per_chunk"]] + [len(c["replicas"]) for c in meta["chunks"]])
            per_line = max(1, (width - 4) // (rf + 3))
            row = Text("    ")
            for i, chunk in enumerate(meta["chunks"]):
                if i and i % per_line == 0:
                    lines.append(row)
                    row = Text("    ")
                row.append_text(self._cell(oid, chunk, rf, is_sel and i == sel_idx and self.has_focus, focus_worker))
                if is_sel and i == sel_idx and not self.has_focus:
                    row.append("◂", style=FG)
                else:
                    row.append(" ")
            lines.append(row)
        room = height - 2
        top = 0
        if sel_oid in starts and starts[sel_oid] + 2 > room:
            top = min(starts[sel_oid], max(0, len(lines) - room))
        for line in lines[top: top + room]:
            out.append_text(line)
            out.append("\n")
        out.append("\n" * max(0, room - len(lines[top: top + room])))
        c = st.counts()
        total = max(1, sum(c.values()))
        bw = max(4, width - 2)
        g, y = round(c["healthy"] / total * bw), round(c["under_replicated"] / total * bw)
        out.append("█" * g, style=GREEN)
        out.append("█" * y, style=YELLOW)
        out.append("█" * max(0, bw - g - y) if c["unavailable"] else "", style=RED)
        out.append("\n")
        out.append(f"● {c['healthy']} healthy  ", style=GREEN)
        out.append(f"● {c['under_replicated']} under-replicated  ", style=YELLOW)
        out.append(f"● {c['unavailable']} unavailable", style=RED)
        return out


# ====================================================================== object
class ObjectPane(Pane):
    TITLE = "OBJECT"
    can_focus = False

    def render(self) -> Text:
        app, st = self.app, self.app.state
        t = Text(no_wrap=True, overflow="ellipsis")
        oid = app.selected_object()
        if oid is None:
            t.append("no object selected", style=DIM)
            return t
        meta = st.objects[oid]

        def row(key, value, style=FG):
            t.append(f"{key:<9}", style=DIM)
            t.append(f"{value}\n", style=style)

        row("name", meta["name"], f"bold {FG}")
        row("id", oid)
        row("size", f"{human_size(meta['size'])} · {meta['chunk_count']} chunk(s) x{meta['copies_per_chunk']}")
        row("type", meta.get("content_type"))
        row("client", meta.get("client_id") or "-")
        row("path", meta.get("source_path") or "-")
        row("created", local_time(meta.get("created_at")))
        if meta.get("modified_at") != meta.get("created_at"):
            row("modified", local_time(meta.get("modified_at")))
        custom = meta.get("custom") or {}
        row("custom", ", ".join(f"{k}={v}" for k, v in custom.items()) or "-")
        row("checksum", meta.get("checksum", "")[:16] + "…", DIM)
        if not meta["chunks"]:
            return t
        idx = min(app.chunk_index, len(meta["chunks"]) - 1)
        chunk = meta["chunks"][idx]
        status = st.chunk_status(oid, chunk)
        t.append("\n")
        t.append(f"chunk {idx + 1} of {len(meta['chunks'])}  ", style=f"bold {FG}")
        t.append({"healthy": "healthy", "under_replicated": "under-replicated",
                  "unavailable": "UNAVAILABLE"}[status] + "\n", style=STATUS_COLORS[status])
        t.append(f"  {chunk['chunk_id']}\n", style=DIM)
        for node in chunk["replicas"]:
            state = st.copy_state(oid, chunk["index"], node)
            t.append("  ● ", style=worker_color(node))
            t.append(f"{node:<10}", style=worker_color(node))
            t.append(state.upper() + "\n", style={"ok": GREEN, "down": RED, "corrupt": PINK}.get(state, YELLOW))
        missing = meta["copies_per_chunk"] - sum(st.copy_state(oid, chunk["index"], n) == "ok" for n in chunk["replicas"])
        if missing > 0:
            t.append(f"  {missing} good copy/copies short: r repairs\n", style=YELLOW)
        return t


# ====================================================================== spread
class SpreadPane(Pane):
    TITLE = "SPREAD"
    can_focus = False

    def render(self) -> Text:
        st = self.app.state
        ids = list(st.workers)
        pairs = st.pair_counts()
        t = Text(no_wrap=True, overflow="crop")
        if len(ids) < 2 or not pairs:
            t.append("chunks shared by each\npair of workers\n\n(nothing stored yet)", style=DIM)
            return t
        top = max(pairs.values())
        shades = [FAINT, "#005f5f", "#008787", "#00afaf", "#00d7d7", "#00ffff"]

        def shade(n):
            return shades[0] if n == 0 else shades[1 + round((n / top) * (len(shades) - 2))]

        if len(ids) <= 7:
            nums = [re.search(r"(\d+)$", w).group(1) if re.search(r"(\d+)$", w) else w[:2] for w in ids]
            t.append("    ")
            for w, n in zip(ids, nums):
                t.append(f"{n:>3}", style=worker_color(w))
            t.append("\n")
            for i, (a, na) in enumerate(zip(ids, nums)):
                t.append(f" {na:>2} ", style=worker_color(a))
                for j, b in enumerate(ids):
                    if j < i:
                        t.append("   ")
                    elif j == i:
                        t.append("  ·", style=FAINT)
                    else:
                        n = pairs.get(tuple(sorted((a, b))), 0) or pairs.get((a, b), 0)
                        t.append(f"{n:>3}", style=f"bold {shade(n)}")
                t.append("\n")
        else:
            for (a, b), n in pairs.most_common(max(3, (self.content_size.height or 8) - 2)):
                t.append(f"{fit(a, 9):>9}", style=worker_color(a))
                t.append("+", style=DIM)
                t.append(f"{fit(b, 9):<9} ", style=worker_color(b))
                t.append_text(bar(n / top, 8, shade(n)))
                t.append(f" {n}\n", style=FG)
        possible = len(ids) * (len(ids) - 1) // 2
        t.append(f"\n{len(pairs)} of {possible} pairs in use", style=DIM)
        return t
