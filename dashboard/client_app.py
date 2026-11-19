"""The client dashboard: what one client sees.

Only what the client's own key gives it: its own files, the files other
clients shared with it, and its own history (uploads, downloads, deletes,
metadata changes, shares, from GET /api/v1/me/activity). Nothing about the
workers, chunk placement, heartbeats or repair: that is the admin's dashboard.

One loop polls the controller every POLL_EVERY seconds (GET /me, the object
list, the activity) and the selected file's metadata; the panes draw
themselves from what came back.
"""
import asyncio
import os
import shlex
import subprocess
from datetime import datetime

from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Grid, Horizontal
from textual.screen import ModalScreen
from textual.widgets import Footer, Input, RichLog, Static

from . import credentials
from .logo import ACE_OF_HEARTS, HEARTS, Deck, badge, deal_text
from .api import Api, ApiError
from .app import CommandInput, parse_pairs
from .docker_ctl import run_shell
from .model import human_size
from .portal import mask
from .screens import ConfirmScreen, TextScreen
from .widgets import DIM, FG, GREEN, KIND_COLORS, PINK, RED, YELLOW, Pane, fit, local_time

POLL_EVERY = 2.0
ACTIVITY_SHOWN = 200

ACTION_STYLE = {          # how each kind of activity is shown
    "upload": ("↑", GREEN, "uploaded"),
    "download": ("↓", "#87d7ff", "downloaded"),
    "delete": ("✕", RED, "deleted"),
    "metadata": ("✎", YELLOW, "changed"),
    "share": ("⇄", "#d787ff", "shared"),
    "register": ("★", "#00e5ff", "registered"),
}

HELP = r"""[b #00e5ff]YOUR DASHBOARD[/]
  Your files, the files other clients shared with you, and everything you did,
  from any program (this dashboard, client.py, curl). Nothing else is visible to you.

[b #00e5ff]MOVING AROUND[/]
  [b]Tab[/] / [b]Shift+Tab[/]   next / previous pane     [b]↑ ↓[/]  select a file
  [b]Enter[/]             all the details of the selected file
  [b]Esc[/]               close a window, cancel the command line

[b #00e5ff]ACTIONS[/]
  [b]u[/]  upload a file             [b]g[/]  download the selected file
  [b]d[/]  delete the selected file  [b]m[/]  change its custom metadata
  [b]s[/]  share it with clients     [b]a[/]  your account and API key
  [b]r[/]  refresh now               [b]:[/]  command line   [b]![/]  shell command (Ctrl+C stops it)
  [b]?[/]  this help                 [b]q[/]  quit

[b #00e5ff]COMMANDS[/] (after [b]:[/]; FILE is a name, an id or the start of an id; default: the selected file)
  upload PATH \[k=v,...]         upload a file from this computer, with optional custom metadata
  download \[FILE] \[DEST]         download into the current folder (checked against its checksum)
  delete \[FILE]                 delete one of your files
  meta \[FILE] k=v,...           add or change custom metadata (k=null removes k)
  share \[FILE] client2,client3  let those clients read it      share \[FILE] -   stop sharing
  info \[FILE]   account   key (shows your API key)   logout (forget the saved key)   clear   help
"""


class FileList(Pane):
    """A list of files (mine, or shared with me) to select from."""
    BINDINGS = [Binding("up", "move(-1)", show=False), Binding("down", "move(1)", show=False),
                Binding("enter", "details", "Details", show=False)]
    KIND = "mine"

    def on_focus(self) -> None:
        self.app.active = self.KIND
        self.app.redraw()

    def action_move(self, step: int) -> None:
        self.app.move(self.KIND, step)

    def action_details(self) -> None:
        self.app.show_details()

    def render(self) -> Text:
        app = self.app
        files = app.files(self.KIND)
        total = sum(o["size"] for o in files)
        self.border_title = (f"MY FILES  {len(files)} · {human_size(total)}" if self.KIND == "mine"
                             else f"SHARED WITH ME  {len(files)}")
        t = Text(no_wrap=True, overflow="ellipsis")
        if not files:
            t.append("nothing yet: press u to upload a file" if self.KIND == "mine"
                     else "no other client has shared a file with you", style=DIM)
            return t
        width = max(30, self.content_size.width or 60)
        sel = app.selected(self.KIND)
        height = max(1, self.content_size.height or 10)
        ids = [o["object_id"] for o in files]
        top = max(0, min(ids.index(sel) - height // 2, len(ids) - height)) if sel in ids else 0
        downloaded = app.downloaded_ids()
        name_w = max(10, width - 40)
        for o in files[top: top + height]:
            is_sel = o["object_id"] == sel
            active = is_sel and app.active == self.KIND
            t.append("▸ " if is_sel else "  ", style=f"bold {FG}")
            t.append(f"{fit(o['name'], name_w):<{name_w}}", style=(f"bold {FG} reverse" if active and self.has_focus
                                                                    else f"bold {FG}" if is_sel else FG))
            t.append("↓" if o["object_id"] in downloaded else " ", style=KIND_COLORS["info"])
            t.append(f" {human_size(o['size']):>8}", style=FG)
            if self.KIND == "mine":
                t.append(f"  {fit(o.get('content_type') or '', 14):<14}", style=DIM)
                t.append(f" {local_time(o.get('created_at'))[5:16]}", style=DIM)
            else:
                t.append(f"  from {fit(o.get('owner') or '?', 18)}", style="#d787ff")
            t.append("\n")
        return t


class SharedList(FileList):
    KIND = "shared"


class DetailsPane(Pane):
    """The selected file: what the system keeps about it and its custom metadata."""
    TITLE = "FILE"
    can_focus = False

    def render(self) -> Text:
        app = self.app
        t = Text(no_wrap=True, overflow="ellipsis")
        oid = app.selected(app.active)
        meta = app.details.get(oid)
        if oid is None:
            t.append("no file selected", style=DIM)
            return t
        if meta is None:
            t.append("loading ...", style=DIM)
            return t

        def row(key, value, style=FG):
            t.append(f"{key:<10}", style=DIM)
            t.append(f"{value}\n", style=style)

        mine = meta.get("client_id") == app.client_id
        self.border_title = "MY FILE" if mine else f"SHARED BY {meta.get('client_id')}"
        row("name", meta["name"], f"bold {FG}")
        row("id", meta["object_id"])
        row("size", f"{human_size(meta['size'])} ({meta['size']:,} bytes)")
        row("type", meta.get("content_type"))
        row("stored", f"{meta['chunk_count']} chunk(s), {meta['copies_per_chunk']} copies of each")
        row("created", local_time(meta.get("created_at")))
        row("modified", local_time(meta.get("modified_at")))
        row("checksum", (meta.get("checksum") or "")[:20] + "…", DIM)
        if not mine:
            row("owner", meta.get("client_id") or "-", "#d787ff")
        if meta.get("source_path"):
            row("from", meta["source_path"], DIM)
        if "readers" in meta:
            row("shared", ", ".join(meta["readers"]) or "with nobody", "#d787ff" if meta["readers"] else DIM)
        custom = meta.get("custom") or {}
        t.append("\ncustom metadata\n", style=f"bold {FG}")
        if not custom:
            t.append("  none   (m adds some)\n" if mine else "  none\n", style=DIM)
        for k, v in custom.items():
            t.append(f"  {k}", style=YELLOW)
            t.append(f" = {v}\n", style=FG)
        mine_here = [e for e in app.activity if e.get("object_id") == oid]
        downloads = [e for e in mine_here if e["action"] == "download"]
        t.append("\nyou ", style=DIM)
        if downloads:
            t.append(f"downloaded it {len(downloads)} time(s), last {local_time(downloads[0]['at'])[5:]}\n", style=FG)
        else:
            t.append("have not downloaded it   (g downloads)\n", style=DIM)
        return t


class ActivityPane(Pane):
    TITLE = "MY ACTIVITY"
    can_focus = False

    def render(self) -> Text:
        app = self.app
        t = Text(no_wrap=True, overflow="ellipsis")
        self.border_title = f"MY ACTIVITY  {len(app.activity)}"
        if not app.activity:
            t.append("nothing yet: your uploads, downloads, changes and shares appear here", style=DIM)
            return t
        width = max(30, self.content_size.width or 60)
        for e in app.activity[: max(1, self.content_size.height or 10)]:
            icon, color, verb = ACTION_STYLE.get(e["action"], ("·", FG, e["action"]))
            t.append(f"{local_time(e['at'])[5:]}  ", style=DIM)
            t.append(f"{icon} {verb:<11}", style=color)
            name = e.get("name") or ""
            t.append(fit(name, max(8, width - 44)), style=FG)
            if e.get("size") is not None and e["action"] in ("upload", "download"):
                t.append(f"  {human_size(e['size'])}", style=DIM)
            if e.get("detail") and e["action"] != "register":
                t.append(f"  {e['detail']}", style=DIM)
            t.append("\n")
        return t


class AccountScreen(TextScreen):
    """Who I am, my limits, and my key (hidden until v)."""
    BINDINGS = [Binding("v", "reveal", "Show key"), Binding("c", "copy", "Copy key")]

    def __init__(self, dash):
        super().__init__("YOUR ACCOUNT", Text())
        self.dash = dash
        self.shown = False
        self.body = self.body_text()

    def body_text(self) -> Text:
        d, me = self.dash, self.dash.me
        t = Text()

        def row(key, value, style=FG):
            t.append(f"{key:<16}", style=DIM)
            t.append(f"{value}\n", style=style)

        row("client id", me.get("client_id"), f"bold {GREEN}")
        row("account", "registered itself" if me.get("registered") else "a fixed client (from the admin)")
        quota = me.get("quota_bytes")
        row("storing", f"{human_size(me.get('stored_bytes', 0))}"
                       + (f" of {human_size(quota)} ({me.get('stored_bytes', 0) / quota * 100:.0f}%)" if quota
                          else "   (no quota)"))
        row("transfers", f"{me.get('transfers_running') or 0} running, at most {me.get('transfers_allowed')} at once")
        rate = me.get("requests_per_second")
        row("requests", f"at most {rate:g} per second" if rate else "no limit")
        t.append("\n")
        row("API key", d.api.api_key if self.shown else mask(d.api.api_key), f"bold {YELLOW}")
        saved_id, saved_key = credentials.load(d.api.api_url)
        row("saved", credentials.path() if saved_key == d.api.api_key else "not on this computer", DIM)
        t.append("\nv shows the key · c copies it · Esc closes\n", style=DIM)
        t.append("Use it with client.py (API_KEY=...) or curl (-H \"Authorization: Bearer <key>\").", style=DIM)
        return t

    def action_reveal(self) -> None:
        self.shown = not self.shown
        self.query_one(Static).update(self.body_text())

    def action_copy(self) -> None:
        self.app.copy_to_clipboard(self.dash.api.api_key)
        self.app.notify("API key copied (if your terminal allows it)", timeout=3)


class ClientDashboard(App):
    CSS_PATH = "onstore.tcss"
    TITLE = "On Store"
    ENABLE_COMMAND_PALETTE = False
    BINDINGS = [
        Binding("u", "upload", "Upload"),
        Binding("g", "download", "Download"),
        Binding("d", "delete", "Delete"),
        Binding("m", "meta", "Metadata"),
        Binding("s", "share", "Share"),
        Binding("a", "account", "Account"),
        Binding("r", "refresh", "Refresh"),
        Binding("colon", "command", ":Cmd"),
        Binding("exclamation_mark", "shell", "!Shell"),
        Binding("question_mark", "help", "?Help"),
        Binding("q", "quit", "Quit"),
        Binding("ctrl+c", "stop_shell", "Stop command", show=False, priority=True),
    ]

    def __init__(self, api_url: str = "http://127.0.0.1:5000", api_key: str = None, me: dict = None,
                 poll: bool = True):
        super().__init__()
        self.api = Api(api_url, api_key)
        self.me = me or {}
        self.poll = poll
        self.controller_ok = bool(me)
        self.error = ""
        self.listing = []              # GET /objects: mine and shared with me, oldest first
        self.details = {}              # object_id -> full metadata (fetched for the selected file)
        self.activity = []             # newest first
        self.sel = {"mine": None, "shared": None}
        self.active = "mine"
        self.shell_proc = None
        self._last_focus = None
        self._loaded = False           # the first answer is not news
        self.deck = Deck(top=ACE_OF_HEARTS)

    @property
    def client_id(self) -> str:
        return self.me.get("client_id")

    # ================================================================ layout
    def compose(self) -> ComposeResult:
        yield Static(id="topbar")
        with Grid(id="client"):
            yield FileList(id="mine")
            yield DetailsPane(id="details")
            yield SharedList(id="shared")
            yield ActivityPane(id="activity")
            yield RichLog(id="output", classes="pane", wrap=True, markup=False, highlight=False, max_lines=2000)
        with Horizontal(id="cmdbar"):
            yield Static("", id="prompt")
            yield CommandInput(id="cmd")
        yield Footer()

    def on_mount(self) -> None:
        self.out = self.query_one("#output", RichLog)
        self.out.border_title = "OUTPUT"
        self.cmdbar = self.query_one("#cmdbar")
        self.cmd_input = self.query_one("#cmd", CommandInput)
        self.prompt = self.query_one("#prompt", Static)
        self.query_one("#mine").focus()
        self.say("info", f"signed in as {self.client_id} at {self.api.base}   (? help, a account)")
        if self.poll:
            self.run_worker(self._poll_forever(), group="poll")
        self.set_interval(1.0, self.redraw)
        self.redraw()

    def redraw(self) -> None:
        self.query_one("#topbar", Static).update(self.topbar())
        for pane in ("#mine", "#shared", "#details", "#activity"):
            self.query_one(pane).refresh()

    def topbar(self) -> Text:
        t = Text()
        t.append_text(badge(HEARTS))
        t.append(" CLIENT ", style="bold #000000 on #00ff5f")
        sep = Text(" │ ", style=DIM)
        t.append(f" {self.client_id}", style=f"bold {FG}")
        t.append_text(sep)
        if not self.controller_ok:
            t.append("● CONTROLLER UNREACHABLE", style=f"bold {RED}")
            t.append(f"  {fit(self.error, 60)}", style=DIM)
            return t
        mine, shared = self.files("mine"), self.files("shared")
        stored, quota = self.me.get("stored_bytes", 0), self.me.get("quota_bytes")
        t.append(f"{len(mine)} file(s) · {human_size(stored)}", style=FG)
        if quota:
            frac = stored / quota
            t.append(f" of {human_size(quota)}", style=RED if frac > .9 else YELLOW if frac > .7 else FG)
        t.append_text(sep)
        t.append(f"{len(shared)} shared with me", style=FG)
        t.append_text(sep)
        t.append("● connected", style=GREEN)
        t.append(f" {self.api.base.replace('http://', '')}", style=DIM)
        if self.shell_proc is not None:
            t.append_text(sep)
            t.append("! running", style=f"bold {YELLOW}")
        t.append_text(sep)
        t.append(datetime.now().strftime("%H:%M:%S"), style=DIM)
        return t

    def say(self, kind: str, text: str) -> None:
        line = Text(datetime.now().strftime("%H:%M:%S "), style=DIM)
        line.append(text, style=KIND_COLORS.get(kind, FG))
        self.out.write(line)

    # ================================================================ data
    def files(self, kind: str) -> list:
        if kind == "mine":
            return [o for o in self.listing if o.get("owner") == self.client_id]
        return [o for o in self.listing if o.get("owner") != self.client_id]

    def downloaded_ids(self) -> set:
        return {e.get("object_id") for e in self.activity if e["action"] == "download"}

    def selected(self, kind: str):
        ids = [o["object_id"] for o in self.files(kind)]
        if not ids:
            return None
        if self.sel[kind] not in ids:
            self.sel[kind] = ids[-1]                  # the newest
        return self.sel[kind]

    def move(self, kind: str, step: int) -> None:
        ids = [o["object_id"] for o in self.files(kind)]
        if ids:
            i = ids.index(self.selected(kind))
            self.sel[kind] = ids[(i + step) % len(ids)]
            self.active = kind
            self.redraw()
            self.run_worker(self.fetch_details(), group="details", exclusive=True)

    def resolve(self, ref: str = None) -> str:
        """A file from its id, its name, or the start of its id; default: the selected one."""
        if ref is None:
            oid = self.selected(self.active)
            if oid is None:
                raise ValueError("no file selected")
            return oid
        ids = [o["object_id"] for o in self.listing]
        if ref in ids:
            return ref
        by_name = [o["object_id"] for o in self.listing if o["name"] == ref]
        if by_name:
            return by_name[-1]
        prefix = [oid for oid in ids if oid.startswith(ref)]
        if len(prefix) == 1:
            return prefix[0]
        raise ValueError(f"no file '{ref}'" if not prefix else f"'{ref}' matches several files")

    def entry(self, oid: str) -> dict:
        return next((o for o in self.listing if o["object_id"] == oid), {"object_id": oid, "name": oid})

    # ================================================================ polling
    async def _poll_forever(self) -> None:
        while True:
            await self.refresh_now()
            await asyncio.sleep(POLL_EVERY)

    async def refresh_now(self) -> None:
        try:
            me, listing, activity = await asyncio.gather(asyncio.to_thread(self.api.me),
                                                         asyncio.to_thread(self.api.objects),
                                                         asyncio.to_thread(self.api.activity, ACTIVITY_SHOWN))
        except ApiError as e:
            if self.controller_ok:
                self.say("bad", f"controller unreachable: {e}" if e.status is None else str(e))
            self.controller_ok, self.error = False, str(e)
            self.redraw()
            return
        except Exception as e:                        # never let the poll loop die
            self.say("bad", f"dashboard error: {type(e).__name__}: {e}")
            return
        if not self.controller_ok and self.error:
            self.say("good", "controller reachable again")
        self.controller_ok, self.error = True, ""
        self._shared_news(listing)
        self.me, self.listing, self.activity = me, listing, activity
        live = {o["object_id"] for o in listing}
        self.details = {k: v for k, v in self.details.items() if k in live}
        await self.fetch_details()
        self.redraw()

    def _shared_news(self, listing: list) -> None:
        """Say so when another client shares a file with me, or stops."""
        if not self._loaded:
            self._loaded = True                       # the first poll: nothing is news yet
            return
        before = {o["object_id"] for o in self.files("shared")}
        now = {o["object_id"]: o for o in listing if o.get("owner") != self.client_id}
        for oid in now.keys() - before:
            self.say("info", f"{now[oid].get('owner')} shared {now[oid]['name']} with you")
        for oid in before - now.keys():
            self.say("warn", f"{self.entry(oid)['name']} is no longer shared with you")

    async def fetch_details(self) -> None:
        oid = self.selected(self.active)
        if oid is None:
            return
        try:
            self.details[oid] = await asyncio.to_thread(self.api.metadata, oid)
        except ApiError:
            pass                                      # deleted or unshared meanwhile: the next poll drops it
        self.redraw()

    # ================================================================ actions
    def check_action(self, action: str, parameters) -> bool:
        if isinstance(self.screen, ModalScreen) and action not in ("stop_shell",):
            return False
        return True

    def action_help(self) -> None:
        self.push_screen(TextScreen("ON STORE  HELP", Text.from_markup(HELP)))

    def action_account(self) -> None:
        self.push_screen(AccountScreen(self))

    def action_refresh(self) -> None:
        self.run_worker(self.refresh_now(), group="refresh", exclusive=True)

    def action_command(self) -> None:
        self.open_command_line("cmd")

    def action_shell(self) -> None:
        self.open_command_line("shell")

    def action_upload(self) -> None:
        self.open_command_line("cmd", "upload ")

    def action_meta(self) -> None:
        self.open_command_line("cmd", "meta ")

    def action_share(self) -> None:
        self.open_command_line("cmd", "share ")

    def action_download(self) -> None:
        self.run_worker(self.run_command("download"), group="command")

    def action_delete(self) -> None:
        oid = self.selected(self.active)
        if oid is None:
            self.say("warn", "nothing to delete")
            return
        o = self.entry(oid)
        if o.get("owner") != self.client_id:
            self.say("bad", f"{o['name']} belongs to {o.get('owner')}: only its owner may delete it")
            return

        def answer(yes):
            if yes:
                self.run_worker(self.do_delete(oid), group="command")
        self.push_screen(ConfirmScreen(f"Delete {o['name']} ({human_size(o['size'])})?"), answer)

    def show_details(self) -> None:
        oid = self.selected(self.active)
        meta = self.details.get(oid)
        if meta is None:
            return
        t = Text()
        for key in ("name", "object_id", "size", "content_type", "created_at", "modified_at", "checksum",
                    "source_path", "client_id", "chunk_count", "copies_per_chunk"):
            t.append(f"{key:<18}", style=DIM)
            t.append(f"{meta.get(key)}\n", style=FG)
        if "readers" in meta:
            t.append(f"{'readers':<18}", style=DIM)
            t.append(f"{', '.join(meta['readers']) or '-'}\n", style=FG)
        t.append("\ncustom metadata\n", style=f"bold {FG}")
        for k, v in (meta.get("custom") or {}).items():
            t.append(f"  {k}", style=YELLOW)
            t.append(f" = {v}\n", style=FG)
        if not meta.get("custom"):
            t.append("  none\n", style=DIM)
        t.append("\nyour history with this file\n", style=f"bold {FG}")
        history = [e for e in self.activity if e.get("object_id") == oid]
        for e in history:
            icon, color, verb = ACTION_STYLE.get(e["action"], ("·", FG, e["action"]))
            t.append(f"  {local_time(e['at'])}  ", style=DIM)
            t.append(f"{icon} {verb}", style=color)
            t.append(f"  {e.get('detail') or ''}\n", style=DIM)
        if not history:
            t.append("  nothing yet\n", style=DIM)
        self.push_screen(TextScreen(meta["name"], t))

    def action_stop_shell(self) -> None:
        proc = self.shell_proc
        if proc is None:
            self.notify("No command running. q quits.", timeout=2)
            return
        if os.name == "nt":
            subprocess.run(["taskkill", "/T", "/F", "/PID", str(proc.pid)], capture_output=True)
        else:
            proc.kill()
        self.say("warn", "^C: command stopped")

    # ================================================================ command line
    def open_command_line(self, mode: str, text: str = "") -> None:
        focused = self.screen.focused
        self._last_focus = focused if focused is not self.cmd_input else self._last_focus
        self.cmdbar.display = True
        self.prompt.update(Text(f" {self.client_id} › " if mode == "cmd" else " shell $ ",
                                style="bold #000000 on " + ("#00ff5f" if mode == "cmd" else "#ffd700")))
        self.cmd_input.placeholder = ("upload PATH [k=v] | download | delete | meta k=v | share c2,c3 | info | "
                                      "account | key | help" if mode == "cmd" else
                                      "any shell command, e.g. dir   (Ctrl+C stops it)")
        self.cmd_input.start(mode, text)

    def close_command_line(self) -> None:
        self.cmdbar.display = False
        self.cmd_input.value = ""
        (self._last_focus or self.query_one("#mine")).focus()

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

    async def run_command(self, text: str) -> None:
        self.say("cmd", f"› {text}")
        try:
            parts = [p.strip('"\'') for p in shlex.split(text, posix=False)]
        except ValueError as e:
            self.say("bad", f"cannot read that command: {e}")
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
                oid = self.resolve(args[0] if args else None)
                if self.entry(oid).get("owner") != self.client_id:
                    raise ValueError(f"{self.entry(oid)['name']} is not yours: only its owner may delete it")
                await self.do_delete(oid)
            elif cmd == "meta":
                if not args:
                    raise ValueError("meta [FILE] key=value,...")
                ref, pairs = (None, args) if "=" in args[0] else (args[0], args[1:])
                oid = self.resolve(ref)
                meta = await asyncio.to_thread(self.api.update_metadata, oid, parse_pairs(" ".join(pairs)))
                self.details[oid] = meta
                self.say("good", f"{meta['name']}: custom metadata now "
                                 + (", ".join(f"{k}={v}" for k, v in meta["custom"].items()) or "empty"))
                await self.refresh_now()
            elif cmd == "share":
                if not args:
                    raise ValueError("share [FILE] client2,client3   (share [FILE] - stops sharing)")
                ref, who = (None, args[0]) if len(args) == 1 else (args[0], args[1])
                oid = self.resolve(ref)
                readers = [] if who.strip() == "-" else [r.strip() for r in who.split(",") if r.strip()]
                shared = await asyncio.to_thread(self.api.share, oid, readers)
                self.say("good", f"{self.entry(oid)['name']}: readable by "
                                 + (", ".join(shared["readers"]) or "nobody but you"))
                await self.refresh_now()
            elif cmd == "info":
                if args:
                    oid = self.resolve(args[0])
                    self.sel[self.active] = oid
                    await self.fetch_details()
                self.show_details()
            elif cmd == "account":
                self.action_account()
            elif cmd == "key":
                self.say("warn", f"your API key: {self.api.api_key}   (keep it secret)")
            elif cmd == "logout":
                gone = credentials.forget(self.api.api_url)
                self.say("info", "the saved key is forgotten on this computer" if gone else "no key was saved here")
                self.exit("logout")
            elif cmd == "clear":
                self.out.clear()
            elif cmd == "deal":                   # not in the help: see logo.py
                self.out.write(deal_text(self.deck))
            else:
                self.say("bad", f"unknown command '{cmd}' (help lists them)")
        except (ValueError, OSError, ApiError) as e:
            self.say("bad", str(e))

    async def do_upload(self, args) -> None:
        if not args:
            raise ValueError("upload PATH [key=value,...]")
        for n in range(len(args), 0, -1):            # a path with spaces may come unquoted
            if os.path.isfile(os.path.expanduser(" ".join(args[:n]))):
                args = [" ".join(args[:n])] + args[n:]
                break
        path = os.path.expanduser(args[0])
        if not os.path.isfile(path):
            raise ValueError(f"no file '{args[0]}'")
        custom = {k: v for k, v in parse_pairs(" ".join(args[1:])).items() if v is not None} if len(args) > 1 else {}
        self.say("info", f"uploading {os.path.basename(path)} ({human_size(os.path.getsize(path))}) ...")
        meta = await asyncio.to_thread(self.api.upload_file, path, custom)
        self.say("good", f"uploaded {meta['name']}: {human_size(meta['size'])}, {meta['chunk_count']} chunk(s)"
                         f"   id {meta['object_id']}")
        self.details[meta["object_id"]] = meta
        self.sel["mine"], self.active = meta["object_id"], "mine"
        await self.refresh_now()

    async def do_download(self, oid: str, dest: str = None) -> None:
        o = self.entry(oid)
        if dest is None:
            base, ext = os.path.splitext(os.path.basename(o["name"]) or oid)
            dest, n = base + ext, 1
            while os.path.exists(dest):
                dest, n = f"{base} ({n}){ext}", n + 1
        elif os.path.exists(dest):
            raise ValueError(f"{dest} already exists")
        self.say("info", f"downloading {o['name']} ...")
        size = await asyncio.to_thread(self.api.download_to, oid, dest)
        self.say("good", f"downloaded {o['name']} ({human_size(size)}, checksum ok) -> {os.path.abspath(dest)}")
        await self.refresh_now()

    async def do_delete(self, oid: str) -> None:
        o = self.entry(oid)
        await asyncio.to_thread(self.api.delete, oid)
        self.say("good", f"deleted {o['name']}")
        await self.refresh_now()

    async def run_shell(self, command: str) -> None:
        if self.shell_proc is not None:
            self.say("warn", "a command is still running (Ctrl+C stops it)")
            return
        self.say("cmd", f"$ {command}")

        def started(proc):
            self.shell_proc = proc
        try:
            code = await run_shell(command, lambda line: self.say("out", line), started)
            self.say("good" if code == 0 else "warn", f"exit code {code}")
        except Exception as e:
            self.say("bad", f"{type(e).__name__}: {e}")
        finally:
            self.shell_proc = None
