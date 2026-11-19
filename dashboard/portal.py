"""The sign-in and registration portal, shown when there is no key that works.

    I have a key   type it: the controller says who it belongs to
    I am new       register (a name of your choice, or one made up): the
                   controller creates your key and shows it ONCE; it is saved
                   in the credentials file (unless you untick "remember"), so
                   next time you go straight to your dashboard

It ends (app.exit) with a Session; __main__ then opens the admin dashboard or
the client dashboard, whichever the key is for.
"""
from dataclasses import dataclass

from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.widgets import Checkbox, Input, Static

from . import credentials
from .api import Api, ApiError
from .logo import wordmark

CYAN, GREEN, YELLOW, RED, DIM, FG = "#00e5ff", "#00ff5f", "#ffd700", "#ff3b3b", "#6c6c6c", "#e8e8e8"

OLD_CONTROLLER = ("This controller is an older version, without sign-in and registration. "
                  "Rebuild it: docker compose up --build -d")


def older_controller(e: ApiError) -> bool:
    """A 404 from Flask itself (no such route), not one of the controller's
    own answers: the controller was built before this endpoint existed."""
    return e.status == 404 and "requested URL was not found" in str(e)


@dataclass
class Session:
    """Who is signed in: the key (None when the controller has no
    authentication) and what GET /me said about it."""
    api_key: str
    me: dict
    saved_to: str = None

    @property
    def admin(self) -> bool:
        return bool(self.me.get("admin"))


def mask(key: str) -> str:
    """sk_AbCd…wxYz: enough to tell keys apart, not enough to use one."""
    if not key:
        return "-"
    return key if len(key) <= 12 else f"{key[:7]}…{key[-4:]}"


class Portal(App):
    CSS = """
    Screen { background: #000000; color: #e8e8e8; align: center middle; }
    #box { width: 84; max-width: 100%; height: auto; border: round #00e5ff; padding: 1 2;
           border-title-color: #000000; border-title-background: #00e5ff; border-title-style: bold;
           border-subtitle-color: #6c6c6c; background: #080808; }
    .label { margin-top: 1; color: #9e9e9e; text-style: bold; }
    Input { border: tall #444444; background: #000000; margin: 0; }
    Input:focus { border: tall #00e5ff; }
    Checkbox { margin-top: 1; background: #080808; border: none; }
    #status { margin-top: 1; height: auto; }
    #logo { width: 100%; height: auto; content-align: center middle; margin-bottom: 1; }
    #forms { height: auto; }
    #issued { height: auto; display: none; }
    #issued-key { margin: 1 0; padding: 0 1; background: #121212; color: #00ff5f; text-style: bold; }
    """
    BINDINGS = [Binding("escape", "quit", "Quit", priority=True)]
    TITLE = "On Store"
    ENABLE_COMMAND_PALETTE = False

    def __init__(self, api_url: str, key: str = None, notice: str = None):
        super().__init__()
        self.api_url = api_url
        self.initial_key = key or ""
        self.notice = notice
        self.session = None                 # set once a key works
        self._busy = False

    def compose(self) -> ComposeResult:
        with Vertical(id="box") as box:
            box.border_title = " ♦ ON STORE  sign in "
            yield Static(wordmark(), id="logo")
            box.border_subtitle = "Enter submits · Tab moves · Esc quits"
            with Vertical(id="forms"):
                yield Static(Text.assemble(("controller  ", DIM), (Api(self.api_url).base, FG)))
                yield Static("I HAVE AN API KEY", classes="label")
                yield Input(value=self.initial_key, password=True, placeholder="paste your key (sk_... or a fixed key)",
                            id="key")
                yield Static("I AM NEW: REGISTER", classes="label")
                yield Input(placeholder="the name you want (letters, digits . _ -), or leave empty for one made up",
                            id="name")
                yield Checkbox("Remember my key on this computer", value=True, id="remember")
            with Vertical(id="issued"):
                yield Static(id="issued-head")
                yield Static(id="issued-key")
                yield Static(id="issued-foot")
            yield Static(id="status")

    def on_mount(self) -> None:
        self.query_one("#key" if self.initial_key or not self.notice else "#name", Input).focus()
        if self.notice:
            self.status(self.notice, YELLOW)

    def status(self, text: str, color: str = DIM) -> None:
        self.query_one("#status", Static).update(Text(text, style=color))

    @property
    def remember(self) -> bool:
        return self.query_one("#remember", Checkbox).value

    # ------------------------------------------------------------ signing in
    def on_input_submitted(self, event: Input.Submitted) -> None:
        if self._busy:
            return
        if event.input.id == "key":
            key = event.value.strip()
            if not key:
                self.status("Type your key, or register below.", YELLOW)
                return
            self._busy = True
            self.status("checking the key ...")
            self.run_worker(self.sign_in(key), exclusive=True)
        elif event.input.id == "name":
            self._busy = True
            self.status("registering ...")
            self.run_worker(self.register(event.value.strip() or None), exclusive=True)

    async def sign_in(self, key: str) -> None:
        import asyncio
        try:
            me = await asyncio.to_thread(Api(self.api_url, key).me)
        except ApiError as e:
            self._busy = False
            self.status("That key is not valid here." if e.status == 401
                        else OLD_CONTROLLER if older_controller(e) else str(e), RED)
            return
        saved = credentials.save(Api(self.api_url).api_url, me["client_id"], key) if self.remember else None
        self.exit(Session(api_key=key, me=me, saved_to=saved))

    async def register(self, name) -> None:
        import asyncio
        api = Api(self.api_url)
        try:
            answer = await asyncio.to_thread(api.register, name)
            me = await asyncio.to_thread(Api(self.api_url, answer["api_key"]).me)
        except ApiError as e:
            self._busy = False
            if older_controller(e):
                why = OLD_CONTROLLER
            else:
                why = {404: "Registration is off on this controller: ask the admin for a key.",
                       409: f"The name '{name}' is taken: choose another (or leave it empty)."}.get(e.status, str(e))
            self.status(why, RED)
            return
        key = answer["api_key"]
        saved = credentials.save(api.api_url, answer["client_id"], key) if self.remember else None
        self.session = Session(api_key=key, me=me, saved_to=saved)
        self.show_issued(answer["client_id"], key, saved)

    # ------------------------------------------------------------ the new key
    def show_issued(self, client_id: str, key: str, saved: str) -> None:
        self.query_one("#forms").display = False
        self.query_one("#issued").display = True
        box = self.query_one("#box")
        box.border_title = " ♦ ON STORE  registered "
        box.border_subtitle = "c copies the key · Enter opens your dashboard"
        self.query_one("#issued-head", Static).update(Text.assemble(
            ("Welcome, ", FG), (client_id, f"bold {CYAN}"), ("!\n\n", FG),
            ("Your API key. It is shown only this once: the controller keeps only a hash of it, "
             "so a lost key cannot be recovered.", FG)))
        self.query_one("#issued-key", Static).update(key)
        foot = Text()
        if saved:
            foot.append("Saved to ", DIM)
            foot.append(saved, FG)
            foot.append(".\nThe dashboard and client.py use it from now on.\n", DIM)
        else:
            foot.append("NOT saved on this computer: copy it now.\n", f"bold {YELLOW}")
        foot.append("Send it as ", DIM)
        foot.append("Authorization: Bearer <key>", FG)
        foot.append(" (curl), or set ", DIM)
        foot.append("API_KEY", FG)
        foot.append(" for client.py.", DIM)
        self.query_one("#issued-foot", Static).update(foot)
        self.status("")
        self.set_focus(None)

    def on_key(self, event) -> None:
        if self.session is None:
            return
        if event.key == "enter":
            self.exit(self.session)
        elif event.key == "c":
            self.copy_to_clipboard(self.session.api_key)
            self.status("Copied to the clipboard (if your terminal allows it).", GREEN)
