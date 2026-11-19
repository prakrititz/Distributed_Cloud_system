"""Pop-up windows: help, details, and the y/n question."""
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Static

HELP = r"""[b #00e5ff]SCREENS[/]
  [b]1[/]  overview          [b]2[/]  flow: heartbeats, requests and repairs, animated

[b #00e5ff]MOVING AROUND[/]
  [b]Tab[/] / [b]Shift+Tab[/]   next / previous pane (the focused pane has a bright border)
  [b]↑ ↓[/]               select a worker or an object; scroll the events
  [b]← →[/]               select a chunk of the object
  [b]Enter[/]             details: what a worker really holds (orphans, missing), or a chunk's copies
  [b]Esc[/]               close a window, cancel the command line

[b #00e5ff]ACTIONS[/]
  [b]u[/]  upload (opens the command line)       [b]d[/]  delete the selected object
  [b]r[/]  repair now                            [b]x[/]  clean up orphaned copies
  [b]k[/]  kill the selected worker (docker)     [b]s[/]  start it again (docker)
  [b]+[/]  add a worker (docker compose)         [b]c[/]  damage a copy of the selected chunk (docker)
  [b]t[/]  client traffic: download a random object every 3 s
  [b]:[/]  command line                          [b]![/]  shell command, e.g. [i]!docker ps[/]
  [b]Ctrl+C[/]  stop the running shell command   [b]q[/]  quit (the system keeps running)

[b #00e5ff]COMMANDS[/] (after [b]:[/])
  upload PATH \[k=v,...]      upload a file from this computer
  upload NAME MB             upload NAME filled with MB megabytes of random bytes
  download \[ID|NAME] \[DEST]  download (default: the selected object) into the current folder
  delete \[ID|NAME]           delete
  meta \[ID|NAME] k=v,...     change custom metadata (k=null removes k)
  repair                     cleanup \[MIN_AGE]            (cleanup 0: no age limit)
  kill W   start W   scale N   corrupt \[W]   clear   help

[b #00e5ff]WHAT THE COLOURS MEAN[/]
  worker  [#00ff5f]UP[/] heartbeat in time  [#ffd700]LATE[/] overdue  [#ff8700]SILENT[/] not answering, controller has not noticed yet  [#ff3b3b]DOWN[/] no heartbeat for 6 s
  chunk   [#00ff5f][██][/] healthy  [#ffd700][█░][/] under-replicated  [#ff3b3b][░░][/] unavailable
  copy    █ good (worker's colour)  ░ worker down  [#ff00af]✖[/] corrupt  [#ffd700]?[/] missing   ◌N orphans on a worker
"""


class TextScreen(ModalScreen):
    """A window with some text. Esc, Enter or q closes it."""
    BINDINGS = [Binding("escape,enter,q", "dismiss", "Close")]

    def __init__(self, title: str, body):
        super().__init__()
        self.title_text = title
        self.body = body

    def compose(self) -> ComposeResult:
        with Vertical(id="dialog") as box:
            box.border_title = self.title_text
            box.border_subtitle = "Esc closes"
            with VerticalScroll():
                yield Static(self.body)

    def action_dismiss(self) -> None:
        self.dismiss(None)


class ConfirmScreen(ModalScreen[bool]):
    BINDINGS = [Binding("y", "answer(True)", "Yes"), Binding("n,escape", "answer(False)", "No")]

    def __init__(self, question: str):
        super().__init__()
        self.question = question

    def compose(self) -> ComposeResult:
        with Vertical(id="dialog", classes="small") as box:
            box.border_title = "CONFIRM"
            text = Text(self.question + "\n\n", style="bold #ffffff")
            text.append("y", style="bold #00ff5f")
            text.append(" yes    ", style="#bcbcbc")
            text.append("n", style="bold #ff3b3b")
            text.append(" no", style="#bcbcbc")
            yield Static(text)

    def action_answer(self, yes: bool) -> None:
        self.dismiss(yes)
