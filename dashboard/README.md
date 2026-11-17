# On Store: terminal dashboard

A live terminal dashboard for the object store. It is a separate program: it reads the controller's REST API, uses the `docker` command for kill, start, add-worker and corrupt, and imports nothing from the rest of the project.

```powershell
pip install -r dashboard/requirements.txt     # once: textual + requests
docker compose up --build -d                  # the system (from the project folder)
python -m dashboard                           # the dashboard (from the project folder)
python -m dashboard --flow                    # ... opening on the flow screen
python -m dashboard --api http://127.0.0.1:5000   # another controller address
```

Use a real terminal (Windows Terminal, the VS Code terminal) at least 140 x 40 characters. `q` quits the dashboard; the system keeps running.

## Screens

- **1 Overview**
  - Workers: state, chunks, heartbeat age, usage, orphans.
  - Chunk map: each object's chunks, one coloured block per copy.
  - Selected object and chunk.
  - Spread: chunks shared by each pair of workers.
  - Events and output.
- **2 Flow**
  - Heartbeats (♥) travel from each worker to the controller.
  - Requests and data (? ■), failed requests (×), repair copies (▣).
  - A 20-second heartbeat strip per worker.
  - Failure detection with the countdown to auto-repair.
  - Repair steps, one at a time.

Everything is real. Heartbeats are found from the heartbeat ages in `GET /nodes`. Uploads from any client and copies moved by auto-repair are found by comparing the metadata between refreshes. `t` makes the dashboard download a random object every 3 s, so there is read traffic to watch.

## Keys

| Key | Does |
|---|---|
| `1` `2` | overview / flow |
| `Tab` `↑↓←→` `Enter` | move between panes, select, details (a worker's orphans and missing chunks; a chunk's copies) |
| `u` `d` | upload / delete the selected object |
| `r` `x` | repair / clean up orphans |
| `k` `s` `+` `c` | kill / start the selected worker, add a worker, damage a copy of the selected chunk (need docker) |
| `t` | client traffic on / off |
| `:` | command line: `upload PATH [k=v,...]`, `upload NAME MB`, `download`, `delete`, `meta k=v`, `repair`, `cleanup [MIN_AGE]`, `kill W`, `start W`, `scale N`, `corrupt [W]`, `clear`, `help` |
| `!` | shell command, output streamed into the events pane (`Ctrl+C` stops it) |
| `?` | help |

## Files

| File | What it does |
|---|---|
| `__main__.py` | command-line options, starts the app |
| `app.py` | the app: polling, events, keys, commands, shell |
| `model.py` | plain Python: worker states, chunk health, events, heartbeat detection |
| `widgets.py` | the overview panes |
| `flow.py` | the flow screen and its animation |
| `screens.py` | help, details and confirm windows |
| `api.py` / `docker_ctl.py` | the REST API / the docker command |
| `onstore.tcss` | the look |

Tests: `tests/test_dashboard.py` covers the model on its own, and the real app driven by keyboard against the 4-worker test cluster.
