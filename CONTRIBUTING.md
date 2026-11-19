# Contributing

Thanks for wanting to help. This is a small distributed object store, built
to learn how chunking, replication, failure detection and repair work. Bug
fixes, tests, docs, new features and better explanations are all welcome.

By contributing you agree that your work is released under the project's
[MIT License](LICENSE).

## Ways to help

- **Report a bug**: open an issue with what you ran, what you expected and
  what happened. Include the output or logs (`docker compose logs controller`)
  if you can.
- **Suggest a feature**: open an issue first for anything big, so we can
  agree on the approach before you write it.
- **Pick up an issue**: comment on it so nobody else starts the same work.
- **Improve the docs**: the README, [GUIDE.md](GUIDE.md) and
  [dashboard/README.md](dashboard/README.md) should let a newcomer run and
  understand everything. If something confused you, it will confuse others.

Security problems (a way to read another client's objects, to bypass the API
key, ...): please report them privately through GitHub's **Security → Report
a vulnerability**, not in a public issue.

## Set up

You need Python 3.11+ and, to run the whole system, Docker with `docker compose`.

```bash
git clone https://github.com/prakrititz/Distributed_Cloud_system
cd Distributed_Cloud_system
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
```

Run the system and try it as described in [GUIDE.md](GUIDE.md):

```bash
docker compose up --build -d
python -m dashboard --key admin-dev-key
```

## Run the tests

```bash
python -m pytest                        # everything (about 3 minutes)
python -m pytest -m "not cluster"       # only the fast tests that start no servers
python -m pytest tests/test_api.py -k delete   # one file, some tests
```

The cluster tests start four real worker processes and the controller on free
local ports. Each test gets the cluster back in a clean state.

GitHub Actions runs the same tests on every push and pull request, on
Python 3.11 and 3.12. It also builds the Docker images, starts the system
with `docker compose`, uploads and downloads through a client container, and
stops a worker. A pull request needs a green run before it is merged.

## Where things are

| Path | What it is |
|---|---|
| `app.py` | the controller's REST API (Flask): authentication, the endpoints |
| `controller/distributor.py` | chunking, placement, replication, download, repair, cleanup |
| `controller/metadata_store.py` | where the metadata is saved (SQLite by default) |
| `controller/access.py` | API keys, registration, rate limits, per-client transfer slots |
| `controller/activity.py` | each client's history |
| `nodes/` | the worker: stores chunk files and answers the controller |
| `client.py` | the command-line client |
| `dashboard/` | On Store, the terminal dashboards (a separate program: it only uses the REST API) |
| `config.py` | every setting, each readable from an environment variable |
| `demo.py` | the demonstrations the project spec asks for |
| `tests/` | unit tests and cluster tests (`tests/cluster.py` starts the real cluster) |

The project spec (1 controller, 4 workers, 1 MB chunks, 2 copies, Docker
Compose, no Kubernetes) is fixed. Changes should keep those defaults, and
`tests/test_docker_files.py` checks several of them.

## Making a change

1. **Fork** the repo and create a branch named after the change:
   `git switch -c chunk-cache`.
2. **Write the code and its tests together.** A bug fix comes with a test
   that fails without it. A feature comes with tests for what it does and for
   what it refuses (bad input, missing objects, other clients' objects).
3. **Update the docs** in the same pull request: the README's API table and
   test table, GUIDE.md if a command changes, dashboard/README.md for the
   dashboards.
4. **Run the tests** (`python -m pytest`) before you push.
5. **Open a pull request** against `main`, saying what changed and why, and
   how you tested it.

### Commits

The history reads as a story, one feature at a time. Most features are two
commits:

```
Store and fetch several chunks at once within one transfer
Tests and docs for parallel chunk transfers
```

- The first line says what the change does, in the imperative ("Add",
  "Keep", "Do not wait..."), in under about 70 characters.
- The body says **why**, and anything a reviewer would otherwise have to work
  out: the problem, what you chose and what you did not.

### Code style

- Plain, readable Python. No new dependency unless it clearly pays for
  itself: the controller and workers need only Flask, waitress and requests.
- Every module, class and non-obvious function starts with a docstring that
  says what it is for, in plain English. Comments explain *why*, not what the
  next line does.
- Errors from the API are JSON with a message a person can act on, and use
  the status codes in the README.
- Names say what things are (`replication_factor`, `heartbeat_age`), not
  abbreviations.
- Settings go in `config.py`, readable from an environment variable, with a
  default that matches the spec.
- The dashboard imports nothing from the rest of the project. It talks to the
  controller only through the REST API.

## Being a good neighbour

Be kind and assume good intent. Review the code, not the person. Newcomers
are welcome: questions in issues and pull requests are always fine.
