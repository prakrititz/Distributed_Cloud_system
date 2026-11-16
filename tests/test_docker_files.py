"""The Docker files, checked without Docker: that each image's allowlist
contains everything its program needs (and nothing it should not have), that
the settings the containers rely on really work, and that the Dockerfiles say
what the spec requires."""
import os
import re
import shutil
import subprocess
import sys

import pytest

from config import parse_workers
from tests.cluster import REPO_ROOT


def read(name):
    with open(os.path.join(REPO_ROOT, name), encoding="utf-8") as f:
        return f.read()


def allowlist(dockerignore_name):
    """The paths a `*` + `!path` .dockerignore lets into the image."""
    lines = [l.strip() for l in read(dockerignore_name).splitlines()]
    assert "*" in lines, "an allowlist starts by excluding everything"
    return [l[1:].rstrip("/") for l in lines if l.startswith("!")]


def build_context(tmp_path, dockerignore_name):
    """Copy only what the image would contain."""
    dest = tmp_path / "image"
    dest.mkdir()
    for entry in allowlist(dockerignore_name):
        src = os.path.join(REPO_ROOT, entry)
        assert os.path.exists(src), f"{dockerignore_name} allows '{entry}' which does not exist"
        if os.path.isdir(src):
            shutil.copytree(src, dest / entry, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        else:
            shutil.copy(src, dest / entry)
    return dest


SETTINGS = ("WORKERS", "HOST", "PORT", "DEBUG", "NODE_ID", "DATA_DIR", "CONTROLLER_URL", "CAPACITY",
            "HEARTBEAT_INTERVAL", "CLIENT_ID", "API_URL")


def run_python(code, cwd, **env):
    """Run python in a clean environment: only the settings the test passes are set."""
    clean = {k: v for k, v in os.environ.items() if k not in SETTINGS}
    return subprocess.run([sys.executable, "-c", code], cwd=cwd, capture_output=True, text=True,
                          env={**clean, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONPATH": "", **env}, timeout=60)


# ----------------------------------------------------------- the allowlists
class TestImageContents:
    def test_the_controller_image_has_what_app_py_needs_and_starts(self, tmp_path):
        ctx = build_context(tmp_path, "Dockerfile.controller.dockerignore")
        r = run_python("import app, config; print(sorted(config.NODE_ADDRESSES))", ctx,
                       WORKERS="worker1=http://worker1:5000,worker2=http://worker2:5000")
        assert r.returncode == 0, r.stderr
        assert "['worker1', 'worker2']" in r.stdout

    def test_the_worker_image_has_what_node_server_needs_and_starts(self, tmp_path):
        ctx = build_context(tmp_path, "Dockerfile.worker.dockerignore")
        r = run_python("import nodes.node_server as s; print(s.NODE_ID, s.DATA_DIR)", ctx,
                       NODE_ID="worker2", DATA_DIR=str(tmp_path / "data"))
        assert r.returncode == 0, r.stderr
        assert "worker2" in r.stdout

    def test_the_controller_image_does_not_contain_node_code_or_the_client(self):
        allowed = allowlist("Dockerfile.controller.dockerignore")
        assert "nodes" not in allowed and "client.py" not in allowed
        assert {"app.py", "config.py", "controller", "helpers", "requirements.txt"} <= set(allowed)

    def test_the_worker_image_does_not_contain_controller_code_or_the_client(self):
        allowed = allowlist("Dockerfile.worker.dockerignore")
        assert not {"app.py", "controller", "client.py"} & set(allowed)
        assert {"config.py", "nodes", "helpers", "requirements.txt"} <= set(allowed)

    @pytest.mark.parametrize("name", ["Dockerfile.controller.dockerignore", "Dockerfile.worker.dockerignore"])
    def test_local_data_and_caches_never_get_into_an_image(self, name):
        allowed = allowlist(name)
        assert not {"data", ".git", ".venv", "tests", ".env"} & set(allowed)

    def test_the_controller_code_never_imports_node_code_and_the_other_way_round(self):
        def imports(folder):
            found = set()
            for root, _, names in os.walk(os.path.join(REPO_ROOT, folder)):
                for n in names:
                    if n.endswith(".py"):
                        found |= set(re.findall(r"^\s*(?:from|import)\s+([\w.]+)", open(os.path.join(root, n), encoding="utf-8").read(), re.M))
            return {i.split(".")[0] for i in found}
        assert "nodes" not in imports("controller") and "controller" not in imports("nodes")
        assert "app" not in imports("controller") | imports("nodes") | imports("helpers")


# ------------------------------------------------------------- the settings
class TestWorkersSetting:
    def test_parse(self):
        assert parse_workers("worker1=http://worker1:5000, worker2=https://w2:6000") == {
            "worker1": "http://worker1:5000", "worker2": "https://w2:6000"}

    @pytest.mark.parametrize("bad", ["", "   ", ",", "a", "a=", "=http://x", "a=ftp://x", "a=http://x,b", "a=x:5000"])
    def test_malformed_values_are_rejected_with_a_clear_message(self, bad):
        with pytest.raises(ValueError, match="WORKERS"):
            parse_workers(bad)

    def test_the_localhost_list_is_the_default(self, tmp_path):
        r = run_python("import config; print(config.NODE_ADDRESSES)", REPO_ROOT)
        assert r.returncode == 0 and "127.0.0.1:5001" in r.stdout

    def test_the_environment_overrides_it(self):
        r = run_python("import config; print(config.NODE_ADDRESSES)", REPO_ROOT, WORKERS="x=http://x:1")
        assert r.stdout.strip() == "{'x': 'http://x:1'}"

    def test_a_bad_value_stops_the_controller_at_startup_instead_of_running_misconfigured(self):
        r = run_python("import config", REPO_ROOT, WORKERS="not-valid")
        assert r.returncode != 0 and "WORKERS" in r.stderr


class TestControllerEntrypoint:
    def test_host_port_and_debug_come_from_the_environment(self):
        source = read("app.py")
        assert 'os.environ.get("HOST"' in source and 'os.environ.get("PORT"' in source
        assert 'os.environ.get("DEBUG") == "1"' in source
        assert "debug=True" not in source          # the interactive debugger must not be on by default


# ----------------------------------------------------------- the Dockerfiles
class TestDockerfiles:
    @pytest.mark.parametrize("name", ["Dockerfile.controller", "Dockerfile.worker"])
    def test_common_requirements(self, name):
        text = read(name)
        assert re.search(r"^FROM python:3\.\d+-slim", text, re.M), "a pinned, small base image"
        assert "COPY requirements.txt" in text and "pip install --no-cache-dir -r requirements.txt" in text
        assert re.search(r"^USER \w+", text, re.M), "must not run as root"
        assert "HEALTHCHECK" in text and "HOST=0.0.0.0" in text
        # dependencies are installed before the code is copied, so the layer is cached
        assert text.index("pip install") < text.index("COPY . .")

    def test_the_worker_keeps_its_chunks_in_a_volume(self):
        text = read("Dockerfile.worker")
        assert 'VOLUME ["/data"]' in text and "DATA_DIR=/data" in text
        assert text.index("chown app:app /data") < text.index("VOLUME"), "the volume must start out writable by the user"

    def test_start_commands(self):
        assert 'CMD ["python", "app.py"]' in read("Dockerfile.controller")
        assert 'CMD ["python", "-m", "nodes.node_server"]' in read("Dockerfile.worker")

    def test_the_worker_points_at_the_controller_by_service_name(self):
        assert "CONTROLLER_URL=http://controller:5000" in read("Dockerfile.worker")

    def test_requirements_are_pinned(self):
        lines = [l.strip() for l in read("requirements.txt").splitlines() if l.strip()]
        assert lines and all(re.fullmatch(r"[A-Za-z0-9_.-]+==\d+(\.\d+)*", l) for l in lines), lines
        assert {"flask", "requests"} <= {l.split("==")[0].lower() for l in lines}
