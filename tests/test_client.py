"""client.py: the command-line client, driven against the real cluster."""
import builtins
import os

import pytest

import client as client_module
from client import Client, parse_pairs


class TestParsePairs:
    def test_pairs(self):
        assert parse_pairs("a=1, b = two ,c=null") == {"a": "1", "b": "two", "c": None}

    def test_empty_and_blank_pieces(self):
        assert parse_pairs("") == {} and parse_pairs(" , ,") == {}

    def test_a_value_may_contain_an_equals_sign(self):
        assert parse_pairs("url=http://x/?a=b") == {"url": "http://x/?a=b"}

    def test_errors(self):
        with pytest.raises(ValueError, match="not key=value"):
            parse_pairs("novalue")
        with pytest.raises(ValueError, match="no key"):
            parse_pairs("=value")


class TestClientConfiguration:
    def test_the_api_address_can_come_from_the_environment(self, monkeypatch):
        import importlib
        monkeypatch.setenv("API_URL", "http://controller:5000/api/v1")
        try:
            reloaded = importlib.reload(client_module)
            assert reloaded.BASE_URL == "http://controller:5000/api/v1"
            assert reloaded.Client().base_url == "http://controller:5000/api/v1"
        finally:
            monkeypatch.delenv("API_URL")
            importlib.reload(client_module)

    def test_the_default_address_is_the_local_controller(self, monkeypatch):
        import importlib
        monkeypatch.delenv("API_URL", raising=False)
        reloaded = importlib.reload(client_module)
        assert reloaded.BASE_URL == "http://127.0.0.1:5000/api/v1"


class TestClientAgainstTheCluster:
    def test_upload_download_round_trip_with_metadata(self, cluster, tmp_path):
        c = Client(base_url=cluster.api, client_id="t")
        src = tmp_path / "todo.txt"
        src.write_bytes(b"buy milk\r\nwrite report\r\n")                 # CRLF must survive
        m = c.upload(str(src), {"project": "demo"})
        assert m["custom"] == {"project": "demo"} and m["content_type"] == "text/plain" and m["size"] == src.stat().st_size
        out = tmp_path / "out"
        out.mkdir()
        os.chdir(out)
        saved = c.retrieve(m["object_id"])
        assert saved == "todo.txt" and (out / "todo.txt").read_bytes() == src.read_bytes()

    def test_download_never_overwrites_an_existing_file(self, cluster, tmp_path, monkeypatch):
        c = Client(base_url=cluster.api, client_id="t")
        src = tmp_path / "f.txt"
        src.write_bytes(b"x")
        m = c.upload(str(src))
        monkeypatch.chdir(tmp_path)
        (tmp_path / "f.txt").write_bytes(b"precious")
        with pytest.raises(ValueError, match="already exists"):
            c.retrieve(m["object_id"])
        assert (tmp_path / "f.txt").read_bytes() == b"precious"

    def test_errors_are_readable(self, cluster, tmp_path):
        c = Client(base_url=cluster.api, client_id="t")
        with pytest.raises(ValueError, match="404"):
            c.retrieve("no-such-id", str(tmp_path / "x"))
        with pytest.raises(ValueError, match="404"):
            c.get_metadata("no-such-id")
        with pytest.raises(ValueError, match="404"):
            c.delete("no-such-id")
        with pytest.raises(FileNotFoundError):
            c.upload(str(tmp_path / "does-not-exist"))

    def test_a_download_damaged_in_transit_is_detected(self, cluster, tmp_path, monkeypatch):
        c = Client(base_url=cluster.api, client_id="t")
        src = tmp_path / "f.bin"
        src.write_bytes(os.urandom(2000))
        m = c.upload(str(src))
        import requests
        real_get = requests.get

        class Damaged:
            """The real streamed response, with the first byte flipped on the way."""
            def __init__(self, r):
                self._r = r
                self.headers = r.headers
                self.ok = r.ok
                self.status_code = r.status_code

            def iter_content(self, size):
                first = True
                for block in self._r.iter_content(size):
                    yield bytes([block[0] ^ 0xFF]) + block[1:] if first else block
                    first = False

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                self._r.close()

        monkeypatch.setattr(client_module.requests, "get",
                            lambda url, *a, **k: Damaged(real_get(url, *a, **k)) if url.endswith(m["object_id"]) else real_get(url, *a, **k))
        with pytest.raises(ValueError, match="damaged in transit"):
            c.retrieve(m["object_id"], str(tmp_path / "out.bin"))
        assert os.listdir(tmp_path) == ["f.bin"]                         # no file, no temporary file left


class TestOriginalBytes:
    """The stored bytes are the file's own bytes (issue #17): the client, curl
    and a browser all get back exactly the original file."""

    def test_uploaded_by_the_client_downloaded_with_plain_http(self, cluster, tmp_path):
        import requests
        data = os.urandom(3 * 1024 * 1024 + 17)
        src = tmp_path / "video.mp4"
        src.write_bytes(data)
        m = Client(base_url=cluster.api, client_id="t").upload(str(src))
        assert m["size"] == len(data) and m["checksum"] == client_module.compute(data)
        r = requests.get(f"{cluster.api}/objects/{m['object_id']}", timeout=60)
        assert r.status_code == 200 and r.content == data
        assert r.headers["Content-Type"].startswith("video/mp4")

    def test_uploaded_with_plain_http_downloaded_by_the_client(self, cluster, tmp_path):
        import requests
        data = os.urandom(5000)
        r = requests.post(cluster.api + "/objects", files={"file": ("raw.bin", data)}, timeout=60)
        out = tmp_path / "raw.bin"
        Client(base_url=cluster.api).retrieve(r.json()["object_id"], str(out))
        assert out.read_bytes() == data

    def test_what_lies_on_the_workers_is_the_file_itself(self, cluster, tmp_path):
        data = os.urandom(1000)
        src = tmp_path / "small.bin"
        src.write_bytes(data)
        m = Client(base_url=cluster.api, client_id="t").upload(str(src))
        chunk = m["chunks"][0]
        assert open(cluster.chunk_path(chunk["replicas"][0], chunk["chunk_id"]), "rb").read() == data


class TestCommands:
    """python client.py <command> ...: the client without the menu, as the
    client containers use it."""

    @pytest.fixture
    def run(self, cluster, monkeypatch, capsys):
        monkeypatch.setenv("CLIENT_ID", "cli")                    # read by Client() like in a container
        real_client = client_module.Client
        monkeypatch.setattr(client_module, "Client", lambda: real_client(base_url=cluster.api))

        def run(*args):
            code = client_module.main(list(args))
            return code, capsys.readouterr().out
        return run

    def test_a_whole_session(self, run, tmp_path):
        src = tmp_path / "notes.txt"
        src.write_bytes(b"hello\r\n")
        code, out = run("upload", str(src), "project=demo")
        assert code == 0 and "as client 'cli'" in out and "chunk 0:" in out
        oid = out.strip().splitlines()[-1].split("object_id: ")[1]

        assert run("list")[1].count(oid) == 1
        code, out = run("info", oid)
        assert code == 0 and '"project": "demo"' in out and '"client_id": "cli"' in out
        code, out = run("update", oid, "project=null,stage=2")
        assert code == 0 and '"stage": "2"' in out and "project" not in out
        code, out = run("download", oid, str(tmp_path / "copy.txt"))
        assert code == 0 and (tmp_path / "copy.txt").read_bytes() == b"hello\r\n"
        code, out = run("delete", oid)              # (the controller's own log lines are in `out` too)
        assert code == 0 and "Deleted." in out
        assert "Nothing stored yet." in run("list")[1]

    def test_errors_give_exit_code_1(self, run):
        code, out = run("info", "no-such-id")
        assert code == 1 and "404" in out
        code, out = run("upload", "does-not-exist.txt")
        assert code == 1 and "Error" in out

    @pytest.mark.parametrize("args", [("frobnicate",), ("upload",), ("list", "extra"), ("update", "id")])
    def test_bad_usage_prints_help_and_gives_2(self, run, args):
        code, out = run(*args)
        assert code == 2 and "python client.py upload FILE" in out

    def test_help(self, run):
        assert run("--help")[0] == 0


class TestMenu:
    def test_a_scripted_session(self, cluster, tmp_path, monkeypatch, capsys):
        src = tmp_path / "src" / "todo.txt"
        src.parent.mkdir()
        src.write_bytes(b"buy milk\r\nwrite report\r\n")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(client_module, "Client", lambda: Client(base_url=cluster.api, client_id="menu"))
        probe = Client(base_url=cluster.api)

        def latest():
            return probe.list_objects()[-1]["object_id"]

        script = [
            ("Choose", "1"), ("File path", str(src)), ("Custom metadata", "project=demo, owner=ansh"),
            ("Choose", "3"),
            ("Choose", "4"), ("Object ID", latest),
            ("Choose", "5"), ("Object ID", latest), ("Changes", "owner=null, stage=2"),
            ("Choose", "2"), ("Object ID", latest), ("Save as", ""),
            ("Choose", "2"), ("Object ID", latest), ("Save as", ""),          # the original name exists now
            ("Choose", "1"), ("File path", str(src)), ("Custom metadata", ""),   # the same path again
            ("Choose", "2"), ("Object ID", lambda: "no-such-id"), ("Save as", "x.txt"),
            ("Choose", "6"), ("Object ID", latest),
            ("Choose", "3"),
            ("Choose", "9"),
            ("Choose", "7"),
        ]
        it = iter(script)

        def fake_input(prompt=""):
            key, value = next(it)
            assert key in prompt, f"expected a '{key}' prompt, got '{prompt}'"
            return value() if callable(value) else value

        monkeypatch.setattr(builtins, "input", fake_input)
        assert client_module.main([]) == 0
        out = capsys.readouterr().out
        assert "Uploaded 'todo.txt'" in out and "Object ID:" in out
        assert '"project": "demo"' in out and '"stage": "2"' in out and "owner" not in out.split("Saved to")[0].split('"stage"')[-1]
        assert "Saved to todo.txt" in out and "already exists" in out
        assert "Server said 409" in out and "Server said 404" in out
        assert "Deleted." in out and "Nothing stored yet." in out and "Please enter 1-7." in out
        assert (tmp_path / "todo.txt").read_bytes() == src.read_bytes()
        assert next(it, None) is None                                     # the whole script was used
