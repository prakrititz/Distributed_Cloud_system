"""Where a client's key is kept on this computer.

The same file, in the same format, as client.py's (`python client.py register`):
~/.objectstore/credentials.json, or OBJECTSTORE_CREDENTIALS, one entry per API
address ("http://127.0.0.1:5000/api/v1": {"client_id", "api_key"}). So a key
saved by either program is used by both. (The dashboard imports nothing from
the rest of the project, so this is its own small copy.)
"""
import json
import os


def path() -> str:
    return os.environ.get("OBJECTSTORE_CREDENTIALS") or os.path.join(
        os.path.expanduser("~"), ".objectstore", "credentials.json")


def _read(file: str) -> dict:
    try:
        with open(file, encoding="utf-8") as f:
            saved = json.load(f)
        return saved if isinstance(saved, dict) else {}
    except (OSError, ValueError):
        return {}


def load(api_url: str, file: str = None):
    """(client_id, api_key) saved for this API address, or (None, None)."""
    entry = _read(file or path()).get(api_url)
    if not isinstance(entry, dict):
        return None, None
    return entry.get("client_id"), entry.get("api_key")


def _write(file: str, saved: dict) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(file)), exist_ok=True)
    fd = os.open(file + ".tmp", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)   # owner only
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(saved, f, indent=1)
    os.replace(file + ".tmp", file)
    try:
        os.chmod(file, 0o600)
    except OSError:
        pass


def save(api_url: str, client_id: str, api_key: str, file: str = None) -> str:
    """Remember the key for this API address. Returns the file's path."""
    file = file or path()
    saved = _read(file)
    saved[api_url] = {"client_id": client_id, "api_key": api_key}
    _write(file, saved)
    return file


def forget(api_url: str, file: str = None) -> bool:
    """Remove the key saved for this API address. True if there was one."""
    file = file or path()
    saved = _read(file)
    if api_url not in saved:
        return False
    del saved[api_url]
    _write(file, saved)
    return True
