"""What each client did: its uploads, downloads, deletes, metadata changes and
shares, newest first. A client sees its own history (GET /api/v1/me/activity),
whichever program it used (client.py, curl, the dashboard), so the client
dashboard can show "the files I uploaded or downloaded".

Kept in the metadata store when it can keep it (SQLite: survives a restart),
otherwise in memory. At most `keep` entries per client are kept: the oldest
go first.
"""
import threading
import time
from collections import defaultdict, deque

ACTIONS = ("register", "upload", "download", "delete", "metadata", "share")


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class ActivityLog:
    def __init__(self, store=None, keep: int = 500):
        self.keep = keep
        self.store = store if store is not None and hasattr(store, "add_activity") else None
        self._memory = defaultdict(lambda: deque(maxlen=keep))     # when the store keeps none
        self._lock = threading.Lock()

    def record(self, client_id: str, action: str, object_id: str = "", name: str = "",
               size: int = None, detail: str = "") -> None:
        """Remember one thing a client did. Never raises: a history that
        cannot be written must not fail the upload or download itself."""
        if not client_id:
            return
        entry = {"at": now_iso(), "action": action, "object_id": object_id or "", "name": name or "",
                 "size": size, "detail": detail or ""}
        try:
            if self.store is not None:
                self.store.add_activity(client_id, entry, self.keep)
            else:
                with self._lock:
                    self._memory[client_id].append(entry)
        except Exception as e:
            print(f"[activity] could not record {action} by {client_id}: {e}", flush=True)

    def recent(self, client_id: str, limit: int = 50) -> list:
        """The client's latest entries, newest first."""
        if not client_id or limit <= 0:
            return []
        if self.store is not None:
            return self.store.load_activity(client_id, limit)
        with self._lock:
            return list(reversed(self._memory.get(client_id, ())))[:limit]
