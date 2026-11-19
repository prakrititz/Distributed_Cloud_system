"""Who is calling, what may they do, and how much of the system may they use.

KeyRing      authentication: maps a secret key to a client id. A client sends
             "Authorization: Bearer <key>"; its identity comes from the key,
             never from what it claims. Only a hash of each key is kept in
             memory.
RateLimiter  a token bucket per client: each client may send `rate` requests
             per second on average, with bursts of up to `burst`.
ClientSlots  how many uploads/downloads each client has running, so no client
             holds more than its share of the controller's transfer slots.

Who may do what with an object (the rule used by app.py):
    owner   (the client that uploaded it)  read, change, delete, share
    reader  (listed in its readers)        read only
    admin   (named in ADMINS)              everything
    anyone else                            nothing: the object looks absent
"""
import hashlib
import hmac
import threading
import time


def _digest(key: str) -> bytes:
    return hashlib.sha256(key.encode("utf-8")).digest()


def parse_keys(text: str) -> dict:
    """'client1=KEY1,client2=KEY2' -> {client_id: key}. Raises ValueError for
    a malformed entry, a key used by two clients, or a short key."""
    keys = {}
    for piece in text.split(","):
        piece = piece.strip()
        if not piece:
            continue
        client_id, sep, key = piece.partition("=")
        client_id, key = client_id.strip(), key.strip()
        if not sep or not client_id or not key:
            raise ValueError(f"API_KEYS: '{client_id or piece}' is not client=key")
        if len(key) < 8:
            raise ValueError(f"API_KEYS: the key of '{client_id}' is too short (at least 8 characters)")
        if client_id in keys:
            raise ValueError(f"API_KEYS: '{client_id}' appears twice")
        if key in keys.values():
            raise ValueError(f"API_KEYS: two clients share the key of '{client_id}'")
        keys[client_id] = key
    return keys


class KeyRing:
    """The clients and their keys. Empty: authentication is off."""

    def __init__(self, keys: dict = None, admins=()):
        self._by_digest = {_digest(key): client_id for client_id, key in (keys or {}).items()}
        self.admins = set(admins)

    @classmethod
    def from_setting(cls, api_keys: str, admins: str) -> "KeyRing":
        return cls(parse_keys(api_keys), [a.strip() for a in admins.split(",") if a.strip()])

    @property
    def enabled(self) -> bool:
        return bool(self._by_digest)

    def identify(self, authorization: str):
        """The client id for an 'Authorization: Bearer <key>' header, or None
        if the header is missing, malformed or the key is unknown."""
        scheme, _, key = (authorization or "").strip().partition(" ")
        if scheme.lower() != "bearer" or not key.strip():
            return None
        digest = _digest(key.strip())
        for known, client_id in self._by_digest.items():   # compare every key in constant time
            if hmac.compare_digest(known, digest):
                return client_id
        return None

    def is_admin(self, client_id) -> bool:
        return client_id in self.admins


class RateLimiter:
    """A token bucket per client. Each bucket holds up to `burst` tokens and
    refills at `rate` tokens per second; a request takes one token. With rate
    0 there is no limit."""

    def __init__(self, rate: float, burst: int, clock=time.monotonic):
        self.rate = rate
        self.burst = max(1, burst)
        self._clock = clock
        self._buckets = {}          # client -> (tokens, time of the last update)
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return self.rate > 0

    def allow(self, client: str):
        """(True, 0) if the client may send a request now, else (False,
        seconds until it may)."""
        if not self.enabled:
            return True, 0.0
        now = self._clock()
        with self._lock:
            tokens, last = self._buckets.get(client, (float(self.burst), now))
            tokens = min(float(self.burst), tokens + (now - last) * self.rate)
            if tokens >= 1:
                self._buckets[client] = (tokens - 1, now)
                return True, 0.0
            self._buckets[client] = (tokens, now)
            return False, (1 - tokens) / self.rate


class ClientSlots:
    """How many transfers each client has running; at most `limit` each."""

    def __init__(self, limit: int):
        self.limit = limit
        self._running = {}
        self._lock = threading.Lock()

    def take(self, client: str) -> bool:
        with self._lock:
            if self._running.get(client, 0) >= self.limit:
                return False
            self._running[client] = self._running.get(client, 0) + 1
            return True

    def give_back(self, client: str) -> None:
        with self._lock:
            left = self._running.get(client, 0) - 1
            if left > 0:
                self._running[client] = left
            else:
                self._running.pop(client, None)

    def running(self, client: str) -> int:
        with self._lock:
            return self._running.get(client, 0)
