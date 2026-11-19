"""Who is calling, what may they do, and how much of the system may they use.

KeyRing      authentication: maps a secret key to a client id. A client sends
             "Authorization: Bearer <key>"; its identity comes from the key,
             never from what it claims. Only a hash of each key is kept in
             memory. Keys come from API_KEYS (fixed clients) or from
ClientRegistry  clients that registered themselves: the controller created
             their key, gave it to them once, and stores only its hash.
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
import re
import secrets
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


CLIENT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class ClientRegistry:
    """Clients that registered themselves. A client asks to register (with a
    name of its choice, or none); the controller creates a random key, gives it
    to the client ONCE, and keeps only its SHA-256 hash, in the metadata store
    (so registrations survive a restart). There is nothing to look a key up
    by except its hash, so a key that is lost cannot be recovered.

    A random 256-bit key cannot be guessed, so a plain SHA-256 is enough (no
    slow password hash), and the lookup is a dictionary lookup by that hash:
    no scan over all clients however many there are."""

    def __init__(self, store):
        self.store = store
        self._lock = threading.Lock()
        self._by_hash = {key_hash: client_id for client_id, key_hash in store.load_clients()}

    def __len__(self) -> int:
        return len(self._by_hash)

    def lookup(self, key: str):
        """The client id this key was issued to, or None."""
        return self._by_hash.get(hashlib.sha256(key.encode("utf-8")).hexdigest())

    def known(self, client_id: str) -> bool:
        return client_id in self._by_hash.values()

    def register(self, client_id: str, created_at: str) -> str:
        """Create a key for a new client and remember its hash. Returns the
        key (the only time it exists outside the client). Raises ClientTaken
        (a ValueError) if the id is already registered."""
        key = "sk_" + secrets.token_urlsafe(32)
        key_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()
        with self._lock:
            self.store.add_client(client_id, key_hash, created_at)   # raises if the id is taken
            self._by_hash[key_hash] = client_id
        return key


class KeyRing:
    """Who a key belongs to: the fixed clients of API_KEYS, plus the clients
    that registered themselves (a ClientRegistry). Authentication is on when
    there are fixed keys or when it is required (AUTH=1, e.g. with only
    registered clients)."""

    def __init__(self, keys: dict = None, admins=(), required: bool = False, registry: "ClientRegistry" = None):
        self._by_digest = {_digest(key): client_id for client_id, key in (keys or {}).items()}
        self.fixed_clients = set((keys or {}).keys())
        self.admins = set(admins)
        self.required = required
        self.registry = registry

    @classmethod
    def from_setting(cls, api_keys: str, admins: str, required: bool = False,
                     registry: "ClientRegistry" = None) -> "KeyRing":
        return cls(parse_keys(api_keys), [a.strip() for a in admins.split(",") if a.strip()],
                   required=required, registry=registry)

    @property
    def enabled(self) -> bool:
        return self.required or bool(self._by_digest)

    def identify(self, authorization: str):
        """The client id for an 'Authorization: Bearer <key>' header, or None
        if the header is missing, malformed or the key is unknown."""
        scheme, _, key = (authorization or "").strip().partition(" ")
        if scheme.lower() != "bearer" or not key.strip():
            return None
        key = key.strip()
        digest = _digest(key)
        for known, client_id in self._by_digest.items():   # fixed keys: compare every one in constant time
            if hmac.compare_digest(known, digest):
                return client_id
        return self.registry.lookup(key) if self.registry is not None else None

    def is_admin(self, client_id) -> bool:
        return client_id in self.admins

    def taken(self, client_id: str) -> bool:
        """True if this id already belongs to someone: a fixed client, an
        admin, or a registered client. (Registering it would hand over that
        client's rights.)"""
        return (client_id in self.fixed_clients or client_id in self.admins
                or (self.registry is not None and self.registry.known(client_id)))


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
