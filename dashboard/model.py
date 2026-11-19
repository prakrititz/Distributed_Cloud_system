"""What the dashboard knows, worked out from the API's answers.

Plain Python (no Textual), so it can be tested on its own. The app polls the
API, builds a State from the answers, and compares each new State with the
previous one to find events ("worker2 went DOWN", "big.bin#2 has a corrupt
copy", "auto-repair ran"). No new endpoint on the controller is needed.
"""
import re
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from itertools import combinations

# The controller's timing (config.py). Not exposed by the API, so the
# dashboard uses the same defaults; --heartbeat-timeout changes them.
HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT = 6.0

UP, LATE, SILENT, DOWN = "UP", "LATE", "SILENT", "DOWN"


def worker_sort_key(worker_id: str):
    """worker2 before worker10."""
    m = re.match(r"^(.*?)(\d+)$", worker_id)
    return (m.group(1), int(m.group(2))) if m else (worker_id, 0)


def human_size(n) -> str:
    if n is None:
        return "?"
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024 or unit == "GB":
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024


@dataclass
class Worker:
    id: str
    reachable: bool                 # answered the controller's health check (GET /nodes)
    heartbeat_age: float = None     # seconds since its last heartbeat, as the controller sees it
    capacity: int = 0
    used: int = 0
    free: int = 0
    chunks: int = 0
    registered: bool = True         # False: named in the metadata, but has not registered (yet)
    inventory: dict = None          # GET /internal/workers/<id>/chunks (orphans, missing)

    @property
    def usage(self) -> float:
        return self.used / self.capacity if self.capacity else 0.0

    @property
    def orphans(self):
        return None if self.inventory is None else len(self.inventory.get("orphans", []))


def worker_state(w: Worker, timeout: float = HEARTBEAT_TIMEOUT, interval: float = HEARTBEAT_INTERVAL) -> str:
    """UP      sending heartbeats
    LATE    heartbeat overdue, still answering
    SILENT  not answering any more, but the controller still counts it alive
            (it notices only after `timeout` seconds without a heartbeat)
    DOWN    no heartbeat for `timeout` seconds: the controller counts it down"""
    if not w.registered or w.heartbeat_age is None or w.heartbeat_age > timeout:
        return DOWN
    if not w.reachable:
        return SILENT
    if w.heartbeat_age > interval + 1.5:
        return LATE
    return UP


@dataclass
class State:
    """One picture of the whole system."""
    controller_ok: bool = False
    error: str = ""
    workers: dict = field(default_factory=dict)       # id -> Worker, sorted
    objects: dict = field(default_factory=dict)       # object_id -> full metadata, oldest first
    health: dict = field(default_factory=dict)        # (object_id, index) -> {"status", "copies": {node: state}}
    replication_factor: int = 2
    auto_repair: dict = None                          # {"enabled", "grace_seconds", "last"}
    timeout: float = HEARTBEAT_TIMEOUT

    # ---------------------------------------------------------------- workers
    def state_of(self, worker_id: str) -> str:
        w = self.workers.get(worker_id)
        return DOWN if w is None else worker_state(w, self.timeout)

    def workers_up(self) -> int:
        return sum(1 for wid in self.workers if self.state_of(wid) in (UP, LATE))

    # ---------------------------------------------------------------- chunks
    def copy_state(self, object_id: str, index: int, node: str) -> str:
        """ok / down / missing / corrupt. From the last replication report; for
        a chunk it does not cover yet, worked out from the worker's state."""
        known = self.health.get((object_id, index), {}).get("copies", {})
        if node in known and not (known[node] == "down" and self.state_of(node) in (UP, LATE)):
            return known[node]
        return "down" if self.state_of(node) in (DOWN, SILENT) else "ok"

    def chunk_status(self, object_id: str, chunk: dict) -> str:
        good = sum(1 for n in chunk["replicas"] if self.copy_state(object_id, chunk["index"], n) == "ok")
        if good >= self.replication_factor:
            return "healthy"
        return "under_replicated" if good else "unavailable"

    def counts(self) -> Counter:
        c = Counter()
        for oid, meta in self.objects.items():
            for chunk in meta["chunks"]:
                c[self.chunk_status(oid, chunk)] += 1
        return c

    def chunk_total(self) -> int:
        return sum(len(m["chunks"]) for m in self.objects.values())

    def pair_counts(self) -> Counter:
        """How many chunks each pair of workers shares. Even counts mean the
        copies are spread over many pairs, so losing two workers loses little."""
        pairs = Counter()
        for meta in self.objects.values():
            for chunk in meta["chunks"]:
                for a, b in combinations(sorted(chunk["replicas"], key=worker_sort_key), 2):
                    pairs[(a, b)] += 1
        return pairs

    def chunks_on(self, worker_id: str) -> int:
        return sum(1 for m in self.objects.values() for c in m["chunks"] if worker_id in c["replicas"])

    def overall(self) -> str:
        if not self.controller_ok:
            return "unavailable"
        c = self.counts()
        if c["unavailable"]:
            return "unavailable"
        if c["under_replicated"]:
            return "under_replicated"
        return "healthy"


def build_workers(nodes: list, objects: dict, inventories: dict = None) -> dict:
    """Workers from GET /nodes, plus workers the metadata names that have not
    registered (after a controller restart, or a worker that never came back)."""
    inventories = inventories or {}
    workers = {}
    for n in nodes:
        workers[n["node"]] = Worker(
            id=n["node"], reachable=n["alive"], heartbeat_age=n.get("heartbeat_age"),
            capacity=n.get("capacity", 0), used=n.get("used", 0), free=n.get("free", 0),
            chunks=n.get("chunks", 0), inventory=inventories.get(n["node"]))
    for meta in objects.values():
        for chunk in meta["chunks"]:
            for node in chunk["replicas"]:
                if node not in workers:
                    workers[node] = Worker(id=node, reachable=False, registered=False)
    return {k: workers[k] for k in sorted(workers, key=worker_sort_key)}


def build_health(replication: dict) -> dict:
    return {(c["object_id"], c["index"]): {"status": c["status"],
                                           "copies": {r["node"]: r["state"] for r in c["replicas"]}}
            for c in replication.get("chunks", [])}


# ====================================================================== events
# Each event is (kind, text). kind: good / bad / warn / info / repair

def label(meta: dict, index: int = None) -> str:
    return meta["name"] if index is None else f"{meta['name']}#{index}"


def worker_events(prev: State, cur: State) -> list:
    events = []
    for wid in cur.workers:
        new = cur.state_of(wid)
        if wid not in prev.workers:
            if cur.workers[wid].registered:
                w = cur.workers[wid]
                events.append(("good", f"{wid} registered ({human_size(w.capacity)} capacity)"))
            continue
        old = prev.state_of(wid)
        if old == new or not prev.controller_ok:
            continue
        if new == SILENT:
            if old == DOWN:
                continue                # a restarted worker coming up: "is back" follows
            events.append(("warn", f"{wid} stopped answering; the controller marks it DOWN after "
                                   f"{cur.timeout:g}s without a heartbeat"))
        elif new == DOWN:
            events.append(("bad", f"{wid}: no heartbeat for {cur.timeout:g}s -> DOWN"))
        elif new == LATE and old == UP:
            events.append(("warn", f"{wid}: heartbeat late ({cur.workers[wid].heartbeat_age:.1f}s)"))
        elif new == UP and old in (DOWN, SILENT):
            events.append(("good", f"{wid} is back: heartbeat received -> UP"))
    for wid in prev.workers:
        if wid not in cur.workers:
            events.append(("info", f"{wid} is no longer listed"))
    return events


def object_events(prev: State, cur: State, own: set = frozenset()) -> tuple:
    """(events, moves). moves: [(object_id, index, removed_nodes, added_nodes)]
    for chunks whose copies moved (a repair or an adoption)."""
    events, moves = [], []
    for oid, meta in cur.objects.items():
        old = prev.objects.get(oid)
        if old is None:
            if oid not in own:
                who = meta.get("client_id") or "a client"
                events.append(("info", f"{who} uploaded {meta['name']} ({human_size(meta['size'])}): "
                                       f"{meta['chunk_count']} chunk(s) x{meta['copies_per_chunk']}"))
            continue
        if old.get("custom") != meta.get("custom"):
            events.append(("info", f"{meta['name']}: custom metadata changed"))
        before = {c["index"]: c["replicas"] for c in old["chunks"]}
        for c in meta["chunks"]:
            was = before.get(c["index"], [])
            if set(was) != set(c["replicas"]):
                removed = [n for n in was if n not in c["replicas"]]
                added = [n for n in c["replicas"] if n not in was]
                moves.append((oid, c["index"], removed, added))
                events.append(("repair", f"{label(meta, c['index'])}: copies {'+'.join(was) or '-'} -> "
                                         f"{'+'.join(c['replicas']) or '-'}"))
    for oid, meta in prev.objects.items():
        if oid not in cur.objects and oid not in own:
            events.append(("info", f"{meta['name']} was deleted"))
    return events, moves


def health_events(prev: State, cur: State) -> list:
    """Chunks that changed health, grouped so one dead worker gives one line."""
    if not prev.health:
        return []
    changed = defaultdict(list)
    for key, h in cur.health.items():
        old = prev.health.get(key)
        meta = cur.objects.get(key[0])
        if meta is None:
            continue
        for node, state in h["copies"].items():
            was = (old or {}).get("copies", {}).get(node)
            if state in ("corrupt", "missing") and was != state:
                changed[f"copy {state}"].append(f"{label(meta, key[1])} on {node}")
        if old is not None and old["status"] != h["status"]:
            changed[h["status"]].append(label(meta, key[1]))
    events = []
    names = {"healthy": ("good", "healthy again"), "under_replicated": ("warn", "under-replicated"),
             "unavailable": ("bad", "UNAVAILABLE (no good copy)"),
             "copy corrupt": ("bad", "CORRUPT copy"), "copy missing": ("warn", "missing copy")}
    for what, items in changed.items():
        kind, text = names[what]
        shown = ", ".join(items[:4]) + (f" and {len(items) - 4} more" if len(items) > 4 else "")
        events.append((kind, f"{text}: {shown}"))
    return events


def auto_repair_events(prev: State, cur: State) -> list:
    last = (cur.auto_repair or {}).get("last")
    if prev.auto_repair is None or not last or last == prev.auto_repair.get("last"):
        return []                               # (the first report only tells what happened before)
    grace = cur.auto_repair.get("grace_seconds")
    return [("repair", f"auto-repair ({', '.join(last['because_down'])} down >= {grace:g}s): "
                       f"{last['repaired']} chunk(s) repaired, {last['copies_created']} copy/copies created"
                       + (f", {last['partial']} partial" if last.get("partial") else "")
                       + (f", {last['unrecoverable']} unrecoverable" if last.get("unrecoverable") else ""))]


# ================================================================= heartbeats
class HeartbeatTracker:
    """Finds each heartbeat from the heartbeat ages that GET /nodes reports:
    arrival time = time of the poll - age. Keeps the last `window` seconds."""

    def __init__(self, window: float = 20.0):
        self.window = window
        self.beats = defaultdict(deque)       # id -> arrival times (time.monotonic)
        self.states = defaultdict(deque)      # id -> (time, state)
        self.last = {}                        # id -> latest arrival (kept when the strip forgets it)

    def observe(self, state: State, now: float = None) -> list:
        """Record one poll. Returns the ids whose new heartbeat arrived since the last poll."""
        now = time.monotonic() if now is None else now
        arrived = []
        for wid, w in state.workers.items():
            if w.heartbeat_age is not None and w.registered:
                at = now - w.heartbeat_age
                if wid not in self.last or at - self.last[wid] > 0.6:
                    self.last[wid] = at
                    self.beats[wid].append(at)
                    arrived.append(wid)
            self.states[wid].append((now, state.state_of(wid)))
            oldest = now - self.window - 5
            while self.beats[wid] and self.beats[wid][0] < oldest:
                self.beats[wid].popleft()
            while self.states[wid] and self.states[wid][0][0] < oldest:
                self.states[wid].popleft()
        return arrived

    def strip(self, wid: str, cells: int, now: float = None) -> list:
        """The last `window` seconds as `cells` cells, oldest first: 'beat',
        'down', 'silent', 'gap' (alive, between heartbeats) or 'none' (no data)."""
        now = time.monotonic() if now is None else now
        step = self.window / max(1, cells)
        beats = self.beats.get(wid, ())
        states = list(self.states.get(wid, ()))
        out = []
        for i in range(cells):
            t0 = now - self.window + i * step
            t1 = t0 + step
            if any(t0 <= b < t1 for b in beats):
                out.append("beat")
                continue
            st = None
            for t, s in states:
                if t <= t1:
                    st = s
                else:
                    break
            out.append({None: "none", DOWN: "down", SILENT: "silent"}.get(st, "gap"))
        return out


def repair_countdown(state: State, wid: str):
    """For a DOWN worker: (down_for, seconds left until auto-repair or None).
    The controller starts its countdown when its check thread (every 2 s)
    first sees the worker down, so this is approximate."""
    w = state.workers.get(wid)
    if w is None or w.heartbeat_age is None:
        return None, None
    down_for = max(0.0, w.heartbeat_age - state.timeout)
    ar = state.auto_repair or {}
    if not ar.get("enabled"):
        return down_for, None
    return down_for, max(0.0, ar["grace_seconds"] - down_for)


# ===================================================================== repair
def repair_steps(result: dict) -> list:
    """The repair endpoint's report as steps to show one at a time:
    [(kind, text, (from_node, to_node) or None)]."""
    steps = []
    for d in result.get("details", []):
        name = f"{d['name']}#{d['index']}"
        good = [n for n, s in d["before"].items() if s == "ok"]
        for n, s in d["before"].items():
            if s != "ok":
                steps.append(("bad", f"{name}  copy on {n}: {s}", None))
        if d.get("outcome") == "unrecoverable":
            steps.append(("bad", f"{name}  no good copy left: cannot rebuild", None))
            continue
        src = good[0] if good else None
        steps.append(("info", f"{name}  < reading good copy from {src}", None))
        steps.append(("good", f"{name}  checksum matches the one recorded at upload", None))
        for a in d.get("added", []):
            steps.append(("repair", f"{name}  > writing new copy to {a}", (src, a)))
        if d.get("outcome") == "partial":
            steps.append(("warn", f"{name}  partial: no other worker has room", None))
        else:
            steps.append(("good", f"{name}  done: {'+'.join(good + d.get('added', []))}", None))
    if not steps:
        steps.append(("good", f"repair: all {result.get('chunks_checked', 0)} chunk(s) already healthy", None))
    return steps
