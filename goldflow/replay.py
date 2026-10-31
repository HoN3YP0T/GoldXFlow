"""Phase 4: deterministic historical event replay.

Every event has an availability time (when it would have been known live) and a replay key
    key = (availability_ms, rank, seq)
  availability_ms = stream time column + time_offset_ms + latency_ms   (config `replay.streams`)
  rank            = stream tie-break rank (lower first; data-quality events are rank 0)
  seq             = day_number * 10**10 + row index inside the stream's Phase-3 partition (already
                    deterministically sorted), so it is unique and increasing within a stream.
Events are delivered in strictly increasing key order. The simulation clock equals the availability time of
the last delivered event and never moves backwards; no event is delivered before the clock reaches its key.

Data is processed one availability-day window [D, D+1) at a time: the window's events from every enabled
stream (reading partitions D-1 and D for streams whose offset/latency can push events across midnight) are
merged with a vectorised lexsort and handed to consumers in chunks. Memory is bounded by one day.
"""
from __future__ import annotations
import hashlib, json, resource, sqlite3, threading, time
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Callable, Iterator

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .config import Config
from .observability import metrics
from .timeutil import DAY_MS, day_start_ms, from_ms, iso, parse_iso, to_ms

QUALITY_RANK = 0
SEQ_DAY_MULT = 10 ** 10
KEY_DTYPE = np.dtype([("avail", "<i8"), ("rank", "i1"), ("seq", "<i8"), ("stream", "i1")])

class ReplayGuardError(RuntimeError):
    """Resume refused: dataset or configuration differs from the checkpoint."""

# ============================================================================ events
@dataclass
class EventChunk:
    """A contiguous run of events in key order.

    keys       structured array (avail, rank, seq, stream) in delivery order
    rows       row index of each event inside its stream's day table
    tables     stream name -> pyarrow Table for the current day window (full canonical columns)
    streams    stream id -> name
    clock_ms   simulation clock after this chunk (= keys['avail'][-1])
    """
    keys: np.ndarray
    rows: np.ndarray
    tables: dict
    streams: list
    clock_ms: int
    quality: list = field(default_factory=list)

    def __len__(self): return len(self.keys)

    def stream_rows(self, name: str) -> np.ndarray:
        sid = self.streams.index(name)
        return self.rows[self.keys["stream"] == sid]

    def stream_table(self, name: str) -> pa.Table:
        """Rows of one stream in delivery order (a take() from the day table)."""
        return self.tables[name].take(pa.array(self.stream_rows(name)))

    def iter_events(self) -> Iterator[tuple[str, dict]]:
        """Per-event iteration (slow; for stepping/debugging)."""
        for k, r in zip(self.keys, self.rows):
            name = self.streams[k["stream"]]
            if name == "quality":
                yield name, self.quality[r]
            else:
                yield name, self.tables[name].slice(int(r), 1).to_pylist()[0]

class Consumer:
    """Base consumer. Later phases (trade processor, analytics, strategies) subclass this."""
    name = "consumer"
    def on_start(self, ctx: dict): pass
    def on_chunk(self, chunk: EventChunk): pass
    def on_seek(self, t_ms: int): pass
    def on_stop(self, ctx: dict): pass
    def get_state(self) -> dict: return {}
    def set_state(self, state: dict): pass

class ChainHasher(Consumer):
    """Digest of the exact delivered event-key sequence, independent of chunking, pacing, stepping and
    checkpoint/resume boundaries.

    Keys are folded in fixed blocks of BLOCK events: state' = sha256(state || sha256(block_bytes)). The unfinished
    block's bytes (at most BLOCK * 18 bytes) are carried in the checkpoint, so a resumed replay reproduces the
    same digest as an uninterrupted one."""
    name = "chain_hash"
    BLOCK = 4096
    REC = KEY_DTYPE.itemsize
    def __init__(self):
        self.state = hashlib.sha256(b"goldflow-replay-v1").digest(); self.pending = b""; self.n = 0
    def on_chunk(self, c):
        data = self.pending + c.keys.tobytes(); self.n += len(c)
        step = self.BLOCK * self.REC
        full = len(data) - len(data) % step
        for i in range(0, full, step):
            self.state = hashlib.sha256(self.state + hashlib.sha256(data[i:i + step]).digest()).digest()
        self.pending = data[full:]
    def get_state(self):
        import base64
        return {"state": self.state.hex(), "pending": base64.b64encode(self.pending).decode(), "n": self.n}
    def set_state(self, s):
        import base64
        if s: self.state, self.pending, self.n = bytes.fromhex(s["state"]), base64.b64decode(s["pending"]), s["n"]
    @property
    def digest(self) -> str:
        st = self.state
        if self.pending: st = hashlib.sha256(st + hashlib.sha256(self.pending).digest()).digest()
        return st.hex()

SequenceHasher = ChainHasher   # backwards-compatible name

class IntegrityChecker(Consumer):
    """Asserts strictly increasing keys, non-decreasing availability and clock >= every delivered event."""
    name = "integrity"
    def __init__(self): self.last = None; self.violations = 0; self.checked = 0
    def on_chunk(self, c):
        k = c.keys
        a = np.stack([k["avail"], k["rank"].astype(np.int64), k["seq"]], axis=1)
        if self.last is not None: a = np.vstack([self.last, a])
        d = np.diff(a, axis=0)
        first_nz = np.where(d[:, 0] != 0, d[:, 0], np.where(d[:, 1] != 0, d[:, 1], d[:, 2]))
        self.violations += int((first_nz <= 0).sum())
        self.violations += int((k["avail"] > c.clock_ms).sum())
        self.checked += len(k); self.last = a[-1:].copy()
    def on_seek(self, t_ms): self.last = None
    def get_state(self): return {"last": self.last.tolist() if self.last is not None else None, "violations": self.violations, "checked": self.checked}
    def set_state(self, s):
        if s: self.last = np.array(s["last"]) if s["last"] is not None else None; self.violations = s["violations"]; self.checked = s["checked"]

# ============================================================================ quality annotations
def load_quality_events(cfg: Config, enabled: list[str]) -> list[dict]:
    """Offline data-quality annotations from the Phase 3 catalog, as replay events.

    Each non-COMPLETE interval produces DQ_BEGIN at its start and DQ_END at its end. These are dataset
    annotations (knowledge="offline_annotation"): consumers may use them only to SUPPRESS calculations or
    signals, never to trigger them, because live operation would detect some gaps only after the fact.
    """
    db = sqlite3.connect(str(cfg.state_db)); db.row_factory = sqlite3.Row
    iv = []
    for r in db.execute("SELECT dataset, hour_utc, status, detail_json FROM hour_status WHERE status!='COMPLETE'"):
        if r["dataset"] not in enabled: continue
        s = to_ms(parse_iso(r["hour_utc"])); iv.append((r["dataset"], s, s + 3_600_000, r["status"], "hour"))
    covered_days = {(d, iso(s)[:10]) for d, s, *_ in iv}
    for r in db.execute("SELECT dataset, date, status, evidence_json FROM day_status WHERE status!='COMPLETE'"):
        if r["dataset"] not in enabled: continue
        ev = json.loads(r["evidence_json"])
        d0 = day_start_ms(date.fromisoformat(r["date"]))
        if r["dataset"] == "bookDepth" and ev.get("holes"):
            for a, b in ev["holes"]:
                iv.append(("bookDepth", to_ms(parse_iso(a)), to_ms(parse_iso(b)), "PARTIAL", "hole"))
        elif r["status"] == "MISSING" and "interval" in ev:
            iv.append((r["dataset"], to_ms(parse_iso(ev["interval"][0])), to_ms(parse_iso(ev["interval"][1])), "MISSING", "day"))
        elif r["status"] in ("MISSING", "UNVERIFIED") or (r["dataset"], r["date"]) not in covered_days:
            iv.append((r["dataset"], d0, d0 + DAY_MS, r["status"], "day"))
    # merge adjacent/overlapping intervals of the same dataset+status
    iv.sort(key=lambda x: (x[0], x[3], x[1]))
    merged = []
    for x in iv:
        if merged and merged[-1][0] == x[0] and merged[-1][3] == x[3] and x[1] <= merged[-1][2]:
            merged[-1] = (x[0], merged[-1][1], max(merged[-1][2], x[2]), x[3], merged[-1][4])
        else: merged.append(x)
    ev = []
    for ds, a, b, st, gran in merged:
        ev.append({"type": "DQ_BEGIN", "dataset": ds, "status": st, "start_ms": a, "end_ms": b, "avail_ms": a,
                   "knowledge": "offline_annotation"})
        ev.append({"type": "DQ_END", "dataset": ds, "status": st, "start_ms": a, "end_ms": b, "avail_ms": b,
                   "knowledge": "offline_annotation"})
    ev.sort(key=lambda e: (e["avail_ms"], 0 if e["type"] == "DQ_END" else 1, e["dataset"], e["status"]))
    for i, e in enumerate(ev): e["seq"] = i
    return ev

# ============================================================================ dataset version
def dataset_version(cfg: Config, streams: list[str], quality: list[dict]) -> str:
    db = sqlite3.connect(str(cfg.state_db))
    h = hashlib.sha256()
    for ds, d, dig in db.execute("SELECT dataset, date, logical_digest FROM partitions ORDER BY dataset, date"):
        if ds in streams: h.update(f"{ds}|{d}|{dig}".encode())
    h.update(json.dumps([(e["type"], e["dataset"], e["status"], e["avail_ms"]) for e in quality]).encode())
    return h.hexdigest()

def config_hash(streams_cfg: dict, extra: dict) -> str:
    return hashlib.sha256(json.dumps({"streams": streams_cfg, **extra}, sort_keys=True).encode()).hexdigest()

# ============================================================================ checkpoints
CKPT_SCHEMA = """CREATE TABLE IF NOT EXISTS replay_checkpoints(
  name TEXT PRIMARY KEY, dataset_version TEXT, config_hash TEXT, range_json TEXT,
  last_avail INTEGER, last_rank INTEGER, last_seq INTEGER, sim_clock INTEGER, events_emitted INTEGER,
  consumer_state_json TEXT, status TEXT, created_utc TEXT, updated_utc TEXT)"""

class CheckpointStore:
    def __init__(self, path: Path):
        from .fsutil import connect
        self.db = connect(path, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute(CKPT_SCHEMA)
    def get(self, name):
        r = self.db.execute("SELECT * FROM replay_checkpoints WHERE name=?", (name,)).fetchone()
        return dict(r) if r else None
    def save(self, name, **f):
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if self.get(name) is None:
                f.update(name=name, created_utc=now, updated_utc=now)
                self.db.execute(f"INSERT INTO replay_checkpoints({','.join(f)}) VALUES({','.join('?' * len(f))})", list(f.values()))
            else:
                f["updated_utc"] = now
                self.db.execute(f"UPDATE replay_checkpoints SET {','.join(k + '=?' for k in f)} WHERE name=?", [*f.values(), name])
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK"); raise
    def delete(self, name): self.db.execute("DELETE FROM replay_checkpoints WHERE name=?", (name,))

# ============================================================================ engine
class ReplayEngine:
    """Deterministic replay with pause/resume/stop, speed, step-event, step-time, seek and checkpoints.

    Thread-safety: run() may execute in a background thread (start_background); the control methods
    (pause/resume/stop/set_speed/step_*/seek) are safe to call from another thread.
    """
    def __init__(self, cfg: Config, consumers: list[Consumer], start: str | int | None = None, end: str | int | None = None,
                 dates: list[str] | None = None, sessions: list[tuple[str, str]] | None = None,
                 streams: dict | None = None, name: str | None = None, speed: float = 0.0,
                 checkpoint: bool = True, data_root: Path | None = None, progress: Callable[[dict], None] | None = None):
        rc = cfg["replay"]
        self.cfg = cfg
        self.root = Path(data_root or cfg.data_root) / "normalized" / "provider=binance" / "market=usdm_perpetual" / f"instrument={cfg.symbol}"
        self.streams_cfg = {k: v for k, v in (streams or rc["streams"]).items() if v["enabled"]}
        for k, v in self.streams_cfg.items():
            if v["time_offset_ms"] + v["latency_ms"] >= DAY_MS or v["time_offset_ms"] + v["latency_ms"] < 0:
                raise ValueError(f"{k}: offset+latency must be in [0, 1 day)")
        self.stream_names = (["quality"] if rc["quality_events"] else []) + sorted(self.streams_cfg, key=lambda s: self.streams_cfg[s]["rank"])
        ranks = [self.streams_cfg[s]["rank"] for s in self.stream_names if s != "quality"]
        if len(set(ranks)) != len(ranks) or min(ranks) <= QUALITY_RANK: raise ValueError("stream ranks must be unique and > 0")
        self.consumers = consumers
        self.max_chunk = int(rc["max_events_per_chunk"]); self.ckpt_every = int(rc["checkpoint_every_events"])
        self.paced_span = int(rc["paced_chunk_sim_ms"])
        avail_days = self._available_days()
        self.start_ms = self._to_ms(start) if start is not None else day_start_ms(date.fromisoformat(avail_days[0]))
        self.end_ms = self._to_ms(end) if end is not None else day_start_ms(date.fromisoformat(avail_days[-1])) + DAY_MS
        self.dates = set(dates) if dates else None
        self.sessions = [(self._hm(a), self._hm(b)) for a, b in (sessions or [])]
        self.quality = load_quality_events(cfg, list(self.streams_cfg)) if "quality" in self.stream_names else []
        self.dataset_version = dataset_version(cfg, list(self.streams_cfg), self.quality)
        self.config_hash = config_hash(self.streams_cfg, {"start": self.start_ms, "end": self.end_ms, "dates": sorted(self.dates or []),
                                                          "sessions": self.sessions, "quality": "quality" in self.stream_names})
        self.name = name or f"replay-{self.config_hash[:12]}"
        self.ckpt = CheckpointStore(cfg.state_db) if checkpoint else None
        self.progress_cb = progress
        # state
        self.clock_ms = self.start_ms; self.last_key = None; self.emitted = 0; self.per_stream = {}
        self._since_ckpt = 0
        self._paused = threading.Event(); self._paused.set()   # set = running
        self._stop = threading.Event(); self._lock = threading.Lock()
        self._speed = float(speed); self._step_events = None; self._step_until = None
        self._seek_to = None; self._thread = None
        self.t_wall0 = None; self.sim0 = None; self.status = "created"

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _to_ms(x): return int(x) if isinstance(x, (int, np.integer)) else to_ms(parse_iso(x))
    @staticmethod
    def _hm(s):
        h, m = s.split(":"); return (int(h) * 60 + int(m)) * 60_000

    def _available_days(self) -> list[str]:
        p = self.root / "data_type=trades"
        days = sorted(d.name.split("=")[1] for d in p.iterdir() if d.is_dir()) if p.exists() else []
        if not days: raise FileNotFoundError(f"no normalised partitions under {self.root}")
        return days

    def _part(self, stream: str, d: str) -> Path:
        return self.root / f"data_type={stream}" / f"date={d}" / "part-000.parquet"

    # ------------------------------------------------------------------ day window assembly
    def _load_window(self, d: date):
        """All events with availability in [D, D+1) from every enabled stream, merged in key order."""
        w0 = day_start_ms(d); w1 = w0 + DAY_MS
        tables, avails, ranks, seqs, sids, rows = {}, [], [], [], [], []
        for sid, name in enumerate(self.stream_names):
            if name == "quality":
                q = [e for e in self.quality if w0 <= e["avail_ms"] < w1]
                if q:
                    avails.append(np.array([e["avail_ms"] for e in q], np.int64)); ranks.append(np.zeros(len(q), np.int8))
                    seqs.append(np.array([e["seq"] for e in q], np.int64)); sids.append(np.full(len(q), sid, np.int8))
                    rows.append(np.array([self.quality.index(e) for e in q], np.int64))
                continue
            sc = self.streams_cfg[name]; shift = sc["time_offset_ms"] + sc["latency_ms"]
            parts = []
            for back, dd in ((1, d - timedelta(days=1)), (0, d)):
                if back and shift == 0: continue
                p = self._part(name, str(dd))
                if not p.exists(): continue
                t = pq.read_table(p)
                av = t.column(sc["time_col"]).cast(pa.int64()).to_numpy() + shift
                m = (av >= w0) & (av < w1)
                if not m.any(): continue
                idx = np.nonzero(m)[0]
                parts.append((t.take(pa.array(idx)), av[idx], (dd - date(1970, 1, 1)).days * SEQ_DAY_MULT + idx))
            if not parts: continue
            t = pa.concat_tables([x[0] for x in parts]) if len(parts) > 1 else parts[0][0]
            av = np.concatenate([x[1] for x in parts]); sq = np.concatenate([x[2] for x in parts])
            tables[name] = t
            avails.append(av); ranks.append(np.full(len(av), sc["rank"], np.int8)); seqs.append(sq)
            sids.append(np.full(len(av), sid, np.int8)); rows.append(np.arange(len(av), dtype=np.int64))
        if not avails: return None
        av = np.concatenate(avails); rk = np.concatenate(ranks); sq = np.concatenate(seqs)
        si = np.concatenate(sids); rw = np.concatenate(rows)
        keep = (av >= self.start_ms) & (av < self.end_ms)
        if self.sessions:
            tod = av % DAY_MS; sm = np.zeros(len(av), bool)
            for a, b in self.sessions: sm |= (tod >= a) & (tod < b) if a < b else (tod >= a) | (tod < b)
            keep &= sm | (si == 0) if "quality" in self.stream_names else sm
        order = np.lexsort((sq, rk, av))
        order = order[keep[order]]
        keys = np.empty(len(order), KEY_DTYPE)
        keys["avail"], keys["rank"], keys["seq"], keys["stream"] = av[order], rk[order], sq[order], si[order]
        return keys, rw[order], tables

    # ------------------------------------------------------------------ controls
    def pause(self): self._paused.clear(); self.status = "paused"
    def resume(self): self._paused.set(); self.status = "running"
    def stop(self): self._stop.set(); self._paused.set()
    def set_speed(self, speed: float):
        with self._lock: self._speed = float(speed); self.t_wall0 = time.monotonic(); self.sim0 = self.clock_ms
    def step_events(self, n: int):
        """Deliver exactly n more events, then pause."""
        with self._lock: self._step_events = n; self._step_until = None
        self._paused.set()
    def step_time(self, ms: int):
        """Deliver every event with availability < clock + ms, then pause."""
        with self._lock: self._step_until = self.clock_ms + ms; self._step_events = None
        self._paused.set()
    def seek(self, t_ms: int | str):
        """Jump: the next delivered event is the first with availability >= t (skipped events are NOT delivered;
        consumers are notified via on_seek and must rebuild their own state)."""
        with self._lock: self._seek_to = self._to_ms(t_ms)
        self._paused.set()
    def start_background(self) -> threading.Thread:
        self._thread = threading.Thread(target=self.run, daemon=True); self._thread.start(); return self._thread

    # ------------------------------------------------------------------ checkpointing
    def _guard_and_restore(self, resume: bool):
        if not self.ckpt: return
        cp = self.ckpt.get(self.name)
        if cp is None or not resume:
            if cp is not None: self.ckpt.delete(self.name)
            return
        if cp["dataset_version"] != self.dataset_version:
            raise ReplayGuardError(f"checkpoint '{self.name}' was taken on dataset {cp['dataset_version'][:12]}, current is {self.dataset_version[:12]}")
        if cp["config_hash"] != self.config_hash:
            raise ReplayGuardError(f"checkpoint '{self.name}' config {cp['config_hash'][:12]} != current {self.config_hash[:12]}")
        if cp["last_avail"] is not None:
            self.last_key = (cp["last_avail"], cp["last_rank"], cp["last_seq"])
            self.clock_ms = cp["sim_clock"]
        self.emitted = cp["events_emitted"] or 0
        st = json.loads(cp["consumer_state_json"] or "{}")
        self.per_stream = st.pop("__engine_per_stream__", {})
        for c in self.consumers: c.set_state(st.get(c.name, {}))

    def _checkpoint(self, status: str):
        if not self.ckpt: return
        st = {c.name: c.get_state() for c in self.consumers}; st["__engine_per_stream__"] = self.per_stream
        lk = self.last_key or (None, None, None)
        self.ckpt.save(self.name, dataset_version=self.dataset_version, config_hash=self.config_hash,
                       range_json=json.dumps({"start": iso(self.start_ms), "end": iso(self.end_ms)}),
                       last_avail=lk[0], last_rank=lk[1], last_seq=lk[2], sim_clock=self.clock_ms,
                       events_emitted=self.emitted, consumer_state_json=json.dumps(st), status=status)
        self._since_ckpt = 0

    # ------------------------------------------------------------------ main loop
    def run(self, resume: bool = False, until: int | str | None = None) -> dict:
        self._guard_and_restore(resume)
        until_ms = self._to_ms(until) if until is not None else None
        ctx = {"name": self.name, "dataset_version": self.dataset_version, "config_hash": self.config_hash,
               "start_ms": self.start_ms, "end_ms": self.end_ms, "streams": self.stream_names, "resumed": resume and self.last_key is not None}
        for c in self.consumers: c.on_start(ctx)
        self.status = "running"; self.t_wall0 = time.monotonic(); self.sim0 = self.clock_ms; t_run0 = time.monotonic()
        self._emitted_at_start = self.emitted
        first_day = from_ms(self.last_key[0] if self.last_key else self.start_ms).date()
        last_day = from_ms(self.end_ms - 1).date()
        d = first_day
        try:
            while d <= last_day and not self._stop.is_set():
                if self.dates is not None and str(d) not in self.dates:
                    d += timedelta(days=1); continue
                win = self._load_window(d)
                if win is None:
                    d += timedelta(days=1); continue
                keys, rows, tables = win
                if self.last_key is not None:   # resume / continue: skip everything <= last delivered key
                    la, lr, ls = self.last_key
                    after = (keys["avail"] > la) | ((keys["avail"] == la) & ((keys["rank"] > lr) | ((keys["rank"] == lr) & (keys["seq"] > ls))))
                    keys, rows = keys[after], rows[after]
                pos = 0
                while pos < len(keys) and not self._stop.is_set():
                    self._paused.wait()
                    if self._stop.is_set(): break
                    with self._lock:
                        seek = self._seek_to; self._seek_to = None
                    if seek is not None:
                        self._checkpoint("seek")
                        for c in self.consumers: c.on_seek(seek)
                        self.last_key = (seek - 1, 127, 2 ** 62); self.clock_ms = max(self.clock_ms, seek)
                        if from_ms(seek).date() != d:
                            d = from_ms(seek).date() - timedelta(days=1); pos = len(keys); break
                        pos = int(np.searchsorted(keys["avail"], seek, side="left")); continue
                    end = min(len(keys), pos + self.max_chunk)
                    with self._lock: speed, step_n, step_t = self._speed, self._step_events, self._step_until
                    if step_n is not None: end = min(end, pos + step_n)
                    if step_t is not None: end = min(end, pos + int(np.searchsorted(keys["avail"][pos:end], step_t, side="left")))
                    if until_ms is not None: end = min(end, pos + int(np.searchsorted(keys["avail"][pos:end], until_ms, side="left")))
                    if speed > 0:
                        lim = keys["avail"][pos] + self.paced_span
                        end = min(end, pos + max(1, int(np.searchsorted(keys["avail"][pos:end], lim, side="right"))))
                    if end <= pos:   # a step/until boundary reached with nothing to deliver
                        if until_ms is not None and keys["avail"][pos] >= until_ms: self._stop.set(); break
                        with self._lock: self._step_events = None; self._step_until = None
                        self.pause(); continue
                    ck = keys[pos:end]; rw = rows[pos:end]
                    if speed > 0:   # pace on the simulation clock
                        target = self.t_wall0 + (int(ck["avail"][-1]) - self.sim0) / 1000.0 / speed
                        delay = target - time.monotonic()
                        if delay > 0: time.sleep(min(delay, 5.0))
                    self.clock_ms = max(self.clock_ms, int(ck["avail"][-1]))
                    chunk = EventChunk(ck, rw, tables, self.stream_names, self.clock_ms, self.quality)
                    t_c = time.monotonic()
                    for c in self.consumers: c.on_chunk(chunk)
                    metrics.observe("replay_consumer_chunk_s", time.monotonic() - t_c)
                    metrics.inc("replay_events", float(len(ck)))
                    metrics.gauge("replay_sim_clock_ms", float(self.clock_ms))
                    self.last_key = (int(ck["avail"][-1]), int(ck["rank"][-1]), int(ck["seq"][-1]))
                    n = end - pos; self.emitted += n; self._since_ckpt += n; pos = end
                    ids, cnt = np.unique(ck["stream"], return_counts=True)
                    for i, c_ in zip(ids, cnt): self.per_stream[self.stream_names[i]] = self.per_stream.get(self.stream_names[i], 0) + int(c_)
                    with self._lock:
                        if self._step_events is not None:
                            self._step_events -= n
                            if self._step_events <= 0: self._step_events = None; self.pause()
                        if self._step_until is not None and (pos >= len(keys) or keys["avail"][pos] >= self._step_until):
                            self._step_until = None; self.pause()
                    if self._since_ckpt >= self.ckpt_every: self._checkpoint("running")
                if not self._stop.is_set():
                    self._checkpoint("running")   # day boundary checkpoint
                    if self.progress_cb: self.progress_cb(self.metrics(t_run0, day=str(d)))
                d += timedelta(days=1)
        finally:
            self.status = "completed" if d > last_day else "stopped"
            self._checkpoint(self.status)
            ctx.update(self.metrics(t_run0)); ctx["status"] = self.status
            for c in self.consumers: c.on_stop(ctx)
        return ctx

    def metrics(self, t_run0: float, **extra) -> dict:
        el = time.monotonic() - t_run0
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # MB on Linux
        this_run = self.emitted - getattr(self, "_emitted_at_start", 0)
        return {"events": self.emitted, "events_this_run": this_run, "per_stream": dict(self.per_stream), "sim_clock": iso(self.clock_ms),
                "wall_s": round(el, 1), "events_per_s": round(this_run / el) if el else None,
                "peak_rss_mb": round(rss), "status": self.status, **extra}
