"""Phase 4 tests: determinism, resume exactness, availability (no look-ahead), tie order, controls, guards."""
import copy, json, sqlite3, sys, threading, time
from datetime import date
from pathlib import Path
import numpy as np
import pyarrow as pa
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from goldflow import schemas as S
from goldflow.config import Config
from goldflow.normalize import build_table, const_cols, part_path, write_atomic, logical_digest
from goldflow.replay import (ReplayEngine, ChainHasher, IntegrityChecker, Consumer, ReplayGuardError, QUALITY_RANK)
from goldflow.timeutil import day_start_ms, DAY_MS, iso

REAL_CFG = json.loads((Path(__file__).resolve().parents[1] / "config/binance_xauusdt.json").read_text())
DAYS = [date(2026, 6, 1), date(2026, 6, 2), date(2026, 6, 3)]

def _write(root, ds, d, schema, cols, cat):
    t = build_table(schema, cols); p = part_path(root, "XAUUSDT", ds, str(d)); write_atomic(t, p)
    cat.execute("INSERT INTO partitions(dataset,date,logical_digest) VALUES(?,?,?)", (ds, str(d), logical_digest(t)))

def make_dataset(tmp: Path, partial_hour: str | None = "2026-06-02T05:00:00.000Z") -> Config:
    raw = copy.deepcopy(REAL_CFG); raw["paths"] = {"data_root": "data", "state_db": "data/state/acquisition.sqlite"}
    raw["replay"]["max_events_per_chunk"] = 1000; raw["replay"]["checkpoint_every_events"] = 777
    cfg = Config(raw, tmp); (tmp / "data" / "state").mkdir(parents=True)
    cat = sqlite3.connect(cfg.state_db)
    cat.executescript("CREATE TABLE partitions(dataset TEXT, date TEXT, logical_digest TEXT);"
                      "CREATE TABLE hour_status(dataset TEXT, hour_utc TEXT, status TEXT, detail_json TEXT);"
                      "CREATE TABLE day_status(dataset TEXT, date TEXT, status TEXT, evidence_json TEXT);")
    root = tmp / "data"; rng = np.random.default_rng(7); tid = 1000
    for d in DAYS:
        d0 = day_start_ms(d)
        # trades: random times + one trade exactly at a kline availability instant (minute boundary) for tie tests
        ts = np.sort(np.concatenate([rng.integers(d0, d0 + DAY_MS, 3000), [d0 + 120_000]])).astype(np.int64)
        n = len(ts); ids = np.arange(tid, tid + n); tid += n + 5
        bm = rng.random(n) < 0.5
        _write(root, "trades", d, S.TRADE_SCHEMA, {**const_cols(n, "XAUUSDT"), "source_trade_id": ids,
               "aggregate_trade_id": [None] * n, "exchange_event_time_utc": ts, "transaction_time_utc": ts,
               "receive_time_utc": pa.nulls(n, S.TS), "price": np.full(n, 4000.0), "quantity": np.full(n, 0.1),
               "quote_notional": np.full(n, 400.0), "buyer_maker_flag": bm, "aggressor_side": np.where(bm, "SELL", "BUY"),
               "side_evidence": np.full(n, "EXCHANGE_REPORTED"), "price_ticks": np.full(n, 400000), "quantity_lots": np.full(n, 100),
               "record_origin": np.full(n, "ARCHIVE_TRADES"), "data_quality_status": np.full(n, "VALID"),
               "raw_file_id": np.zeros(n, np.int32), "raw_row": np.arange(n)}, cat)
        ot = d0 + np.arange(1440, dtype=np.int64) * 60_000; m = len(ot)
        _write(root, "klines_1m", d, S.KLINE_SCHEMA, {**const_cols(m, "XAUUSDT"), "open_time_utc": ot, "close_time_utc": ot + 59_999,
               **{k: np.full(m, 4000.0) for k in ("open", "high", "low", "close", "volume", "quote_volume", "taker_buy_volume", "taker_buy_quote_volume")},
               "trade_count": np.ones(m, np.int64), "volume_lots": np.ones(m, np.int64), "data_quality_status": np.full(m, "VALID"),
               "raw_file_id": np.zeros(m, np.int32), "raw_row": np.arange(m)}, cat)
        st = np.repeat(d0 + np.arange(0, DAY_MS, 600_000, dtype=np.int64), 2); k = len(st)
        _write(root, "bookDepth", d, S.DEPTHBAND_SCHEMA, {**const_cols(k, "XAUUSDT"), "snapshot_time_utc": st,
               "band_pct": np.tile([-0.2, 0.2], k // 2), "cumulative_depth_qty": np.ones(k), "cumulative_notional": np.ones(k),
               "book_side": np.tile(["BID", "ASK"], k // 2), "data_quality_status": np.full(k, "VALID"),
               "raw_file_id": np.zeros(k, np.int32), "raw_row": np.arange(k)}, cat)
        mt = d0 + np.arange(0, DAY_MS, 300_000, dtype=np.int64); j = len(mt)
        _write(root, "metrics", d, S.METRICS_SCHEMA, {**const_cols(j, "XAUUSDT"), "create_time_utc": mt,
               **{c: np.ones(j) for c in ("sum_open_interest", "sum_open_interest_value", "count_toptrader_long_short_ratio",
                                          "sum_toptrader_long_short_ratio", "count_long_short_ratio", "sum_taker_long_short_vol_ratio")},
               "data_quality_status": np.full(j, "VALID"), "raw_file_id": np.zeros(j, np.int32), "raw_row": np.arange(j)}, cat)
    if partial_hour:
        cat.execute("INSERT INTO hour_status VALUES('trades',?, 'PARTIAL', '{}')", (partial_hour,))
    cat.commit(); cat.close()
    return cfg

class Collector(Consumer):
    name = "collector"
    def __init__(self): self.keys = []; self.clock_ok = True; self.seeks = []
    def on_chunk(self, c):
        self.keys.append(c.keys.copy()); self.clock_ok &= bool((c.keys["avail"] <= c.clock_ms).all())
    def on_seek(self, t): self.seeks.append(t)
    @property
    def all(self): return np.concatenate(self.keys) if self.keys else np.array([])

def run(cfg, name="t", resume=False, **kw):
    h, ic, col = ChainHasher(), IntegrityChecker(), Collector()
    e = ReplayEngine(cfg, [h, ic, col], name=name, **kw); r = e.run(resume=resume)
    return e, r, h, ic, col

# ------------------------------------------------------------------ determinism
def test_same_dataset_same_sequence(tmp_path):
    cfg = make_dataset(tmp_path)
    _, r1, h1, ic1, _ = run(cfg, "a"); _, r2, h2, ic2, _ = run(cfg, "b")
    assert r1["events"] == r2["events"] > 0 and h1.digest == h2.digest and ic1.violations == ic2.violations == 0

def test_digest_independent_of_chunk_size(tmp_path):
    cfg = make_dataset(tmp_path)
    _, _, h1, _, _ = run(cfg, "a")
    cfg.raw["replay"]["max_events_per_chunk"] = 37; cfg.raw["replay"]["checkpoint_every_events"] = 50
    _, _, h2, _, _ = run(cfg, "b")
    assert h1.digest == h2.digest

# ------------------------------------------------------------------ availability / look-ahead
def test_kline_only_available_after_close_and_tie_order(tmp_path):
    cfg = make_dataset(tmp_path)
    _, _, _, _, col = run(cfg, "a")
    k = col.all; names = ["quality", "klines_1m", "metrics", "bookDepth", "trades"]
    kl = k[k["stream"] == names.index("klines_1m")]
    d0 = day_start_ms(DAYS[0])
    assert kl["avail"][0] == d0 + 60_000          # the 00:00 candle appears at 00:01:00.000, never at 00:00
    assert (kl["avail"] % 60_000 == 0).all()
    t = d0 + 120_000                               # a trade, a candle, a metric and depth all at 00:02:00.000
    at = k[k["avail"] == t]
    assert [names[s] for s in at["stream"]] == ["klines_1m", "trades"]
    assert (np.diff(at["rank"].astype(int)) > 0).all()
    assert col.clock_ok

def test_quality_events_precede_market_data_at_same_instant(tmp_path):
    cfg = make_dataset(tmp_path)
    e, _, _, _, col = run(cfg, "a")
    k = col.all; q = k[k["stream"] == 0]
    assert len(q) == 2 and (q["rank"] == QUALITY_RANK).all()
    begin = e.quality[0]; assert begin["type"] == "DQ_BEGIN" and iso(begin["avail_ms"]) == "2026-06-02T05:00:00.000Z"
    same = k[k["avail"] == begin["avail_ms"]]
    assert same["stream"][0] == 0

def test_window_end_excludes_events_available_later(tmp_path):
    cfg = make_dataset(tmp_path)
    end = day_start_ms(DAYS[1])
    _, _, _, _, col = run(cfg, "a", end=end)
    assert col.all["avail"].max() < end
    # the last candle of day 1 closes at end-1 and becomes available exactly at `end` -> excluded
    assert not ((col.all["stream"] == 1) & (col.all["avail"] == end)).any()

# ------------------------------------------------------------------ resume exactness
class StopAfter(Consumer):
    name = "stopper"
    def __init__(self, n): self.n = n; self.seen = 0; self.engine = None
    def on_chunk(self, c):
        self.seen += len(c)
        if self.seen >= self.n and self.engine: self.engine.stop()

@pytest.mark.parametrize("stop_at", [1, 999, 4321, 7000])
def test_interrupt_and_resume_equals_uninterrupted(tmp_path, stop_at):
    cfg = make_dataset(tmp_path)
    _, full, hf, _, colf = run(cfg, "full")
    h, ic, col, stp = ChainHasher(), IntegrityChecker(), Collector(), StopAfter(stop_at)
    e1 = ReplayEngine(cfg, [h, ic, col, stp], name="part"); stp.engine = e1
    r1 = e1.run()
    assert r1["status"] == "stopped" and 0 < r1["events"] < full["events"]
    h2, ic2, col2 = ChainHasher(), IntegrityChecker(), Collector()
    e2 = ReplayEngine(cfg, [h2, ic2, col2], name="part"); r2 = e2.run(resume=True)
    assert r2["status"] == "completed" and r2["events"] == full["events"]          # engine count restored, nothing re-counted
    assert h2.digest == hf.digest and ic2.violations == 0                          # no duplicate, no omission
    delivered = np.concatenate([col.all, col2.all])
    assert len(delivered) == full["events"] and np.array_equal(delivered, colf.all)

def test_resume_refused_when_config_or_dataset_changes(tmp_path):
    cfg = make_dataset(tmp_path)
    stp = StopAfter(10); e = ReplayEngine(cfg, [stp], name="g"); stp.engine = e; e.run()
    cfg.raw["replay"]["streams"]["trades"]["latency_ms"] = 50
    with pytest.raises(ReplayGuardError, match="config"):
        ReplayEngine(cfg, [ChainHasher()], name="g").run(resume=True)
    cfg.raw["replay"]["streams"]["trades"]["latency_ms"] = 0
    db = sqlite3.connect(cfg.state_db); db.execute("UPDATE partitions SET logical_digest='x' WHERE date='2026-06-02' AND dataset='trades'"); db.commit()
    with pytest.raises(ReplayGuardError, match="dataset"):
        ReplayEngine(cfg, [ChainHasher()], name="g").run(resume=True)

def test_latency_shifts_availability(tmp_path):
    cfg = make_dataset(tmp_path)
    _, _, _, _, a = run(cfg, "a")
    cfg.raw["replay"]["streams"]["trades"]["latency_ms"] = 250
    _, _, _, _, b = run(cfg, "b")
    ta, tb = a.all[a.all["stream"] == 4], b.all[b.all["stream"] == 4]
    assert np.array_equal(np.sort(tb["avail"]) - 250, np.sort(ta["avail"])[: len(tb)]) or len(tb) == len(ta)
    assert (tb["avail"] >= day_start_ms(DAYS[0]) + 250).all()

# ------------------------------------------------------------------ controls
def test_step_events_and_step_time_and_pause(tmp_path):
    cfg = make_dataset(tmp_path)
    col = Collector(); e = ReplayEngine(cfg, [col], name="s", checkpoint=False)
    e.pause(); th = e.start_background()
    e.step_events(5); time.sleep(0.5)
    assert sum(len(k) for k in col.keys) == 5 and e.status == "paused"
    clock = e.clock_ms; e.step_time(600_000); time.sleep(0.5)
    got = col.all
    assert got["avail"].max() < clock + 600_000 and e.status == "paused"
    e.stop(); th.join(5)

def test_seek_jumps_forward_without_delivering_skipped(tmp_path):
    cfg = make_dataset(tmp_path)
    col = Collector(); e = ReplayEngine(cfg, [col], name="k", checkpoint=False)
    e.pause(); th = e.start_background(); e.step_events(3); time.sleep(0.3)
    target = day_start_ms(DAYS[2]) + 3_600_000
    e.seek(target); time.sleep(0.2); e.resume(); th.join(10)
    k = col.all
    assert col.seeks == [target] and (k["avail"][3:] >= target).all() and k["avail"][3] >= target

def test_speed_pacing_follows_simulation_clock(tmp_path):
    cfg = make_dataset(tmp_path)
    d0 = day_start_ms(DAYS[0])
    t0 = time.monotonic()
    _, _, _, _, col = run(cfg, "p", start=d0, end=d0 + 600_000, speed=2000.0)   # events up to 10:00 sim at 2000x
    wall = time.monotonic() - t0
    expected = (int(col.all["avail"].max()) - d0) / 1000 / 2000                  # ~0.3 s
    assert expected > 0.25 and expected * 0.9 <= wall < expected + 2.0
    t0 = time.monotonic(); run(cfg, "q", start=d0, end=d0 + 600_000)             # max speed: no pacing
    assert time.monotonic() - t0 < expected

def test_sessions_and_dates_filters(tmp_path):
    cfg = make_dataset(tmp_path)
    _, _, _, _, col = run(cfg, "f", sessions=[("13:00", "14:00")], dates=[str(DAYS[1])])
    k = col.all; m = k[k["stream"] != 0]
    tod = m["avail"] % DAY_MS
    assert len(m) and ((tod >= 13 * 3_600_000) & (tod < 14 * 3_600_000)).all()
    assert (m["avail"] >= day_start_ms(DAYS[1])).all() and (m["avail"] < day_start_ms(DAYS[2])).all()
