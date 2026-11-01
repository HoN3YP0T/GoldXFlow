"""Phase 5 tests: classification, exact aggregation, idempotency, finalisation (no look-ahead), sessions and DST,
snapshot/restore, chunk independence, live/replay parity of the processor, footprint store and reconciliation."""
import copy, json, sqlite3, sys
from datetime import date
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from goldflow import schemas as S
from goldflow.config import Config
from goldflow.normalize import build_table, const_cols, part_path, write_atomic, logical_digest
from goldflow.replay import ReplayEngine, ChainHasher, EventChunk, KEY_DTYPE, ReplayGuardError
from goldflow.processing import TradeProcessor, ProcessorSink, FootprintWriter, read_manifest, read_footprint, SessionCalendar
from goldflow.processing.aggregation import Rows, LevelArrays, bars_from_rows, ROW_COLS
from goldflow.processing.classify import classify_buyer_maker, tick_rule
from goldflow.processing.sessions import local_to_ms
from goldflow.processing.settings import ProcessingSettings
from goldflow.processing.reconcile import reconcile
from goldflow.timeutil import day_start_ms, DAY_MS, to_ms, parse_iso

REAL_CFG = json.loads((Path(__file__).resolve().parents[1] / "config/binance_xauusdt.json").read_text())
DAYS = [date(2026, 10, 23), date(2026, 10, 24), date(2026, 10, 25), date(2026, 10, 26)]   # Fri, Sat, Sun (UK DST ends), Mon
MS = lambda s: to_ms(parse_iso(s))


# ============================================================================ synthetic dataset
def _write(root, ds, d, schema, cols, cat):
    t = build_table(schema, cols); write_atomic(t, part_path(root, "XAUUSDT", ds, str(d)))
    cat.execute("INSERT INTO partitions(dataset,date,logical_digest) VALUES(?,?,?)", (ds, str(d), logical_digest(t)))


def make_dataset(tmp: Path, n_per_day=4000, partial_hour="2026-10-26T05:00:00.000Z", extra_times=None, processing=None) -> Config:
    raw = copy.deepcopy(REAL_CFG); raw["paths"] = {"data_root": "data", "state_db": "data/state/acquisition.sqlite"}
    raw["replay"]["max_events_per_chunk"] = 997; raw["replay"]["checkpoint_every_events"] = 1500
    if processing: raw["processing"].update(processing)
    cfg = Config(raw, tmp); (tmp / "data" / "state").mkdir(parents=True)
    cat = sqlite3.connect(cfg.state_db)
    cat.executescript("CREATE TABLE partitions(dataset TEXT, date TEXT, logical_digest TEXT);"
                      "CREATE TABLE hour_status(dataset TEXT, hour_utc TEXT, status TEXT, detail_json TEXT);"
                      "CREATE TABLE day_status(dataset TEXT, date TEXT, status TEXT, evidence_json TEXT);")
    root = tmp / "data"; rng = np.random.default_rng(11); tid = 5000; price = 410000
    for d in DAYS:
        d0 = day_start_ms(d)
        ts = np.sort(rng.integers(d0, d0 + DAY_MS, n_per_day)).astype(np.int64)
        if extra_times:
            ts = np.sort(np.concatenate([ts, [t for t in extra_times if d0 <= t < d0 + DAY_MS]])).astype(np.int64)
        n = len(ts)
        steps = rng.integers(-3, 4, n); px = (price + np.cumsum(steps)).astype(np.int64); price = int(px[-1]) if n else price
        lots = rng.integers(1, 5000, n).astype(np.int64)
        gaps = rng.integers(1, 4, n); ids = tid + np.cumsum(gaps); tid = (int(ids[-1]) if n else tid) + 10   # increasing, not contiguous
        bm = rng.random(n) < 0.5; null = rng.random(n) < 0.01
        flag = pa.array([None if z else bool(b) for b, z in zip(bm, null)], pa.bool_())
        side = np.where(null, "UNKNOWN", np.where(bm, "SELL", "BUY"))
        rec = rng.random(n) < 0.005
        _write(root, "trades", d, S.TRADE_SCHEMA, {**const_cols(n, "XAUUSDT"), "source_trade_id": ids,
               "aggregate_trade_id": [None] * n, "exchange_event_time_utc": ts, "transaction_time_utc": ts,
               "receive_time_utc": pa.nulls(n, S.TS), "price": px / 100.0, "quantity": lots / 1000.0,
               "quote_notional": px * lots / 1e5, "buyer_maker_flag": flag, "aggressor_side": side,
               "side_evidence": np.where(null, "UNKNOWN", "EXCHANGE_REPORTED"), "price_ticks": px, "quantity_lots": lots,
               "record_origin": np.where(rec, "RECOVERED_FROM_AGGTRADES", "ARCHIVE_TRADES"),
               "data_quality_status": np.where(rec, "RECOVERED", "VALID"), "raw_file_id": np.zeros(n, np.int32), "raw_row": np.arange(n)}, cat)
        # klines computed from the trades (as Binance would): the independent per-minute reference
        ot = d0 + np.arange(1440, dtype=np.int64) * 60_000; mi = (ts - d0) // 60_000
        cnt = np.bincount(mi, minlength=1440); vol = np.bincount(mi, weights=lots, minlength=1440).astype(np.int64)
        tb = np.bincount(mi, weights=np.where(~bm & ~null, lots, 0), minlength=1440).astype(np.int64)
        o = np.zeros(1440, np.int64); h = np.zeros(1440, np.int64); lo = np.zeros(1440, np.int64); c = np.zeros(1440, np.int64)
        for k in np.unique(mi):
            sel = px[mi == k]; o[k], h[k], lo[k], c[k] = sel[0], sel.max(), sel.min(), sel[-1]
        _write(root, "klines_1m", d, S.KLINE_SCHEMA, {**const_cols(1440, "XAUUSDT"), "open_time_utc": ot, "close_time_utc": ot + 59_999,
               "open": o / 100.0, "high": h / 100.0, "low": lo / 100.0, "close": c / 100.0, "volume": vol / 1000.0,
               "quote_volume": np.zeros(1440), "trade_count": cnt.astype(np.int64), "taker_buy_volume": tb / 1000.0,
               "taker_buy_quote_volume": np.zeros(1440), "volume_lots": vol, "data_quality_status": np.full(1440, "VALID"),
               "raw_file_id": np.zeros(1440, np.int32), "raw_row": np.arange(1440)}, cat)
        cat.execute("INSERT INTO day_status VALUES('trades',?, 'COMPLETE', '{}')", (str(d),))
    if partial_hour:
        cat.execute("INSERT INTO hour_status VALUES('trades',?, 'PARTIAL', '{}')", (partial_hour,))
    cat.commit(); cat.close()
    return cfg


def trades_of(cfg, d):
    return pq.read_table(part_path(cfg.data_root, "XAUUSDT", "trades", str(d)))


class Collect(ProcessorSink):
    name = "collect"
    def __init__(self): self.bars = {}; self.levels = {}; self.windows = []; self.clock_violations = 0; self.proc = None
    def on_start(self, ctx, proc): self.proc = proc
    def on_bars(self, iv, bars, levels, fu):
        self.bars.setdefault(iv, []).append(bars); self.levels.setdefault(iv, []).append(levels)
        if self.proc.clock is not None and (bars["available_at"] > self.proc.clock).any(): self.clock_violations += 1
    def on_window(self, s, lv):
        if self.proc.clock is not None and s["available_at_ms"] > self.proc.clock: self.clock_violations += 1
        self.windows.append(s)
    def all(self, iv, what="bars"):
        parts = (self.bars if what == "bars" else self.levels).get(iv, [])
        return {k: np.concatenate([p[k] for p in parts]) for k in parts[0]} if parts else {}


def run(cfg, name="p", sinks=None, resume=False, writer=True, replace=None, until=None, **kw):
    sinks = list(sinks or [])
    if writer: sinks.append(FootprintWriter(cfg.data_root, cfg.symbol, replace=(not resume) if replace is None else replace))
    p = TradeProcessor(cfg, sinks=sinks); h = ChainHasher()
    e = ReplayEngine(cfg, [h, p], name=name, **kw); r = e.run(resume=resume, until=until)
    return p, r, h


# ============================================================================ classification
def test_classification_exchange_semantics_and_unknown():
    s = classify_buyer_maker(pa.array([False, True, None], pa.bool_()))
    assert s.tolist() == [1, -1, 0]                     # false -> BUY, true -> SELL, missing -> UNKNOWN


def test_tick_rule_is_chunk_independent():
    p = np.array([100, 101, 101, 100, 100, 100, 102, 102], np.int64)
    whole, _, _ = tick_rule(p, None, 0)
    assert whole.tolist() == [0, 1, 1, -1, -1, -1, 1, 1]
    a, lp, ld = tick_rule(p[:3], None, 0); b, _, _ = tick_rule(p[3:], lp, ld)
    assert np.concatenate([a, b]).tolist() == whole.tolist()


# ============================================================================ aggregation structures
def _rand_rows(rng, n, buckets=5, levels=8):
    c = {k: rng.integers(0, 50, n).astype(np.int64) for k in ROW_COLS}
    c["bucket"] = rng.integers(0, buckets, n) * 1000; c["level"] = 400000 + rng.integers(0, levels, n)
    c["first_seq"] = c["last_seq"] = np.arange(n); return Rows(c)


def test_reduce_is_chunk_independent_and_merge_sorted_equals_reduce():
    rng = np.random.default_rng(0); r = _rand_rows(rng, 500)
    whole = r.reduce()
    parts = [r.take(slice(i, i + 77)).reduce() for i in range(0, 500, 77)]
    inc = Rows()
    for p in parts: inc = Rows.concat([inc, p]).reduce()
    for k in ROW_COLS: assert np.array_equal(whole[k], inc[k])
    # merge_sorted on time-ordered inputs equals a full reduce
    rs = r.take(np.argsort(r["bucket"], kind="stable")); a, b = rs.take(slice(0, 260)).reduce(), rs.take(slice(260, None)).reduce()
    m = a.merge_sorted(b); full = rs.reduce()
    for k in ROW_COLS: assert np.array_equal(m[k], full[k])


def test_bars_from_rows_hand_computed_ohlc():
    # one bar, trades (seq: price, lots, side): 0:101 BUY 5, 1:103 SELL 2, 2:100 BUY 1, 3:102 SELL 4
    p = np.array([101, 103, 100, 102]); q = np.array([5, 2, 1, 4]); b = np.array([1, 0, 1, 0]); n = 4
    r = Rows({"bucket": np.zeros(n, np.int64), "level": p, "buy": q * b, "sell": q * (1 - b), "unk": np.zeros(n), "cnt": np.ones(n),
              "bcnt": b, "scnt": 1 - b, "rcnt": np.zeros(n), "notional": p * q, "ibuy": np.zeros(n), "isell": np.zeros(n),
              "first_ts": np.arange(n), "last_ts": np.arange(n), "first_seq": np.arange(n), "last_seq": np.arange(n),
              "min_id": np.arange(n) + 10, "max_id": np.arange(n) + 10}).reduce()
    bar = bars_from_rows(r)
    assert (bar["open"][0], bar["high"][0], bar["low"][0], bar["close"][0]) == (101, 103, 100, 102)
    assert bar["buy"][0] == 6 and bar["sell"][0] == 6 and bar["cnt"][0] == 4 and bar["n_levels"][0] == 4
    assert bar["min_id"][0] == 10 and bar["max_id"][0] == 13 and bar["notional"][0] == int((p * q).sum())


def test_level_arrays_grow_both_ways_and_rebin_exact():
    la = LevelArrays(); rng = np.random.default_rng(3)
    a = _rand_rows(rng, 100, levels=5); b = _rand_rows(rng, 100, levels=5).with_key(level=_rand_rows(rng, 100)["level"] - 2000)
    c = _rand_rows(rng, 100).with_key(level=_rand_rows(rng, 100)["level"] + 5000)
    for x in (a, b, c): la.add_rows(x)
    tot = la.totals(); allr = Rows.concat([a, b, c])
    assert tot["buy"] == int(allr["buy"].sum()) and tot["cnt"] == int(allr["cnt"].sum())
    v = la.view(50)
    assert int(v["buy"].sum()) == tot["buy"] and (np.diff(v["level"]) > 0).all()
    rt = LevelArrays.from_state(la.to_state())
    assert rt.totals() == tot


# ============================================================================ sessions and DST
def test_session_bounds_across_dst_transitions():
    cal = SessionCalendar.from_config(REAL_CFG["sessions"])
    b = lambda n, d: tuple(map(lambda x: __import__("goldflow.timeutil", fromlist=["iso"]).iso(x), cal.session_bounds(n, d)))
    # London: BST (UTC+1) until 2026-10-25, GMT after; BST from 2026-03-29
    assert b("london", date(2026, 10, 23)) == ("2026-10-23T06:00:00.000Z", "2026-10-23T15:00:00.000Z")
    assert b("london", date(2026, 10, 26)) == ("2026-10-26T07:00:00.000Z", "2026-10-26T16:00:00.000Z")
    assert b("london", date(2026, 3, 27)) == ("2026-03-27T07:00:00.000Z", "2026-03-27T16:00:00.000Z")
    assert b("london", date(2026, 3, 30)) == ("2026-03-30T06:00:00.000Z", "2026-03-30T15:00:00.000Z")
    # New York: EDT (UTC-4) until 2026-11-01, EST after; EDT from 2026-03-08. Between 25 Oct and 1 Nov UK and US differ.
    assert b("new_york", date(2026, 10, 26)) == ("2026-10-26T12:20:00.000Z", "2026-10-26T21:00:00.000Z")
    assert b("new_york", date(2026, 11, 2)) == ("2026-11-02T13:20:00.000Z", "2026-11-02T22:00:00.000Z")
    assert b("new_york", date(2026, 3, 6)) == ("2026-03-06T13:20:00.000Z", "2026-03-06T22:00:00.000Z")
    assert b("new_york", date(2026, 3, 9)) == ("2026-03-09T12:20:00.000Z", "2026-03-09T21:00:00.000Z")
    assert b("asia", date(2026, 10, 26)) == ("2026-10-25T23:00:00.000Z", "2026-10-26T08:00:00.000Z")
    # trading day: 18:00 New York previous day; the trading day containing the US change is 23 hours or 25 hours long
    a, e = cal.trading_day_bounds(date(2026, 11, 2)); assert (e - a) == 24 * 3_600_000
    a, e = cal.trading_day_bounds(date(2026, 11, 1)); assert (e - a) == 25 * 3_600_000   # Sat 18:00 EDT -> Sun 18:00 EST
    a, e = cal.trading_day_bounds(date(2026, 3, 8)); assert (e - a) == 23 * 3_600_000


def test_nonexistent_local_time_is_refused():
    with pytest.raises(ValueError, match="DST gap"):
        local_to_ms(date(2026, 3, 8), 2 * 60 + 30, "America/New_York")


def test_weekend_flag_follows_regular_schedule():
    cal = SessionCalendar.from_config(REAL_CFG["sessions"])
    t = np.array([MS("2026-10-23T20:59:59Z"), MS("2026-10-23T21:00:00Z"), MS("2026-10-25T21:59:59Z"), MS("2026-10-25T22:00:00Z"),
                  MS("2026-10-24T12:00:00Z"), MS("2026-10-21T03:00:00Z")])
    assert cal.is_weekend(t).tolist() == [False, True, True, False, True, False]


def test_instances_cover_kinds_and_are_unique():
    cal = SessionCalendar.from_config(REAL_CFG["sessions"])
    inst = cal.instances(MS("2026-10-26T00:00:00Z"), MS("2026-10-26T23:59:59Z"))
    kinds = {(i.kind, i.name) for i in inst}
    assert {("utc_day", "utc_day"), ("trading_day", "trading_day"), ("trading_week", "trading_week"),
            ("session", "london"), ("session", "new_york"), ("session", "asia")} <= kinds
    assert len({i.key for i in inst}) == len(inst)
    wk = [i for i in inst if i.kind == "trading_week" and i.label == "2026-10-26"][0]
    assert wk.start_ms == MS("2026-10-25T22:00:00Z")      # Sunday 18:00 New York (EDT)


# ============================================================================ settings validation
@pytest.mark.parametrize("patch,msg", [({"intervals": ["2s", "3s"]}, "multiple"), ({"intervals": ["1m", "13m"]}, "divide one UTC day"),
                                       ({"persist_intervals": ["1h"], "intervals": ["1m"]}, "not in processing.intervals"),
                                       ({"intervals": ["1m", "4h"]}, "straddles"), ({"footprint_bin_ticks": 0}, ">= 1")])
def test_invalid_settings_fail_at_startup(patch, msg):
    raw = copy.deepcopy(REAL_CFG); raw["processing"].update(patch)
    with pytest.raises(ValueError, match=msg): ProcessingSettings.from_config(Config(raw, Path(".")))


# ============================================================================ processor on replay
def test_totals_reconcile_exactly_per_day_and_sides_never_reassigned(tmp_path):
    cfg = make_dataset(tmp_path); col = Collect()
    p, r, _ = run(cfg, sinks=[col])
    assert r["status"] == "completed"
    for iv in p.st.intervals:
        b = col.all(iv)
        assert (b["buy_lots"] + b["sell_lots"] + b["unknown_lots"] == b["volume_lots"]).all()
        assert (b["delta_lots"] == b["buy_lots"] - b["sell_lots"]).all()
        for d in DAYS:
            t = trades_of(cfg, d); m = (b["bar_start"] >= day_start_ms(d)) & (b["bar_start"] < day_start_ms(d) + DAY_MS)
            q = t.column("quantity_lots").to_numpy(); side = np.asarray(t.column("aggressor_side").cast(pa.string()).to_pylist())
            assert int(b["volume_lots"][m].sum()) == int(q.sum())
            assert int(b["buy_lots"][m].sum()) == int(q[side == "BUY"].sum())
            assert int(b["sell_lots"][m].sum()) == int(q[side == "SELL"].sum())
            assert int(b["unknown_lots"][m].sum()) == int(q[side == "UNKNOWN"].sum()) > 0   # unknown stays unknown
            assert int(b["trade_count"][m].sum()) == t.num_rows
            assert int(b["recovered_count"][m].sum()) == int(np.sum(np.asarray(t.column("record_origin").cast(pa.string()).to_pylist()) == "RECOVERED_FROM_AGGTRADES"))
    assert p.stats["side_mismatch_vs_phase3"] == 0 and p.stats["duplicates_ignored"] == 0
    assert col.clock_violations == 0


def test_footprint_store_and_reconciliation_against_klines(tmp_path):
    cfg = make_dataset(tmp_path)
    run(cfg)
    rep = reconcile(cfg)
    assert rep["summary"]["pass"] == len(DAYS) and not rep["summary"]["fail"]
    assert rep["summary"]["complete_days_checked_against_klines"] == len(DAYS)
    for x in rep["days"]: assert all(v == 0 for v in x["mismatches"].values())
    man = read_manifest(cfg.data_root, "XAUUSDT", "1m")
    assert sorted(man["dates"]) == [str(d) for d in DAYS] and all(v["date_complete"] for v in man["dates"].values())
    assert not list((tmp_path / "data" / "derived").rglob("_staging/*.parquet"))   # staging cleaned up
    # tamper with one bar -> reconciliation fails
    bars, levels = read_footprint(cfg.data_root, "XAUUSDT", "1m", "2026-10-26")
    v = bars.column("volume_lots").to_numpy().copy(); v[100] += 1
    i = bars.schema.get_field_index("volume_lots")
    write_atomic(bars.set_column(i, bars.schema.field(i), pa.array(v)), tmp_path / "data/derived/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/footprint/interval=1m/date=2026-10-26/bars.parquet")
    rep = reconcile(cfg)
    assert rep["summary"]["fail"] == ["2026-10-26"]


def test_bars_finalised_only_after_their_end_no_look_ahead(tmp_path):
    cfg = make_dataset(tmp_path); col = Collect()
    until = MS("2026-10-26T10:30:30.500Z")
    p, r, _ = run(cfg, sinks=[col], until=until)
    assert r["status"] == "stopped" and col.clock_violations == 0
    for iv in p.st.intervals:
        b = col.all(iv); I = p.st.interval_ms[iv]
        assert (b["bar_end"] <= p.clock).all() and (b["available_at"] == b["bar_end"]).all()
        dev, _ = p.developing_bars(iv)
        if dev:
            assert not dev["is_final"].any() and (dev["bar_start"] + I > p.clock).all()
    # the 10:30 1m bar is still developing; 10:29 is final
    assert MS("2026-10-26T10:29:00Z") in col.all("1m")["bar_start"]
    assert MS("2026-10-26T10:30:00Z") not in col.all("1m")["bar_start"]
    lw = p.window("session", "london")
    assert lw is not None and lw[0]["is_final"] is False


def test_idempotent_redelivery_and_late_and_unverifiable(tmp_path):
    cfg = make_dataset(tmp_path)
    p = TradeProcessor(cfg)
    t = np.array([1000, 2000, 3000, 4000], np.int64) + MS("2026-10-26T10:00:00Z")
    ids = np.array([10, 20, 30, 40]); px = np.full(4, 400000); q = np.array([1, 2, 3, 4]); side = np.array([1, -1, 1, 0], np.int8)
    rec = np.zeros(4, bool)
    p.ingest(t, ids, px, q, side, rec); d1 = p.dev["1m"].c["cnt"].sum(), p.dev["1m"].c["buy"].sum()
    p.ingest(t, ids, px, q, side, rec)                                     # same events again
    assert (p.dev["1m"].c["cnt"].sum(), p.dev["1m"].c["buy"].sum()) == d1 and p.stats["duplicates_ignored"] == 4
    p.ingest(t[:2], np.array([25, 25]), px[:2], q[:2], side[:2], rec[:2])  # late new id (twice in one chunk)
    assert p.stats["late_accepted"] == 1 and p.stats["duplicates_ignored"] == 5 and p.stats["trades_processed"] == 5
    # fill the dedup ring so its floor moves past id 10, then deliver an unseen old id
    W = p.st.dedup_window_ids; big = np.arange(1000, 1000 + W, dtype=np.int64)
    p.ingest(np.full(W, t[-1]), big, np.full(W, 400000), np.ones(W, np.int64), np.ones(W, np.int8), np.zeros(W, bool))
    p.ingest(t[:1], np.array([15]), px[:1], q[:1], side[:1], rec[:1])
    assert p.stats["unverifiable_dropped"] == 1


def test_snapshot_restore_equals_uninterrupted(tmp_path):
    cfg = make_dataset(tmp_path)
    p0, _, h0 = run(cfg, "full")
    m0 = read_manifest(cfg.data_root, "XAUUSDT", "1m")["dates"]
    dig0 = p0.state_digest()
    for cut in ("2026-10-23T09:17:42.123Z", "2026-10-24T00:00:00.000Z", "2026-10-25T13:00:00.000Z", "2026-10-26T23:00:00.000Z"):
        _, r1, _ = run(cfg, "cut", until=MS(cut))
        assert r1["status"] == "stopped"
        p2, r2, h2 = run(cfg, "cut", resume=True)
        assert r2["status"] == "completed" and h2.digest == h0.digest
        assert p2.state_digest() == dig0, cut
        m2 = read_manifest(cfg.data_root, "XAUUSDT", "1m")["dates"]
        assert {d: (v["bars_logical_digest"], v["levels_logical_digest"]) for d, v in m2.items()} == \
               {d: (v["bars_logical_digest"], v["levels_logical_digest"]) for d, v in m0.items()}, cut


def test_chunk_size_does_not_change_results(tmp_path):
    cfg = make_dataset(tmp_path)
    p1, _, _ = run(cfg, "a", writer=False)
    cfg.raw["replay"]["max_events_per_chunk"] = 53
    p2, _, _ = run(cfg, "b", writer=False)
    assert p1.state_digest() == p2.state_digest()


def test_same_processor_for_live_style_chunks(tmp_path):
    """A live bus delivers EventChunks too. Feeding hand-built chunks (different chunking, no replay engine) gives the
    same state as replay, so analytics can't tell history from live."""
    cfg = make_dataset(tmp_path, partial_hour=None)      # no DQ events: the hand-built live chunks carry none
    p_rep, _, _ = run(cfg, "rep", writer=False)
    p_live = TradeProcessor(cfg)
    p_live.on_start({"start_ms": day_start_ms(DAYS[0])})
    for d in DAYS:
        t = trades_of(cfg, d); ts = t.column("exchange_event_time_utc").cast(pa.int64()).to_numpy()
        kl = pq.read_table(part_path(cfg.data_root, "XAUUSDT", "klines_1m", str(d)))
        kav = kl.column("close_time_utc").cast(pa.int64()).to_numpy() + 1
        av = np.concatenate([ts, kav]); st = np.concatenate([np.ones(len(ts), np.int8), np.zeros(len(kav), np.int8)])
        rows = np.concatenate([np.arange(len(ts)), np.arange(len(kav))])
        o = np.lexsort((st, av))   # klines first at equal times, like replay ranks
        av, st, rows = av[o], st[o], rows[o]
        keep = av < day_start_ms(DAYS[-1]) + DAY_MS
        av, st, rows = av[keep], st[keep], rows[keep]
        for i in range(0, len(av), 1234):
            k = np.empty(len(av[i:i + 1234]), KEY_DTYPE); k["avail"] = av[i:i + 1234]; k["stream"] = st[i:i + 1234]
            k["rank"] = 0; k["seq"] = 0
            p_live.on_chunk(EventChunk(k, rows[i:i + 1234], {"klines_1m": kl, "trades": t}, ["klines_1m", "trades"], int(k["avail"][-1])))
    p_live.on_stop({"status": "completed", "end_ms": day_start_ms(DAYS[-1]) + DAY_MS})
    assert p_live.stats["trades_processed"] == p_rep.stats["trades_processed"]
    for iv in p_rep.st.intervals:
        assert p_live.bars_finalised[iv] == p_rep.bars_finalised[iv]
    strip = lambda w: [{k: v for k, v in x.items()} for x in w]
    assert strip(p_live.window_log) == strip(p_rep.window_log)


def test_sessions_respect_dst_in_processor(tmp_path):
    # trades at 06:30 UTC: inside London on Fri 23 Oct (BST, opens 06:00 UTC) but not on Mon 26 Oct (GMT, opens 07:00 UTC)
    extra = [MS("2026-10-23T06:30:00.000Z"), MS("2026-10-26T06:30:00.000Z")]
    cfg = make_dataset(tmp_path, n_per_day=0, extra_times=extra, partial_hour=None); col = Collect()
    run(cfg, sinks=[col], writer=False)
    lon = {w["label"]: w for w in col.windows if w["kind"] == "session" and w["name"] == "london"}
    assert lon["2026-10-23"]["trade_count"] == 1 and lon["2026-10-26"]["trade_count"] == 0
    assert lon["2026-10-23"]["start_utc"] == "2026-10-23T06:00:00.000Z" and lon["2026-10-26"]["start_utc"] == "2026-10-26T07:00:00.000Z"
    asia = {w["label"]: w for w in col.windows if w["name"] == "asia"}
    assert asia["2026-10-26"]["trade_count"] == 1               # 06:30 UTC Monday = 15:30 Tokyo
    days = {w["label"]: w for w in col.windows if w["kind"] == "utc_day"}
    assert days["2026-10-24"]["trade_count"] == 0 and days["2026-10-24"]["coverage"] == "FULL"
    # every finalised window is emitted no earlier than its end
    assert col.clock_violations == 0


def test_weekend_and_dq_labels_on_bars(tmp_path):
    cfg = make_dataset(tmp_path); col = Collect()
    run(cfg, sinks=[col], writer=False)
    b = col.all("1h")
    sat = (b["bar_start"] >= MS("2026-10-24T00:00:00Z")) & (b["bar_start"] < MS("2026-10-25T00:00:00Z"))
    assert b["weekend"][sat].all()
    mon = (b["bar_start"] >= MS("2026-10-26T00:00:00Z")); assert not b["weekend"][mon].any()
    dq = dict(zip(b["bar_start"].tolist(), b["dq_status"].tolist()))
    assert dq[MS("2026-10-26T05:00:00Z")] == "PARTIAL" and dq[MS("2026-10-26T06:00:00Z")] == "OK"
    m1 = col.all("1m"); s = m1["bar_start"]
    assert set(m1["dq_status"][(s >= MS("2026-10-26T05:00:00Z")) & (s < MS("2026-10-26T06:00:00Z"))]) == {"PARTIAL"}


def test_inferred_side_is_separate_and_off_by_default(tmp_path):
    cfg = make_dataset(tmp_path); col0 = Collect()
    run(cfg, "a", sinks=[col0], writer=False)
    b0 = col0.all("1h"); assert int(b0["inferred_buy_lots"].sum() + b0["inferred_sell_lots"].sum()) == 0
    cfg2 = make_dataset(tmp_path / "x", processing={"inferred_side": {"enabled": True, "method": "tick_rule"}}); col1 = Collect()
    run(cfg2, "b", sinks=[col1], writer=False)
    b1 = col1.all("1h")
    for k in ("buy_lots", "sell_lots", "unknown_lots", "volume_lots"): assert np.array_equal(b0[k], b1[k])   # never mixed in
    assert 0 < int((b1["inferred_buy_lots"] + b1["inferred_sell_lots"]).sum()) <= int(b1["unknown_lots"].sum())


def test_resume_refused_when_processing_settings_change(tmp_path):
    cfg = make_dataset(tmp_path)
    run(cfg, "g", until=MS("2026-10-24T12:00:00Z"))
    cfg.raw["processing"]["footprint_bin_ticks"] = 10
    with pytest.raises(ReplayGuardError, match="processing settings"):
        run(cfg, "g", resume=True)


def test_markets_are_never_merged(tmp_path):
    cfg = make_dataset(tmp_path)
    p = TradeProcessor(cfg)
    t = trades_of(cfg, DAYS[0]); i = t.schema.get_field_index("instrument_id")
    other = pa.DictionaryArray.from_arrays(pa.array(np.zeros(t.num_rows, np.int8)), pa.array(["binance:usdm_perpetual:PAXGUSDT"]))
    with pytest.raises(ValueError, match="never merged"):
        p._columns(t.set_column(i, t.schema.field(i), other))


def test_seek_discards_developing_and_marks_partial_start(tmp_path):
    cfg = make_dataset(tmp_path); col = Collect()
    p = TradeProcessor(cfg, sinks=[col]); h = ChainHasher()
    e = ReplayEngine(cfg, [h, p], name="sk"); e.run(until=MS("2026-10-24T10:00:30Z"))
    e.seek(MS("2026-10-26T10:00:30Z")); e._stop.clear(); e.run(resume=True)
    assert p.stats["seeks"] == 1 and p.stats["developing_discarded_on_seek"] > 0
    h1 = col.all("1h"); m1 = col.all("1m")
    assert h1["coverage"][h1["bar_start"] == MS("2026-10-26T10:00:00Z")][0] == "PARTIAL_START"   # began mid-bar
    assert not ((h1["bar_start"] > MS("2026-10-24T09:00:00Z")) & (h1["bar_start"] < MS("2026-10-26T10:00:00Z"))).any()
    assert set(m1["coverage"][m1["bar_start"] >= MS("2026-10-26T10:01:00Z")]) == {"FULL"}


def test_invalid_processing_config_fails_at_config_load(tmp_path):
    """Phase 1 integration: Config.load validates the Phase 5 blocks at startup (ConfigError -> CLI exit 2)."""
    from goldflow.settings import ConfigError
    raw = copy.deepcopy(REAL_CFG); raw["sessions"]["sessions"][1]["tz"] = "Europe/Atlantis"
    (tmp_path / "config").mkdir(); p = tmp_path / "config" / "c.json"; p.write_text(json.dumps(raw))
    with pytest.raises(ConfigError, match="processing/sessions"):
        Config.load(p)
    raw = copy.deepcopy(REAL_CFG); p.write_text(json.dumps(raw))
    assert Config.load(p).symbol == "XAUUSDT"
