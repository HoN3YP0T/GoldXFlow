"""Phase 7 tests: hand-computed delta paths, CVD resets, imbalances, taker orders, quantile thresholds, the event
lifecycle (no back-dating), absorption / exhaustion / divergence rules, and the orderflow engine on replay (no
look-ahead, side uncertainty, determinism across restore / chunking / live-style chunks, verification and tamper)."""
import copy, json, sys
from fractions import Fraction
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_processing import DAYS, MS, REAL_CFG, make_dataset, trades_of  # noqa: E402

from goldflow.config import Config  # noqa: E402
from goldflow.normalize import write_atomic  # noqa: E402
from goldflow.orderflow.absorption import AbsorptionDetector  # noqa: E402
from goldflow.orderflow.big_trades import (EDGES, RollingQuantile, TakerOrderBuilder, TimeOfDayQuantile,  # noqa: E402
                                           group_orders, quantile_edge, size_bin, taker_breaks)
from goldflow.orderflow.cvd import PathAgg  # noqa: E402
from goldflow.orderflow.divergence import DivergenceDetector  # noqa: E402
from goldflow.orderflow.events import Tracker, event_row  # noqa: E402
from goldflow.orderflow.exhaustion import ExhaustionDetector  # noqa: E402
from goldflow.orderflow.flow_engine import OrderflowEngine  # noqa: E402
from goldflow.orderflow.flow_settings import OrderflowSettings  # noqa: E402
from goldflow.orderflow.flow_store import OrderflowStore, orderflow_root, read_manifest, read_table  # noqa: E402
from goldflow.orderflow.flow_verify import verify  # noqa: E402
from goldflow.orderflow.imbalance import imbalances, rebin_levels  # noqa: E402
from goldflow.processing import TradeProcessor  # noqa: E402
from goldflow.replay import KEY_DTYPE, ChainHasher, EventChunk, ReplayEngine, ReplayGuardError  # noqa: E402
from goldflow.settings import ConfigError, validate_config  # noqa: E402
from goldflow.timeutil import DAY_MS, day_start_ms  # noqa: E402

I64 = np.int64
LOOSE = {  # small synthetic days: thresholds that let every detector fire
    "big_trades": {"rolling_quantile": {"enabled": True, "q": "0.98", "hours": 6, "min_samples": 200},
                   "time_of_day": {"enabled": True, "q": "0.9", "bucket": "30m", "lookback_days": 2, "min_samples": 5},
                   "min_qty_lots": 9000},
    "absorption": {"zone_quantile": "0.6", "min_samples": 30, "quantile_hours": 6, "min_active_seconds": 2, "max_progress_bins": 3,
                   "tol_bins": 1, "bin_ticks": 2, "confirm_bins": 2, "confirm_window": "10m"},
    "exhaustion": {"pushes": 2, "lookback_bars": 3, "bin_ticks": 2, "confirm_bins": 2},
    "divergence": {"swing_bars": 2, "bin_ticks": 2, "inval_bins": 2, "track_bars": 12},
    "imbalance": {"bin_ticks": 2, "min_lots": 500, "ratio": "2"},
}  # fmt: skip


def loosen(cfg):
    o = cfg.raw["orderflow"]
    for k, v in LOOSE.items():
        if isinstance(v, dict):
            for kk, vv in v.items():
                o[k][kk] = copy.deepcopy(vv)
    return cfg


# ============================================================================ delta paths and CVD
def test_path_agg_reset_and_continuous_hand_computed():
    s = np.array([0, 1000, 2000, 60_000, 61_000], I64); d = np.array([5, -8, 2, 3, -1], I64)
    a = PathAgg(60_000, reset=True); a.add(s, d)
    assert a.pop(0) == [0, 0, 5, -3, -1]          # path 5, -3, -1 (open 0 included)
    assert a.pop(60_000) == [60_000, 0, 3, 0, 2]
    c = PathAgg(60_000, reset=False); c.add(s, d)
    assert c.pop(0) == [0, 0, 5, -3, -1] and c.pop(60_000) == [60_000, -1, 2, -1, 1]   # continues from -1
    # split delivery equals one-shot delivery
    for reset in (True, False):
        one = PathAgg(60_000, reset); one.add(s, d)
        two = PathAgg(60_000, reset); two.add(s[:2], d[:2]); two.add(s[2:], d[2:])
        assert one.state() == two.state()


# ============================================================================ imbalance
def lv(rows):
    b, l, buy, sell = (np.array(x, I64) for x in zip(*rows))
    z = np.zeros(len(b), I64)
    return {"bar_start": b, "level": l, "buy_lots": buy, "sell_lots": sell, "unknown_lots": z,
            "first_trade_id": np.arange(len(b)) + 10, "last_trade_id": np.arange(len(b)) + 20}


def test_diagonal_imbalance_hand_computed_and_zero_policy():
    # one bar, bins 100..103 (bin_ticks 1). buy(101)=900 vs sell(100)=300 -> 3.0 >= 3 -> BUY at 101
    r = rebin_levels(lv([(0, 100, 50, 300), (0, 101, 900, 40), (0, 102, 100, 2000), (0, 103, 10, 0)]), 1)
    imb, st = imbalances(r, "diagonal", Fraction(3), 100, "none", 3)
    got = {(int(s), int(p), int(x), int(o)) for s, p, x, o in zip(imb["side"], imb["bin"], imb["lots"], imb["opposite_lots"])}
    # sell(102)=2000 vs buy(103)=10 -> SELL at 102; sell(101)=40 < 100 min; buy(103)=10 < min; buy(100) vs empty 99 -> none
    assert got == {(1, 101, 900, 300), (-1, 102, 2000, 10)} and not len(st["bar_start"])
    imb2, _ = imbalances(r, "diagonal", Fraction(3), 40, "any", 3)        # empty comparison bins now count
    assert (1, 100) in {(int(s), int(p)) for s, p in zip(imb2["side"], imb2["bin"])}
    h, _ = imbalances(r, "horizontal", Fraction(3), 100, "none", 3)       # same price: buy(101)=900 vs sell(101)=40
    assert {(int(s), int(p)) for s, p in zip(h["side"], h["bin"])} == {(1, 101), (-1, 100), (-1, 102)}
    # bins of 10 ticks: 1-tick rows are re-summed exactly
    r10 = rebin_levels(lv([(0, 1000, 5, 1), (0, 1009, 5, 1), (0, 1010, 1, 7)]), 10)
    assert r10["bin"].tolist() == [100, 101] and r10["buy"].tolist() == [10, 1] and r10["sell"].tolist() == [2, 7]


def test_stacked_imbalance_needs_adjacent_bins():
    rows = [(0, 100, 0, 100)] + [(0, 101 + i, 1000, 100) for i in range(3)] + [(0, 105, 1000, 100)]
    imb, st = imbalances(rebin_levels(lv(rows), 1), "diagonal", Fraction(3), 100, "none", 3)
    # buys at 101, 102, 103 (adjacent run of 3); 105 vs empty 104 -> not an imbalance, so the run stops at 103
    assert st["low_bin"].tolist() == [101] and st["high_bin"].tolist() == [103] and st["n_bins"].tolist() == [3]
    assert st["lots"].tolist() == [3000]


# ============================================================================ taker orders and quantiles
def test_taker_orders_rules_and_chunk_independence():
    t = np.array([5, 5, 5, 5, 5, 6, 6, 9], I64); p = np.array([100, 101, 103, 102, 102, 90, 90, 95], I64)
    sd = np.array([1, 1, 1, 1, -1, 0, 0, 1], np.int8); q = np.array([1, 2, 3, 4, 5, 6, 7, 8], I64); ids = np.arange(8, dtype=I64) + 100
    new = taker_breaks(t, p, sd, 0, 25); new[0] = True
    g = group_orders(t, ids, p, q, sd, new)
    # 100,101,103 (buy, rising) | 102 steps against the buyer -> new order | sell | unknown pair | later buy
    assert g["lots"].tolist() == [6, 4, 5, 13, 8] and g["n_fills"].tolist() == [3, 1, 1, 2, 1]
    assert taker_breaks(np.array([1, 1]), np.array([100, 130]), np.array([1, 1], np.int8), 0, 25)[1]   # step > max_step
    for cut in range(1, 8):
        b = TakerOrderBuilder(0, 25); out = []
        for sl in (slice(0, cut), slice(cut, 8)):
            r = b.ingest(t[sl], ids[sl], p[sl], q[sl], sd[sl])
            if r: out += r[0]["lots"].tolist()
        assert b.flush(9) is None                     # time 9 + 0 is not < floor 9: the last order may still grow
        out += b.flush(10)[0]["lots"].tolist()
        assert out == [6, 4, 5, 13, 8], cut


def test_histogram_edges_are_exact_and_quantile_is_nearest_rank():
    for k in (1, 37, 64, 640, 2000):
        e = int(EDGES[1024 + k])                      # smallest integer >= 1024 * 2**(k/64)
        assert e**64 >= 1024**64 * 2**k and (e - 1) ** 64 < 1024**64 * 2**k
    assert size_bin(np.array([1, 1023, 1024, 1035, 1036])).tolist() == [1, 1023, 1024, 1024, 1025]
    h = np.zeros(len(EDGES), I64); h[[3, 5, 9]] = [5, 4, 1]   # sizes 3 x5, 5 x4, 9 x1 -> n = 10
    assert quantile_edge(h, Fraction(1, 2), 1) == (3, 10)      # rank ceil(5) = 5 -> 3
    assert quantile_edge(h, Fraction(9, 10), 1) == (5, 10)     # rank 9 -> 5
    assert quantile_edge(h, Fraction(95, 100), 1) == (9, 10)   # rank ceil(9.5) = 10 -> 9
    assert quantile_edge(h, Fraction(1, 2), 11) == (None, 10)  # below min_samples: unavailable, not guessed


def test_rolling_quantile_uses_only_the_past():
    rq = RollingQuantile(3 * 60_000, Fraction(1, 2), 1)
    rq.add(0, np.array([10, 10])); rq.add(60_000, np.array([20])); rq.add(120_000, np.array([30, 30, 30]))
    assert rq.threshold(120_000) == (10, 3)           # minutes 0 and 1 only (not the current minute 2)
    assert rq.threshold(180_000) == (20, 6)           # [0, 3): 10,10,20,30,30,30 -> nearest rank ceil(3) = 3 -> 20
    with pytest.raises(AssertionError):
        rq.add(60_000, np.array([1]))                 # a sample in a minute the window already passed
    assert rq.threshold(240_000) == (30, 4)           # minute 0 left the window
    tq = TimeOfDayQuantile(30 * 60_000, 2, Fraction(1, 2), 1)
    tq.add(0, False, np.array([5])); tq.add(DAY_MS, False, np.array([7, 7]))
    assert tq.threshold(2 * DAY_MS, False) == (7, 3) and tq.threshold(DAY_MS, False) == (5, 1)
    assert tq.threshold(2 * DAY_MS + 30 * 60_000, False) == (None, 0)   # other bucket: no history


def test_rolling_quantile_threshold_definition():
    # documented above: [0, 3 min) holds 10,10,20,30,30,30; nearest rank ceil(0.5 * 6) = 3 -> 20
    rq = RollingQuantile(3 * 60_000, Fraction(1, 2), 1)
    rq.add(0, np.array([10, 10])); rq.add(60_000, np.array([20])); rq.add(120_000, np.array([30, 30, 30]))
    rq.threshold(120_000)
    assert rq.threshold(180_000) == (20, 6)


# ============================================================================ event lifecycle
def mk_event(direction=-1, confirm=90, inval=110, end=60_000, deadline=360_000):
    side = "BUY" if direction < 0 else "SELL"
    row = event_row("ABSORPTION", "CANDIDATE", side, "1m", 1, 0, end, end, 0, 95, 105, 95, confirm, inval, deadline, 10, 1, 2, 0.0,
                    "OK", "FULL", {})
    return Tracker(row, direction)


def test_tracker_confirm_invalidate_ambiguous_expire_and_no_backdating():
    s = np.array([0, 30_000, 60_000, 61_000, 62_000], I64)
    hi = np.array([200, 200, 100, 100, 100], I64); lo = np.array([0, 0, 95, 89, 95], I64)
    r = mk_event().scan(s, hi, lo, 1000, 63_000, 0)
    # bars before the evidence end (0 s, 30 s) are ignored even though they touch both levels
    assert r["status"] == "CONFIRMED" and r["event_time"] == 62_000 and r["resolution_price_ticks"] == 89
    assert r["event_time"] >= r["evidence_end"]
    hi2 = hi.copy(); hi2[3] = 111
    r2 = mk_event().scan(s, hi2, lo, 1000, 63_000, 0)
    assert r2["status"] == "INVALIDATED" and r2["reason"] == "both_in_same_base_bar"
    t = mk_event(deadline=120_000)
    assert t.scan(s[:3], hi[:3], lo[:3], 1000, 61_000, 0) is None          # not final yet: keep waiting
    r3 = t.scan(np.array([60_000], I64), np.array([100], I64), np.array([95], I64), 1000, 120_000, 0)
    assert r3["status"] == "EXPIRED" and r3["event_time"] == 120_000
    up = mk_event(direction=1, confirm=110, inval=90).scan(s, hi, lo, 1000, 63_000, 0)
    assert up["status"] == "INVALIDATED" and up["resolution_price_ticks"] == 89 and up["reason"] is None


# ============================================================================ detectors on hand-built bars
def settings(**patch):
    raw = copy.deepcopy(REAL_CFG)
    for blk, v in patch.items():
        raw["orderflow"][blk].update(v)
    return OrderflowSettings.from_config(Config(raw, Path(".")))


def bar(s, hi, lo, buy=100, sell=100, unk=0, interval="1m", I=60_000):
    return {"bar_start": s, "bar_end": s + I, "available_at": s + I, "interval": interval, "high_ticks": hi, "low_ticks": lo,
            "volume_lots": buy + sell + unk, "buy_lots": buy, "sell_lots": sell, "unknown_lots": unk, "delta_lots": buy - sell,
            "unknown_share": unk / (buy + sell + unk), "first_trade_id": s, "last_trade_id": s + 1, "dq_status": "OK", "coverage": "FULL"}


def base_levels(rows):
    s, l, buy, sell = (np.array(x, I64) for x in zip(*rows))
    return {"bar_start": s, "level": l, "buy_lots": buy, "sell_lots": sell, "first_trade_id": s, "last_trade_id": s + 1}


def test_absorption_rules_and_single_trade_is_never_absorption():
    fs = settings(absorption={"min_zone_lots": 1000, "zone_quantile": None, "tol_bins": 0, "bin_ticks": 1, "max_progress_bins": 1,
                              "min_active_seconds": 3, "max_second_share": "0.5"})
    det = AbsorptionDetector(fs.absorption, 60_000, 1000, fs.max_unknown_share, 0)
    # buyers hit 1000 repeatedly over 4 seconds; the high is 1001 (1 bin of progress allowed)
    det.add_base_levels(base_levels([(0, 1000, 400, 0), (1000, 1000, 400, 0), (2000, 1000, 300, 0), (3000, 1001, 50, 0)]))
    out = det.evaluate(bar(0, 1001, 1000, buy=1150, sell=0))
    assert len(out) == 1 and out[0].row["side"] == "BUY" and out[0].row["lots"] == 1100 and out[0].direction == -1
    assert out[0].row["confirm_ticks"] == 1000 - fs.absorption.confirm_bins and out[0].row["invalidate_ticks"] == 1002
    # one big order: all lots in one second -> never absorption, however big
    det.add_base_levels(base_levels([(60_000, 1000, 5000, 0), (61_000, 1000, 10, 0), (62_000, 1000, 10, 0)]))
    assert det.evaluate(bar(60_000, 1000, 1000, buy=5020, sell=0)) == [] and det.stats["rejected_single_burst"] == 1
    # price ran 3 bins beyond the zone: progress -> no absorption
    det.add_base_levels(base_levels([(120_000 + i * 1000, 1000, 400, 0) for i in range(4)] + [(125_000, 1003, 1, 0)]))
    assert det.evaluate(bar(120_000, 1003, 1000, buy=1601, sell=0)) == [] and det.stats["rejected_progress"] == 1
    # too much unknown-side volume: the bar is not evaluated at all
    det.add_base_levels(base_levels([(180_000 + i * 1000, 1000, 400, 0) for i in range(4)]))
    assert det.evaluate(bar(180_000, 1000, 1000, buy=1600, sell=0, unk=1000)) == [] and det.stats["suppressed_unknown_side"] == 1


def test_exhaustion_needs_rising_extremes_falling_volume_and_cvd_divergence():
    fs = settings(exhaustion={"pushes": 3, "lookback_bars": 2, "bin_ticks": 1, "confirm_bins": 4, "inval_bins": 1})
    ex = ExhaustionDetector(fs.exhaustion, 60_000, fs.max_unknown_share, 0)
    seq = [(0, 100, 95, 50, 10), (1, 103, 98, 900, 30), (2, 105, 100, 600, 40), (3, 107, 101, 300, 35)]
    out = []
    for i, hi, lo, buy, cvd in seq:
        out += ex.evaluate(bar(i * 60_000, hi, lo, buy=buy, sell=100), cvd, "td:1")
    assert len(out) == 1
    e = out[0].row
    assert e["event_type"] == "EXHAUSTION" and e["side"] == "BUY" and e["reference_ticks"] == 107 and e["evidence_end"] == 240_000
    assert e["confirm_ticks"] == 103 and e["invalidate_ticks"] == 108 and json.loads(e["detail"])["pushes"][0]["bar_start"] == 60_000
    ex2 = ExhaustionDetector(fs.exhaustion, 60_000, fs.max_unknown_share, 0)       # CVD keeps rising: no divergence
    assert not [x for i, hi, lo, buy, cvd in seq for x in ex2.evaluate(bar(i * 60_000, hi, lo, buy=buy, sell=100), i * 10, "td:1")]


def test_divergence_confirms_after_right_bars_and_needs_contiguous_swings():
    fs = settings(divergence={"swing_bars": 2, "bin_ticks": 1, "inval_bins": 1, "track_bars": 10, "max_gap_bars": 20})
    I = 300_000
    dv = DivergenceDetector(fs.divergence, I, fs.max_unknown_share, 0)
    highs = [100, 101, 105, 102, 101, 103, 107, 104, 103]
    cvds = [0, 5, 50, 30, 20, 25, 40, 30, 25]       # second pivot high 107 > 105 while CVD 40 < 50
    out = []
    for i, (h, c) in enumerate(zip(highs, cvds)):
        out += [(i, x) for x in dv.evaluate(bar(i * I, h, h - 10, interval="5m", I=I), c, "td:1")]
    assert len(out) == 1 and out[0][0] == 8                  # known only once bar 8 (= pivot 6 + 2) closed
    e = out[0][1].row
    assert e["side"] == "BEARISH" and e["status"] == "CONFIRMED" and e["detected_at"] == 9 * I and e["evidence_start"] == 2 * I
    assert e["invalidate_ticks"] == 108 and e["deadline"] == 9 * I + 10 * I
    dv2 = DivergenceDetector(fs.divergence, I, fs.max_unknown_share, 0)       # a missing bar breaks the swing window
    assert not [x for i, (h, c) in enumerate(zip(highs, cvds)) if i != 7
                for x in dv2.evaluate(bar(i * I, h, h - 10, interval="5m", I=I), c, "td:1")]


def test_invalid_orderflow_settings_fail_at_config_load():
    for patch, msg in [({"imbalance": {"ratio": 3.0}}, "decimal string"), ({"imbalance": {"interval": "2m"}}, "not in processing"),
                       ({"absorption": {"max_second_share": "1"}}, "single burst"), ({"divergence": {"cvd_reset": "session"}}, "continuous"),
                       ({"big_trades": {"combine": "most"}}, "any"), ({"absorption": {"zone_quantile": "1.5"}}, "zone_quantile")]:  # fmt: skip
        raw = copy.deepcopy(REAL_CFG)
        for blk, v in patch.items():
            raw["orderflow"][blk].update(v)
        with pytest.raises(ConfigError, match=msg):
            validate_config(raw, None, check_discovery=False)
    raw = copy.deepcopy(REAL_CFG); raw["orderflow"]["typo"] = 1
    with pytest.raises(ConfigError, match="unknown keys"):
        validate_config(raw, None, check_discovery=False)


# ============================================================================ engine on replay
class Listen:
    def __init__(self):
        self.rows = {}; self.violations = 0; self.proc = None
    def __call__(self, table, rows):
        if self.proc is not None and self.proc.clock is not None and any(r["available_at"] > self.proc.clock for r in rows):
            self.violations += 1
        self.rows.setdefault(table, []).extend(rows)


def orun(cfg, name="o", resume=False, until=None, store=True, listen=None, sinks=(), **kw):
    lis = [listen] if listen else []
    eng = OrderflowEngine(cfg, store=OrderflowStore(cfg.data_root, cfg.symbol, replace=not resume) if store else None, listeners=lis)
    p = TradeProcessor(cfg, sinks=[eng, *sinks])
    if listen: listen.proc = p
    r = ReplayEngine(cfg, [ChainHasher(), p], name=name, **kw).run(resume=resume, until=until)
    return eng, p, r


def bursts(day_ms, n=40, k=6, seed=3):
    rng = np.random.default_rng(seed)
    return [int(t) for t in rng.integers(day_ms, day_ms + DAY_MS, n) for _ in range(k)]


@pytest.fixture(scope="module")
def base(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("flow")
    extra = [t for d in DAYS for t in bursts(day_start_ms(d))]
    cfg = loosen(make_dataset(tmp, n_per_day=3000, extra_times=extra))
    lis = Listen()
    eng, p, r = orun(cfg, "base", listen=lis)
    return cfg, eng, p, r, lis


def test_engine_outputs_no_look_ahead_and_side_uncertainty(base):
    cfg, eng, p, r, lis = base
    assert r["status"] == "completed" and lis.violations == 0
    s = eng.stats
    assert s["flow_bars"] > 0 and s["cvd_rows"] > 0 and s["imbalances"] > 0 and s["big_trades"] > 0
    ev = lis.rows["events"]
    kinds = {e["event_type"] for e in ev}
    assert kinds == {"ABSORPTION", "EXHAUSTION", "DIVERGENCE"}, eng.counts
    assert all(e["event_time"] >= e["evidence_end"] and e["available_at"] == e["event_time"] for e in ev)
    # side uncertainty: unknown lots are carried and never moved into buy/sell
    bars = [b for b in lis.rows["bars"] if b["interval"] == "1m"]
    assert sum(b["unknown_lots"] for b in bars) > 0
    assert all(b["buy_lots"] + b["sell_lots"] + b["unknown_lots"] == b["volume_lots"] and b["delta_lots"] == b["buy_lots"] - b["sell_lots"]
               and b["delta_low"] <= 0 <= b["delta_high"] for b in bars)
    tot = sum(int(pa.compute.sum(trades_of(cfg, d).column("quantity_lots")).as_py()) for d in DAYS)
    assert sum(b["volume_lots"] for b in bars) == tot
    assert any(b["side"] == "UNKNOWN" for b in lis.rows["big_trades"]) or s["taker_orders_unknown_side"] > 0
    assert all(len(b["trade_ids"]) == b["n_fills"] and b["trade_ids"][0] == b["first_trade_id"] for b in lis.rows["big_trades"])


def test_processor_state_is_unchanged_by_the_orderflow_sink(base):
    cfg, eng, p, r, lis = base
    p2 = TradeProcessor(cfg)
    ReplayEngine(cfg, [ChainHasher(), p2], name="bare").run()
    assert p2.state_digest() == p.state_digest()


def test_cvd_resets_at_sessions_across_dst(base):
    cfg, eng, p, r, lis = base
    starts = {c["series_key"]: c["series_start"] for c in lis.rows["cvd"] if c["series"] == "session:london"}
    assert starts["session:london:2026-10-23"] == MS("2026-10-23T06:00:00Z")      # BST
    assert starts["session:london:2026-10-26"] == MS("2026-10-26T07:00:00Z")      # GMT after the 25 Oct change
    first = [c for c in lis.rows["cvd"] if c["series_key"] == "session:london:2026-10-26" and c["interval"] == "1m"]
    first.sort(key=lambda c: c["bar_start"])
    assert first[0]["cvd_open"] == 0 and first[0]["bar_start"] >= MS("2026-10-26T07:00:00Z")


def test_verification_passes_and_detects_tampering(base):
    cfg, eng, p, r, lis = base
    rep = verify(cfg, report=False)
    s = rep["summary"]
    assert s["fail"] == [] and s["pass"] == len(DAYS), rep["dates"]
    assert s["big_trades_checked"] > 0 and s["resolutions_rederived"] > 0 and s["candidate_evidence_checked"] > 0
    assert s["no_backdating_violations"] == 0 and rep["catalogue_totals"]
    root = orderflow_root(cfg.data_root, cfg.symbol)
    for table, col in (("big_trades", "lots"), ("cvd", "cvd_close"), ("bars", "delta_high"), ("imbalances", "lots")):
        d = next(dd for dd in sorted(read_manifest(cfg.data_root, cfg.symbol)["dates"]) if read_table(cfg.data_root, cfg.symbol, table, dd).num_rows)
        f = root / f"date={d}" / f"{table}.parquet"
        t = pq.read_table(f); orig = t
        cov = t.column("coverage").to_pylist() if "coverage" in t.column_names else ["FULL"] * t.num_rows
        v = t.column(col).to_pylist(); i = next(k for k, x in enumerate(v) if x is not None and cov[k] == "FULL"); v[i] += 1
        j = t.schema.get_field_index(col)
        write_atomic(t.set_column(j, t.schema.field(j), pa.array(v, t.schema.field(j).type)), f)
        try:
            assert verify(cfg, dates=[d], report=False)["summary"]["fail"] == [d], table
        finally:
            write_atomic(orig, f)


def test_snapshot_restore_and_chunking_do_not_change_results(tmp_path):
    extra = [t for d in DAYS for t in bursts(day_start_ms(d), n=20)]
    cfg = loosen(make_dataset(tmp_path, n_per_day=1500, extra_times=extra))
    e0, _, _ = orun(cfg, "full")
    dig0 = e0.state_digest()
    man = lambda: {d: {t: v[t]["logical_digest"] for t in ("bars", "cvd", "imbalances", "stacked", "big_trades", "events")}
                   for d, v in read_manifest(cfg.data_root, cfg.symbol)["dates"].items()}  # noqa: E731
    m0 = man()
    for cut in ("2026-10-23T09:17:42.123Z", "2026-10-25T00:00:00.500Z"):
        _, _, r1 = orun(cfg, "cut", until=MS(cut))
        assert r1["status"] == "stopped"
        e2, _, r2 = orun(cfg, "cut", resume=True)
        assert r2["status"] == "completed" and e2.state_digest() == dig0, cut
        assert man() == m0, cut
    cfg.raw["replay"]["max_events_per_chunk"] = 61
    e3, _, _ = orun(cfg, "small", store=False)
    assert e3.state_digest() == dig0
    cfg.raw["replay"]["max_events_per_chunk"] = 997
    cfg.raw["orderflow"]["imbalance"]["ratio"] = "4"
    orun(cfg, "chg", until=MS("2026-10-24T00:00:00Z"), store=False)
    cfg.raw["orderflow"]["imbalance"]["ratio"] = "2"
    with pytest.raises(ReplayGuardError, match="orderflow settings"):
        orun(cfg, "chg", resume=True, store=False)


def test_live_style_chunks_equal_replay(tmp_path):
    cfg = loosen(make_dataset(tmp_path, n_per_day=1200, partial_hour=None))
    l1 = Listen(); e1, _, _ = orun(cfg, "a", store=False, listen=l1)
    l3 = Listen(); e3 = OrderflowEngine(cfg, listeners=[l3]); p3 = TradeProcessor(cfg, sinks=[e3])
    p3.on_start({"start_ms": day_start_ms(DAYS[0])})
    for d in DAYS:
        t = trades_of(cfg, d); ts = t.column("exchange_event_time_utc").cast(pa.int64()).to_numpy()
        rows = np.arange(len(ts))
        for i in range(0, len(ts), 333):
            k = np.empty(len(ts[i:i + 333]), KEY_DTYPE); k["avail"] = ts[i:i + 333]; k["stream"] = 0; k["rank"] = 0; k["seq"] = 0
            p3.on_chunk(EventChunk(k, rows[i:i + 333], {"trades": t}, ["trades"], int(k["avail"][-1])))
    p3.on_stop({"status": "completed", "end_ms": day_start_ms(DAYS[-1]) + DAY_MS})
    key = lambda L, t: sorted(json.dumps(r, sort_keys=True, default=str) for r in L.rows.get(t, []))  # noqa: E731
    for t in ("bars", "cvd", "imbalances", "stacked", "big_trades", "events"):
        assert key(l3, t) == key(l1, t), t


def test_seek_discards_open_state_and_marks_partial_start(tmp_path):
    cfg = loosen(make_dataset(tmp_path, n_per_day=1500))
    lis = Listen(); eng = OrderflowEngine(cfg, listeners=[lis]); p = TradeProcessor(cfg, sinks=[eng]); lis.proc = p
    e = ReplayEngine(cfg, [ChainHasher(), p], name="sk"); e.run(until=MS("2026-10-24T10:00:30Z"))
    e.seek(MS("2026-10-26T10:00:00Z")); e._stop.clear(); e.run(resume=True)
    assert eng.stats["seeks"] == 1 and lis.violations == 0
    after = [c for c in lis.rows["cvd"] if c["bar_start"] >= MS("2026-10-26T10:00:00Z") and c["series"] == "utc_day:utc_day"]
    assert after and all(c["coverage"] == "PARTIAL_START" for c in after)
    assert not [b for b in lis.rows["bars"] if MS("2026-10-24T10:01:00Z") < b["bar_start"] < MS("2026-10-26T10:00:00Z")]
