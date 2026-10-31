"""Phase 7 tests: delta/CVD, footprint imbalance, taker orders and Big Trades, absorption, exhaustion, divergence,
no-look-ahead timing, traceability, determinism, snapshot/restore and the catalogue/validation outputs."""

from __future__ import annotations

import copy
import json
import math
import random
import sys
from fractions import Fraction
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_processing import MS, REAL_CFG, make_dataset, trades_of  # noqa: E402

from goldflow.config import Config  # noqa: E402
from goldflow.orderflow import OrderflowAnalytics, OrderflowSettings, RollingQuantile, compute_imbalances  # noqa: E402
from goldflow.orderflow.absorption import AbsorptionDetector  # noqa: E402
from goldflow.orderflow.big_trades import BigTradeDetector, TakerOrderBuilder  # noqa: E402
from goldflow.orderflow.cvd import ResetCVD  # noqa: E402
from goldflow.orderflow.divergence import DivergenceDetector  # noqa: E402
from goldflow.orderflow.exhaustion import ExhaustionDetector  # noqa: E402
from goldflow.orderflow.report import summarize_events  # noqa: E402
from goldflow.orderflow.rolling import DailyBucketBaseline  # noqa: E402
from goldflow.orderflow.settings import DEFAULT_ORDERFLOW  # noqa: E402
from goldflow.orderflow.validate import _imbalances_ref  # noqa: E402
from goldflow.processing import SessionCalendar, TradeProcessor  # noqa: E402
from goldflow.replay import ReplayEngine, ReplayGuardError  # noqa: E402

MIN = 60_000
T0 = MS("2026-10-26T10:00:00Z")  # Monday, London and New York sessions


# ------------------------------------------------------------------------------------------------- helpers
def run_of(cfg, name="o", resume=False, until=None, **kw):
    of = OrderflowAnalytics(cfg)
    proc = TradeProcessor(cfg, sinks=[of])
    eng = ReplayEngine(cfg, [proc], name=name, **kw)
    res = eng.run(resume=resume, until=until)
    return of, proc, res


def bars_block(rows, interval_ms=MIN):
    """Bar columns from (start, high, low, buy, sell, unk[, first_id, last_id]) tuples."""
    cols = {
        k: []
        for k in (
            "bar_start",
            "bar_end",
            "available_at",
            "high_ticks",
            "low_ticks",
            "buy_lots",
            "sell_lots",
            "unknown_lots",
            "first_trade_id",
            "last_trade_id",
            "dq_status",
            "weekend",
            "coverage",
        )
    }
    for i, r in enumerate(rows):
        s, hi, lo, b, se, u = r[:6]
        cols["bar_start"].append(s)
        cols["bar_end"].append(s + interval_ms)
        cols["available_at"].append(s + interval_ms)
        cols["high_ticks"].append(hi)
        cols["low_ticks"].append(lo)
        cols["buy_lots"].append(b)
        cols["sell_lots"].append(se)
        cols["unknown_lots"].append(u)
        cols["first_trade_id"].append(r[6] if len(r) > 6 else 100 + 10 * i)
        cols["last_trade_id"].append(r[7] if len(r) > 7 else 109 + 10 * i)
        cols["dq_status"].append("OK")
        cols["weekend"].append(False)
        cols["coverage"].append("FULL")
    return {
        k: np.array(v, object if k in ("dq_status", "coverage") else (bool if k == "weekend" else np.int64))
        for k, v in cols.items()
    }


def levels_block(rows):
    """Footprint level columns from (bar_start, level_ticks, buy, sell, unk) tuples (1-tick bins)."""
    rows = sorted(rows)
    n = len(rows)
    return {
        "bar_start": np.array([r[0] for r in rows], np.int64),
        "level": np.array([r[1] for r in rows], np.int64),
        "buy_lots": np.array([r[2] for r in rows], np.int64),
        "sell_lots": np.array([r[3] for r in rows], np.int64),
        "unknown_lots": np.array([r[4] for r in rows], np.int64),
        "trade_count": np.ones(n, np.int64),
        "first_trade_id": np.arange(n, dtype=np.int64) * 10 + 1,
        "last_trade_id": np.arange(n, dtype=np.int64) * 10 + 5,
    }


# ------------------------------------------------------------------------------------------------- settings
def test_settings_defaults_validation_and_config_hash_unchanged():
    from goldflow.settings import ConfigError, config_hash, validate_config

    cfg = Config(copy.deepcopy(REAL_CFG), Path("."))
    s = OrderflowSettings.from_config(cfg)
    assert s["imbalance"]["mode"] == "diagonal" and "orderflow" not in REAL_CFG  # defaults live in code
    assert config_hash(REAL_CFG) == config_hash(
        json.loads((Path(__file__).resolve().parents[1] / "config/binance_xauusdt.json").read_text())
    )
    bad = [
        {"imbalance": {"mode": "x"}},
        {"imbalance": {"ratio": "0.5"}},
        {"imbalance": {"interval": "2m"}},
        {"cvd": {"resets": ["weekly"]}},
        {"big_trades": {"combine": "most"}},
        {"exhaustion": {"k_pushes": 1}},
        {"big_trades": {"quantile": {"enabled": False}}},
        {"divergence": {"oscillators": ["rsi"]}},
        {"big_trades": {"time_of_day": {"bucket_minutes": 7}}},
    ]
    for b in bad:
        with pytest.raises((ValueError, ConfigError)):
            OrderflowSettings.from_config(Config({**copy.deepcopy(REAL_CFG), "orderflow": b}, Path(".")))
    with pytest.raises(ConfigError):  # also refused at Config load / validate_config
        validate_config({**copy.deepcopy(REAL_CFG), "orderflow": {"imbalance": {"mode": "x"}}})


# ------------------------------------------------------------------------------------------------- imbalance
def test_diagonal_and_horizontal_imbalance_hand_computed():
    # one bar, 1-tick bins: buy(101)=300 vs sell(100)=100 -> buy imbalance (3.0x); sell(100)=100 vs buy(101)=300 no
    lv = levels_block([(0, 100, 0, 100, 7), (0, 101, 300, 50, 0), (0, 102, 299, 0, 0), (0, 103, 10, 0, 0)])
    rows, runs = compute_imbalances(lv, 1, 1, Fraction(3), 50, "diagonal")
    got = {(int(b), int(s)) for b, s in zip(rows["bin"], rows["side"])}
    # 101: 300 >= 3*100 yes; 102: 299 >= 3*50=150 yes; 103: 10 < min_lots; sells: 100 vs buy(101)=300 no; 101: 50 vs 299 no
    assert got == {(101, 1), (102, 1)}
    assert rows["unknown_lots"].tolist() == [0, 0]
    assert runs == []
    # zero policy: an empty comparison bin counts only with zero_policy="imbalance" (bin 100 buy vs empty 99)
    lv2 = levels_block([(0, 100, 600, 0, 0), (0, 101, 0, 100, 0)])
    assert len(compute_imbalances(lv2, 1, 1, Fraction(3), 50, "diagonal")[0]["bin"]) == 0
    r2 = compute_imbalances(lv2, 1, 1, Fraction(3), 50, "diagonal", "imbalance")[0]
    assert {(int(b), int(s)) for b, s in zip(r2["bin"], r2["side"])} == {(100, 1), (101, -1)}
    # horizontal compares the same price
    r3 = compute_imbalances(levels_block([(0, 100, 300, 100, 0)]), 1, 1, Fraction(3), 50, "horizontal")[0]
    assert r3["side"].tolist() == [1]
    # exact ratio boundary: 299 vs 100 at ratio 3 is not an imbalance, 300 is
    assert len(compute_imbalances(levels_block([(0, 100, 0, 100, 0), (0, 101, 299, 0, 0)]), 1, 1, Fraction(3), 1)[0]["bin"]) == 0


def test_stacked_imbalance_and_rebinning():
    # bins of 10 ticks; buys stack over bins 41..44 (4 consecutive) in bar 0; bar 60000 has an isolated one
    rows = (
        [(0, 400, 0, 100, 0)] + [(0, 410 + 10 * k, 400, 0, 3) for k in range(4)] + [(MIN, 400, 0, 100, 0), (MIN, 410, 400, 0, 0)]
    )
    r, runs = compute_imbalances(levels_block(rows), 1, 10, Fraction(3), 100, "diagonal", "imbalance", stack_n=3)
    assert len(runs) == 1 and runs[0]["bin_lo"] == 41 and runs[0]["bin_hi"] == 44 and runs[0]["n"] == 4
    assert runs[0]["unknown_lots"] == 12 and runs[0]["lots"] == 1600
    st = {(int(b), int(s)): bool(x) for b, s, x, bs in zip(r["bin"], r["side"], r["stacked"], r["bar_start"]) if bs == 0}
    assert st[(41, 1)] and st[(44, 1)]
    with pytest.raises(ValueError):
        compute_imbalances(levels_block(rows), 2, 3, Fraction(3), 1)


def test_imbalances_match_independent_pandas_recomputation(tmp_path):
    cfg = make_dataset(tmp_path, n_per_day=6000)
    of, proc, _ = run_of(cfg)
    im = of.of["imbalance"]
    imb = of.store.table("imbalances")
    import pandas as pd

    for d in ("2026-10-23", "2026-10-26"):
        t = trades_of(cfg, d)
        df = pd.DataFrame(
            {
                "id": t.column("source_trade_id").to_numpy(),
                "t": t.column("exchange_event_time_utc").cast("int64").to_numpy(),
                "p": t.column("price_ticks").to_numpy(),
                "q": t.column("quantity_lots").to_numpy(),
                "s": [{"BUY": 1, "SELL": -1}.get(x, 0) for x in t.column("aggressor_side").to_pylist()],
            }
        )
        ref = _imbalances_ref(
            df, MIN, int(im["bin_ticks"]), Fraction(im["ratio"]), int(im["min_lots"]), im["mode"], im["zero_policy"]
        )
        d0 = MS(d + "T00:00:00Z")
        bs = imb.column("bar_start_utc").cast("int64").to_numpy()
        m = (bs >= d0) & (bs < d0 + 86_400_000)
        got = set(
            zip(bs[m].tolist(), np.asarray(imb.column("bin"))[m].tolist(), np.asarray(imb.column("side").to_pylist())[m].tolist())
        )
        assert ref and got == ref


# ------------------------------------------------------------------------------------------------- CVD
def test_cvd_resets_and_unknown_never_assigned():
    cal = SessionCalendar.from_config(REAL_CFG["sessions"])
    c = ResetCVD("1m", MIN, ["utc_day", "session"], cal)
    d0 = MS("2026-10-26T00:00:00Z")
    rows = [
        (d0 + 0, 1, 1, 5, 2, 1),
        (d0 + MIN, 1, 1, 1, 4, 0),
        (d0 + 86_400_000 - MIN, 1, 1, 7, 0, 2),
        (d0 + 86_400_000, 1, 1, 3, 3, 9),
    ]
    out = c.on_bars(bars_block(rows))
    day = [b for b in out if b["series"][0] == "utc_day"]
    assert [b["cvd_lots"].tolist() for b in day] == [[3, 0, 7], [0]]  # reset at the next UTC day
    assert [b["cum_unknown_lots"].tolist() for b in day] == [[1, 1, 3], [9]]
    assert all((b["cum_buy_lots"] - b["cum_sell_lots"] == b["cvd_lots"]).all() for b in out)
    # London (07:00-16:00 GMT on 26 Oct): only bars inside it
    lon = ResetCVD("1m", MIN, ["session"], cal)
    blk = lon.on_bars(
        bars_block(
            [
                (MS("2026-10-26T06:59:00Z"), 1, 1, 9, 0, 0),
                (MS("2026-10-26T07:00:00Z"), 1, 1, 2, 1, 0),
                (MS("2026-10-26T07:01:00Z"), 1, 1, 0, 5, 0),
            ]
        )
    )
    lb = [b for b in blk if b["series"][0] == "session:london"][0]
    assert lb["cvd_lots"].tolist() == [1, -4] and lb["instance"][0] == "session:london:2026-10-26"
    a = ResetCVD("1m", MIN, [], cal)
    a.add_anchor("fomc", d0 + MIN)
    blk = a.on_bars(bars_block(rows))
    assert blk[0]["series"][0] == "anchor:fomc" and blk[0]["cvd_lots"].tolist() == [-3, 4, 4]
    with pytest.raises(ValueError):
        a.add_anchor("x", d0 + 5)


def test_cvd_day_end_equals_partition_totals(tmp_path):
    cfg = make_dataset(tmp_path, n_per_day=3000)
    of, _, _ = run_of(cfg)
    cv = of.store.table("cvd").to_pandas()
    for d in ("2026-10-23", "2026-10-24", "2026-10-25", "2026-10-26"):
        t = trades_of(cfg, d)
        side = np.array(t.column("aggressor_side").to_pylist())
        q = t.column("quantity_lots").to_numpy()
        day = cv[(cv.series == "utc_day") & (cv.instance == f"utc_day:utc_day:{d}")].sort_values("bar_start_utc")
        assert day.cvd_lots.iloc[-1] == q[side == "BUY"].sum() - q[side == "SELL"].sum()
        assert day.cum_unknown_lots.iloc[-1] == q[side == "UNKNOWN"].sum() > 0  # unknown visible, never assigned


# ------------------------------------------------------------------------------------------------- taker orders, big trades
def test_taker_order_rules():
    b = TakerOrderBuilder(cluster_ms=0, sweep_bin_ticks=10, keep_ids=100)
    t = np.array([5, 5, 5, 5, 5, 6, 6, 7], np.int64)
    p = np.array([100, 101, 105, 104, 104, 104, 103, 103], np.int64)
    s = np.array([1, 1, 1, 1, -1, -1, -1, 0], np.int64)
    o, ids = b.on_trades(t, np.arange(8) + 1, p, np.ones(8, np.int64), s)
    # [100,101,105] buy sweep; 104 < 105 breaks (against direction); sell 104 at 5 ms; a new ms starts a new sell order
    # [104, 103] at 6 ms; the unknown-side trade at 7 ms stays open as its own order
    assert o["n"].tolist() == [3, 1, 1, 2] and o["side"].tolist() == [1, 1, -1, -1]
    assert ids.get(0) == [1, 2, 3] and ids.get(3) == [6, 7]
    o2, _ = b.close_upto(7)  # trades at 7 ms may still arrive while the floor is 7
    assert o2["n"].tolist() == []
    o3, _ = b.close_upto(10**9)
    assert o3["side"].tolist() == [0] and o3["n"].tolist() == [1]  # unknown side is always its own order


def test_rolling_quantile_never_uses_own_or_future_samples():
    rq = RollingQuantile(Fraction("0.5"), 10_000, 3, 1000)
    for t, v in ((100, 1), (200, 2), (300, 3)):
        rq.add(t, v)
    assert rq.threshold(900) == (None, 0)  # samples at 100..300 are at/after boundary 0
    assert rq.threshold(1500) == (2, 3)  # boundary 1000 sees all three
    rq.add(1600, 1000)
    assert rq.threshold(1999) == (2, 3)  # same boundary: the new large sample is not used yet
    assert rq.threshold(2000) == (2, 4)  # nearest rank: ceil(0.5 * 4) = 2nd smallest of (1, 2, 3, 1000)
    bb = DailyBucketBaseline(Fraction("0.5"), 30, 2, 2, 10)
    assert bb.threshold(5)[2] == "WARMUP"
    for day in range(3):
        for v in (day + 1, day + 2):
            bb.add(day * 86_400_000 + 1000, v)
        if day == 1:
            assert bb.threshold(day * 86_400_000 + 2000)[2] == "WARMUP"  # only day 0 is in the past
    assert bb.threshold(2 * 86_400_000 + 5000) == (2, 2, "OK")  # days 0,1 -> 1,2,2,3 -> nearest-rank median 2
    assert bb.threshold(3 * 86_400_000 + 5)[0] == 3  # days 1,2 -> 2,3,3,4 -> median 3; today never included
    with pytest.raises(ValueError):
        bb.threshold(5)  # backwards in time is refused


def test_big_trade_thresholds_exact_and_explained():
    cfg = copy.deepcopy(DEFAULT_ORDERFLOW["big_trades"])
    cfg.update(abs_lots=5000, notional_usdt="2000", combine="any")
    cfg["quantile"].update(min_samples=2, update_ms=1000)
    det = BigTradeDetector(cfg, shift_ms=0)
    b = TakerOrderBuilder(0, 10, 100)

    def orders(t, p, q, s):
        o, ids = b.on_trades(np.array(t), np.arange(len(t)) + 100 * t[0], np.array(p), np.array(q), np.array(s))
        o2, ids2 = b.close_upto(10**12)
        ids.extend(ids2)
        return {k: np.concatenate([o[k], o2[k]]) for k in o}, ids

    # 4,000 lots at 50,000 ticks = 4 oz * 500 USDT = 2,000 USDT exactly -> notional criterion passes (>=)
    ev = det.process(*orders([1000], [50_000], [4000], [1]))
    assert len(ev) == 1 and json.loads(ev[0]["details"])["criteria"]["notional_usdt"]["pass"]
    assert json.loads(ev[0]["details"])["criteria"]["quantile"]["ready"] is False
    assert ev[0]["event_time_ms"] == 1000 + 0 + 1 and ev[0]["evidence_end_ms"] == 1001
    ev = det.process(*orders([2000], [49_999], [4000], [-1]))  # 1,999.96 USDT, below abs -> quantile not ready yet
    assert ev == []
    det.process(*orders([2500], [100], [3], [1]))
    ev = det.process(*orders([3000], [100], [4000], [1]))  # quantile ready: median of (4000, 4000, 3) = 4000 -> 4000 >= 4000
    assert len(ev) == 1 and json.loads(ev[0]["details"])["criteria"]["quantile"]["pass"]
    assert ev[0]["trade_ids"] == [300000] and ev[0]["n_trades"] == 1 and ev[0]["unknown_lots"] == 0


# ------------------------------------------------------------------------------------------------- absorption
def _abs_cfg(**kw):
    c = copy.deepcopy(DEFAULT_ORDERFLOW["absorption"])
    c.update(
        abs_lots=1500,
        window_bars=3,
        bin_ticks=10,
        tol_bins=0,
        min_orders=2,
        max_progress_bins=0,
        confirm_bins=2,
        confirm_window_ms=5 * MIN,
        break_bins=1,
        **kw,
    )
    c["quantile"]["enabled"] = False
    return c


def _orders_cols(rows):
    keys = ("side", "first_t", "last_t", "size", "notional", "n", "first_id", "last_id", "first_p", "last_p", "min_p", "max_p")
    return {k: np.array([r.get(k, 0) for r in rows], np.int64) for k in keys}


def test_absorption_candidate_then_confirmed_never_backdated():
    d = AbsorptionDetector(_abs_cfg(), "1m", MIN, 1, shift_ms=7, cluster_ms=0)
    # three bars of heavy buying into bin 410 (4100..4109 ticks) with no progress above it
    lv = levels_block([(T0 + k * MIN, 4101, 600, 0, 5) for k in range(3)])
    bars = bars_block([(T0 + k * MIN, 4105, 4095, 600, 0, 5) for k in range(3)])
    d.add_orders(
        _orders_cols(
            [
                {"side": 1, "first_t": T0 + 10, "last_t": T0 + 10, "size": 600, "min_p": 4101, "max_p": 4101},
                {"side": 1, "first_t": T0 + MIN + 5, "last_t": T0 + MIN + 5, "size": 600, "min_p": 4101, "max_p": 4101},
                {"side": 1, "first_t": T0 + 2 * MIN, "last_t": T0 + 2 * MIN, "size": 600, "min_p": 4101, "max_p": 4101},
            ]
        )
    )
    ev = d.on_bars(bars, lv, T0 + 3 * MIN)
    cand = [e for e in ev if e["status"] == "CANDIDATE"]
    assert len(cand) == 1 and cand[0]["ref_price_ticks"] == 4100 and cand[0]["value_lots"] == 1800
    assert cand[0]["evidence_end_ms"] == T0 + 3 * MIN and cand[0]["event_time_ms"] == T0 + 3 * MIN + 7
    assert cand[0]["unknown_lots"] == 15 and cand[0]["first_trade_id"] is not None
    # next bar: no move; the one after drops two bins below -> CONFIRMED at that bar's end
    ev = d.on_bars(
        bars_block([(T0 + 3 * MIN, 4104, 4095, 0, 10, 0)]), levels_block([(T0 + 3 * MIN, 4100, 0, 10, 0)]), T0 + 4 * MIN
    )
    assert not [e for e in ev if e["status"] in ("CONFIRMED", "FAILED")]
    ev = d.on_bars(
        bars_block([(T0 + 4 * MIN, 4099, 4080, 0, 10, 0)]), levels_block([(T0 + 4 * MIN, 4085, 0, 10, 0)]), T0 + 5 * MIN
    )
    conf = [e for e in ev if e["status"] == "CONFIRMED"]
    assert len(conf) == 1 and conf[0]["parent_id"] == cand[0]["event_id"]
    assert conf[0]["event_time_ms"] == T0 + 5 * MIN + 7 > cand[0]["evidence_end_ms"]


def test_single_large_trade_is_never_absorption_and_expiry():
    d = AbsorptionDetector(_abs_cfg(), "1m", MIN, 1, shift_ms=0, cluster_ms=0)
    lv = levels_block([(T0 + k * MIN, 4101, 600, 0, 0) for k in range(3)])
    bars = bars_block([(T0 + k * MIN, 4105, 4095, 600, 0, 0) for k in range(3)])
    d.add_orders(_orders_cols([{"side": 1, "first_t": T0, "last_t": T0 + 2 * MIN, "size": 1800, "min_p": 4101, "max_p": 4101}]))
    assert d.on_bars(bars, lv, T0 + 3 * MIN) == []  # one taker order: not absorption
    d2 = AbsorptionDetector(_abs_cfg(), "1m", MIN, 1, shift_ms=0, cluster_ms=0)
    d2.add_orders(
        _orders_cols(
            [
                {"side": 1, "first_t": T0 + k * MIN, "last_t": T0 + k * MIN, "size": 600, "min_p": 4101, "max_p": 4101}
                for k in range(3)
            ]
        )
    )
    assert len(d2.on_bars(bars, lv, T0 + 3 * MIN)) == 1
    ev = d2.on_advance(T0 + 20 * MIN) + d2.on_advance(T0 + 21 * MIN)  # no bars at all: expires at the deadline
    assert [(e["status"], e["event_time_ms"]) for e in ev] == [("EXPIRED", T0 + 3 * MIN + 5 * MIN)]


# ------------------------------------------------------------------------------------------------- exhaustion, divergence
def test_exhaustion_declining_pushes_with_delta_divergence():
    cfg = copy.deepcopy(DEFAULT_ORDERFLOW["exhaustion"])
    cfg.update(lookback_bars=10, k_pushes=3, confirm_bins=2, confirm_window_bars=5, bin_ticks=10)
    d = ExhaustionDetector(cfg, "1m", MIN, 0)
    base = [(T0 + k * MIN, 1000, 990, 10, 10, 0) for k in range(3)]
    pushes = [
        (T0 + 3 * MIN, 1010, 995, 900, 100, 0),
        (T0 + 4 * MIN, 1020, 1005, 700, 200, 0),
        (T0 + 5 * MIN, 1030, 1015, 500, 450, 4),
    ]
    ev = d.on_bars(bars_block(base + pushes), T0 + 6 * MIN)
    c = [e for e in ev if e["status"] == "CANDIDATE" and e["side"] == "BUY"]
    assert len(c) == 1 and c[0]["ref_price_ticks"] == 1030 and c[0]["evidence_end_ms"] == T0 + 6 * MIN
    assert json.loads(c[0]["details"])["push_volumes"] == [900, 700, 500]
    ev = d.on_bars(bars_block([(T0 + 6 * MIN, 1029, 1009, 5, 5, 0)]), T0 + 7 * MIN)  # 1009//10=100 <= 103-2
    assert [e["status"] for e in ev] == ["CONFIRMED"] and ev[0]["event_time_ms"] == T0 + 7 * MIN
    # rising delta at the newest push: no divergence, no candidate
    d2 = ExhaustionDetector(cfg, "1m", MIN, 0)
    p2 = pushes[:2] + [(T0 + 5 * MIN, 1030, 1015, 500, 0, 0)]
    assert not [e for e in d2.on_bars(bars_block(base + p2), T0 + 6 * MIN) if e["status"] == "CANDIDATE" and e["side"] == "BUY"]


def test_divergence_confirmed_only_after_right_bars_close():
    cal = SessionCalendar.from_config(REAL_CFG["sessions"])
    cfg = copy.deepcopy(DEFAULT_ORDERFLOW["divergence"])
    cfg.update(
        swing_left=2, swing_right=2, max_pivot_gap_bars=20, oscillators=["cvd", "delta"], cvd_reset="utc_day", inval_bins=1
    )
    I = 5 * MIN
    d = DivergenceDetector(cfg, "5m", I, 0, cal)
    highs = [100, 101, 110, 102, 101, 103, 104, 120, 105, 104, 106]
    deltas = [5, 5, 50, -5, -5, 5, 5, 10, -5, -5, 0]  # P1 (110): cvd 60, delta 50; P2 (120): cvd 70, delta 10
    rows = [(T0 + k * I, h, h - 5, max(dd, 0) + 10, max(-dd, 0) + 10, 0) for k, (h, dd) in enumerate(zip(highs, deltas))]
    out = []
    for k, r in enumerate(rows):
        ev = d.on_bars(bars_block([r], I))
        for e in ev:
            assert e["event_time_ms"] >= e["evidence_end_ms"] == T0 + (k + 1) * I  # confirmed when bar k closes
        out += ev
    kinds = {(e["event_type"], e["status"]) for e in out}
    assert ("DIVERGENCE_DELTA", "DETECTED") in kinds and ("DIVERGENCE_CVD", "DETECTED") not in kinds  # cvd 70 > 60
    det = [e for e in out if e["event_type"] == "DIVERGENCE_DELTA"][0]
    assert det["evidence_end_ms"] == T0 + 10 * I  # pivot at bar 7, confirmed at the end of bar 9 (7 + R)
    inv = d.on_bars(bars_block([(T0 + 11 * I, 131, 120, 10, 10, 0)], I))
    assert [(e["status"], e["parent_id"]) for e in inv] == [("INVALIDATED", det["event_id"])]


# ------------------------------------------------------------------------------------------------- end to end
def test_end_to_end_invariants_and_traceability(tmp_path):
    cfg = make_dataset(tmp_path, n_per_day=8000)
    cfg.raw["orderflow"] = {"big_trades": {"quantile": {"min_samples": 200}}}  # the synthetic days are sparse
    of, proc, res = run_of(cfg)
    assert res["status"] == "completed"
    ev = of.events()
    s = summarize_events(ev)
    assert not s["fail"], s["checks"]
    types = {k.split("|")[0] for k in s["by_type_status"]}
    assert {"BIG_TRADE", "ABSORPTION", "EXHAUSTION", "DIVERGENCE_CVD", "DIVERGENCE_DELTA"} <= types
    assert of.counters["confirm_before_evidence_end"] == 0
    for r in ev.to_pylist():
        assert r["event_time_utc"] >= r["evidence_end_utc"]
        assert r["unknown_lots"] is not None and r["sessions"] and r["dq_status"] in ("OK", "UNVERIFIED", "PARTIAL", "MISSING")
    # the partial hour of the synthetic dataset is labelled on overlapping events
    assert "PARTIAL" in set(ev.column("dq_status").to_pylist())
    man = json.loads((of.root / "_manifest.json").read_text())
    assert man["tables"]["events"]["rows"] == ev.num_rows and not (of.root / "_parts").exists()


def test_reproducible_across_runs_and_chunk_sizes(tmp_path):
    cfg = make_dataset(tmp_path, n_per_day=5000)
    a, _, _ = run_of(cfg, "a")
    da = {k: v["logical_digest"] for k, v in a.manifest["tables"].items()}
    b, _, _ = run_of(cfg, "b")
    assert {k: v["logical_digest"] for k, v in b.manifest["tables"].items()} == da
    cfg.raw["replay"]["max_events_per_chunk"] = 61
    c, _, _ = run_of(cfg, "c")
    assert {k: v["logical_digest"] for k, v in c.manifest["tables"].items()} == da


@pytest.mark.parametrize("cut", ["2026-10-23T13:07:11.500Z", "2026-10-25T23:59:59.999Z"])
def test_interrupt_and_resume_equals_uninterrupted(tmp_path, cut):
    cfg = make_dataset(tmp_path, n_per_day=5000)
    ref, _, _ = run_of(cfg, "ref")
    want = {k: v["logical_digest"] for k, v in ref.manifest["tables"].items()}
    _, _, r1 = run_of(cfg, "cut", until=MS(cut))
    assert r1["status"] == "stopped"
    of2, _, r2 = run_of(cfg, "cut", resume=True)
    assert r2["status"] == "completed"
    assert {k: v["logical_digest"] for k, v in of2.manifest["tables"].items()} == want


def test_resume_refused_after_orderflow_settings_change(tmp_path):
    cfg = make_dataset(tmp_path, n_per_day=2000)
    run_of(cfg, "g", until=MS("2026-10-24T12:00:00Z"))
    cfg.raw["orderflow"] = {"imbalance": {"ratio": "4.0"}}
    with pytest.raises(ReplayGuardError):
        run_of(cfg, "g", resume=True)


def test_duplicate_delivery_does_not_change_the_catalogue(tmp_path):
    from goldflow.replay import EventChunk

    cfg = make_dataset(tmp_path, n_per_day=3000)
    ref, _, _ = run_of(cfg, "ref")
    want = {k: v["logical_digest"] for k, v in ref.manifest["tables"].items()}

    class Twice(TradeProcessor):
        def on_chunk(self, c: EventChunk) -> None:
            super().on_chunk(c)
            super().on_chunk(c)  # every chunk delivered twice

    of = OrderflowAnalytics(cfg)
    proc = Twice(cfg, sinks=[of])
    ReplayEngine(cfg, [proc], name="twice").run()
    assert proc.stats["duplicates_ignored"] > 0
    assert {k: v["logical_digest"] for k, v in of.manifest["tables"].items()} == want


def test_cli_orderflow_and_validation_report(tmp_path, capsys):
    from goldflow import cli

    cfg = make_dataset(tmp_path, n_per_day=3000)
    (tmp_path / "config").mkdir()
    cp = tmp_path / "config" / "binance_xauusdt.json"
    cp.write_text(json.dumps(cfg.raw))
    assert cli.main(["orderflow", "--config", str(cp), "--name", "cli"]) == 0
    rep = json.loads((cfg.data_root / "reports" / "orderflow_catalogue.json").read_text())
    assert not rep["summary"]["fail"] and rep["summary"]["rows"] > 0
    assert "## Initial events by session" in (cfg.data_root / "reports" / "orderflow_catalogue.md").read_text()
    files = sorted((cfg.data_root / "derived").glob("**/orderflow/events/date=*/part-000.parquet"))
    assert files and pq.read_table(files[0]).schema.names[0] == "event_id"


def test_random_streams_big_trade_detector_matches_bruteforce():
    """Rolling-quantile Big Trades equal a brute-force evaluation of the definition on random order streams."""
    for seed in range(5):
        r = random.Random(seed)
        cfg = copy.deepcopy(DEFAULT_ORDERFLOW["big_trades"])
        cfg["quantile"].update(q="0.9", lookback_hours=1, min_samples=20, update_ms=60_000)
        det = BigTradeDetector(cfg, 0)
        t = sorted(r.randint(0, 4 * 3_600_000) for _ in range(800))
        size = [r.choice([r.randint(1, 50), r.randint(1, 5000)]) for _ in t]
        n = len(t)
        orders = {
            k: np.zeros(n, np.int64)
            for k in (
                "side",
                "first_t",
                "last_t",
                "size",
                "notional",
                "n",
                "first_id",
                "last_id",
                "first_p",
                "last_p",
                "min_p",
                "max_p",
            )
        }
        orders.update(
            side=np.ones(n, np.int64),
            first_t=np.array(t),
            last_t=np.array(t),
            size=np.array(size),
            n=np.ones(n, np.int64),
            first_id=np.arange(n),
            last_id=np.arange(n),
            notional=np.array(size) * 100,
        )

        class Ids:
            def get(self, i):
                return [i]

        got = {e["first_trade_id"] for e in det.process(orders, Ids())}
        exp = set()
        for i in range(n):
            c = t[i] + 1
            B = c // 60_000 * 60_000
            w = sorted(size[j] for j in range(n) if B - 3_600_000 <= t[j] + 1 < B)
            if len(w) >= 20 and size[i] >= w[max(1, math.ceil(Fraction("0.9") * len(w))) - 1]:
                exp.add(i)
        assert got == exp
