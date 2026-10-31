"""Phase 12 tests: broker spec and tick import (MT5 formats, server time zone, quarantine), basis model (window, median,
no look-ahead), swap rollovers, hand-computed fills, exits, gap rule, costs and risk limits on known ticks, exact ledger
reconciliation, refusal of an unconfigured broker and of synthetic data, and the full pipeline on SYNTHETIC broker ticks
(determinism, restore, latency, fills on bid/ask)."""
import copy, json, sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_processing import DAYS, MS, REAL_CFG, make_dataset, trades_of  # noqa: E402
from test_strategies import loose  # noqa: E402

from goldflow.backtesting import (BacktestSettings, Backtester, BasisModel, BrokerNotConfigured, BrokerSpec, BrokerTicks,  # noqa: E402
                                  Rollovers, Simulator, import_ticks, stats)
from goldflow.config import Config  # noqa: E402
from goldflow.orderflow import ProfileEngine  # noqa: E402
from goldflow.orderflow.auction_engine import AuctionEngine  # noqa: E402
from goldflow.orderflow.flow_engine import OrderflowEngine  # noqa: E402
from goldflow.orderflow.initial_balance import IBEngine  # noqa: E402
from goldflow.processing import TradeProcessor  # noqa: E402
from goldflow.replay import ChainHasher, ReplayEngine  # noqa: E402
from goldflow.settings import ConfigError, validate_config  # noqa: E402
from goldflow.strategies import StrategyEngine, build_strategies  # noqa: E402

BROKER = {"name": "TestBroker", "server_tz": "UTC", "commission_per_lot_side": "3.5", "swap_long_per_lot": "-10", "swap_short_per_lot": "2"}


def settings(broker=None, backtest=None, tmp=Path(".")):
    raw = copy.deepcopy(REAL_CFG)
    raw["broker"] = {**raw["broker"], **(broker if broker is not None else BROKER)}
    for k, v in (backtest or {}).items():
        raw["backtest"][k] = {**raw["backtest"][k], **v} if isinstance(v, dict) else v
    cfg = Config(raw, tmp)
    return cfg, BrokerSpec.from_raw(raw["broker"]), BacktestSettings.from_config(cfg)


def test_broker_spec_is_required_and_validated():
    _, spec, _ = settings(broker={"name": None})
    assert spec is None
    for patch, msg in [({"name": "X", "server_tz": None}, "server_tz is required"), ({"name": "X", "server_tz": "UTC", "contract_size": 100.0}, "decimal string"),
                       ({"name": "X", "server_tz": "Mars/Base"}, "")]:
        raw = copy.deepcopy(REAL_CFG); raw["broker"].update(patch)
        with pytest.raises(ConfigError, match=msg):
            validate_config(raw, None, check_discovery=False)


def test_mt5_tick_import_server_time_forward_fill_and_quarantine(tmp_path):
    _, spec, _ = settings(broker={**BROKER, "server_tz": "Europe/Athens"})
    f = tmp_path / "ticks.tsv"
    rows = ["<DATE>\t<TIME>\t<BID>\t<ASK>\t<LAST>\t<VOLUME>\t<FLAGS>",
            "2026.10.23\t15:00:00.000\t4010.25\t4010.45\t\t\t6",     # EEST (UTC+3) -> 12:00:00 UTC
            "2026.10.23\t15:00:00.500\t4010.30\t\t\t\t2",            # ask unchanged -> carried forward
            "2026.10.23\t15:00:01.000\t4010.50\t4010.40\t\t\t6",     # crossed
            "2026.10.23\t15:00:02.000\t4010.505\t4010.70\t\t\t6",    # not on the 0.01 point
            "2026.10.23\t14:59:59.000\t4010.10\t4010.30\t\t\t6",     # backwards
            "2026.10.26\t10:00:00.000\t4020.00\t4020.20\t\t\t6"]     # EET (UTC+2) after 25 Oct -> 08:00 UTC
    f.write_text("\n".join(rows) + "\n")
    r = import_ticks(tmp_path / "data", spec, f, "mt5")
    assert r["stored"] == 3 and r["quarantined"] == {"CROSSED_QUOTE": 1, "PRICE_NOT_ON_POINT": 1, "TIME_BACKWARDS": 1}
    bt = BrokerTicks(tmp_path / "data", spec)
    x = bt.between(MS("2026-10-23T00:00:00Z"), MS("2026-10-27T00:00:00Z"))
    assert x["t"].tolist() == [MS("2026-10-23T12:00:00Z"), MS("2026-10-23T12:00:00.500Z"), MS("2026-10-26T08:00:00Z")]
    assert x["bid"].tolist() == [401025, 401030, 402000] and x["ask"].tolist() == [401045, 401045, 402020]
    assert not bt.synthetic and len(bt.dataset_version) == 64
    g = tmp_path / "ticks.csv"
    g.write_text("time_msc,bid,ask\n" + f"{MS('2026-10-23T15:00:05Z')},4011.00,4011.20\n")
    assert import_ticks(tmp_path / "data", spec, g, "csv")["stored"] == 1


def test_basis_median_window_translation_and_no_look_ahead():
    b2 = BasisModel(2, 5_000, 2)  # half-units below: 2 x close - (bid + ask) = 200, 220, 260
    b2.add(np.array([1000, 2000, 3000]), np.array([400100, 400110, 400130]), np.array([399990] * 3), np.array([400010] * 3))
    assert b2.at(3000) == (220, 3)                       # lower median of 200, 220, 260
    assert b2.at(1500) == (None, 1)                      # fewer than min_samples before 1.5 s
    assert b2.to_broker(400000, 220) == 399890           # 400000 - 1.10 = 3998.90
    assert b2.to_broker(400000, 221) == 399889 + 1       # half a point rounds up
    assert b2.at(4000) == (220, 3)
    with pytest.raises(AssertionError, match="look-ahead"):
        b2.add(np.array([3500]), np.array([1]), np.array([1]), np.array([1]))   # a sample for a time already queried


def test_rollovers_close_server_days_and_triple_wednesday():
    _, spec, _ = settings()
    r = Rollovers(spec)
    wed = MS("2026-10-21T12:00:00Z")
    t1 = r.next_after(wed)
    assert t1 == MS("2026-10-22T00:00:00Z") and r.multiplier(t1) == 3            # closes Wednesday
    t2 = r.next_after(t1); t3 = r.next_after(t2); t4 = r.next_after(t3)
    assert (t2, r.multiplier(t2)) == (MS("2026-10-23T00:00:00Z"), 1)
    assert (t3, r.multiplier(t3)) == (MS("2026-10-24T00:00:00Z"), 1)              # closes Friday
    assert t4 == MS("2026-10-27T00:00:00Z")                                        # Saturday and Sunday are not charged


# ============================================================================ simulator on known ticks
def sim(backtest=None, broker=None):
    _, spec, bt = settings(broker=broker, backtest={"basis": {"min_samples": 1}, **(backtest or {})})
    b = BasisModel(2, bt.basis_window_ms, bt.basis_min_samples)
    b.add(np.array([4000]), np.array([400000]), np.array([399990]), np.array([400010]))   # basis 0
    return Simulator(spec, bt, b, Rollovers(spec)), spec, bt


def SIG(t=10_000, d="LONG", e=400000, sl=399500, tp=401000, sid="s1", exp=None):
    return {"signal_id": sid, "strategy_id": "x", "direction": d, "event_time": t, "expiry_at": exp or t + 300_000,
            "entry_price_ticks": e, "stop_loss_ticks": sl, "take_profit_ticks": tp}


def ticks(*rows):
    t, b, a = (np.array(x, np.int64) for x in zip(*rows))
    return t, b, a


def test_long_entry_on_ask_with_latency_and_target_exit_reconciles():
    s, spec, bt = sim()
    t, b, a = ticks((5000, 399990, 400010), (10_100, 399990, 400010), (10_300, 399995, 400015), (20_000, 400500, 400520), (30_000, 401000, 401020))
    s.run(t, b, a, [(SIG(), [])], 40_000)
    o = s.out["orders"][0]
    assert o["status"] == "FILLED" and o["fill_time"] == 10_300 and o["fill_points"] == 400017 and o["lots"] == "1/10"   # ask + 2 slippage
    tr = s.out["trades"][0]
    assert (tr["exit_reason"], tr["exit_points"], tr["gross_cents"], tr["commission_cents"], tr["net_cents"]) == ("TP", 401000, 9830, -70, 9760)
    st = stats(s.out, 1_000_000)
    assert st["final_cents"] == 1_009_760 and st["reconciled"] and st["trades"] == 1


def test_stop_gap_rule_rejections_and_expiry():
    s, _, _ = sim()
    t, b, a = ticks((5000, 399990, 400010), (10_300, 399995, 400015), (20_000, 399400, 399420))
    s.run(t, b, a, [(SIG(), [])], 30_000)
    assert s.out["trades"][0]["exit_reason"] == "SL" and s.out["trades"][0]["exit_points"] == 399398        # gapped bid - slippage
    g, _, _ = sim()
    g.path = {"t": np.array([100_000], np.int64), "lo": np.array([399400], np.int64), "hi": np.array([400600], np.int64)}
    t, b, a = ticks((5000, 399990, 400010), (10_300, 399995, 400015), (200_000, 401100, 401120))
    g.run(t, b, a, [(SIG(), [])], 300_000)
    assert g.out["trades"][0]["exit_reason"] == "GAP_ASSUMED_STOP" and g.counts["gap_assumed_stop"] == 1     # target after a gap, stop first
    r, _, _ = sim(backtest={"reject_rate": "0.9999"})
    t, b, a = ticks((5000, 399990, 400010), (10_300, 399995, 400015))
    r.run(t, b, a, [(SIG(), [])], 30_000)
    assert r.out["orders"][0]["status"] == "REJECTED_BROKER" and not r.out["trades"]
    lv, _, _ = sim(broker={**BROKER, "stops_level_points": 600})
    lv.run(t, b, a, [(SIG(), [])], 30_000)
    assert lv.out["orders"][0]["status"] == "REJECTED_STOPS_LEVEL"
    ex, _, _ = sim()
    t2, b2, a2 = ticks((5000, 399990, 400010), (400_000, 399995, 400015))
    ex.run(t2, b2, a2, [(SIG(exp=200_000), [])], 500_000)
    assert ex.out["orders"][0]["status"] == "EXPIRED"
    st, _, _ = sim()
    st.run(*ticks((5000, 399990, 400010)), [(SIG(t=100_000), [])], 200_000)
    assert st.out["orders"][0]["status"] == "REJECTED_STALE_QUOTE"                                          # last quote 95 s old


def test_swap_and_risk_limits():
    s, _, _ = sim()
    T0 = MS("2026-10-21T23:00:00Z")
    s.basis.add(np.array([T0 - 1000]), np.array([400000]), np.array([399990]), np.array([400010]))
    t, b, a = ticks((T0, 399990, 400010), (T0 + 1000, 399995, 400015), (T0 + 2 * 3_600_000, 400200, 400220), (T0 + 3 * 3_600_000, 401000, 401020))
    s.run(t, b, a, [(SIG(t=T0 + 500), [])], T0 + 4 * 3_600_000)
    tr = s.out["trades"][0]
    assert tr["swap_cents"] == -300 and [c["kind"] for c in s.out["cash"]] == ["COMMISSION", "SWAP", "PNL", "COMMISSION"]   # -10 x 0.1 lot x 3
    assert stats(s.out, 1_000_000)["reconciled"]
    d, _, _ = sim(backtest={"risk": {"daily_loss_limit": "0.001", "max_concurrent_positions": 1}})
    t, b, a = ticks((5000, 399990, 400010), (10_300, 399995, 400015), (20_000, 399400, 399420), (40_000, 399400, 399420))
    d.run(t, b, a, [(SIG(), []), (SIG(t=30_000, sid="s2"), [])], 50_000)
    assert [o["status"] for o in d.out["orders"]] == ["FILLED", "REJECTED_DAILY_LOSS"]
    c, _, _ = sim()
    t, b, a = ticks((5000, 399990, 400010), (10_300, 399995, 400015), (12_000, 399995, 400015))
    c.run(t, b, a, [(SIG(), []), (SIG(t=11_000, sid="s2"), [])], 50_000)
    assert [o["status"] for o in c.out["orders"]] == ["FILLED", "REJECTED_MAX_CONCURRENT"] or [o["status"] for o in c.out["orders"]] == ["REJECTED_MAX_CONCURRENT"]
    z, _, _ = sim(backtest={"risk": {"risk_per_trade": "0.00001"}})
    z.run(*ticks((5000, 399990, 400010), (10_300, 399995, 400015)), [(SIG(), [])], 50_000)
    assert z.out["orders"][0]["status"] == "REJECTED_SIZE_BELOW_MIN"


def test_backtester_refuses_without_broker_or_on_synthetic_data(tmp_path):
    cfg, spec, _ = settings(broker={"name": None}, tmp=tmp_path)
    se = type("S", (), {"listeners": []})()
    with pytest.raises(BrokerNotConfigured, match="D2"):
        Backtester(cfg, se)
    cfg, spec, _ = settings(tmp=tmp_path)
    cfg.raw["paths"] = {"data_root": "data", "state_db": "data/state/x.sqlite"}
    f = tmp_path / "t.csv"; f.write_text("time_msc,bid,ask\n1792713600000,4000.00,4000.20\n")
    import_ticks(cfg.data_root, spec, f, "csv", synthetic=True)
    with pytest.raises(BrokerNotConfigured, match="SYNTHETIC"):
        Backtester(cfg, se)


# ============================================================================ full pipeline on SYNTHETIC broker ticks
def synthetic_broker(cfg, basis_cents=150, half_spread=10):
    """Broker quotes at every Binance trade time: Binance price - $1.50, spread 0.20. SYNTHETIC: for engine tests only."""
    rows = ["time_msc,bid,ask"]
    for d in DAYS:
        t = trades_of(cfg, d)
        ts = t.column("exchange_event_time_utc").cast(pa.int64()).to_numpy(); px = t.column("price_ticks").to_numpy()
        _, first = np.unique(ts, return_index=True)
        for x, p in zip(ts[first].tolist(), px[first].tolist()):
            rows.append(f"{x},{(p - basis_cents - half_spread) / 100:.2f},{(p - basis_cents + half_spread) / 100:.2f}")
    f = cfg.base_dir / "synthetic_ticks.csv"; f.write_text("\n".join(rows) + "\n")
    return import_ticks(cfg.data_root, BrokerSpec.from_raw(cfg.raw["broker"]), f, "csv", synthetic=True)


def brun(cfg, name="b", resume=False, until=None):
    prof, flow, ib = ProfileEngine(cfg), OrderflowEngine(cfg), IBEngine(cfg)
    auc = AuctionEngine(cfg, prof, ib_engine=ib)
    strat = StrategyEngine(cfg, build_strategies(cfg), {"profile": prof, "orderflow": flow, "ib": ib, "auction": auc})
    bt = Backtester(cfg, strat, run_name=name, allow_synthetic=True)
    p = TradeProcessor(cfg, sinks=[prof, flow, ib, auc, strat, bt])
    r = ReplayEngine(cfg, [ChainHasher(), p], name=name).run(resume=resume, until=until)
    return bt, r


def test_pipeline_on_synthetic_broker_ticks(tmp_path):
    cfg = loose(make_dataset(tmp_path, n_per_day=2500, partial_hour=None))
    cfg.raw["broker"] = {**cfg.raw["broker"], **BROKER}
    cfg.raw["backtest"]["risk"]["max_concurrent_positions"] = 2
    cfg.raw["backtest"].update({"basis": {"window": "30m", "min_samples": 5}, "max_tick_gap": "10m"})   # sparse synthetic quotes
    imp = synthetic_broker(cfg)
    assert imp["synthetic"] and imp["stored"] > 0
    bt, r = brun(cfg, "full")
    assert r["status"] == "completed"
    out = tmp_path / "data" / "backtests" / "full"
    rec = json.loads((out / "run.json").read_text())
    st = rec["statistics"]
    assert st["reconciled"] and rec["broker_data_synthetic"] and "SYNTHETIC BROKER DATA - NOT A RESULT" in (out / "report.md").read_text()
    assert rec["backtester_counts"]["signals"] > 0 and st["trades"] > 0, rec
    assert bt.basis.samples and all(v == 300 for _, v in list(bt.basis.samples)[-50:])           # basis $1.50 = 300 half-cents
    orders = pq.read_table(out / "orders.parquet").to_pylist()
    tk = BrokerTicks(cfg.data_root, bt.spec)
    for o in [o for o in orders if o["status"] == "FILLED"]:
        assert o["fill_time"] >= o["event_time"] + 250                                          # latency on the event clock
        q = tk.between(o["fill_time"], o["fill_time"] + 1)
        side = q["ask"][0] + 2 if o["direction"] == "LONG" else q["bid"][0] - 2
        assert o["fill_points"] == side                                                         # buy at ask, sell at bid, +slippage
    trades = pq.read_table(out / "trades.parquet").to_pylist()
    bt2, _ = brun(cfg, "again")
    assert pq.read_table(tmp_path / "data" / "backtests" / "again" / "trades.parquet").to_pylist() == trades   # deterministic
    brun(cfg, "cut", until=MS("2026-10-25T13:00:30Z"))
    brun(cfg, "cut", resume=True)
    assert pq.read_table(tmp_path / "data" / "backtests" / "cut" / "trades.parquet").to_pylist() == trades     # restore = uninterrupted
