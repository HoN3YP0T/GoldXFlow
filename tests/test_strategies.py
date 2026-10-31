"""Phase 11 tests: the signal schema and its look-ahead check, strategy parameter and capability refusal, the pure
build() of each example strategy on hand-built evidence, engine gating (warm-up, open DQ interval, evidence DQ,
duplicate setups), and the full pipeline on replay (clock check, re-derivation from evidence, determinism, restore)."""
import copy, json, sys
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_processing import MS, REAL_CFG, make_dataset  # noqa: E402

from goldflow.config import Config  # noqa: E402
from goldflow.orderflow import ProfileEngine  # noqa: E402
from goldflow.orderflow.auction_engine import AuctionEngine  # noqa: E402
from goldflow.orderflow.flow_engine import OrderflowEngine  # noqa: E402
from goldflow.orderflow.initial_balance import IBEngine  # noqa: E402
from goldflow.processing import TradeProcessor  # noqa: E402
from goldflow.providers.base import CapabilityUnavailable  # noqa: E402
from goldflow.replay import ChainHasher, ReplayEngine, ReplayGuardError  # noqa: E402
from goldflow.settings import ConfigError, validate_config  # noqa: E402
from goldflow.strategies import (AbsorptionAtLevel, Evidence, FailedAuctionValueEdge, IBFailedBreak, SignalStore,  # noqa: E402
                                 StrategyEngine, TradingSignal, build_strategies)
from goldflow.strategies.library import DepthImbalanceExample  # noqa: E402
from goldflow.strategies.verify import verify  # noqa: E402

META = {"event_time": 1_000_000, "provider": "binance", "instrument": "XAUUSDT", "trigger_id": "T1"}
REG = Evidence("regime", {"auction_state": "BALANCE"}, None, 1_000_000)


def sig(direction="LONG", entry=100, stop=90, target=130, ev_t=1_000_000):
    return TradingSignal("x", "1", "binance", "XAUUSDT", 1_000_000, direction, "market", entry, stop, target, "1R", 0.5,
                         (Evidence("m", 1, None, ev_t, ("T1",), True),), 1_300_000, stop, ("historical_trades",), "OK", "T1")


def test_signal_schema_rules_and_exact_rr():
    s = sig()
    assert s.check() == [] and s.rr == 3 and s.signal_id.startswith("sig-") and s.signal_id == sig().signal_id
    assert "stop on the wrong side of entry" in sig(stop=110).check()
    assert "target on the wrong side of entry" in sig(direction="SHORT", stop=110, target=120).check()
    assert "evidence later than the signal (look-ahead)" in sig(ev_t=1_000_001).check()
    r = sig(entry=101, stop=90, target=130).row(1_000_000)
    assert r["rr_exact"] == "29/11" and json.loads(r["evidence"])[0]["timestamp"] == 1_000_000 and r["score_calibrated"] is False


def test_strategy_parameters_and_capabilities_are_enforced(tmp_path):
    with pytest.raises(ValueError, match="unknown parameters"):
        FailedAuctionValueEdge({"stop_buffer": 3})
    with pytest.raises(ValueError, match="decimal values are strings"):
        FailedAuctionValueEdge({"min_rr": 1.5})
    raw = copy.deepcopy(REAL_CFG); raw["strategies"]["enabled"] = ["nope"]
    with pytest.raises(ConfigError, match="unknown strategies"):
        validate_config(raw, None, check_discovery=False)
    cfg = Config(copy.deepcopy(REAL_CFG), tmp_path)
    with pytest.raises(CapabilityUnavailable, match="historical_l2_depth"):
        StrategyEngine(cfg, [DepthImbalanceExample()], {})                 # Binance has no historical L2: refused
    with pytest.raises(ValueError, match="needs analytics engines"):
        StrategyEngine(cfg, [IBFailedBreak()], {"orderflow": SimpleNamespace(listeners=[])})


def test_failed_auction_value_edge_build():
    s = FailedAuctionValueEdge()
    fa = {"side": "ABOVE", "level_id": "w:VAH", "level_type": "VAH", "level_price_ticks": 1000.0, "probe_extreme_ticks": 1040, "confirmed_at": 1,
          "weak_volume": True, "responsive_delta": False, "dq_status": "OK", "coverage": "FULL"}
    bar = {"bar_start": 0, "close_ticks": 998, "delta_lots": -50, "volume_lots": 200, "dq_status": "OK", "coverage": "FULL"}
    ev = (Evidence("auction.FAILED_AUCTION", fa, "r", 1, ("T1",), True), Evidence("orderflow.confirm_bar", bar, "d", 1),
          Evidence("auction.target_level", {"level_id": "w:POC", "price_exact": "1909/2"}, None, 0), REG)
    assert s.build(ev, META) is None                                            # R:R 43/52 < default min_rr 1.0
    s2 = FailedAuctionValueEdge({"min_rr": "0.5"})
    g2 = s2.build(ev, META)
    assert (g2.direction, g2.entry_price_ticks, g2.stop_loss_ticks, g2.take_profit_ticks) == ("SHORT", 998, 1050, 955)  # POC 954.5 -> ceil
    assert g2.rr == Fraction(43, 52) and g2.check() == []
    no_delta = ev[:1] + (Evidence("orderflow.confirm_bar", {**bar, "delta_lots": 30}, "d", 1),) + ev[2:]
    assert s2.build(no_delta, META) is None                                    # delta disagrees and the probe was not responsive


def test_absorption_and_ib_failed_break_build():
    ab = {"event_id": "T1", "side": "SELL", "level_low_ticks": 990, "level_high_ticks": 999, "lots": 5, "resolution_price_ticks": 1003,
          "invalidate_ticks": 970, "evidence_start": 0, "evidence_end": 1, "event_time": 1, "dq_status": "OK", "coverage": "FULL", "unknown_share": 0.0}
    ev = (Evidence("orderflow.ABSORPTION", ab, "c", 1, ("T1",), True),
          Evidence("profile.level", {"type": "HVN", "price_ticks": 995.5, "distance_ticks": 0.0}, 20, 1), REG)
    g = AbsorptionAtLevel().build(ev, META)
    assert (g.direction, g.stop_loss_ticks, g.take_profit_ticks) == ("LONG", 965, 1003 + 2 * 38) and g.rr == 2
    fb = {"event_id": "T1", "event_type": "FAILED_BREAK_UP", "event_time": 1, "price_ticks": 1095, "coverage": "FULL", "breakout_at": 0}
    ib = {"ib_key": "k", "high_ticks": 1100, "low_ticks": 1001, "dq_status": "OK", "coverage": "FULL"}
    ev2 = (Evidence("ib.FAILED_BREAK", fb, "r", 1, ("T1",), True), Evidence("ib.final", ib, None, 0),
           Evidence("orderflow.break_extreme", {"extreme_ticks": 1110}, None, 1), REG)
    g2 = IBFailedBreak({"min_rr": "0.5"}).build(ev2, META)
    assert (g2.direction, g2.stop_loss_ticks, g2.take_profit_ticks) == ("SHORT", 1120, 1051)        # mid 1050.5 -> ceil
    g3 = IBFailedBreak({"target": "opposite", "min_rr": "0.5"}).build(ev2, META)
    assert g3.take_profit_ticks == 1001


def test_engine_gating_suppresses_only(tmp_path):
    cfg = Config(copy.deepcopy(REAL_CFG), tmp_path)
    eng = StrategyEngine(cfg, [FailedAuctionValueEdge()], {"auction": SimpleNamespace(listeners=[]), "orderflow": SimpleNamespace(listeners=[])})
    got = []
    eng.listeners.append(lambda t, rows: got.extend((t, r) for r in rows))
    proc = SimpleNamespace(clock=10**12, observe_from=0)
    s = eng.strategies[0]
    def mk(t, dq="OK", d="SHORT"):
        stop, tgt = (110, 80) if d == "SHORT" else (90, 120)
        return TradingSignal(s.id, "1", "binance", "XAUUSDT", t, d, "market", 100, stop, tgt, "1R", 0.5,
                             (Evidence("m", 1, None, t, ("T",), True),), t + 300_000, stop, (), dq, f"T{t}")
    eng._gate(s, mk(1000), proc)                                         # inside the 24 h warm-up
    eng.dq.append([90_000_000, 91_000_000, "PARTIAL"])
    eng._gate(s, mk(90_500_000), proc)                                   # DQ interval open
    eng._gate(s, mk(95_000_000, dq="UNVERIFIED"), proc)                  # evidence not OK
    eng._gate(s, mk(96_000_000), proc)                                   # emitted
    eng._gate(s, mk(96_100_000), proc)                                   # same direction, previous one still valid
    eng._gate(s, mk(96_200_000, d="LONG"), proc)                         # other direction: emitted
    assert [(t, r.get("reason")) for t, r in got] == [("suppressed", "WARMUP"), ("suppressed", "DQ_OPEN"), ("suppressed", "DQ_EVIDENCE"),
                                                     ("signals", None), ("suppressed", "DUPLICATE_ACTIVE"), ("signals", None)]
    with pytest.raises(AssertionError, match="look-ahead"):
        eng._gate(s, mk(10**12 + 1), proc)


# ============================================================================ full pipeline on replay
def loose(cfg):
    o, a = cfg.raw["orderflow"], cfg.raw["auction"]
    o["absorption"].update({"zone_quantile": "0.5", "min_samples": 30, "min_active_seconds": 2, "max_progress_bins": 3, "bin_ticks": 2,
                            "confirm_bins": 2, "confirm_window": "10m"})
    a["rejection"].update({"probe_bins": 1, "reject_time": "60m"})
    a["failed_auction"].update({"fa_vol_frac": "0.6", "fa_return_time": "60m", "fa_hold_bars": 2})
    cfg.raw["initial_balance"]["definitions"] = [{"name": "new_york", "session": "new_york", "offset": "0m", "length": "30m"},
                                                 {"name": "london", "session": "london", "start": "08:00", "length": "30m"}]
    cfg.raw["strategies"].update({"warmup": "1h", "params": {"fa_value_edge": {"min_rr": "0.2", "require_delta": False, "level_types": "VAH,VAL,POC,HIGH,LOW,VWAP"},
                                                             "absorption_level": {"min_rr": "0.5", "near_ticks": 400},
                                                             "ib_failed_break": {"min_rr": "0.2"}}})
    return cfg


class Listen:
    def __init__(self):
        self.rows = {}; self.violations = 0; self.proc = None
    def __call__(self, t, rows):
        if self.proc is not None and any(r["available_at"] > self.proc.clock for r in rows):
            self.violations += 1
        self.rows.setdefault(t, []).extend(rows)


def srun(cfg, name="s", resume=False, until=None, store=True, listen=None):
    prof, flow, ib = ProfileEngine(cfg), OrderflowEngine(cfg), IBEngine(cfg)
    auc = AuctionEngine(cfg, prof, ib_engine=ib)
    eng = StrategyEngine(cfg, build_strategies(cfg), {"profile": prof, "orderflow": flow, "ib": ib, "auction": auc},
                         store=SignalStore(cfg.data_root, cfg.symbol, replace=not resume) if store else None, listeners=[listen] if listen else [])
    p = TradeProcessor(cfg, sinks=[prof, flow, ib, auc, eng])
    if listen: listen.proc = p
    r = ReplayEngine(cfg, [ChainHasher(), p], name=name).run(resume=resume, until=until)
    return eng, p, r


def test_pipeline_signals_are_timely_explainable_and_deterministic(tmp_path):
    cfg = loose(make_dataset(tmp_path, n_per_day=2500, partial_hour=None))
    lis = Listen()
    eng, p, r = srun(cfg, "full", listen=lis)
    sigs = lis.rows.get("signals", [])
    assert r["status"] == "completed" and lis.violations == 0 and sigs, eng.summary()
    assert {x["strategy_id"] for x in sigs} >= {"fa_value_edge"}, eng.summary()
    for x in sigs:
        assert all(e["timestamp"] <= x["event_time"] for e in json.loads(x["evidence"]))
    rep = verify(cfg, report=False)
    assert not rep["summary"]["fail"] and rep["summary"]["rederived_ok"] == len(sigs), rep["problems"]
    dig = eng.state_digest()
    srun(cfg, "cut", until=MS("2026-10-25T13:00:30Z"))
    e2, _, r2 = srun(cfg, "cut", resume=True)
    assert r2["status"] == "completed" and e2.state_digest() == dig
    cfg.raw["strategies"]["params"]["ib_failed_break"]["min_rr"] = "0.3"
    srun(cfg, "chg", until=MS("2026-10-24T00:00:00Z"), store=False)
    cfg.raw["strategies"]["params"]["ib_failed_break"]["min_rr"] = "0.2"
    with pytest.raises(ReplayGuardError, match="different strategies"):
        srun(cfg, "chg", resume=True, store=False)
