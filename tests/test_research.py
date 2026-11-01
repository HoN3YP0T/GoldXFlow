"""Phase 13 tests: performance metrics with reconciliation and intervals, research ledger hash chain and ordering,
test-set lock, walk-forward windows/selection/stitching, robustness and approval (with a deterministic fake runner),
and a full study through the real pipeline on SYNTHETIC broker ticks."""
import copy, json, sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_backtest import BROKER, synthetic_broker  # noqa: E402
from test_processing import MS, REAL_CFG, make_dataset  # noqa: E402
from test_strategies import loose  # noqa: E402

from goldflow.config import Config  # noqa: E402
from goldflow.research import (LedgerError, PipelineRunner, ResearchLedger, ResearchSettings, TestSetLock, TestSetReused,  # noqa: E402
                               monte_carlo, performance, run_study, walk_forward, wilson, windows)
from goldflow.research.walkforward import cost_stress, sensitivity, stability  # noqa: E402

DAY = 86_400_000


def T(i, net, gross=None):
    return {"order_id": f"o{i}", "signal_id": f"s{i}", "strategy_id": "x", "direction": "LONG", "entry_time": i * 3_600_000,
            "exit_time": i * 3_600_000 + 600_000, "gross_cents": gross if gross is not None else net + 70, "commission_cents": -70,
            "swap_cents": 0, "net_cents": net, "r_multiple": net / 100}


def test_performance_reconciles_and_reports_uncertainty():
    nets = [100, -50, 200, -50, -50, 300]
    tr = [T(i, x) for i, x in enumerate(nets)]
    cash = [{"amount_cents": x} for x in nets]
    p = performance(tr, cash, 10_000, min_trades=100, boot=(500, 3))
    assert p["profit_factor"] == 4.0 and p["net_cents"] == 450 and p["max_drawdown_cents"] == 100 and p["recovery_trades"] == 1
    assert p["max_consecutive_losses"] == 2 and p["sample"] == "INSUFFICIENT_SAMPLE" and p["cost_impact_cents"] == 420
    lo, hi = p["win_rate_ci95"]
    assert lo < 0.5 < hi and wilson(3, 6) == p["win_rate_ci95"]
    assert p["profit_factor_ci95"] is not None and performance(tr, cash, 10_000, boot=(500, 3)) == p        # seeded: deterministic
    with pytest.raises(AssertionError, match="not reconciled"):
        performance(tr, cash[:-1], 10_000)
    mc = monte_carlo(nets, 10_000, 300, 1)
    assert mc == monte_carlo(nets, 10_000, 300, 1) and mc["max_drawdown_pct"]["p95"] >= mc["max_drawdown_pct"]["p50"]


def test_research_ledger_order_and_tamper(tmp_path):
    led = ResearchLedger(tmp_path)
    with pytest.raises(LedgerError, match="register hypothesis"):
        led.append("RESULT", hypothesis="H1", outcome="x")
    with pytest.raises(LedgerError, match="success_criteria"):
        led.append("HYPOTHESIS", id="H1", statement="s", strategy="x")
    led.append("HYPOTHESIS", id="H1", statement="s", strategy="x", success_criteria="PF >= 1.2 OOS")
    led.append("RESULT", hypothesis="H1", outcome="ok")
    with pytest.raises(LedgerError, match="already registered"):
        led.append("HYPOTHESIS", id="H1", statement="s2", strategy="x", success_criteria="c")
    assert [e["kind"] for e in led.verify()] == ["HYPOTHESIS", "RESULT"]
    lines = led.path.read_text().splitlines()
    led.path.write_text(lines[0] + "\n" + lines[1].replace('"ok"', '"great"') + "\n")
    with pytest.raises(LedgerError, match="modified"):
        led.verify()


def test_test_set_lock(tmp_path):
    lock = TestSetLock(tmp_path, (100, 200))
    lock.guard(0, 100, "dev")
    with pytest.raises(TestSetReused):
        lock.guard(150, 300, "wf")
    lock.open_for("cand-A", "final")
    assert len(lock.open_for("cand-A", "regenerate report")["accesses"]) == 2
    with pytest.raises(TestSetReused, match="already opened"):
        lock.open_for("cand-B", "another idea")


def rsettings(tmp, **research):
    raw = copy.deepcopy(REAL_CFG)
    raw["research"] = {**raw["research"], **research}
    return Config(raw, tmp), ResearchSettings.from_config(Config(raw, tmp))


def test_walk_forward_windows_never_touch_the_test_period(tmp_path):
    _, rs = rsettings(tmp_path)
    ws = windows(rs)
    assert len(ws) == 7 and ws[0][0] == MS("2026-04-30T00:00:00Z") and all(d <= rs.splits["validation"][1] for *_, d in ws)
    assert all(b == c and b - a == 56 * DAY and d - c == 14 * DAY for a, b, c, d in ws)
    with pytest.raises(Exception, match="chronological"):
        rsettings(tmp_path, splits={"development": ["2026-04-30", "2026-09-10"], "validation": ["2026-09-01", "2026-09-30"], "test": ["2026-10-01", "2026-10-29"]})


class FakeRunner:
    """Deterministic trades: one per day; net = 100 x (param a) - 30 on even days, -40 x b on odd days."""
    def __init__(self, rs, root):
        self.calls = []; self.lock = TestSetLock(root, rs.splits["test"])
    def run(self, strategy, params, a, b, tag, backtest=None, test_access=False):
        if not test_access:
            self.lock.guard(a, b, tag)
        self.calls.append((tag, params, backtest))
        mult = 2 if backtest and backtest.get("spread_mult") else 1
        tr, cash = [], []
        for i, d in enumerate(range(a // DAY, b // DAY)):
            net = (100 * params["a"] - 30 * mult) if d % 2 == 0 else -40 * params["b"]
            t = {**T(i, net), "entry_time": d * DAY, "exit_time": d * DAY + 1000, "order_id": f"{tag}-{i}"}
            tr.append(t); cash.append({"amount_cents": net, "order_id": t["order_id"]})
        return {"trades": tr, "cash": cash, "fills": [], "signals": {}, "start_cents": 1_000_000}


def test_walk_forward_selection_stitching_robustness_and_approval(tmp_path):
    _, rs = rsettings(tmp_path, min_trades_in_sample=10, bootstrap={"samples": 200, "seed": 1})
    fr = FakeRunner(rs, tmp_path)
    wf = walk_forward(fr.run, rs, "x", {"a": [1, 3, 2], "b": [1]}, "t")
    assert wf["windows_tested"] == 7 and all(w["chosen"] == {"a": 3, "b": 1} for w in wf["windows"])        # best objective chosen
    st = wf["stitched_oos"]
    assert st["trades"] == 7 * 14 and st["net_cents"] == sum(w["oos"]["net_cents"] for w in wf["windows"]) and wf["positive_share"] == 1.0
    assert not [c for c in fr.calls if c[0].endswith("-test")]
    stab = stability(fr.run, rs, "x", {"a": 3, "b": 1}, {}, *rs.splits["development"], "t")
    assert stab["stable"] is True and {v["param"] for v in stab["variants"]} == {"a"}                       # b=1 x 0.8 rounds to 1: skipped
    cs = cost_stress(fr.run, rs, "x", {"a": 1, "b": 2}, *rs.splits["development"], "t", 2)
    assert cs["survives"] is False and cs["spread_mult"] == "3/2"
    sens = sensitivity(fr.run, rs, "x", {"a": 1, "b": 1}, "a", [1, 2, 3], "b", [1, 2], *rs.splits["development"], "t")
    assert len(sens["cells"]) == 6 and 0 <= sens["plateau_score"] <= 1
    from goldflow.research import approval
    ap = approval(rs, wf, stab, cs, None)
    status = {c["criterion"].split(" (")[0]: c["status"] for c in ap["criteria"]}
    assert status["max drawdown"] == "NOT_SET" and not ap["approved"]                                     # D8 must be set first
    assert any(c["status"] == "FAIL" for c in ap["criteria"])                                             # cost stress fails


def test_study_with_fake_runner_records_and_locks(tmp_path):
    cfg, rs = rsettings(tmp_path, min_trades_in_sample=10, bootstrap={"samples": 100, "seed": 1},
                        approval={**REAL_CFG["research"]["approval"], "max_oos_drawdown": "0.5"})
    cfg.raw["paths"] = {"data_root": "data", "state_db": "data/state/x.sqlite"}
    fr = FakeRunner(rs, cfg.data_root / "research")
    plan = {"hypothesis": {"id": "H1", "statement": "a matters", "success_criteria": "PF >= 1.2 OOS"}, "strategy": "fa_value_edge",
            "grid": {"a": [1, 3], "b": [1]}, "sensitivity": {"p1": "a", "v1": [1, 3], "p2": "b", "v2": [1, 2]}, "open_test": True}
    out = run_study(cfg, plan, "s1", runner=fr)
    rep = (cfg.data_root / "research" / "studies" / "s1" / "report.md").read_text()
    for h in ("## In-sample (development)", "## Validation", "## Walk-forward (out-of-sample, stitched)", "## Test (opened once)", "## Approval"):
        assert h in rep
    assert out["test"]["trades"] == 29 and (cfg.data_root / "research" / "studies" / "s1" / "sensitivity.svg").exists()
    assert [e["kind"] for e in ResearchLedger(cfg.data_root / "research").verify()] == ["HYPOTHESIS", "RESULT"]
    with pytest.raises(TestSetReused):
        run_study(cfg, {**plan, "hypothesis": {**plan["hypothesis"], "id": "H2"}, "grid": {"a": [2], "b": [1]}}, "s2", runner=fr)
    with pytest.raises(ValueError, match="different content"):
        run_study(cfg, {**plan, "grid": {"a": [5], "b": [1]}, "open_test": False}, "s3", runner=fr)


def test_study_through_the_real_pipeline_on_synthetic_broker_ticks(tmp_path):
    cfg = loose(make_dataset(tmp_path, n_per_day=2000, partial_hour=None))
    cfg.raw["broker"] = {**cfg.raw["broker"], **BROKER}
    cfg.raw["backtest"].update({"basis": {"window": "30m", "min_samples": 5}, "max_tick_gap": "10m"})
    cfg.raw["backtest"]["risk"]["max_concurrent_positions"] = 2
    synthetic_broker(cfg)
    cfg.raw["research"] = {**cfg.raw["research"], "splits": {"development": ["2026-10-23", "2026-10-24"], "validation": ["2026-10-25", "2026-10-25"],
                           "test": ["2026-10-26", "2026-10-26"]}, "walk_forward": {"in_sample_days": 1, "out_of_sample_days": 1, "step_days": 1, "warmup_days": 0},
                           "objective": "net_cents", "min_trades_in_sample": 0, "bootstrap": {"samples": 100, "seed": 1}}  # fmt: skip
    rs = ResearchSettings.from_config(cfg)
    runner = PipelineRunner(cfg, rs, allow_synthetic=True)
    with pytest.raises(TestSetReused):
        runner.run("fa_value_edge", {}, MS("2026-10-25T00:00:00Z"), MS("2026-10-27T00:00:00Z"), "leak")
    plan = {"hypothesis": {"id": "HS", "statement": "synthetic engine exercise", "success_criteria": "n/a (synthetic)"}, "strategy": "ib_failed_break",
            "grid": {"stop_buffer_ticks": [5, 10]}, "open_test": False}
    out = run_study(cfg, plan, "syn", runner=runner)
    wf = out["walk_forward"]
    assert len(wf["windows"]) == 2 and wf["final_candidate"] is not None
    assert out["in_sample"] is not None and out["validation"] is not None and "test" not in out
    assert out["approval"]["approved"] is False
    assert json.loads((tmp_path / "data" / "research" / "studies" / "syn" / "study.json").read_text())["hypothesis"] == "HS"
