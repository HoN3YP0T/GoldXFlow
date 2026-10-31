"""Phase 13: a research study from a plan file: hypothesis → walk-forward → robustness → (once) test → approval → report.

Plan (JSON)::

    {"hypothesis": {"id": "H1", "statement": "...", "success_criteria": "..."},
     "strategy": "fa_value_edge", "grid": {"stop_buffer_ticks": [5, 10, 20], "min_rr": ["1", "1.5"]},
     "sensitivity": {"p1": "stop_buffer_ticks", "v1": [5, 10, 20], "p2": "min_rr", "v2": ["1", "1.5", "2"]},
     "open_test": false}

Steps (each recorded in the research ledger):
1. the hypothesis is registered (refused if one with that id exists with different content);
2. walk-forward on development + validation; the final candidate is the parameter set chosen in the last window;
3. in-sample (development) and validation backtests of the final candidate, kept separate;
4. robustness on development: stability (±param_change), cost stress (spread x cost_multiplier, slippage x 2),
   two-parameter sensitivity with a heatmap and plateau score, trade-order Monte Carlo;
5. only with ``open_test: true``: the test period is opened **once** for this candidate (lock file) and backtested;
6. approval criteria evaluated; report written with in-sample, validation and out-of-sample sections separate.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ..strategies.library import REGISTRY
from ..timeutil import iso
from .ledger import ResearchLedger
from .metrics import performance
from .runner import PipelineRunner
from .settings import ResearchSettings
from .walkforward import cost_stress, heatmap_svg, sensitivity, stability, walk_forward


def approval(
    rs: ResearchSettings, wf: dict[str, Any], stab: dict[str, Any], cost: dict[str, Any], test: dict[str, Any] | None
) -> dict[str, Any]:
    ap = rs.approval
    oos = test if test is not None else (wf.get("stitched_oos") or {})
    src = "test" if test is not None else "walk-forward stitched out-of-sample"
    crit = []

    def c(name: str, value: Any, need: Any, ok: bool | None) -> None:
        crit.append({"criterion": name, "value": value, "threshold": None if need is None else str(need),
                     "status": "NOT_SET" if need is None else ("NO_DATA" if ok is None else ("PASS" if ok else "FAIL"))})  # fmt: skip

    pf = oos.get("profit_factor")
    c(
        f"profit factor after costs ({src})",
        pf,
        ap["oos_profit_factor"],
        None if pf is None else pf >= float(ap["oos_profit_factor"] or 0),
    )
    ps = wf.get("positive_share")
    c(
        "share of walk-forward windows with positive expectancy",
        ps,
        ap["wf_positive_share"],
        None if ps is None else ps >= float(ap["wf_positive_share"] or 0),
    )
    dd = oos.get("max_drawdown_pct")
    c(
        f"max drawdown ({src})",
        dd,
        ap["max_oos_drawdown"],
        None if dd is None or ap["max_oos_drawdown"] is None else dd <= float(ap["max_oos_drawdown"]),
    )
    c(f"stable under ±{ap['param_change']} parameter changes", stab.get("stable"), ap["param_change"], stab.get("stable"))
    c(
        f"profitable with costs x{ap['cost_multiplier']} (spread) and slippage x2",
        cost.get("survives"),
        ap["cost_multiplier"],
        cost.get("survives"),
    )
    n = oos.get("trades", 0)
    c(f"sample size >= {rs.min_trades_conclusion} trades ({src})", n, rs.min_trades_conclusion, n >= rs.min_trades_conclusion)
    return {
        "criteria": crit,
        "approved": all(x["status"] == "PASS" for x in crit),
        "note": "thresholds are proposals until you set them (D8)",
    }


def run_study(
    cfg: Any, plan: dict[str, Any], name: str, allow_synthetic: bool = False, runner: PipelineRunner | None = None
) -> dict[str, Any]:
    rs = ResearchSettings.from_config(cfg)
    root = Path(cfg.data_root) / "research"
    led = ResearchLedger(root)
    runner = runner or PipelineRunner(cfg, rs, allow_synthetic=allow_synthetic)
    h = plan["hypothesis"]
    strategy, grid = plan["strategy"], plan["grid"]
    hyp = {"id": h["id"], "statement": h["statement"], "strategy": strategy, "grid": grid, "success_criteria": h["success_criteria"],
           "windows": {k: [iso(a), iso(b)] for k, (a, b) in rs.splits.items()}, "research_settings_hash": rs.hash}  # fmt: skip
    old = next((e for e in led.verify() if e["kind"] == "HYPOTHESIS" and e["id"] == h["id"]), None)
    if old is None:
        led.append("HYPOTHESIS", **hyp)
    elif any(old.get(k) != v for k, v in json.loads(json.dumps(hyp)).items()):
        raise ValueError(f"hypothesis {h['id']} is already registered with different content; register a new id")
    run = runner.run
    wf = walk_forward(run, rs, strategy, grid, f"{name}-wf")
    cand = wf["final_candidate"]
    out: dict[str, Any] = {"name": name, "hypothesis": h["id"], "strategy": strategy, "walk_forward": wf, "final_candidate": cand}
    if cand is None:
        led.append(
            "RESULT",
            hypothesis=h["id"],
            study=name,
            outcome="NO_CANDIDATE",
            detail="no window produced an eligible parameter set",
        )
        out["approval"] = {"approved": False, "criteria": [], "note": "no candidate"}
        _write(root, name, out)
        return out
    dev, val, tst = rs.splits["development"], rs.splits["validation"], rs.splits["test"]

    def perf(r: dict[str, Any]) -> dict[str, Any]:
        return performance(
            r["trades"],
            r["cash"],
            r["start_cents"],
            r["fills"],
            r["signals"],
            rs.min_trades_conclusion,
            (rs.boot_samples, rs.boot_seed),
        )

    out["in_sample"] = perf(run(strategy, cand, dev[0], dev[1], f"{name}-is"))
    out["validation"] = perf(run(strategy, cand, val[0], val[1], f"{name}-val"))
    defaults = {k: v for k, (v, _) in REGISTRY[strategy].defaults.items()}
    out["stability"] = stability(run, rs, strategy, cand, defaults, dev[0], dev[1], name)
    out["cost_stress"] = cost_stress(
        run, rs, strategy, cand, dev[0], dev[1], name, int(cfg.raw.get("backtest", {}).get("slippage_points", 2))
    )
    if plan.get("sensitivity"):
        sp = plan["sensitivity"]
        out["sensitivity"] = sensitivity(run, rs, strategy, cand, sp["p1"], sp["v1"], sp["p2"], sp["v2"], dev[0], dev[1], name)
    test = None
    if plan.get("open_test"):
        ch = hashlib.sha256(
            json.dumps({"strategy": strategy, "params": cand, "cfg": cfg.config_hash}, sort_keys=True).encode()
        ).hexdigest()
        out["test_lock"] = runner.lock.open_for(ch, f"study {name}: final candidate of hypothesis {h['id']}")
        test = perf(runner.run(strategy, cand, tst[0], tst[1], f"{name}-test", test_access=True))
        out["test"] = test
    out["approval"] = approval(rs, wf, out["stability"], out["cost_stress"], test)
    led.append("RESULT", hypothesis=h["id"], study=name, candidate=cand, approved=out["approval"]["approved"],
               oos_profit_factor=(test or wf.get("stitched_oos") or {}).get("profit_factor"), test_opened=bool(plan.get("open_test")))  # fmt: skip
    _write(root, name, out)
    return out


def _write(root: Path, name: str, out: dict[str, Any]) -> None:
    d = root / "studies" / name
    d.mkdir(parents=True, exist_ok=True)
    (d / "study.json").write_text(json.dumps(out, indent=1, default=str))
    if out.get("sensitivity"):
        (d / "sensitivity.svg").write_text(heatmap_svg(out["sensitivity"]))
        cells = out["sensitivity"]["cells"]
        (d / "sensitivity.csv").write_text(
            "\n".join([",".join(cells[0].keys())] + [",".join(str(v) for v in c.values()) for c in cells]) + "\n"
        )
    (d / "report.md").write_text(report(out))


def _sec(title: str, p: dict[str, Any] | None) -> list[str]:
    if not p:
        return [f"## {title}", "", "Not run.", ""]

    def usd(c: Any) -> str:
        return "n/a" if c is None else f"{c / 100:,.2f}"

    return [f"## {title}", "", f"Sample: **{p['sample']}** ({p['trades']} trades).", "", "| Metric | Value |", "|---|---|",
            f"| Net / gross P&L | {usd(p['net_cents'])} / {usd(p['gross_cents'])} (cost impact {usd(p['cost_impact_cents'])}) |",
            f"| Profit factor (95 % bootstrap) | {p['profit_factor']} ({p['profit_factor_ci95']}) |",
            f"| Expectancy per trade (95 %) | {usd(p['expectancy_cents'])} ({p['expectancy_cents_ci95']}); {p['expectancy_r']} R |",
            f"| Win rate (Wilson 95 %) | {p['win_rate']} ({p['win_rate_ci95']}) |",
            f"| Max drawdown | {usd(p['max_drawdown_cents'])} ({p['max_drawdown_pct']:.2%}); recovered after {p['recovery_trades']} trades |",
            f"| Max consecutive losses | {p['max_consecutive_losses']} |", f"| Monte Carlo max DD p95 | {(p['monte_carlo'] or {}).get('max_drawdown_pct', {}).get('p95')} |",
            ""]  # fmt: skip


def report(out: dict[str, Any]) -> str:
    wf = out["walk_forward"]
    lines = [f"# Research study {out['name']}", "", f"Hypothesis `{out['hypothesis']}`, strategy `{out['strategy']}`, final candidate `{out.get('final_candidate')}`.",
             "", "In-sample, validation and out-of-sample results are reported separately; only out-of-sample results count for approval.", ""]  # fmt: skip
    lines += _sec("In-sample (development)", out.get("in_sample"))
    lines += _sec("Validation", out.get("validation"))
    lines += ["## Walk-forward (out-of-sample, stitched)", "", f"Windows tested: {wf['windows_tested']}; positive share: {wf['positive_share']}.", "",
              "| Window | In-sample | Out-of-sample | Chosen | OOS trades | OOS net (cents) | OOS PF |", "|---|---|---|---|---|---|---|"]  # fmt: skip
    for w in wf["windows"]:
        o = w["oos"] or {}
        d = [str(iso(x))[:10] for x in (*w["in_sample"], *w["out_of_sample"])]
        lines.append(f"| {w['window']} | {d[0]}–{d[1]} | {d[2]}–"
                     f"{d[3]} | {w['chosen']} | {o.get('trades')} | {o.get('net_cents')} | {o.get('profit_factor')} |")  # fmt: skip
    lines += [""] + _sec("Walk-forward stitched out-of-sample", wf.get("stitched_oos"))
    lines += _sec("Test (opened once)", out.get("test")) if out.get("test") else ["## Test", "", "Not opened.", ""]
    st, cs = out.get("stability") or {}, out.get("cost_stress") or {}
    lines += ["## Robustness (development)", "", f"- Stability ±{st.get('change')}: stable = {st.get('stable')} ({len(st.get('variants', []))} variants).",
              f"- Cost stress: spread x{cs.get('spread_mult')}, slippage {cs.get('slippage_points')} points: PF {cs.get('profit_factor')}, survives = {cs.get('survives')}.",
              f"- Sensitivity plateau score: {(out.get('sensitivity') or {}).get('plateau_score')} (heatmap: sensitivity.svg).", "",
              "## Approval", "", "| Criterion | Value | Threshold | Status |", "|---|---|---|---|"]  # fmt: skip
    for c in out["approval"]["criteria"]:
        lines.append(f"| {c['criterion']} | {c['value']} | {c['threshold']} | {c['status']} |")
    lines += ["", f"**Approved: {out['approval']['approved']}.** {out['approval']['note']}"]
    return "\n".join(lines) + "\n"
