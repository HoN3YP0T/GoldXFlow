"""Phase 13: walk-forward optimisation and robustness checks.

**Walk-forward** over development + validation only (the test period is refused): rolling windows of
``in_sample_days`` followed by ``out_of_sample_days``, stepping ``step_days``. In each window every parameter set of the
grid is backtested on the in-sample part; the best by the configured objective (among sets with at least
``min_trades_in_sample`` trades; ties go to the earlier grid entry) is then run, unchanged, on the out-of-sample part.
Each window's choice and results are recorded; the out-of-sample trades are stitched into one equity curve. The share of
windows with positive out-of-sample expectancy is reported.

**Robustness** (on development data):
* ``sensitivity``: a two-parameter grid → objective and net per cell (CSV + SVG heatmap) and a plateau score: the share
  of the best cell's neighbours whose objective is within ``param_change`` of the best (a plateau, not a peak);
* ``stability``: each numeric parameter moved by ±``param_change`` (one at a time); stable when every variant stays
  profitable (profit factor >= 1);
* ``cost_stress``: rerun with the broker spread x ``cost_multiplier`` and slippage x 2.
"""

from __future__ import annotations

import itertools
import math
from fractions import Fraction
from typing import Any, Callable

from .metrics import performance

Runner = Callable[..., dict[str, Any]]


def grid_points(grid: dict[str, list[Any]]) -> list[dict[str, Any]]:
    keys = sorted(grid)
    return [dict(zip(keys, vals)) for vals in itertools.product(*(grid[k] for k in keys))]


def objective(perf: dict[str, Any], name: str) -> float | None:
    v = perf.get(name)
    return None if v is None else float(v)


def windows(rs: Any) -> list[tuple[int, int, int, int]]:
    a, end = rs.splits["development"][0], rs.splits["validation"][1]
    out = []
    s = a
    while s + rs.is_ms + rs.oos_ms <= end:
        out.append((s, s + rs.is_ms, s + rs.is_ms, s + rs.is_ms + rs.oos_ms))
        s += rs.step_ms
    return out


def _perf(r: dict[str, Any], rs: Any) -> dict[str, Any]:
    return performance(
        r["trades"],
        r["cash"],
        r["start_cents"],
        r["fills"],
        r["signals"],
        rs.min_trades_conclusion,
        (rs.boot_samples, rs.boot_seed),
    )


def walk_forward(run: Runner, rs: Any, strategy: str, grid: dict[str, list[Any]], tag: str) -> dict[str, Any]:
    pts = grid_points(grid)
    out_windows = []
    stitched_trades: list[dict[str, Any]] = []
    stitched_cash: list[dict[str, Any]] = []
    start_cents = None
    for wi, (a, b, c, d) in enumerate(windows(rs)):
        cands = []
        for pi, p in enumerate(pts):
            r = run(strategy, p, a, b, f"{tag}-w{wi:02d}-is-p{pi:03d}")
            pf = _perf(r, rs)
            ok = pf["trades"] >= rs.min_trades_is and objective(pf, rs.objective) is not None
            cands.append(
                {
                    "params": p,
                    "trades": pf["trades"],
                    "objective": objective(pf, rs.objective),
                    "net_cents": pf["net_cents"],
                    "eligible": ok,
                }
            )
        elig = [x for x in cands if x["eligible"]]
        best = (
            max(elig, key=lambda x: x["objective"]) if elig else None
        )  # max() keeps the first of equal values: earlier grid entry
        w: dict[str, Any] = {
            "window": wi,
            "in_sample": [a, b],
            "out_of_sample": [c, d],
            "candidates": cands,
            "chosen": best["params"] if best else None,
        }
        if best is not None:
            r = run(strategy, best["params"], c, d, f"{tag}-w{wi:02d}-oos")
            pf = _perf(r, rs)
            start_cents = r["start_cents"] if start_cents is None else start_cents
            w["oos"] = {
                k: pf[k] for k in ("trades", "net_cents", "profit_factor", "expectancy_cents", "max_drawdown_pct", "sample")
            }
            stitched_trades += r["trades"]
            stitched_cash += r["cash"]
        else:
            w["oos"] = None
            w["note"] = f"no parameter set reached {rs.min_trades_is} in-sample trades: window skipped"
        out_windows.append(w)
    tested = [w for w in out_windows if w["oos"] is not None]
    stitched = (
        performance(
            stitched_trades,
            stitched_cash,
            start_cents or 0,
            None,
            None,
            rs.min_trades_conclusion,
            (rs.boot_samples, rs.boot_seed),
        )
        if tested
        else None
    )
    pos = sum(1 for w in tested if (w["oos"]["expectancy_cents"] or 0) > 0)
    return {"windows": out_windows, "windows_tested": len(tested), "positive_share": (pos / len(tested)) if tested else None,
            "stitched_oos": stitched, "final_candidate": tested[-1]["chosen"] if tested else None}  # fmt: skip


def _vary(v: Any, f: Fraction) -> Any | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        n = int(round(v * f))
        return n if n != v and n > 0 else None
    if isinstance(v, str):
        try:
            q = Fraction(v) * f
        except (ValueError, ZeroDivisionError):
            return None
        return str(q.limit_denominator(10_000)) if "/" not in v else None
    return None


def stability(
    run: Runner, rs: Any, strategy: str, params: dict[str, Any], base_defaults: dict[str, Any], a: int, b: int, tag: str
) -> dict[str, Any]:
    c = rs.approval["param_change"] or Fraction(1, 5)
    rows = []
    full = {**base_defaults, **params}
    for k in sorted(full):
        for f in (1 - c, 1 + c):
            v = _vary(full[k], f)
            if v is None:
                continue
            r = run(strategy, {**params, k: v}, a, b, f"{tag}-stab-{k}-{'dn' if f < 1 else 'up'}")
            pf = _perf(r, rs)
            rows.append(
                {
                    "param": k,
                    "value": v,
                    "trades": pf["trades"],
                    "profit_factor": pf["profit_factor"],
                    "net_cents": pf["net_cents"],
                }
            )
    stable = all(x["profit_factor"] is not None and x["profit_factor"] >= 1 for x in rows) if rows else None
    return {"change": str(c), "variants": rows, "stable": stable}


def cost_stress(
    run: Runner, rs: Any, strategy: str, params: dict[str, Any], a: int, b: int, tag: str, slippage_points: int
) -> dict[str, Any]:
    m = rs.approval["cost_multiplier"] or Fraction(3, 2)
    r = run(strategy, params, a, b, f"{tag}-cost", backtest={"spread_mult": str(m), "slippage_points": 2 * slippage_points})
    pf = _perf(r, rs)
    return {"spread_mult": str(m), "slippage_points": 2 * slippage_points, "trades": pf["trades"], "profit_factor": pf["profit_factor"],
            "net_cents": pf["net_cents"], "survives": pf["profit_factor"] is not None and pf["profit_factor"] >= 1}  # fmt: skip


def sensitivity(run: Runner, rs: Any, strategy: str, params: dict[str, Any], p1: str, v1: list[Any], p2: str, v2: list[Any], a: int, b: int,
                tag: str) -> dict[str, Any]:  # fmt: skip
    cells = []
    for i, x in enumerate(v1):
        for j, y in enumerate(v2):
            r = run(strategy, {**params, p1: x, p2: y}, a, b, f"{tag}-sens-{i}-{j}")
            pf = _perf(r, rs)
            cells.append(
                {
                    "i": i,
                    "j": j,
                    p1: x,
                    p2: y,
                    "trades": pf["trades"],
                    "objective": objective(pf, rs.objective),
                    "net_cents": pf["net_cents"],
                }
            )
    vals = [c for c in cells if c["objective"] is not None]
    plateau = None
    if vals:
        best = max(vals, key=lambda c: c["objective"])
        tol = float(rs.approval["param_change"] or Fraction(1, 5))
        nb = [c for c in cells if abs(c["i"] - best["i"]) <= 1 and abs(c["j"] - best["j"]) <= 1 and c is not best]
        good = [c for c in nb if c["objective"] is not None and c["objective"] >= (1 - tol) * best["objective"]]
        plateau = len(good) / len(nb) if nb else None
    return {"p1": p1, "v1": v1, "p2": p2, "v2": v2, "cells": cells, "plateau_score": plateau}


def heatmap_svg(sens: dict[str, Any], metric: str = "objective") -> str:
    v1, v2 = sens["v1"], sens["v2"]
    vals = [c[metric] for c in sens["cells"] if c[metric] is not None]
    lo, hi = (min(vals), max(vals)) if vals else (0.0, 1.0)
    W, H, cw, ch = 120 + 80 * len(v2), 60 + 40 * len(v1), 80, 40
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" font-family="sans-serif" font-size="11">',
           '<rect width="100%" height="100%" fill="#ffffff"/>', f'<text x="10" y="16">{metric}: {sens["p1"]} (rows) x {sens["p2"]} (columns)</text>']  # fmt: skip
    for j, y in enumerate(v2):
        out.append(f'<text x="{120 + j * cw + 4}" y="40">{y}</text>')
    for c in sens["cells"]:
        x, y = 120 + c["j"] * cw, 48 + c["i"] * ch
        v = c[metric]
        t = 0.5 if v is None or hi == lo else (v - lo) / (hi - lo)
        col = "#d0d7de" if v is None else f"rgb({int(255 * (1 - t))},{int(180 * t + 60)},{int(120 * (1 - t) + 60)})"
        out.append(f'<rect x="{x}" y="{y}" width="{cw - 2}" height="{ch - 2}" fill="{col}"/>')
        out.append(f'<text x="{x + 4}" y="{y + 24}">{"n/a" if v is None else (f"{v:.2f}" if math.isfinite(v) else v)}</text>')
    for i, xv in enumerate(v1):
        out.append(f'<text x="10" y="{48 + i * ch + 24}">{xv}</text>')
    out.append("</svg>")
    return "\n".join(out)
