"""Phase 13: performance report from a backtest's ledger, with uncertainty.

Every figure is computed from the trade list of a run whose cash ledger has already been reconciled to the cent
(``backtesting.stats``); ``performance`` re-checks Σ trade net = Σ cash flows and refuses otherwise.

* Net and gross P&L, win rate (Wilson 95 % interval), profit factor, expectancy in money and in R, average win and
  loss, R distribution (quantiles), maximum drawdown and its recovery time, maximum consecutive losses, duration,
  frequency, **cost impact** (gross vs net: commission, swap, and slippage measured from fill vs quote).
* Splits by strategy, side, session and regime (auction state and volatility tercile from the signal's evidence).
* A conclusion needs at least ``min_trades_conclusion`` trades (default 100): below that the report says
  ``INSUFFICIENT_SAMPLE`` next to every ratio. Bootstrap 95 % intervals (seeded) for expectancy and profit factor.
* ``monte_carlo``: trade-order bootstrap -> distribution of maximum drawdown and of the worst losing streak.
"""

from __future__ import annotations

import json
import math
import random
from typing import Any

DAY = 86_400_000


def _pf(xs: list[int]) -> float | None:
    gw, gl = sum(x for x in xs if x > 0), -sum(x for x in xs if x <= 0)
    return (gw / gl) if gl else None


def _dd(xs: list[int], start: int) -> tuple[int, float, int, int | None]:
    """(max drawdown cents, as a fraction of the peak, index where it bottomed, trades until recovered or None)."""
    bal = peak = start
    mdd, pct, at = 0, 0.0, -1
    peaks = []
    for i, x in enumerate(xs):
        bal += x
        if bal > peak:
            peak = bal
        peaks.append(peak)
        if peak - bal > mdd:
            mdd, pct, at = peak - bal, (peak - bal) / peak if peak else 0.0, i
    rec = None
    if at >= 0:
        bal = start + sum(xs[: at + 1])
        target = peaks[at]
        for j in range(at + 1, len(xs)):
            bal += xs[j]
            if bal >= target:
                rec = j - at
                break
    return mdd, pct, at, rec


def wilson(k: int, n: int, z: float = 1.959964) -> tuple[float, float] | None:
    if n == 0:
        return None
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (c - h, c + h)


def bootstrap(xs: list[float], fn: Any, samples: int, seed: int) -> tuple[float, float] | None:
    if len(xs) < 2:
        return None
    rng = random.Random(seed)
    vals = sorted(v for v in (fn([xs[rng.randrange(len(xs))] for _ in xs]) for _ in range(samples)) if v is not None)
    if not vals:
        return None
    return vals[int(0.025 * (len(vals) - 1))], vals[int(0.975 * (len(vals) - 1))]


def monte_carlo(nets: list[int], start: int, samples: int, seed: int) -> dict[str, Any]:
    if not nets:
        return {}
    rng = random.Random(seed)
    dds, streaks = [], []
    for _ in range(samples):
        xs = nets[:]
        rng.shuffle(xs)
        dds.append(_dd(xs, start)[1])
        run = best = 0
        for x in xs:
            run = run + 1 if x <= 0 else 0
            best = max(best, run)
        streaks.append(best)
    dds.sort()
    streaks.sort()

    def q(a: list[Any], p: float) -> Any:
        return a[int(p * (len(a) - 1))]

    return {"samples": samples, "seed": seed, "max_drawdown_pct": {"p50": q(dds, 0.5), "p95": q(dds, 0.95), "p99": q(dds, 0.99)},
            "worst_losing_streak": {"p50": q(streaks, 0.5), "p95": q(streaks, 0.95), "p99": q(streaks, 0.99)}}  # fmt: skip


def _regime(sig: dict[str, Any] | None) -> dict[str, Any]:
    if not sig:
        return {}
    for e in json.loads(sig["evidence"]):
        if e["metric"] == "regime":
            return dict(e["value"])
    return {}


def performance(trades: list[dict[str, Any]], cash: list[dict[str, Any]], start_cents: int, fills: list[dict[str, Any]] | None = None,
                signals: dict[str, dict[str, Any]] | None = None, min_trades: int = 100, boot: tuple[int, int] = (2000, 7),
                calendar: Any = None) -> dict[str, Any]:  # fmt: skip
    trades = sorted(trades, key=lambda t: (t["exit_time"], t["order_id"]))
    nets = [int(t["net_cents"]) for t in trades]
    total_cash = sum(int(c["amount_cents"]) for c in cash)
    if sum(nets) != total_cash:
        raise AssertionError("performance: Σ trade net P&L != Σ cash flows (ledger not reconciled)")
    n = len(trades)
    wins = [x for x in nets if x > 0]
    losses = [x for x in nets if x <= 0]
    rs = [float(t["r_multiple"]) for t in trades if t.get("r_multiple") is not None]
    mdd, mdd_pct, _, rec = _dd(nets, start_cents)
    run = best = 0
    for nv in nets:
        run = run + 1 if nv <= 0 else 0
        best = max(best, run)
    slip = 0
    for f in fills or []:
        slip += abs(int(f["price_points"]) - int(f["quote_points"])) if f["reason"] != "TP" else 0
    span_days = ((trades[-1]["exit_time"] - trades[0]["entry_time"]) / DAY) if n else 0
    enough = n >= min_trades
    rq = sorted(rs)
    out: dict[str, Any] = {
        "trades": n, "sample": "OK" if enough else "INSUFFICIENT_SAMPLE", "net_cents": sum(nets),
        "gross_cents": sum(int(t["gross_cents"]) for t in trades), "commission_cents": sum(int(t["commission_cents"]) for t in trades),
        "swap_cents": sum(int(t["swap_cents"]) for t in trades), "slippage_points_total": slip,
        "win_rate": len(wins) / n if n else None, "win_rate_ci95": wilson(len(wins), n),
        "profit_factor": _pf(nets), "profit_factor_ci95": bootstrap([float(x) for x in nets], lambda xs: _pf([int(x) for x in xs]), *boot),
        "expectancy_cents": sum(nets) / n if n else None, "expectancy_cents_ci95": bootstrap([float(x) for x in nets], lambda xs: sum(xs) / len(xs), *boot),
        "expectancy_r": sum(rs) / len(rs) if rs else None,
        "r_quantiles": {p: rq[int(p * (len(rq) - 1))] for p in (0.05, 0.25, 0.5, 0.75, 0.95)} if rq else {},
        "avg_win_cents": sum(wins) / len(wins) if wins else None, "avg_loss_cents": sum(losses) / len(losses) if losses else None,
        "max_drawdown_cents": mdd, "max_drawdown_pct": mdd_pct, "recovery_trades": rec, "max_consecutive_losses": best,
        "avg_duration_min": sum(t["exit_time"] - t["entry_time"] for t in trades) / n / 60_000 if n else None,
        "trades_per_day": n / span_days if span_days > 0 else None,
        "cost_impact_cents": sum(int(t["gross_cents"]) for t in trades) - sum(nets),
    }  # fmt: skip
    splits: dict[str, dict[str, Any]] = {}
    vols = sorted(
        r["realized_vol_bps"]
        for t in trades
        if (r := _regime((signals or {}).get(t["signal_id"]))).get("realized_vol_bps") is not None
    )
    cut = (vols[len(vols) // 3], vols[2 * len(vols) // 3]) if len(vols) >= 3 else None
    for t in trades:
        reg = _regime((signals or {}).get(t["signal_id"]))
        keys = [f"strategy:{t['strategy_id']}", f"side:{t['direction']}", f"auction_state:{reg.get('auction_state')}"]
        sess = reg.get("sessions")
        keys.append(f"session:{'+'.join(sess) if sess else 'outside'}" if sess is not None else "session:unknown")
        rv = reg.get("realized_vol_bps")
        if cut and rv is not None:
            keys.append(f"volatility:{'low' if rv < cut[0] else 'mid' if rv < cut[1] else 'high'}")
        for k in keys:
            x: dict[str, Any] = splits.setdefault(k, {"trades": 0, "net_cents": 0, "wins": 0, "nets": []})
            x["trades"] += 1
            x["net_cents"] += int(t["net_cents"])
            x["wins"] += int(t["net_cents"]) > 0
            x["nets"].append(int(t["net_cents"]))
    for sx in splits.values():
        sx["profit_factor"] = _pf(sx.pop("nets"))
        sx["sample"] = "OK" if sx["trades"] >= min_trades else "INSUFFICIENT_SAMPLE"
    out["splits"] = dict(sorted(splits.items()))
    out["monte_carlo"] = monte_carlo(nets, start_cents, *boot)
    return out
