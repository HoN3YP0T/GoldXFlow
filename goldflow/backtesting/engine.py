"""Phase 12: the event-driven backtester (``ProcessorSink``, placed after the ``StrategyEngine``).

One simulation clock. The Phase 4 replay drives the Phase 5–11 consumers exactly as in research and live. On every
processor advance to clock X the backtester:
1. reads the broker ticks with time < X (nothing later exists for it yet);
2. turns the Binance 1 s bars that ended by X into basis samples against the broker quote in force just before each
   second's end (skipping seconds where the broker quote is stale, e.g. while the broker is closed);
3. runs the simulator over those ticks merged with the new signals (each signal's event time <= X).

Outputs per run, under ``data/backtests/<run>/``:
* ``orders``, ``fills``, ``trades``, ``cash``, ``equity`` (Parquet), the full ledger;
* ``run.json``, the reproducibility record: run id, Binance dataset version, broker dataset version, config hash, code
  version, strategy versions, broker spec, execution model, starting capital, seed;
* ``report.md``: the execution assumptions printed in full, then the performance statistics, each of which is
  recomputed from the ledger and reconciled exactly (to the cent) before the report is written.
A run on **synthetic** broker ticks (tests) is refused unless ``allow_synthetic`` is set, and its report then says
``SYNTHETIC BROKER DATA - NOT A RESULT`` on every page.
"""

from __future__ import annotations

import json
from fractions import Fraction
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ..processing.trade_processor import ProcessorSink
from ..replay import ReplayGuardError
from ..timeutil import DAY_MS, iso
from .basis import BasisModel
from .broker import BrokerTicks
from .settings import BacktestSettings, BrokerSpec
from .simulator import Simulator

I64 = np.int64


class BrokerNotConfigured(RuntimeError):
    pass


class Rollovers:
    """Swap rollover instants: ``swap_time`` in broker server time, once per server day. A rollover belongs to the
    server day it **ends** (the day of the millisecond before it), so with the usual 00:00 rollover the one at 00:00
    Thursday closes Wednesday. Charged for days Mon–Fri; triple when that day is ``triple_swap_day`` (MT5's "3-day
    swap: Wednesday"). Confirm both with your broker's contract specification (decision D2)."""

    def __init__(self, spec: BrokerSpec) -> None:
        self.tz = ZoneInfo(spec.server_tz)
        self.minute = spec.swap_time_min
        self.triple = spec.triple_swap_day
        self._cache: dict[date, int] = {}

    def _at(self, d: date) -> int:
        if d not in self._cache:
            dt = datetime(d.year, d.month, d.day, self.minute // 60, self.minute % 60, tzinfo=self.tz)
            self._cache[d] = int(dt.astimezone(timezone.utc).timestamp() * 1000)
        return self._cache[d]

    def day_of(self, r: int) -> int:
        """Weekday (0 = Monday) of the server day that the rollover at ``r`` closes."""
        return datetime.fromtimestamp((r - 1) / 1000, tz=self.tz).weekday()

    def next_after(self, t: int) -> int:
        d = datetime.fromtimestamp(t / 1000, tz=self.tz).date() - timedelta(days=1)
        for _ in range(12):
            r = self._at(d)
            if r > t and self.day_of(r) < 5:
                return r
            d += timedelta(days=1)
        raise AssertionError("no rollover found")

    def multiplier(self, r: int) -> int:
        wd = self.day_of(r)
        return 3 if wd == self.triple else (1 if wd < 5 else 0)


def code_version(root: Path) -> str:
    try:
        return (
            subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, timeout=10).stdout.strip()
            or "unknown"
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"


class Backtester(ProcessorSink):
    name = "backtest"

    def __init__(
        self,
        cfg: Any,
        strategy_engine: Any,
        run_name: str = "bt",
        allow_synthetic: bool = False,
        out_root: Path | None = None,
        accept_from_ms: int | None = None,
    ) -> None:
        self.cfg = cfg
        self.accept_from = accept_from_ms  # signals before this time are warm-up only: never traded (walk-forward windows)
        self.bt = BacktestSettings.from_config(cfg)
        spec = BrokerSpec.from_raw(cfg.raw.get("broker"))
        if spec is None:
            raise BrokerNotConfigured(
                "broker.name is not set (decision D2): a backtest must fill on your broker's XAUUSD bid/ask. Fill in the broker "
                "block and import its tick history with `goldflow broker-import`; Binance prices are never used for fills."
            )
        self.spec = spec
        self.ticks = BrokerTicks(cfg.data_root, spec)
        if self.ticks.synthetic and not allow_synthetic:
            raise BrokerNotConfigured("the broker tick dataset is SYNTHETIC (test data); refusing to produce a backtest from it")
        self.basis = BasisModel(spec.digits, self.bt.basis_window_ms, self.bt.basis_min_samples)
        self.sim = Simulator(spec, self.bt, self.basis, Rollovers(spec))
        self.strategies = strategy_engine
        strategy_engine.listeners.append(self._on_signals)
        self.run_name = run_name
        self.out_root = Path(out_root) if out_root else Path(cfg.data_root) / "backtests"
        self.proc: Any = None
        self.pending: list[dict[str, Any]] = []
        self.bars: list[list[int]] = []  # Binance 1 s bars [end, close, low, high] not yet turned into samples
        self.cursor: int | None = None
        self.carry: list[int] | None = None  # last broker tick before the cursor
        self.stats = {
            "basis_samples": 0,
            "basis_skipped_stale_quote": 0,
            "signals": 0,
            "broker_ticks": 0,
            "signals_before_window": 0,
        }
        self.ctx: dict[str, Any] = {}

    # ------------------------------------------------------------------ inputs
    def _on_signals(self, table: str, rows: list[dict[str, Any]]) -> None:
        if table == "signals":
            for r in rows:
                if self.accept_from is not None and int(r["event_time"]) < self.accept_from:
                    self.stats["signals_before_window"] += 1
                else:
                    self.pending.append(r)
                    self.sim.out.setdefault("signals", []).append(r)  # traded signals with their evidence (regime splits)

    def on_start(self, ctx: dict[str, Any], proc: Any) -> None:
        self.proc = proc
        sinks = list(proc.sinks)
        if sinks[-1] is not self or self.strategies not in sinks:
            raise ValueError("Backtester must be the last sink, after its StrategyEngine")
        if not ctx.get("resumed"):
            self.ctx = {
                "dataset_version": ctx.get("dataset_version"),
                "start_ms": ctx.get("start_ms"),
                "end_ms": ctx.get("end_ms"),
            }

    def on_bars(self, interval: str, bars: dict[str, Any], levels: dict[str, Any], final_upto_ms: int) -> None:
        if interval != self.proc.st.intervals[0] or not len(bars["bar_start"]):
            return
        e = (np.asarray(bars["bar_start"], I64) + self.proc.st.base_ms).tolist()
        for row in zip(
            e,
            np.asarray(bars["close_ticks"]).tolist(),
            np.asarray(bars["low_ticks"]).tolist(),
            np.asarray(bars["high_ticks"]).tolist(),
        ):
            self.bars.append(list(row))

    def on_advance(self, proc: Any) -> None:
        if proc.clock is None:
            return
        X = int(proc.clock)
        a = (
            self.cursor
            if self.cursor is not None
            else min([r[0] for r in self.bars] + [int(s["event_time"]) for s in self.pending] + [X]) - DAY_MS
        )
        tk = self.ticks.between(a, X)
        if self.bt.spread_mult != 1 and len(tk["t"]):  # cost stress: widen the spread symmetrically around the mid
            m = self.bt.spread_mult
            widen = [int((Fraction(int(x)) * (m - 1) / 2).__ceil__()) for x in (tk["ask"] - tk["bid"]).tolist()]
            w = np.array(widen, I64)
            tk = {"t": tk["t"], "bid": tk["bid"] - w, "ask": tk["ask"] + w}
        self.cursor = X
        self.stats["broker_ticks"] += len(tk["t"])
        # basis samples and the Binance 1 s path, for seconds that ended by X
        if self.bars:
            B = np.array(self.bars, I64)
            self.bars = []
            ct = np.concatenate([[self.carry[0]] if self.carry else [], tk["t"]]).astype(I64)
            cb = np.concatenate([[self.carry[1]] if self.carry else [], tk["bid"]]).astype(I64)
            ca = np.concatenate([[self.carry[2]] if self.carry else [], tk["ask"]]).astype(I64)
            j = np.searchsorted(ct, B[:, 0], "left") - 1
            fresh = (j >= 0) & (B[:, 0] - ct[np.maximum(j, 0)] <= self.bt.max_tick_gap_ms)
            self.stats["basis_skipped_stale_quote"] += int((~fresh).sum())
            if fresh.any():
                jj = j[fresh]
                self.basis.add(B[fresh, 0], B[fresh, 1], cb[jj], ca[jj])
                self.stats["basis_samples"] += int(fresh.sum())
            P = self.sim.path
            keep = P["t"] > X - 2 * DAY_MS
            self.sim.path = {"t": np.concatenate([P["t"][keep], B[:, 0]]), "lo": np.concatenate([P["lo"][keep], B[:, 2]]),
                             "hi": np.concatenate([P["hi"][keep], B[:, 3]])}  # fmt: skip
        sigs = []
        for s in self.pending:
            if int(s["event_time"]) > X:
                raise AssertionError("signal later than the processor clock (look-ahead)")
            sigs.append((s, self._sessions(int(s["event_time"]))))
        self.stats["signals"] += len(sigs)
        self.pending = []
        self.sim.run(tk["t"], tk["bid"], tk["ask"], sigs, X)
        if len(tk["t"]):
            self.carry = [int(tk["t"][-1]), int(tk["bid"][-1]), int(tk["ask"][-1])]

    def _sessions(self, t: int) -> list[str]:
        cal = self.proc.cal
        return sorted(i.name for i in cal.instances(t, t, []) if i.kind == "session" and i.start_ms <= t < i.end_ms)

    def on_stop(self, ctx: dict[str, Any], proc: Any) -> None:
        if ctx.get("status") != "completed":
            return
        self.sim.close_all("END_OF_DATA")
        self.write()

    # ------------------------------------------------------------------ outputs
    def record(self) -> dict[str, Any]:
        strategies = [s.describe() for s in self.strategies.strategies]
        return {
            "run": self.run_name, "created_utc": iso(int(datetime.now(timezone.utc).timestamp() * 1000)),
            "binance_dataset_version": self.ctx.get("dataset_version"), "broker_dataset_version": self.ticks.dataset_version,
            "broker_data_synthetic": self.ticks.synthetic, "config_hash": self.cfg.config_hash, "backtest_settings_hash": self.bt.hash,
            "strategies_hash": self.strategies.hash, "code_version": code_version(Path(self.cfg.base_dir)),
            "strategies": strategies, "broker": self.spec.as_dict(), "execution_model": self.bt.raw, "starting_capital": str(self.bt.starting_capital),
            "seed": self.bt.seed, "range": {"start": iso(self.ctx.get("start_ms")), "end": iso(self.ctx.get("end_ms"))},
        }  # fmt: skip

    def write(self) -> Path:
        d = self.out_root / self.run_name
        d.mkdir(parents=True, exist_ok=True)
        out = self.sim.out
        for name, rows in out.items():
            tbl = (
                pa.Table.from_pylist(
                    [{k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in r.items()} for r in rows]
                )
                if rows
                else pa.table({})
            )
            pq.write_table(tbl, d / f"{name}.parquet")
        rec = self.record()
        st = stats(out, int(round(self.bt.starting_capital * 100)))
        rec["statistics"] = st
        rec["simulator_counts"] = self.sim.counts
        rec["backtester_counts"] = self.stats
        (d / "run.json").write_text(json.dumps(rec, indent=1, default=str))
        (d / "report.md").write_text(report(rec, st))
        return d

    # ------------------------------------------------------------------ state
    def get_state(self) -> dict[str, Any]:
        out: dict[str, Any] = json.loads(json.dumps({"hash": self.bt.hash, "pending": self.pending, "bars": self.bars, "cursor": self.cursor, "carry": self.carry,
                                      "stats": self.stats, "ctx": self.ctx, "basis": self.basis.state(), "sim": self.sim.state()}, default=str))  # fmt: skip
        return out

    def set_state(self, s: dict[str, Any]) -> None:
        if not s:
            return
        if s.get("hash") != self.bt.hash:
            raise ReplayGuardError(
                "backtest checkpoint was taken with different backtest or broker settings; start a new run name"
            )
        self.pending, self.bars, self.cursor, self.carry = (
            list(s["pending"]),
            [list(x) for x in s["bars"]],
            s["cursor"],
            s["carry"],
        )
        self.stats, self.ctx = dict(s["stats"]), dict(s["ctx"])
        self.basis.load(s["basis"])
        self.sim.load(s["sim"])


# ============================================================================ statistics, reconciliation, report
def stats(out: dict[str, list[dict[str, Any]]], start_cents: int) -> dict[str, Any]:
    """Performance statistics from the trade list, reconciled exactly against the cash ledger (raises otherwise)."""
    tr, cash = out["trades"], out["cash"]
    final = cash[-1]["balance_cents"] if cash else start_cents
    total_cash = sum(c["amount_cents"] for c in cash)
    net = sum(t["net_cents"] for t in tr)
    if final != start_cents + total_cash:
        raise AssertionError("ledger does not reconcile: balance != start + cash flows")
    if net != total_cash:
        raise AssertionError("ledger does not reconcile: sum of trade net P&L != sum of cash flows")
    per_order: dict[str, int] = {}
    for c in cash:
        per_order[c["order_id"]] = per_order.get(c["order_id"], 0) + c["amount_cents"]
    for t in tr:
        if per_order.get(t["order_id"]) != t["net_cents"]:
            raise AssertionError(f"trade {t['order_id']} net P&L does not equal its cash flows")
    wins = [t for t in tr if t["net_cents"] > 0]
    losses = [t for t in tr if t["net_cents"] <= 0]
    gw, gl = sum(t["net_cents"] for t in wins), -sum(t["net_cents"] for t in losses)
    bal, peak, mdd, mdd_pct = start_cents, start_cents, 0, 0.0
    for c in cash:
        bal = c["balance_cents"]
        peak = max(peak, bal)
        if peak - bal > mdd:
            mdd, mdd_pct = peak - bal, (peak - bal) / peak
    run = best = 0
    for t in tr:
        run = run + 1 if t["net_cents"] <= 0 else 0
        best = max(best, run)
    rs = [float(t["r_multiple"]) for t in tr if t["r_multiple"] is not None]
    by: dict[str, dict[str, Any]] = {}
    for t in tr:
        for k in (f"strategy:{t['strategy_id']}", f"side:{t['direction']}"):
            x = by.setdefault(k, {"trades": 0, "net_cents": 0, "wins": 0})
            x["trades"] += 1
            x["net_cents"] += t["net_cents"]
            x["wins"] += t["net_cents"] > 0
    orders = out["orders"]
    status: dict[str, int] = {}
    for o in orders:
        status[o["status"]] = status.get(o["status"], 0) + 1
    return {
        "trades": len(tr), "wins": len(wins), "losses": len(losses), "win_rate": len(wins) / len(tr) if tr else None,
        "net_cents": net, "gross_cents": sum(t["gross_cents"] for t in tr), "commission_cents": sum(t["commission_cents"] for t in tr),
        "swap_cents": sum(t["swap_cents"] for t in tr), "profit_factor": (gw / gl) if gl else None,
        "expectancy_cents": net / len(tr) if tr else None, "expectancy_r": sum(rs) / len(rs) if rs else None,
        "avg_win_cents": gw / len(wins) if wins else None, "avg_loss_cents": -gl / len(losses) if losses else None,
        "max_drawdown_cents": mdd, "max_drawdown_pct": mdd_pct, "max_consecutive_losses": best,
        "avg_duration_min": sum((t["exit_time"] - t["entry_time"]) for t in tr) / len(tr) / 60_000 if tr else None,
        "starting_cents": start_cents, "final_cents": final, "exit_reasons": _count(tr, "exit_reason"), "orders_by_status": status,
        "splits": by, "reconciled": True,
    }  # fmt: skip


def _count(rows: list[dict[str, Any]], k: str) -> dict[str, int]:
    c: dict[str, int] = {}
    for r in rows:
        c[str(r[k])] = c.get(str(r[k]), 0) + 1
    return dict(sorted(c.items()))


def report(rec: dict[str, Any], st: dict[str, Any]) -> str:
    syn = rec["broker_data_synthetic"]
    banner = ["", "> **SYNTHETIC BROKER DATA - NOT A RESULT.** This run exercises the engine only.", ""] if syn else []
    ex, b = rec["execution_model"], rec["broker"]

    def usd(c: Any) -> str:
        return "n/a" if c is None else f"{c / 100:,.2f}"

    lines = [f"# Backtest {rec['run']}", *banner, "## Execution assumptions (read these first)", "",
             f"- **Fills on broker {b['name']} {b['symbol']} quotes**: buy at ask, sell at bid; never on Binance prices.",
             f"- Signal venue: Binance XAUUSDT; levels translated to the broker with the basis known at signal time "
             f"(median of 1 s samples over {ex['basis']['window']}, at least {ex['basis']['min_samples']}; Binance last trade vs broker mid).",
             f"- Latency signal -> broker: {ex['latency_ms']} ms. Slippage: {ex['slippage_points']} points + {ex['slippage_vol_mult']} x sigma "
             f"of the last {ex['slippage_vol_ticks']} mid changes, against the trader. Stops fill at the quote that triggered them (gaps included); targets at the target price.",
             f"- Stop/target ambiguity: first broker tick decides; after a gap > {ex['max_tick_gap']} the stop is assumed first if the Binance path reached it.",
             f"- Broker rejections: {ex['reject_rate']} (seed {ex['seed']}); stops level {b['stops_level_points']} points. Partial fills: off.",
             f"- Costs: commission {b['commission_per_lot_side']} {b['account_currency']} per lot per side; swap long {b['swap_long_per_lot']}, "
             f"short {b['swap_short_per_lot']} per lot per rollover at {b['swap_time_min'] // 60:02d}:{b['swap_time_min'] % 60:02d} server time "
             f"({b['server_tz']}), Mon–Fri, triple on weekday {b['triple_swap_day']}.",
             f"- Contract: {b['contract_size']} oz per lot, lots {b['volume_min']}–{b['volume_max']} step {b['volume_step']}.",
             f"- Risk: {ex['risk']['risk_per_trade']} of equity per trade; max {ex['risk']['max_concurrent_positions']} positions, "
             f"{ex['risk']['max_exposure_lots']} lots; daily loss limit {ex['risk']['daily_loss_limit']}; max drawdown {ex['risk']['max_drawdown']}; "
             f"pause after {ex['risk']['max_consecutive_losses']} consecutive losses; sessions {ex['risk']['sessions'] or 'all'}; max hold {ex['max_hold']}.",
             "", "## Reproducibility", "",
             *[f"- {k}: `{rec[k]}`" for k in ("binance_dataset_version", "broker_dataset_version", "config_hash", "backtest_settings_hash",
                                             "strategies_hash", "code_version", "seed", "starting_capital")],
             f"- strategies: {', '.join(s['id'] + ' v' + s['version'] for s in rec['strategies'])}", f"- range: {rec['range']}",
             "", "## Results (reconciled to the cent against the cash ledger)", "", "| Metric | Value |", "|---|---|",
             f"| Trades | {st['trades']} (wins {st['wins']}, losses {st['losses']}) |",
             f"| Net / gross P&L ({b['account_currency']}) | {usd(st['net_cents'])} / {usd(st['gross_cents'])} |",
             f"| Commission / swap | {usd(st['commission_cents'])} / {usd(st['swap_cents'])} |",
             f"| Win rate | {st['win_rate']} |", f"| Profit factor | {st['profit_factor']} |",
             f"| Expectancy | {usd(st['expectancy_cents'])} per trade; {st['expectancy_r']} R |",
             f"| Average win / loss | {usd(st['avg_win_cents'])} / {usd(st['avg_loss_cents'])} |",
             f"| Max drawdown | {usd(st['max_drawdown_cents'])} ({st['max_drawdown_pct']:.2%}) |",
             f"| Max consecutive losses | {st['max_consecutive_losses']} |", f"| Average duration (min) | {st['avg_duration_min']} |",
             f"| Balance | {usd(st['starting_cents'])} -> {usd(st['final_cents'])} |",
             f"| Exit reasons | {st['exit_reasons']} |", f"| Orders by status | {st['orders_by_status']} |", "",
             "Splits by strategy and side are in run.json. A result on few trades is not evidence (Phase 13: >= 100 trades, out of sample).",
             *banner]  # fmt: skip
    return "\n".join(lines) + "\n"


__all__ = ["Backtester", "BrokerNotConfigured", "Rollovers", "stats", "report"]
