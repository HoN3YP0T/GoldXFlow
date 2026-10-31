"""Phase 13: run one full-pipeline backtest over a date range with parameter overrides.

The same code as research and live: replay → Phase 5–10 analytics → one strategy → Phase 12 backtester on broker
ticks. The replay starts ``warmup`` before the range so analytics and strategy warm-up are complete at the range start;
signals before the range start are warm-up only and never traded (``Backtester(accept_from_ms=...)``). Positions still
open at the range end are closed at the last broker quote (``END_OF_DATA``), so windows never leak into each other.
Every run refuses a range that touches the untouched test period unless it comes through ``TestSetLock.open_for``.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from ..timeutil import iso
from .ledger import TestSetLock
from .settings import ResearchSettings


class PipelineRunner:
    def __init__(self, cfg: Any, rs: ResearchSettings, out_root: Path | None = None, allow_synthetic: bool = False) -> None:
        self.cfg, self.rs = cfg, rs
        self.out_root = Path(out_root) if out_root else Path(cfg.data_root) / "research" / "runs"
        self.allow_synthetic = allow_synthetic
        self.lock = TestSetLock(Path(cfg.data_root) / "research", rs.splits["test"])

    def config_for(self, strategy: str, params: dict[str, Any], backtest: dict[str, Any] | None = None) -> Any:
        from ..config import Config

        raw = copy.deepcopy(self.cfg.raw)
        st = raw.setdefault("strategies", {})
        st["enabled"] = [strategy]
        st.setdefault("params", {})
        st["params"][strategy] = {**(st["params"].get(strategy) or {}), **params}
        for k, v in (backtest or {}).items():
            raw.setdefault("backtest", {})[k] = {**raw["backtest"].get(k, {}), **v} if isinstance(v, dict) else v
        return Config(raw, self.cfg.base_dir)

    def run(self, strategy: str, params: dict[str, Any], start_ms: int, end_ms: int, tag: str, backtest: dict[str, Any] | None = None,
            test_access: bool = False) -> dict[str, Any]:  # fmt: skip
        from ..backtesting import Backtester
        from ..orderflow import ProfileEngine
        from ..orderflow.auction_engine import AuctionEngine
        from ..orderflow.flow_engine import OrderflowEngine
        from ..orderflow.initial_balance import IBEngine
        from ..processing import TradeProcessor
        from ..replay import ReplayEngine
        from ..strategies import StrategyEngine, build_strategies

        if not test_access:
            self.lock.guard(start_ms, end_ms, f"run {tag}")
        cfg = self.config_for(strategy, params, backtest)
        prof, flow, ib = ProfileEngine(cfg), OrderflowEngine(cfg), IBEngine(cfg)
        auc = AuctionEngine(cfg, prof, ib_engine=ib)
        strat = StrategyEngine(cfg, build_strategies(cfg), {"profile": prof, "orderflow": flow, "ib": ib, "auction": auc})
        sigs: dict[str, dict[str, Any]] = {}
        strat.listeners.append(lambda t, rows: sigs.update({r["signal_id"]: r for r in rows}) if t == "signals" else None)
        bt = Backtester(
            cfg, strat, run_name=tag, allow_synthetic=self.allow_synthetic, out_root=self.out_root, accept_from_ms=start_ms
        )
        proc = TradeProcessor(cfg, sinks=[prof, flow, ib, auc, strat, bt])
        avail = self._available(start_ms - self.rs.warmup_ms, end_ms)
        res = ReplayEngine(cfg, [proc], start=avail[0], end=avail[1], name=f"research-{tag}").run()
        if res["status"] != "completed":
            raise RuntimeError(f"research run {tag} did not complete: {res['status']}")
        out = bt.sim.out
        return {"tag": tag, "strategy": strategy, "params": params, "range": [iso(start_ms), iso(end_ms)], "trades": out["trades"],
                "cash": out["cash"], "fills": out["fills"], "orders": out["orders"], "signals": sigs, "start_cents": int(round(bt.bt.starting_capital * 100)),
                "run_dir": str(self.out_root / tag), "record": bt.record()}  # fmt: skip

    def _available(self, a: int, b: int) -> tuple[int, int]:
        """Clip a range to the replayable data (the replay refuses dates it does not have)."""
        from ..fsutil import connect

        db = connect(self.cfg.state_db)
        try:
            rows = db.execute("SELECT MIN(date), MAX(date) FROM partitions WHERE dataset='trades'").fetchone()
        finally:
            db.close()
        from datetime import date

        lo = (date.fromisoformat(rows[0]).toordinal() - date(1970, 1, 1).toordinal()) * 86_400_000 if rows and rows[0] else a
        hi = (date.fromisoformat(rows[1]).toordinal() - date(1970, 1, 1).toordinal() + 1) * 86_400_000 if rows and rows[1] else b
        return max(a, lo), min(b, hi)
