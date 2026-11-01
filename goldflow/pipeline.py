"""How the phases connect: one builder for the analytics and strategy stack, and the ``pipeline`` command.

**The stack.** Every place that runs the analytics builds them the same way, and in the order the data flows:
- historical replay (``signals``, ``backtest``, ``pipeline``);
- live (``live-run``, ``bridge-run``);
- the parity harness, research runs and other venues (``venue-run``).

::

    Phase 4 ReplayEngine / Phase 14 LiveBus            (EventChunk: trades + quality events)
      -> Phase 5  TradeProcessor                       (bars, footprint, windows, DQ labels)
           sinks, in order:
           -> Phase 6  ProfileEngine                   (profiles, POC/VA, VWAP)      -> listeners: Phase 9
           -> Phase 7  OrderflowEngine                 (delta, CVD, imbalances, events)
           -> Phase 10 IBEngine                        (Initial Balance)             -> listeners: Phase 9 levels
           -> Phase 9  AuctionEngine(profile, ib)      (levels, acceptance, failed auctions, state)
           -> Phase 11 StrategyEngine(all of the above)(signals, gating)             -> listeners: Phase 16 bridge
           -> Phase 12 Backtester(strategy engine)     (fills on broker bid/ask; only with a broker, D2)

``build_stack`` returns these objects, wired as above.
- With ``stores``, each engine also writes its dataset; without it, everything stays in memory.
- ``pre_sinks`` go in front of the engines; collectors use them to see bars before anyone else.
- A sink always comes after the engines it reads from. The processor delivers finalised bars to the sinks in list
  order, so an engine never sees a later phase's output before its own inputs.

**The pipeline** (``pipeline --name N``) runs Phases 5–11 in **one** replay with every dataset store attached, then
every independent verifier:
- Phase 5 reconciliation;
- Phase 6 profile verification;
- Phase 7 orderflow verification;
- Phase 10 IB verification;
- Phase 9 auction verification;
- Phase 11 signal verification.

The result goes to ``data/reports/pipeline_<name>.json``. The datasets are the same ones the single-phase commands
write (tested), produced in one pass instead of five. With ``--backtest`` and a filled ``broker`` block (D2), the
Phase 12 backtester runs in the same pass.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

STORES = ("footprint", "profile", "orderflow", "ib", "auction", "signals")


@dataclass
class Stack:
    proc: Any
    profile: Any
    orderflow: Any
    ib: Any
    auction: Any
    strategies: Any
    backtester: Any = None
    signal_listeners: list[Callable[[str, list[dict[str, Any]]], None]] = field(default_factory=list)

    @property
    def engines(self) -> dict[str, Any]:
        return {
            "profile": self.profile,
            "orderflow": self.orderflow,
            "ib": self.ib,
            "auction": self.auction,
            "strategies": self.strategies,
        }

    def digests(self) -> dict[str, str]:
        out = {"processor": self.proc.state_digest()}
        out.update({k: v.state_digest() for k, v in self.engines.items()})
        return out


def build_stack(cfg: Any, *, stores: tuple[str, ...] | list[str] = (), resume: bool = False, pre_sinks: list[Any] | None = None,
                backtest: dict[str, Any] | None = None, signals_root: Path | None = None) -> Stack:  # fmt: skip
    """The Phase 5-12 stack, wired in data-flow order.

    stores        datasets to write: any of footprint, profile, orderflow, ib, auction, signals (replaced unless resume)
    pre_sinks     processor sinks placed before the engines (collectors, writers)
    backtest      kwargs for the Phase 12 Backtester (run_name, allow_synthetic, out_root, accept_from_ms); None = no backtest
    signals_root  data root for the signals dataset (live runs keep theirs under data/live/<name>)
    """
    from .orderflow.auction_engine import AuctionEngine, AuctionStore
    from .orderflow.engine import ProfileEngine
    from .orderflow.flow_engine import OrderflowEngine
    from .orderflow.flow_store import OrderflowStore
    from .orderflow.initial_balance import IBEngine, IBStore
    from .orderflow.store import ProfileStore
    from .processing.footprint_store import FootprintWriter
    from .processing.trade_processor import TradeProcessor
    from .strategies import SignalStore, StrategyEngine, build_strategies

    bad = sorted(set(stores) - set(STORES))
    if bad:
        raise ValueError(f"unknown stores {bad}; known: {list(STORES)}")
    rep = not resume
    root, sym = cfg.data_root, cfg.symbol
    prof = ProfileEngine(cfg, store=ProfileStore(root, sym, replace=rep) if "profile" in stores else None)
    flow = OrderflowEngine(cfg, store=OrderflowStore(root, sym, replace=rep) if "orderflow" in stores else None)
    ib = IBEngine(cfg, store=IBStore(root, sym, replace=rep) if "ib" in stores else None)
    auc = AuctionEngine(cfg, prof, store=AuctionStore(root, sym, replace=rep) if "auction" in stores else None, ib_engine=ib)
    sig_store = SignalStore(signals_root or root, sym, replace=rep) if "signals" in stores else None
    strat = StrategyEngine(
        cfg, build_strategies(cfg), {"profile": prof, "orderflow": flow, "ib": ib, "auction": auc}, store=sig_store
    )
    sinks: list[Any] = list(pre_sinks or [])
    if "footprint" in stores:
        sinks.append(FootprintWriter(root, sym, replace=rep))
    sinks += [prof, flow, ib, auc, strat]
    bt = None
    if backtest is not None:
        from .backtesting import Backtester

        bt = Backtester(cfg, strat, **backtest)
        sinks.append(bt)
    stack = Stack(TradeProcessor(cfg, sinks=sinks), prof, flow, ib, auc, strat, bt)

    def fanout(table: str, rows: list[dict[str, Any]]) -> None:
        for f in stack.signal_listeners:
            f(table, rows)

    strat.listeners.append(fanout)
    return stack


def run_pipeline(cfg: Any, name: str, *, start: Any = None, end: Any = None, dates: list[str] | None = None, resume: bool = False,
                 until: Any = None, backtest: bool = False, verify: bool = True, footprint: bool = True,
                 stop_event: Any = None, progress: Callable[[dict[str, Any]], None] | None = None) -> dict[str, Any]:  # fmt: skip
    """Phases 5-11 (and 12 with a broker) in one replay, every dataset written, then every verifier."""
    from .replay import ChainHasher, IntegrityChecker, ReplayEngine

    bt_kwargs = {"run_name": name} if backtest else None
    stores = ("footprint",) * footprint + ("profile", "orderflow", "ib", "auction", "signals")
    st = build_stack(cfg, stores=stores, resume=resume, backtest=bt_kwargs)
    hasher, checker = ChainHasher(), IntegrityChecker()  # type: ignore[no-untyped-call]
    eng = ReplayEngine(
        cfg, [hasher, checker, st.proc], start=start, end=end, dates=dates, name=f"pipeline-{name}", progress=progress
    )
    if stop_event is not None:
        stop_event.append(eng.stop)
    res = eng.run(resume=resume, until=until)
    out: dict[str, Any] = {"name": name, "inputs": _inputs(cfg, eng, backtest), "status": res["status"], "events": res["events"], "wall_s": res["wall_s"], "events_per_s": res["events_per_s"],
                           "peak_rss_mb": res["peak_rss_mb"], "sequence_digest": hasher.digest, "integrity_violations": checker.violations,
                           "state_digests": st.digests(), "processor": st.proc.summary(), "strategies": st.strategies.summary()}  # fmt: skip
    if st.backtester is not None and res["status"] == "completed":
        out["backtest"] = str(st.backtester.out_root / name)
    if verify and res["status"] == "completed":
        out["verification"] = verify_all(cfg)
    out["ok"] = (
        res["status"] == "completed"
        and not checker.violations
        and all(not v.get("fail") for v in (out.get("verification") or {}).values())
    )
    d = Path(cfg.data_root) / "reports"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"pipeline_{name}.json").write_text(json.dumps(out, indent=1, default=str))
    return out


def _inputs(cfg: Any, eng: Any, backtest: bool) -> dict[str, Any]:
    """What a pipeline result depends on: the data (dataset version incl. quality events), the full configuration, the
    replay range and streams, the code version, and whether the backtest ran."""
    from . import __version__

    return {"dataset_version": eng.dataset_version, "config_hash": cfg.config_hash, "replay_config_hash": eng.config_hash,
            "code_version": __version__, "backtest": bool(backtest)}  # fmt: skip


def up_to_date(
    cfg: Any, name: str, *, start: Any = None, end: Any = None, dates: list[str] | None = None, backtest: bool = False
) -> str | None:
    """None if ``data/reports/pipeline_<name>.json`` is a completed, verified run on exactly the current inputs;
    otherwise the reason it must run."""
    from .replay import ReplayEngine

    p = Path(cfg.data_root) / "reports" / f"pipeline_{name}.json"
    if not p.exists():
        return "no previous run"
    prev = json.loads(p.read_text())
    if prev.get("status") != "completed" or not prev.get("ok"):
        return "the previous run did not complete cleanly"
    eng = ReplayEngine(cfg, [], start=start, end=end, dates=dates, name=f"pipeline-{name}", checkpoint=False)
    now = _inputs(cfg, eng, backtest)
    diff = sorted(k for k in now if prev.get("inputs", {}).get(k) != now[k])
    return None if not diff else f"inputs changed: {diff}"


def verify_all(cfg: Any) -> dict[str, dict[str, Any]]:
    """Every independent verifier on the datasets just written (each recomputes from the Phase 3 partitions)."""
    from .orderflow.auction_verify import verify as au
    from .orderflow.flow_verify import verify as of
    from .orderflow.ib_verify import verify as ibv
    from .orderflow.verify import verify as pv
    from .processing.reconcile import reconcile
    from .strategies.verify import verify as sv

    out: dict[str, dict[str, Any]] = {}
    checks: dict[str, Callable[[], dict[str, Any]]] = {
        "phase5_reconcile": lambda: reconcile(cfg, interval="1m"), "phase6_profile": lambda: pv(cfg), "phase7_orderflow": lambda: of(cfg),
        "phase10_ib": lambda: ibv(cfg), "phase9_auction": lambda: au(cfg), "phase11_signals": lambda: sv(cfg)}  # fmt: skip
    for name, fn in checks.items():
        try:
            out[name] = fn()["summary"]
        except Exception as e:  # noqa: BLE001 - a crashing verifier is a failure to report, not to hide
            out[name] = {"fail": [f"verifier crashed: {e!r}"]}
    return out
