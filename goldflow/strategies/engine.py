"""Phase 11: the strategy engine (``ProcessorSink``, placed **last**). Gating, dispatch, signal dataset.

Wiring: ``StrategyEngine(cfg, strategies, sources={"orderflow": ..., "auction": ..., "ib": ..., "profile": ...})``
registers itself as a listener of each analytics engine it is given. During a processor advance those engines emit
rows; the strategy engine buffers them and, in its own ``on_advance`` (after every other sink), dispatches them in
``available_at`` order. All rows with the same ``available_at`` are applied to the state **before** any trigger at
that time is evaluated, and nothing later is ever visible: a strategy sees exactly what was known at the trigger's
time.

Gating is engine-enforced and may only **suppress** (each suppression is recorded with its reason):
* ``CAPABILITY``: a strategy whose required capabilities the provider cannot meet is refused at construction
  (``CapabilityUnavailable``); one whose required analytics engines are missing is refused too.
* ``WARMUP``: before ``observe_from + warmup``.
* ``DQ_OPEN``: a data-quality interval (Phase 3/4 quality events, or live feed health later) is open at the signal time.
* ``DQ_EVIDENCE``: the signal's evidence is not data-quality OK.
* ``DUPLICATE_ACTIVE``: the same strategy already has a signal in the same direction that has not expired yet (one
  setup, one signal; two triggers confirmed by the same bar would otherwise double the exposure).
Every emitted signal is checked (``TradingSignal.check``): evidence never later than the signal, stop and target on the
correct sides, expiry after the event. A failure is a bug and stops the run.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Callable

import pyarrow as pa

from .. import schemas as S
from ..orderflow.flow_store import DerivedStore
from ..processing.aggregation import digest_tree
from ..processing.settings import parse_interval
from ..processing.trade_processor import ProcessorSink
from ..providers import create_provider
from ..replay import ReplayGuardError
from .base import AnalyticsState, Strategy
from .library import REGISTRY
from .regime import RegimeTracker

Listener = Callable[[str, list[dict[str, Any]]], None]
DEFAULT_STRATEGIES: dict[str, Any] = {
    "version": 1,
    "enabled": ["fa_value_edge", "absorption_level", "ib_failed_break"],
    "params": {},
    "warmup": "24h",
    "regime": {"vol_bars": 12, "volume_lookback_days": 20},
    "persist": True,
}


def strategy_settings(cfg: Any) -> dict[str, Any]:
    user = cfg.raw.get("strategies") or {}
    raw = {**DEFAULT_STRATEGIES, **{k: v for k, v in user.items() if not k.startswith("_")}}
    bad = sorted(set(raw) - set(DEFAULT_STRATEGIES))
    if bad:
        raise ValueError(f"strategies: unknown keys {bad}")
    unknown = [s for s in raw["enabled"] if s not in REGISTRY]
    if unknown:
        raise ValueError(f"strategies.enabled: unknown strategies {unknown}; available {sorted(REGISTRY)}")
    for sid, p in (raw["params"] or {}).items():
        if sid not in REGISTRY:
            raise ValueError(f"strategies.params: unknown strategy {sid}")
        REGISTRY[sid]({k: v for k, v in p.items() if not k.startswith("_")})  # validates the parameters
    parse_interval(str(raw["warmup"]))
    return raw


def build_strategies(cfg: Any) -> list[Strategy]:
    raw = strategy_settings(cfg)
    out = []
    for sid in raw["enabled"]:
        p = {k: v for k, v in ((raw["params"] or {}).get(sid) or {}).items() if not k.startswith("_")}
        s = REGISTRY[sid](p)
        s.warmup_ms = parse_interval(str(raw["warmup"]))
        out.append(s)
    return out


_T, _I, _F, _B, _X = S.TS, pa.int64(), pa.float64(), pa.bool_(), pa.string()


class SignalStore(DerivedStore):
    """Phase 11 dataset: emitted signals and suppressed ones (with the gating reason)."""

    NAME = "signals"
    SCHEMA = "TradingSignal"
    FIELDS = {
        "signals": [("signal_id", _X), ("strategy_id", _X), ("strategy_version", _X), ("provider", _X), ("instrument", _X),
                    ("event_time", _T), ("available_at", _T), ("direction", _X), ("entry_type", _X), ("entry_price_ticks", _I),
                    ("stop_loss_ticks", _I), ("take_profit_ticks", _I), ("rr", _F), ("rr_exact", _X), ("risk_intent", _X),
                    ("setup_score", _F), ("score_calibrated", _B), ("evidence", _X), ("score_detail", _X), ("expiry_at", _T),
                    ("invalidation_ticks", _I), ("required_capabilities", _X), ("data_quality_status", _X), ("trigger_id", _X)],
        "suppressed": [("signal_id", _X), ("strategy_id", _X), ("event_time", _T), ("available_at", _T), ("reason", _X),
                       ("trigger_id", _X), ("direction", _X), ("data_quality_status", _X)],
    }  # fmt: skip
    SORT = {"signals": [("event_time", "ascending"), ("signal_id", "ascending")],
            "suppressed": [("event_time", "ascending"), ("signal_id", "ascending")]}  # fmt: skip
    DOC = {"spec": "docs/SIGNALS_V1.md", "prices": "signal-venue ticks (Binance XAUUSDT)",
           "evidence": "JSON list of {metric, value, threshold, timestamp, source_ids, passed}; build(evidence) re-derives the signal"}  # fmt: skip
    MATCH_KEYS = ("strategies_hash", "processing_settings_hash", "source_dataset_version")


class StrategyEngine(ProcessorSink):
    name = "strategies"

    def __init__(self, cfg: Any, strategies: list[Strategy], sources: dict[str, Any], store: Any = None,
                 listeners: list[Listener] | tuple[Listener, ...] = ()) -> None:  # fmt: skip
        self.raw = strategy_settings(cfg)
        self.strategies = list(strategies)
        self.sources = dict(sources)
        caps = create_provider(cfg).capabilities()
        for s in self.strategies:
            caps.require(s.required_capabilities, cfg["source"])  # refuses unsupported inputs (CapabilityUnavailable)
            missing = sorted(s.required_inputs - set(self.sources))
            if missing:
                raise ValueError(f"strategy {s.id} needs analytics engines {missing}; pass them in sources")
        for name, eng in self.sources.items():
            if name == "profile":
                eng.listeners.append(self._on_profile)
            else:
                eng.listeners.append(lambda t, rows, _n=name: self._on_rows(f"{_n}.{t}", rows))
        self.store = store
        self.listeners = list(listeners)
        self.provider = cfg["source"]
        self.instrument = cfg.symbol
        rg = self.raw["regime"]
        self.rg_cfg = (int(rg["vol_bars"]), int(rg["volume_lookback_days"]))
        body = {"strategies": self.raw, "versions": {s.id: [s.version, s.params] for s in self.strategies},
                "upstream": {k: getattr(getattr(v, "fs", None) or getattr(v, "st", None) or getattr(v, "ps", None), "hash", None)
                             for k, v in sorted(self.sources.items())}}  # fmt: skip
        self.hash = hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()
        self.proc: Any = None
        self._reset()

    def _reset(self) -> None:
        self.buf: list[tuple[int, int, str, dict[str, Any]]] = []
        self.seq = 0
        self.state = AnalyticsState()
        self.regime = RegimeTracker(*self.rg_cfg)
        self.dq: list[list[Any]] = []
        self.active: dict[str, list[Any]] = {}  # strategy|direction -> [signal_id, expiry_at]
        self.stats: dict[str, int] = {"rows_dispatched": 0, "signals": 0, "suppressed": 0, "seeks": 0}
        self.by_strategy: dict[str, dict[str, int]] = {s.id: {"signals": 0} for s in self.strategies}

    # ------------------------------------------------------------------ inputs
    def _on_rows(self, table: str, rows: list[dict[str, Any]]) -> None:
        if table.endswith(".ib_level"):
            return
        for r in rows:
            self.buf.append((int(r["available_at"]), self.seq, table, r))
            self.seq += 1

    def _on_profile(self, recs: list[dict[str, Any]], nodes: list[dict[str, Any]]) -> None:
        for r in recs:
            if r["is_final"]:
                continue
            hv = [n["price_ticks"] + (int(r["bin_ticks"]) - 1) / 2 for n in nodes if n["node_type"] == "HVN" and n["window_key"] == r["window_key"]
                  and n["as_of"] == r["as_of"] and n["bin_ticks"] == r["bin_ticks"]]  # fmt: skip
            keep = {
                k: r[k] for k in ("kind", "name", "window_key", "as_of", "available_at", "bin_ticks", "vwap_ticks", "sigma_ticks")
            }
            keep.update({k: v for k, v in r.items() if k.startswith("band_")}, hvn_prices=hv)
            self.buf.append((int(r["available_at"]), self.seq, "profile.snapshot", keep))
            self.seq += 1

    # ------------------------------------------------------------------ sink interface
    def on_start(self, ctx: dict[str, Any], proc: Any) -> None:
        self.proc = proc
        sinks = list(proc.sinks)
        if sinks[-1] is not self:
            raise ValueError("StrategyEngine must be the last sink of the processor")
        for name, eng in self.sources.items():
            if eng not in sinks:
                raise ValueError(f"source engine {name} is not a sink of this processor")
        if self.store is not None:
            self.store.start({"strategies_hash": self.hash, "processing_settings_hash": proc.st.hash, "source_dataset_version": ctx.get("dataset_version"),
                              "strategies": [s.describe() for s in self.strategies], "observe_from": proc.observe_from}, bool(ctx.get("resumed")))  # fmt: skip

    def on_dq(self, start_ms: int, end_ms: int, status: str) -> None:
        x = [int(start_ms), int(end_ms), str(status)]
        if x not in self.dq and status != "OK":
            self.dq.append(x)

    def on_advance(self, proc: Any) -> None:
        if not self.buf:
            return
        items = sorted(self.buf, key=lambda x: (x[0], x[1]))
        self.buf = []
        i = 0
        while i < len(items):
            t = items[i][0]
            j = i
            while j < len(items) and items[j][0] == t:
                j += 1
            group = items[i:j]
            for _, _, table, row in group:  # everything known at t is applied before any trigger at t
                self.state.apply(table, row)
                if table == "orderflow.bars":
                    self.regime.on_bar(row)
            for _, _, table, row in group:
                self.stats["rows_dispatched"] += 1
                hit = [s for s in self.strategies if table in s.triggers]
                if not hit:
                    continue
                self.state.regime = self._regime(t)
                for s in hit:
                    meta = {"event_time": t, "provider": self.provider, "instrument": self.instrument,
                            "trigger_id": str(row.get("event_id") or row.get("signal_id") or f"{table}:{t}")}  # fmt: skip
                    sig = s.on_update(table, row, self.state, meta)
                    if sig is not None:
                        self._gate(s, sig, proc)
            i = j
        if self.store is not None and proc.clock is not None:
            self.store.flush_complete(int(proc.clock))

    def _regime(self, t: int) -> dict[str, Any]:
        import numpy as np

        cal = self.proc.cal if self.proc is not None else None
        sess = (
            sorted(i.name for i in cal.instances(t, t, []) if i.kind == "session" and i.start_ms <= t < i.end_ms) if cal else []
        )
        wk = bool(cal.is_weekend(np.array([t]))[0]) if cal else False
        st = next((v for k, v in sorted(self.state.auction_state.items()) if k.startswith("trading_day")), None)
        return self.regime.snapshot(st, sess, wk)

    def _gate(self, s: Strategy, sig: Any, proc: Any) -> None:
        probs = sig.check()
        if probs:
            raise AssertionError(f"strategy {s.id} produced an invalid signal: {probs}")
        if proc.clock is not None and sig.event_time > proc.clock:
            raise AssertionError(f"strategy {s.id}: signal time after the processor clock (look-ahead)")
        reason = None
        of = proc.observe_from or 0
        if sig.event_time < of + s.warmup_ms:
            reason = "WARMUP"
        elif any(a <= sig.event_time < b for a, b, _ in self.dq):
            reason = "DQ_OPEN"
        elif sig.data_quality_status != "OK":
            reason = "DQ_EVIDENCE"
        elif (a := self.active.get(f"{s.id}|{sig.direction}")) is not None and sig.event_time < a[1]:
            reason = "DUPLICATE_ACTIVE"
        if reason:
            self.stats["suppressed"] += 1
            self.by_strategy[s.id][reason] = self.by_strategy[s.id].get(reason, 0) + 1
            self._emit("suppressed", [{"signal_id": sig.signal_id, "strategy_id": s.id, "event_time": sig.event_time, "available_at": sig.event_time,
                                       "reason": reason, "trigger_id": sig.trigger_id, "direction": sig.direction,
                                       "data_quality_status": sig.data_quality_status}])  # fmt: skip
            return
        self.stats["signals"] += 1
        self.by_strategy[s.id]["signals"] += 1
        self.active[f"{s.id}|{sig.direction}"] = [sig.signal_id, sig.expiry_at]
        self._emit("signals", [sig.row(sig.event_time)])

    def _emit(self, table: str, rows: list[dict[str, Any]]) -> None:
        if self.store is not None and self.raw["persist"]:
            self.store.add(table, rows)
        for f in self.listeners:
            f(table, rows)

    def on_seek(self, t_ms: int) -> None:
        stats = dict(self.stats)
        self._reset()
        self.stats = {**stats, "seeks": stats["seeks"] + 1}
        if self.store is not None:
            self.store.discard_unwritten()

    def on_stop(self, ctx: dict[str, Any], proc: Any) -> None:
        if ctx.get("status") == "completed" and self.store is not None:
            self.store.flush_complete(int(proc.clock or 0), force=True)

    # ------------------------------------------------------------------ state
    def _tree(self) -> dict[str, Any]:
        return {"hash": self.hash, "buf": [list(x) for x in self.buf], "seq": self.seq, "state": self.state.tree(), "regime": self.regime.tree(),
                "dq": self.dq, "active": dict(sorted(self.active.items())), "stats": dict(self.stats), "by_strategy": self.by_strategy}  # fmt: skip

    def state_digest(self) -> str:
        return digest_tree(self._tree())

    def get_state(self) -> dict[str, Any]:
        t = json.loads(json.dumps(self._tree(), default=float))
        t["store"] = self.store.get_state() if self.store is not None else {}
        return t  # type: ignore[no-any-return]

    def set_state(self, s: dict[str, Any]) -> None:
        if not s:
            return
        if s.get("hash") != self.hash:
            raise ReplayGuardError(
                "strategy engine checkpoint was taken with different strategies or settings; start a new run name"
            )
        self._reset()
        self.buf = [(int(a), int(b), str(c), dict(d)) for a, b, c, d in s["buf"]]
        self.seq = int(s["seq"])
        self.state.load(s["state"])
        self.regime.load(s["regime"])
        self.dq = [list(x) for x in s["dq"]]
        self.active = {k: list(v) for k, v in s["active"].items()}
        self.stats = {**self.stats, **s["stats"]}
        self.by_strategy = {k: dict(v) for k, v in s["by_strategy"].items()}
        if self.store is not None:
            self.store.set_state(s.get("store") or {})

    def summary(self) -> dict[str, Any]:
        return {
            "stats": dict(self.stats),
            "by_strategy": self.by_strategy,
            "hash": self.hash[:16],
            "state_digest": self.state_digest(),
        }
