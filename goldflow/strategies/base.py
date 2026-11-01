"""Phase 11: the strategy contract and the analytics state strategies read.

A strategy declares what it needs and produces signals from evidence:

* ``required_capabilities``: provider capabilities (Phase 1 contract). The engine refuses to start a strategy whose
  needs the provider cannot meet. A depth strategy, for example, needs ``historical_l2_depth``, which Binance does not
  provide for this history.
* ``required_inputs``: the analytics engines it reads (``orderflow``, ``auction``, ``ib``, ``profile``).
* ``triggers``: the tables whose new rows it reacts to.
* ``defaults``: its config schema (every parameter, its default and its meaning). Unknown or mistyped parameters are
  refused.
* ``warmup_ms``: no signal before ``observe_from + warmup`` (the engine suppresses).
* ``evidence_for(table, row, state)`` gathers evidence from the state available **now**. ``build(evidence, meta)``
  is a pure function from that evidence to a signal (or None). ``signals-verify`` re-runs ``build`` on the stored
  evidence, so every signal is explainable from its evidence alone.
"""

from __future__ import annotations

import abc
from collections import OrderedDict
from fractions import Fraction
from typing import Any

from ..providers.base import Capability
from .signal import Evidence, TradingSignal


class Strategy(abc.ABC):
    id: str = ""
    version: str = "1"
    hypothesis: str = ""
    required_capabilities: frozenset[Capability] = frozenset({Capability.HISTORICAL_TRADES, Capability.AGGRESSOR_SIDE})
    required_inputs: frozenset[str] = frozenset()
    triggers: frozenset[str] = frozenset()
    defaults: dict[str, tuple[Any, str]] = {}
    warmup_ms: int = 24 * 3_600_000

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        p = dict(params or {})
        bad = sorted(set(p) - set(self.defaults))
        if bad:
            raise ValueError(f"strategy {self.id}: unknown parameters {bad}; allowed {sorted(self.defaults)}")
        self.params: dict[str, Any] = {}
        for k, (dflt, _doc) in self.defaults.items():
            v = p.get(k, dflt)
            if isinstance(dflt, str) and not isinstance(v, str):
                raise ValueError(f"strategy {self.id}.{k}: expected a string (decimal values are strings), got {v!r}")
            if isinstance(dflt, bool) != isinstance(v, bool) or (isinstance(dflt, int) and not isinstance(v, int)):
                raise ValueError(f"strategy {self.id}.{k}: expected {type(dflt).__name__}, got {v!r}")
            self.params[k] = v

    def frac(self, k: str) -> Fraction:
        return Fraction(str(self.params[k]))

    @abc.abstractmethod
    def evidence_for(self, table: str, row: dict[str, Any], state: "AnalyticsState") -> tuple[Evidence, ...] | None:
        """Evidence available at the trigger's time, or None when the trigger does not qualify."""

    @abc.abstractmethod
    def build(self, evidence: tuple[Evidence, ...], meta: dict[str, Any]) -> TradingSignal | None:
        """Pure: the signal implied by the evidence (and the strategy's parameters), or None."""

    def on_update(self, table: str, row: dict[str, Any], state: "AnalyticsState", meta: dict[str, Any]) -> TradingSignal | None:
        if table not in self.triggers:
            return None
        ev = self.evidence_for(table, row, state)
        return None if ev is None else self.build(ev, meta)

    def describe(self) -> dict[str, Any]:
        return {"id": self.id, "version": self.version, "hypothesis": self.hypothesis, "triggers": sorted(self.triggers),
                "required_capabilities": sorted(c.value for c in self.required_capabilities), "required_inputs": sorted(self.required_inputs),
                "params": {k: {"value": self.params[k], "doc": d} for k, (_, d) in self.defaults.items()}, "warmup_ms": self.warmup_ms}  # fmt: skip


class AnalyticsState:
    """What strategies may read: everything already available at the dispatch clock (the engine applies every update
    with available_at <= the trigger's before dispatching it). Bounded memory: recent bars, active levels, latest
    states and IBs."""

    KEEP_MS = 2 * 86_400_000

    def __init__(self) -> None:
        self.clock = 0
        self.bars: dict[str, OrderedDict[int, dict[str, Any]]] = {}
        self.levels: dict[str, dict[str, Any]] = {}
        self.auction_state: dict[str, dict[str, Any]] = {}
        self.ib_final: dict[str, dict[str, Any]] = {}
        self.profile_dev: dict[str, dict[str, Any]] = {}
        self.regime: dict[str, Any] = {}

    def apply(self, table: str, row: dict[str, Any]) -> None:
        self.clock = max(self.clock, int(row["available_at"]))
        if table == "orderflow.bars":
            d = self.bars.setdefault(str(row["interval"]), OrderedDict())
            d[int(row["bar_start"])] = row
            while d and next(iter(d)) < self.clock - self.KEEP_MS:
                d.popitem(last=False)
        elif table == "auction.levels":
            self.levels[str(row["level_id"])] = row
            for k in [k for k, v in self.levels.items() if v["available_at"] < self.clock - self.KEEP_MS]:
                del self.levels[k]
        elif table == "auction.states":
            self.auction_state[str(row["series"])] = row
        elif table == "ib.ib" and row.get("is_final"):
            self.ib_final[str(row["ib_key"])] = row
            for k in [k for k, v in self.ib_final.items() if v["available_at"] < self.clock - self.KEEP_MS]:
                del self.ib_final[k]
        elif table == "profile.snapshot":
            self.profile_dev[f"{row['kind']}:{row['name']}:{row['bin_ticks']}"] = row

    def bar_ending(self, interval: str, end: int) -> dict[str, Any] | None:
        d: OrderedDict[int, dict[str, Any]] = self.bars.get(interval, OrderedDict())
        for r in reversed(d.values()):
            if int(r["bar_end"]) == end:
                return r
            if int(r["bar_end"]) < end:
                return None
        return None

    def bars_between(self, interval: str, a: int, b: int) -> list[dict[str, Any]]:
        return [r for s, r in self.bars.get(interval, {}).items() if a <= s and int(r["bar_end"]) <= b]

    def tree(self) -> dict[str, Any]:
        return {"clock": self.clock, "bars": {k: list(v.values()) for k, v in sorted(self.bars.items())},
                "levels": dict(sorted(self.levels.items())), "auction_state": dict(sorted(self.auction_state.items())),
                "ib_final": dict(sorted(self.ib_final.items())), "profile_dev": dict(sorted(self.profile_dev.items())),
                "regime": self.regime}  # fmt: skip

    def load(self, t: dict[str, Any]) -> None:
        self.clock = int(t["clock"])
        self.bars = {k: OrderedDict((int(r["bar_start"]), dict(r)) for r in v) for k, v in t["bars"].items()}
        self.levels, self.auction_state = dict(t["levels"]), dict(t["auction_state"])
        self.ib_final, self.profile_dev, self.regime = dict(t["ib_final"]), dict(t["profile_dev"]), dict(t["regime"])
