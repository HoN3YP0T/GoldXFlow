"""Phase 11: the TradingSignal schema (docs/SIGNALS_V1.md).

A signal is an explainable, timestamped proposal; it is not an order. Every field comes from the strategy's ``build``
function applied to its evidence list, so the signal can be re-derived from that list alone (``signals-verify``).

Prices are in ticks of the **signal venue** (Binance XAUUSDT). Phase 12 translates them to the broker through the basis
known at signal time. R:R is exact: |target - entry| / |entry - stop| as a rational.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from fractions import Fraction
from typing import Any

DIRECTIONS = ("LONG", "SHORT")
ENTRY_TYPES = ("market", "limit")


@dataclass(frozen=True)
class Evidence:
    metric: str  # e.g. "auction.FAILED_AUCTION", "orderflow.bar_delta", "regime.state"
    value: Any
    threshold: Any  # the rule it was compared with (None when it is context, not a test)
    timestamp: int  # available_at of the source row (ms); never after the signal's event time
    source_ids: tuple[str, ...] = ()
    passed: bool | None = None


@dataclass(frozen=True)
class TradingSignal:
    strategy_id: str
    strategy_version: str
    provider: str
    instrument: str
    event_time: int  # ms; the moment the triggering evidence became available
    direction: str
    entry_type: str
    entry_price_ticks: int
    stop_loss_ticks: int
    take_profit_ticks: int
    risk_intent: str  # e.g. "1R": the sizing intent, resolved to a quantity by the risk layer (Phase 12/16)
    setup_score: float
    evidence: tuple[Evidence, ...]
    expiry_at: int  # an un-filled signal is void after this time
    invalidation_ticks: int  # price at which the setup is wrong (normally the stop)
    required_capabilities: tuple[str, ...]
    data_quality_status: str
    trigger_id: str
    score_detail: dict[str, Any] = field(default_factory=dict)

    @property
    def rr(self) -> Fraction:
        risk = abs(self.entry_price_ticks - self.stop_loss_ticks)
        return Fraction(abs(self.take_profit_ticks - self.entry_price_ticks), risk) if risk else Fraction(0)

    @property
    def signal_id(self) -> str:
        body = [self.strategy_id, self.strategy_version, self.instrument, self.event_time, self.direction, self.trigger_id]
        return "sig-" + hashlib.sha256(json.dumps(body).encode()).hexdigest()[:20]

    def check(self) -> list[str]:
        """Structural rules every signal must satisfy (also used by signals-verify)."""
        p = []
        if self.direction not in DIRECTIONS:
            p.append(f"direction {self.direction!r}")
        if self.entry_type not in ENTRY_TYPES:
            p.append(f"entry_type {self.entry_type!r}")
        up = self.direction == "LONG"
        if (self.stop_loss_ticks < self.entry_price_ticks) != up or self.stop_loss_ticks == self.entry_price_ticks:
            p.append("stop on the wrong side of entry")
        if (self.take_profit_ticks > self.entry_price_ticks) != up or self.take_profit_ticks == self.entry_price_ticks:
            p.append("target on the wrong side of entry")
        if not self.evidence:
            p.append("no evidence")
        if any(e.timestamp > self.event_time for e in self.evidence):
            p.append("evidence later than the signal (look-ahead)")
        if self.expiry_at <= self.event_time:
            p.append("expiry not after the event time")
        return p

    def row(self, available_at: int) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id, "strategy_id": self.strategy_id, "strategy_version": self.strategy_version,
            "provider": self.provider, "instrument": self.instrument, "event_time": self.event_time, "available_at": available_at,
            "direction": self.direction, "entry_type": self.entry_type, "entry_price_ticks": self.entry_price_ticks,
            "stop_loss_ticks": self.stop_loss_ticks, "take_profit_ticks": self.take_profit_ticks, "rr": float(self.rr),
            "rr_exact": f"{self.rr.numerator}/{self.rr.denominator}", "risk_intent": self.risk_intent,
            "setup_score": self.setup_score, "score_calibrated": False,
            "evidence": json.dumps([asdict(e) for e in self.evidence], sort_keys=True, default=str),
            "score_detail": json.dumps(self.score_detail, sort_keys=True, default=str), "expiry_at": self.expiry_at,
            "invalidation_ticks": self.invalidation_ticks, "required_capabilities": json.dumps(list(self.required_capabilities)),
            "data_quality_status": self.data_quality_status, "trigger_id": self.trigger_id,
        }  # fmt: skip


def evidence_from_json(s: str) -> tuple[Evidence, ...]:
    out = []
    for e in json.loads(s):
        out.append(Evidence(e["metric"], e["value"], e["threshold"], int(e["timestamp"]), tuple(e["source_ids"]), e["passed"]))
    return tuple(out)
