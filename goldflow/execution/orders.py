"""Phase 16: order lifecycle. **Internal state changes only on broker confirmation.**

States: CREATED → TRANSMITTED → ACCEPTED → PARTIALLY_FILLED → FILLED → CLOSED. An order may also end in REJECTED or
CANCELLED, or become UNKNOWN when the EA never acknowledged it.

| Event | Source | Transition |
|---|---|---|
| sent to the EA | engine | CREATED → TRANSMITTED (a retry keeps TRANSMITTED; same key) |
| ``ACK`` | EA received it and passed it to the broker | no state change (it is **not** a fill) |
| ``TXN kind=REQUEST`` retcode DONE/PLACED | broker | → ACCEPTED |
| ``TXN kind=DEAL entry=IN`` | broker | → PARTIALLY_FILLED or FILLED (by filled volume) |
| ``TXN kind=DEAL entry=OUT`` | broker | position volume down; at 0 → CLOSED (reason SL / TP / CLIENT / EXPERT / SO) |
| ``REJECT`` or ``TXN kind=REQUEST`` with an error retcode | EA / broker | → REJECTED |
| ``TXN kind=ORDER_CANCELLED`` | broker | → CANCELLED |
| no ``ACK`` after every retry | engine | → UNKNOWN (trading blocked until reconciliation resolves it) |
| reconciliation: UNKNOWN and absent at the broker | engine | → TRANSMITTED again (same key, re-sent) while the signal is valid; else → REJECTED |

Any other transition is refused and recorded as an anomaly. A wrong ordering of broker events never moves an order
backwards.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any

STATES = ("CREATED", "TRANSMITTED", "ACCEPTED", "PARTIALLY_FILLED", "FILLED", "CLOSED", "REJECTED", "CANCELLED", "UNKNOWN")
TERMINAL = {"CLOSED", "REJECTED", "CANCELLED"}
ALLOWED = {
    "CREATED": {"TRANSMITTED", "REJECTED"},
    "TRANSMITTED": {"ACCEPTED", "PARTIALLY_FILLED", "FILLED", "REJECTED", "CANCELLED", "UNKNOWN"},
    "UNKNOWN": {"TRANSMITTED", "ACCEPTED", "PARTIALLY_FILLED", "FILLED", "REJECTED", "CANCELLED", "CLOSED"},
    "ACCEPTED": {"PARTIALLY_FILLED", "FILLED", "REJECTED", "CANCELLED"},
    "PARTIALLY_FILLED": {"PARTIALLY_FILLED", "FILLED", "CANCELLED", "CLOSED"},
    "FILLED": {"CLOSED"},
}
OK_RETCODES = {10008, 10009, 10010}  # PLACED, DONE, DONE_PARTIAL


@dataclass
class Order:
    key: str
    signal_id: str
    strategy_id: str
    symbol: str
    side: str
    volume: str
    sl: str
    tp: str
    expected_price: float
    created_ms: int
    expiry_ms: int | None = None
    state: str = "CREATED"
    position: int | None = None
    filled: str = "0"
    open_volume: str = "0"
    avg_price: float | None = None
    close_reason: str | None = None
    realized: float = 0.0
    attempts: int = 0
    last_sent_ms: int | None = None
    acked: bool = False
    history: list[list[Any]] = field(default_factory=list)

    def to(self, state: str, at_ms: int, why: str) -> bool:
        if state == self.state and state != "PARTIALLY_FILLED":
            return False
        if state not in ALLOWED.get(self.state, set()):
            self.history.append([at_ms, self.state, f"REFUSED->{state}", why])
            return False
        self.history.append([at_ms, self.state, state, why])
        self.state = state
        return True

    @property
    def active(self) -> bool:
        return self.state not in TERMINAL

    def row(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def load(cls, d: dict[str, Any]) -> "Order":
        return cls(**d)


class OrderBook:
    """The engine's view of its orders, keyed by idempotency key; positions map to keys."""

    def __init__(self) -> None:
        self.orders: dict[str, Order] = {}
        self.by_position: dict[int, str] = {}
        self.anomalies: list[dict[str, Any]] = []

    def add(self, o: Order) -> None:
        if o.key in self.orders:
            raise ValueError(f"order {o.key} exists (idempotency)")
        self.orders[o.key] = o

    def resolve(self, key: str, position: int | None) -> Order | None:
        if key and key in self.orders:
            return self.orders[key]
        if position is not None and position in self.by_position:
            return self.orders[self.by_position[position]]
        return None

    def on_txn(self, m: dict[str, Any], at_ms: int) -> tuple[Order | None, str]:
        """Apply a broker transaction. Returns (order, effect) where effect names what happened."""
        o = self.resolve(str(m.get("key") or ""), int(m["position"]) if m.get("position") else None)
        if o is None:
            self.anomalies.append({"at": at_ms, "kind": "TXN_FOR_UNKNOWN_ORDER", "msg": m})
            return None, "unknown"
        kind = m["kind"]
        if kind == "REQUEST":
            rc = int(m.get("retcode", 0))
            if rc in OK_RETCODES:
                o.to("ACCEPTED", at_ms, f"retcode {rc}")
                return o, "accepted"
            o.to("REJECTED", at_ms, f"retcode {rc} {m.get('reason', '')}")
            return o, "rejected"
        if kind == "ORDER_CANCELLED":
            o.to("CANCELLED", at_ms, str(m.get("reason", "")))
            return o, "cancelled"
        if kind == "DEAL":
            vol, px = Decimal(str(m["volume"])), float(m["price"])
            if m.get("position"):
                o.position = int(m["position"])
                self.by_position[o.position] = o.key
            if m.get("entry") == "IN":
                filled = Decimal(o.filled) + vol
                o.avg_price = px if o.avg_price is None else (o.avg_price * float(o.filled) + px * float(vol)) / float(filled)
                o.filled = str(filled)
                o.open_volume = str(Decimal(o.open_volume) + vol)
                st = "FILLED" if filled >= Decimal(o.volume) else "PARTIALLY_FILLED"
                if not o.to(st, at_ms, f"deal {m.get('deal')} {vol} @ {px}") and o.state not in ("FILLED", "PARTIALLY_FILLED"):
                    self.anomalies.append({"at": at_ms, "kind": "FILL_IN_STATE", "key": o.key, "state": o.state})
                return o, "fill"
            if m.get("entry") == "OUT":
                o.open_volume = str(max(Decimal("0"), Decimal(o.open_volume) - vol))
                o.realized += float(m.get("profit", 0.0))
                if Decimal(o.open_volume) == 0:
                    o.close_reason = str(m.get("reason", ""))
                    o.to("CLOSED", at_ms, f"closed by {o.close_reason}")
                    return o, "closed"
                return o, "reduced"
        self.anomalies.append({"at": at_ms, "kind": "UNHANDLED_TXN", "msg": m})
        return o, "ignored"

    def open_positions(self) -> list[Order]:
        return [o for o in self.orders.values() if o.state in ("PARTIALLY_FILLED", "FILLED") and Decimal(o.open_volume) > 0]

    def pending(self) -> list[Order]:
        return [o for o in self.orders.values() if o.state in ("CREATED", "TRANSMITTED", "ACCEPTED", "UNKNOWN")]

    def state(self) -> dict[str, Any]:
        return {"orders": {k: o.row() for k, o in self.orders.items()}, "by_position": {str(k): v for k, v in self.by_position.items()},
                "anomalies": self.anomalies[-500:]}  # fmt: skip

    def load(self, s: dict[str, Any]) -> None:
        self.orders = {k: Order.load(v) for k, v in s["orders"].items()}
        self.by_position = {int(k): v for k, v in s["by_position"].items()}
        self.anomalies = list(s["anomalies"])
