"""Phase 16: reconciliation of the engine's view with the broker's.

It runs on start, on every reconnect and every ``reconcile_every_s``. The EA's ``POSITIONS`` snapshot lists open
positions and pending orders with our magic number. Each one carries the ticket, the idempotency key read from the
comment, side, volume, open price, SL and TP.

| Situation | Outcome |
|---|---|
| Broker position with a known key, same volume / SL / TP | match |
| Same key, different SL/TP or a smaller volume (manual change, partial close) | **ADOPTED**: engine takes the broker values, logged |
| Engine order open, broker has no position, and a close was reported (TXN OUT) | already CLOSED: match |
| Engine order open, broker has no position, no close seen (closed while disconnected) | **ADOPTED** as CLOSED (reason ``RECONCILED_MISSING``), logged; the realised P&L is taken from the broker's balance change |
| Engine order TRANSMITTED / UNKNOWN, broker has a position with its key | **ADOPTED** as FILLED (the broker's word wins) |
| Engine order TRANSMITTED / UNKNOWN, no position and no pending order with its key | **not resolved from absence** (it may still be in flight): listed as ``absent``. The bridge keeps re-sending a TRANSMITTED order with the same key, and re-sends an UNKNOWN one while its signal is valid; after the signal expires it is REJECTED (``RECONCILED_ABSENT_EXPIRED``) |
| Broker position with our magic but a key the engine does not know | **MISMATCH**: trading blocked, operator alert |
| Broker position larger than the engine's | **MISMATCH**: trading blocked, operator alert (a duplicate would show up here) |

Adopted changes are logged in the audit log. Any MISMATCH blocks new orders until a clean reconciliation.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from .orders import OrderBook


def _px(x: Any) -> float | None:
    """A price as MT5 reports it (0 = none)."""
    return None if x in (None, "", 0, "0") or float(x) == 0 else round(float(x), 8)


def reconcile(book: OrderBook, snap: dict[str, Any], at_ms: int) -> dict[str, Any]:
    pos = {str(p.get("key") or ""): p for p in snap["positions"]}
    pend = {str(o.get("key") or "") for o in snap["orders"]}
    adopted: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    absent: list[str] = []
    for p in snap["positions"]:
        k = str(p.get("key") or "")
        o = book.orders.get(k)
        if o is None:
            mismatches.append({"kind": "UNKNOWN_BROKER_POSITION", "position": p})
            continue
        bv, ov = Decimal(str(p["volume"])), Decimal(o.open_volume)
        if o.state in ("TRANSMITTED", "UNKNOWN", "ACCEPTED"):
            o.position = int(p["ticket"])
            book.by_position[o.position] = o.key
            o.filled = o.open_volume = str(bv)
            o.avg_price = float(p["price_open"])
            o.to("FILLED" if bv >= Decimal(o.volume) else "PARTIALLY_FILLED", at_ms, "reconciled: broker has the position")
            adopted.append({"kind": "FILL_ADOPTED", "key": k, "volume": str(bv)})
        elif bv > ov:
            mismatches.append({"kind": "BROKER_VOLUME_LARGER", "key": k, "broker": str(bv), "engine": str(ov)})
        elif bv < ov:
            o.open_volume = str(bv)
            adopted.append({"kind": "VOLUME_REDUCED_AT_BROKER", "key": k, "broker": str(bv), "engine": str(ov)})
        if o.state in ("FILLED", "PARTIALLY_FILLED") and (_px(p.get("sl")) != _px(o.sl) or _px(p.get("tp")) != _px(o.tp)):
            adopted.append(
                {"kind": "STOPS_CHANGED_AT_BROKER", "key": k, "sl": p.get("sl"), "tp": p.get("tp"), "was": [o.sl, o.tp]}
            )
            o.sl, o.tp = str(p.get("sl")), str(p.get("tp"))
    for o in list(book.orders.values()):
        if o.key in pos:
            continue
        if o.state in ("FILLED", "PARTIALLY_FILLED") and Decimal(o.open_volume) > 0:
            o.open_volume = "0"
            o.close_reason = "RECONCILED_MISSING"
            o.to("CLOSED", at_ms, "reconciled: position no longer at the broker")
            adopted.append({"kind": "CLOSE_ADOPTED", "key": o.key})
        elif o.state in ("TRANSMITTED", "UNKNOWN") and o.key not in pend:
            absent.append(o.key)  # not resolved from absence: it may still be in flight (the bridge re-sends or expires it)
    return {"at": at_ms, "clean": not mismatches, "adopted": adopted, "mismatches": mismatches, "absent": absent,
            "broker_positions": len(snap["positions"]), "broker_orders": len(snap["orders"])}  # fmt: skip
