"""Phase 16: a SIMULATED MT5 executor EA and broker, for tests and fault injection only. It never touches a real
account, and every price it produces is synthetic.

It follows the same protocol and the same rules as ``mt5/GoldflowExecutor.mq5``:
- **Session.** Every message is ignored until an authenticated HELLO, whose session it adopts.
- **ORDER_NEW dedup.** If the key is in the comment of an open position, a pending order or a recent history deal,
  the reply is ``ACK DUPLICATE`` and no order is sent.
- **Validation.** Volume must be within min, max and step, and stops must be on the right side and at least
  ``stops_level`` away. Failures give ``REJECT`` with an MT5 retcode (10014 invalid volume, 10016 invalid stops).
- **Execution.** ``ACK`` (sent to the broker), then ``TXN REQUEST`` (10009 DONE), then one ``TXN DEAL IN`` per fill
  (optionally two partial fills).
- **SL/TP** trigger on the price path: ``TXN DEAL OUT`` with reason ``SL`` or ``TP``. Manual close (``CLIENT``) and
  stop-out (``SO``) are operator and fault actions.
- **FLATTEN_ALL** closes everything (reason ``EXPERT``).

Faults the tests can inject:
- drop the next N incoming or outgoing messages;
- drop only ACKs;
- duplicate outgoing messages;
- the next order is rejected with a retcode;
- disconnect / reconnect;
- **restart**: in-memory state is lost, and the broker's positions and history survive;
- tamper with a MAC;
- clock skew.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from .protocol import Endpoint, ProtocolError


class SimBroker:
    """Broker-side state; survives an EA restart."""

    def __init__(self, trade_mode: str = "DEMO", balance: float = 10_000.0, contract_size: float = 100.0) -> None:
        self.trade_mode, self.balance, self.cs = trade_mode, balance, contract_size
        self.positions: dict[int, dict[str, Any]] = {}
        self.history_keys: set[str] = set()
        self.deals: list[dict[str, Any]] = []
        self.next_ticket = 1000
        self.bid, self.ask = 2400.00, 2400.30

    def equity(self) -> float:
        fl = 0.0
        for p in self.positions.values():
            px = self.bid if p["side"] == "BUY" else self.ask
            fl += (px - p["price_open"]) * (1 if p["side"] == "BUY" else -1) * p["volume"] * self.cs
        return round(self.balance + fl, 2)


class SimEA:
    SYMBOL = {"symbol": "XAUUSD", "digits": 2, "point": 0.01, "tick_size": 0.01, "tick_value": 1.0, "contract_size": 100.0,
              "volume_min": 0.01, "volume_max": 50.0, "volume_step": 0.01, "stops_level": 0, "trade_mode": "FULL"}  # fmt: skip

    def __init__(
        self, end: Any, secret: bytes, broker: SimBroker, now: Any, magic: int = 20261031, symbol: dict[str, Any] | None = None
    ) -> None:
        self.end, self.secret, self.b, self.now, self.magic = end, secret, broker, now, magic
        self.sym = dict(symbol or self.SYMBOL)
        self.ep = Endpoint(secret, "ea", "", 5000)
        self.ready = False
        self.sent_keys: set[str] = set()  # as g_sentKeys in the EA: recorded before OrderSend, lost on restart
        self.drop_in = self.drop_out = 0
        self.drop_acks = False
        self.duplicate_out = False
        self.reject_next: tuple[int, str] | None = None
        self.partial_fills = False
        self.slippage_points = 0
        self.skew_ms = 0
        self.tamper_next = False
        self.received: list[dict[str, Any]] = []
        self.orders_sent = 0
        self.last_hb = 0

    # ------------------------------------------------------------------ plumbing
    def _send(self, mtype: str, **body: Any) -> None:
        if not self.ready and mtype != "HELLO":
            return
        _, line = self.ep.make(mtype, self.now() + self.skew_ms, **body)
        if self.tamper_next:
            self.tamper_next = False
            line = ("0" * 64) + line[64:]
        if self.drop_out > 0:
            self.drop_out -= 1
            return
        if self.drop_acks and mtype == "ACK":
            return
        self.end.send(line)
        if self.duplicate_out:
            self.end.send(line)

    def restart(self) -> None:
        """EA restart: in-memory state lost (session, sequence numbers); broker state persists."""
        self.ep = Endpoint(self.secret, "ea", "", 5000)
        self.ready = False
        self.sent_keys = set()
        self.end.pipe.connections += 1

    def tick(self) -> None:
        for line in self.end.receive():
            if self.drop_in > 0:
                self.drop_in -= 1
                continue
            try:
                m = self.ep.parse(line, self.now())
            except ProtocolError:
                continue
            if not self.ready and m["type"] != "HELLO":
                continue
            self.received.append(m)
            self._on(m)
        if self.ready and self.now() - self.last_hb >= 1000:
            self.last_hb = self.now()
            self._hb()
        self._check_stops()

    def _hb(self) -> None:
        b = self.b
        self._send("HEARTBEAT", bid=b.bid, ask=b.ask, spread_points=round((b.ask - b.bid) / self.sym["point"]), balance=b.balance,
                   equity=b.equity(), time_msc=self.now())  # fmt: skip

    def _positions(self) -> None:
        ps = [{"ticket": t, "key": p["key"], "side": p["side"], "volume": p["volume"], "price_open": p["price_open"], "sl": p["sl"], "tp": p["tp"],
               "magic": self.magic} for t, p in sorted(self.b.positions.items())]  # fmt: skip
        self._send("POSITIONS", positions=ps, orders=[], balance=self.b.balance, equity=self.b.equity())

    # ------------------------------------------------------------------ commands
    def _on(self, m: dict[str, Any]) -> None:
        t = m["type"]
        if t == "HELLO":
            self.ready = True
            self._send("HELLO", account=5001234, trade_mode=self.b.trade_mode, margin_mode="HEDGING", server="Sim-Demo", currency="USD",
                       balance=self.b.balance, equity=self.b.equity(), build=0)  # fmt: skip
        elif t == "SYMBOL_INFO_REQ":
            self._send("SYMBOL_INFO", **self.sym)
        elif t == "POSITIONS_REQ":
            self._positions()
        elif t == "HEARTBEAT":
            self._hb()
        elif t == "ORDER_NEW":
            self._order(m)
        elif t == "ORDER_MODIFY":
            p = self.b.positions.get(int(m["position"]))
            if p is None:
                self._send("REJECT", ref=m["seq"], key=m["key"], retcode=10036, reason="position not found")
                return
            p["sl"], p["tp"] = float(m["sl"]), float(m["tp"])
            self._send("TXN", key=p["key"], kind="MODIFY", position=int(m["position"]), volume=p["volume"], price=0.0, sl=m["sl"], tp=m["tp"],
                       time_msc=self.now())  # fmt: skip
        elif t == "ORDER_CLOSE":
            if int(m["position"]) in self.b.positions:
                self._close(int(m["position"]), "EXPERT")
        elif t == "FLATTEN_ALL":
            for tk in sorted(self.b.positions):
                self._close(tk, "EXPERT")
            self._positions()

    def _order(self, m: dict[str, Any]) -> None:
        key = m["key"]
        if key in self.sent_keys or key in self.b.history_keys or any(p["key"] == key for p in self.b.positions.values()):
            self._send("ACK", ref=m["seq"], key=key, status="DUPLICATE")
            return
        vol = Decimal(m["volume"])
        s = self.sym
        step = Decimal(str(s["volume_step"]))
        if vol < Decimal(str(s["volume_min"])) or vol > Decimal(str(s["volume_max"])) or vol % step != 0:
            self._send("REJECT", ref=m["seq"], key=key, retcode=10014, reason="invalid volume")
            return
        side = m["side"]
        px = self.b.ask if side == "BUY" else self.b.bid
        sl, tp = float(m["sl"]), float(m["tp"])
        sgn = 1 if side == "BUY" else -1
        if (sl and (px - sl) * sgn < s["stops_level"] * s["point"]) or (tp and (tp - px) * sgn <= 0):
            self._send("REJECT", ref=m["seq"], key=key, retcode=10016, reason="invalid stops")
            return
        self.sent_keys.add(key)
        self._send("ACK", ref=m["seq"], key=key, status="SENT")
        if self.reject_next is not None:
            rc, why = self.reject_next
            self.reject_next = None
            self._send(
                "TXN", key=key, kind="REQUEST", position=0, volume=0.0, price=0.0, retcode=rc, reason=why, time_msc=self.now()
            )
            return
        self.orders_sent += 1
        fill = px + sgn * self.slippage_points * s["point"]
        tk = self.b.next_ticket
        self.b.next_ticket += 1
        self.b.history_keys.add(key)
        self._send(
            "TXN",
            key=key,
            kind="REQUEST",
            position=0,
            volume=float(vol),
            price=0.0,
            retcode=10009,
            reason="done",
            time_msc=self.now(),
        )
        parts = [vol / 2, vol - vol / 2] if self.partial_fills and vol >= 2 * step else [vol]
        parts = [(p / step).to_integral_value() * step for p in parts]
        parts[-1] = vol - sum(parts[:-1], Decimal(0))
        self.b.positions[tk] = {"key": key, "side": side, "volume": 0.0, "price_open": round(fill, 2), "sl": sl, "tp": tp}
        for k, p in enumerate(parts):
            self.b.positions[tk]["volume"] = float(Decimal(str(self.b.positions[tk]["volume"])) + p)
            self._send("TXN", key=key, kind="DEAL", order=tk, deal=tk * 10 + k, position=tk, entry="IN", side=side, volume=float(p),
                       price=round(fill, 2), reason="EXPERT", time_msc=self.now())  # fmt: skip

    def _close(self, tk: int, reason: str, price: float | None = None) -> None:
        p = self.b.positions.pop(tk)
        px = price if price is not None else (self.b.bid if p["side"] == "BUY" else self.b.ask)
        pnl = round((px - p["price_open"]) * (1 if p["side"] == "BUY" else -1) * p["volume"] * self.b.cs, 2)
        self.b.balance = round(self.b.balance + pnl, 2)
        self.b.deals.append({"ticket": tk, "key": p["key"], "reason": reason, "profit": pnl})
        self._send("TXN", key=p["key"], kind="DEAL", order=tk, deal=tk * 10 + 9, position=tk, entry="OUT", side="SELL" if p["side"] == "BUY" else "BUY",
                   volume=p["volume"], price=round(px, 2), reason=reason, profit=pnl, time_msc=self.now())  # fmt: skip

    def _check_stops(self) -> None:
        for tk, p in sorted(self.b.positions.items()):
            if p["side"] == "BUY":
                if p["sl"] and self.b.bid <= p["sl"]:
                    self._close(tk, "SL", min(self.b.bid, p["sl"]))
                elif p["tp"] and self.b.bid >= p["tp"]:
                    self._close(tk, "TP", p["tp"])
            else:
                if p["sl"] and self.b.ask >= p["sl"]:
                    self._close(tk, "SL", max(self.b.ask, p["sl"]))
                elif p["tp"] and self.b.ask <= p["tp"]:
                    self._close(tk, "TP", p["tp"])

    # ------------------------------------------------------------------ operator / fault actions
    def manual_close(self, tk: int) -> None:
        self._close(tk, "CLIENT")

    def manual_close_silent(self, tk: int) -> None:
        """Closed while the EA was not running: no TXN is ever reported (reconciliation must adopt it)."""
        p = self.b.positions.pop(tk)
        px = self.b.bid if p["side"] == "BUY" else self.b.ask
        self.b.balance = round(
            self.b.balance + (px - p["price_open"]) * (1 if p["side"] == "BUY" else -1) * p["volume"] * self.b.cs, 2
        )

    def set_price(self, bid: float, spread: float = 0.30) -> None:
        self.b.bid, self.b.ask = round(bid, 2), round(bid + spread, 2)
