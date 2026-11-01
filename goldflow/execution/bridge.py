"""Phase 16: the execution bridge. Validated signals go to the MT5 executor EA, under the risk engine, and order state
changes only on broker confirmation.

Lifecycle of one run:
1. **Start.**
   - The persisted state is loaded: orders, risk state and the emergency latch.
   - Trading is blocked by ``NO_EA`` (no HELLO from the EA yet), ``NO_SYMBOL_CHECK`` and ``NOT_RECONCILED``.
   - ``HELLO`` is sent.
2. **EA HELLO.** The account mode must match the run mode:
   - demo mode accepts ``DEMO`` accounts only;
   - live mode accepts ``REAL``, and only after the live gates (``profiles.assert_profile_allowed``).

   A mismatch blocks with ``ACCOUNT_MODE``. ``SYMBOL_INFO_REQ`` and ``POSITIONS_REQ`` follow.
3. **SYMBOL_INFO.** Checked against the ``broker`` block. A mismatch blocks with ``SYMBOL_MISMATCH``.
4. **POSITIONS.** Reconciliation (``reconcile.py``). Only a clean result unblocks ``NOT_RECONCILED`` and
   ``RECONCILE_MISMATCH``.
5. **Signal.** It must not be expired and not already known (idempotent per ``signal_id``). It is translated to the
   broker price: BUY at ask, SELL at bid, with the signal's stop and target distances. Then:
   - the **risk engine** decides and sizes it;
   - the order is created and **sent** as ``ORDER_NEW`` with its idempotency key, which makes it TRANSMITTED.
6. **ACK / REJECT / TXN** drive the order state (``orders.py``). An ACK is not a fill.
7. **Every tick.**
   - Heartbeats go out; **no heartbeat from the EA for ``heartbeat_timeout_ms`` blocks trading** (``NO_HEARTBEAT``).
   - An unacknowledged order is re-sent with the **same key**, up to ``max_retries``. After that it is UNKNOWN and
     trading is blocked until reconciliation resolves it.
   - Reconciliation runs every ``reconcile_every_s``.
   - The emergency-stop file is checked.
   - State is saved.
8. **Emergency stop.** It is latched, ``FLATTEN_ALL`` is sent, and it is repeated until the broker reports no positions.
   No new order can pass while it is latched. Only an operator reset clears it.

Every message, decision and state change is written to the audit log.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from .audit import AuditLog
from .orders import Order, OrderBook
from .protocol import Endpoint, ProtocolError, idempotency_key
from .reconcile import reconcile
from .risk import RiskEngine
from .settings import BridgeSettings, RiskSettings
from .symbol import check_symbol, snap

log = logging.getLogger("goldflow.execution")


class ExecutionBridge:
    def __init__(self, cfg: Any, name: str, transport: Any, secret: bytes, mode: str = "demo", now: Callable[[], int] | None = None,
                 dq_open: Callable[[int], bool] | None = None, calendar: Any = None, root: Path | None = None) -> None:  # fmt: skip
        if mode not in ("demo", "live"):
            raise ValueError("mode: demo | live")
        self.cfg, self.name, self.t, self.mode = cfg, name, transport, mode
        self.bs, self.rs = BridgeSettings.from_config(cfg), RiskSettings.from_config(cfg)
        self.broker = dict(cfg.raw.get("broker") or {})
        if not self.broker.get("name"):
            raise ValueError("bridge: the broker block is not filled in (decision D2); the broker symbol cannot be validated")
        self.now = now or (lambda: int(time.time() * 1000))
        self.dq_open = dq_open or (lambda t: False)
        self.root = Path(root or Path(cfg.data_root) / "execution" / name)
        self.root.mkdir(parents=True, exist_ok=True)
        self.audit = AuditLog(self.root / "audit.jsonl")
        self.ep = Endpoint(secret, "py", f"{name}-{secrets.token_hex(4)}", self.bs.max_skew_ms)
        self.book = OrderBook()
        self.risk = RiskEngine(self.rs, calendar)
        self.hello: dict[str, Any] | None = None
        self.sym: dict[str, Any] | None = None
        self.market: dict[str, Any] = {}
        self.last_hb_in: int | None = None
        self.last_hb_out = 0
        self.last_reconcile_req = 0
        self.last_reconcile: dict[str, Any] | None = None
        self.flatten_pending = False
        self.sent: dict[int, str] = {}  # seq -> order key
        self.conn_seen = getattr(transport, "connections", None)
        self.signals_seen: set[str] = set()

    # ------------------------------------------------------------------ persistence
    @property
    def state_path(self) -> Path:
        return self.root / "state.json"

    def save(self) -> None:
        st = {"orders": self.book.state(), "risk": self.risk.state(), "signals_seen": sorted(self.signals_seen)[-5000:],
              "saved_ms": self.now()}  # fmt: skip
        tmp = self.state_path.with_name("state.json.tmp")
        tmp.write_text(json.dumps(st, default=str))
        os.replace(tmp, self.state_path)

    def start(self) -> None:
        if self.state_path.exists():
            s = json.loads(self.state_path.read_text())
            self.book.load(s["orders"])
            self.risk.load(s["risk"])
            self.signals_seen = set(s.get("signals_seen", []))
        for r, d in (("NO_EA", "no HELLO from the EA yet"), ("NO_SYMBOL_CHECK", "broker SymbolInfo not checked yet"),
                     ("NOT_RECONCILED", "positions not reconciled since start")):  # fmt: skip
            self.risk.block(r, d)
        self.audit.write(self.now(), "START", {"mode": self.mode, "session": self.ep.session, "restored_orders": len(self.book.orders),
                                               "emergency": self.risk.st.emergency})  # fmt: skip
        self._send("HELLO")

    # ------------------------------------------------------------------ sending
    def _send(self, mtype: str, **body: Any) -> int:
        msg, line = self.ep.make(mtype, self.now(), **body)
        ok = self.t.send(line)
        self.audit.write(self.now(), "OUT", {"line": line.rstrip("\n"), "delivered": ok})
        return int(msg["seq"])

    def _transmit(self, o: Order) -> None:
        seq = self._send("ORDER_NEW", key=o.key, symbol=o.symbol, side=o.side, volume=o.volume, sl=o.sl, tp=o.tp,
                         deviation=int(self.bs.deviation_points))  # fmt: skip
        self.sent[seq] = o.key
        o.attempts += 1
        o.last_sent_ms = self.now()
        if o.state == "CREATED":
            o.to("TRANSMITTED", self.now(), f"sent seq {seq}")
            self.audit.write(self.now(), "ORDER", {"key": o.key, "state": o.state, "attempt": o.attempts})

    # ------------------------------------------------------------------ signals
    def on_signal(self, table: str, rows: list[dict[str, Any]]) -> None:
        """StrategyEngine listener: only ``signals`` rows are considered. A persistence failure blocks trading and
        never propagates into the analytics pipeline."""
        if table != "signals":
            return
        for r in rows:
            try:
                self.submit(r)
            except OSError as e:
                self.risk.block("PERSISTENCE_FAILED", repr(e))
                log.critical("execution bridge cannot persist (%r); signal %s not traded, trading blocked", e, r.get("signal_id"))

    def submit(self, sig: dict[str, Any]) -> Order | None:
        now = self.now()
        if "PERSISTENCE_FAILED" in self.risk.st.blocked:
            self.save()  # raises again while the disk is still failing; otherwise it clears the block
            self.risk.unblock("PERSISTENCE_FAILED")
        sid = str(sig["signal_id"])
        self.audit.write(now, "SIGNAL", {k: sig.get(k) for k in ("signal_id", "strategy_id", "direction", "entry_price_ticks", "stop_loss_ticks",
                                                               "take_profit_ticks", "expiry_at", "available_at")})  # fmt: skip
        key = idempotency_key(sid)
        if sid in self.signals_seen or key in self.book.orders:
            self.audit.write(now, "RISK", {"signal_id": sid, "allowed": False, "reason": "DUPLICATE_SIGNAL"})
            return None
        self.signals_seen.add(sid)
        if sig.get("expiry_at") is not None and now >= int(sig["expiry_at"]):
            self.audit.write(now, "RISK", {"signal_id": sid, "allowed": False, "reason": "SIGNAL_EXPIRED"})
            return None
        if self.sym is None or not self.market:
            self.audit.write(now, "RISK", {"signal_id": sid, "allowed": False, "reason": "NO_MARKET", "failures": ["NO_MARKET"]})
            return None
        side = "BUY" if sig["direction"] == "LONG" else "SELL"
        tick_bn = float(Decimal(str(self.cfg["instrument"]["tick_size"])))
        entry = float(self.market["ask"] if side == "BUY" else self.market["bid"])
        sgn = 1 if side == "BUY" else -1
        sl_d = abs(int(sig["entry_price_ticks"]) - int(sig["stop_loss_ticks"])) * tick_bn
        tp_d = (
            abs(int(sig["take_profit_ticks"]) - int(sig["entry_price_ticks"])) * tick_bn
            if sig.get("take_profit_ticks") is not None
            else None
        )
        digits, tick = int(self.sym["digits"]), float(self.sym["tick_size"])
        e_t, s_t = int(sig["entry_price_ticks"]), int(sig["stop_loss_ticks"])
        geometry_ok = (s_t - e_t) * sgn < 0 and (
            sig.get("take_profit_ticks") is None or (int(sig["take_profit_ticks"]) - e_t) * sgn > 0
        )
        sl: float | None = (
            float(snap(entry - sgn * sl_d, tick, digits)) if geometry_ok else None
        )  # a malformed signal has no valid stop
        tp = float(snap(entry + sgn * tp_d, tick, digits)) if tp_d else None
        open_lots = sum((Decimal(o.open_volume) for o in self.book.open_positions()), Decimal(0))
        d = self.risk.check(side=side, entry=entry, sl=sl, equity=float(self.market.get("equity", 0)), sym=self.sym,
                            spread_points=float(self.market.get("spread_points", 0)), open_lots=open_lots,
                            open_count=len(self.book.open_positions()), pending_count=len(self.book.pending()), dq_open=self.dq_open(now),
                            now_ms=now)  # fmt: skip
        self.audit.write(now, "RISK", {"signal_id": sid, "allowed": d.allowed, "reason": d.reason, "failures": d.failures, "volume": d.volume,
                                       "risk_money": d.risk_money, "blocked": dict(self.risk.st.blocked), **d.detail})  # fmt: skip
        if not d.allowed or d.volume is None:
            return None
        assert sl is not None
        o = Order(key, sid, str(sig.get("strategy_id", "")), str(self.broker["symbol"]), side, d.volume, snap(sl, tick, digits),
                  snap(tp, tick, digits) if tp else "0", entry, now, int(sig["expiry_at"]) if sig.get("expiry_at") is not None else None)  # fmt: skip
        self.book.add(o)
        self._transmit(o)
        self.save()
        return o

    # ------------------------------------------------------------------ receiving
    def _on(self, m: dict[str, Any]) -> None:
        now = self.now()
        t = m["type"]
        if t == "HELLO":
            self.hello = m
            want = "DEMO" if self.mode == "demo" else "REAL"
            if m["trade_mode"] != want:
                self.risk.block("ACCOUNT_MODE", f"{self.mode} run but the EA reports a {m['trade_mode']} account")
                self.audit.write(now, "ALERT", {"kind": "ACCOUNT_MODE", "trade_mode": m["trade_mode"], "mode": self.mode})
            else:
                self.risk.unblock("ACCOUNT_MODE")
            self.risk.unblock("NO_EA")
            self._send("SYMBOL_INFO_REQ", symbol=str(self.broker["symbol"]))
            self._request_positions()
        elif t == "SYMBOL_INFO":
            bad = check_symbol(m, self.broker, self.hello)
            if bad:
                self.risk.block("SYMBOL_MISMATCH", "; ".join(bad))
                self.audit.write(now, "ALERT", {"kind": "SYMBOL_MISMATCH", "problems": bad})
            else:
                self.sym = m
                self.risk.unblock("SYMBOL_MISMATCH")
                self.risk.unblock("NO_SYMBOL_CHECK")
        elif t == "HEARTBEAT":
            self.last_hb_in = now
            self.market = {k: m[k] for k in ("bid", "ask", "spread_points", "balance", "equity", "time_msc")}
            self.risk.unblock("NO_HEARTBEAT")
            self._equity(float(m["equity"]))
        elif t == "POSITIONS":
            self.last_hb_in = now
            self.market.update({"balance": m["balance"], "equity": m["equity"]})
            rep = reconcile(self.book, m, now)
            self.last_reconcile = rep
            self.audit.write(now, "RECONCILE", rep)
            if rep["clean"]:
                self.risk.unblock("NOT_RECONCILED")
                self.risk.unblock("RECONCILE_MISMATCH")
                if not any(o.state == "UNKNOWN" for o in self.book.orders.values()):
                    self.risk.unblock("UNKNOWN_ORDER")
            else:
                self.risk.block("RECONCILE_MISMATCH", json.dumps(rep["mismatches"])[:500])
                self.audit.write(now, "ALERT", {"kind": "RECONCILE_MISMATCH", "mismatches": rep["mismatches"]})
            for k in rep["absent"]:
                ao = self.book.orders[k]
                if ao.state != "UNKNOWN":
                    continue
                if ao.expiry_ms is not None and now >= ao.expiry_ms:
                    ao.to("REJECTED", now, "RECONCILED_ABSENT_EXPIRED: never reached the broker and the signal expired")
                    self.audit.write(now, "ORDER", {"key": ao.key, "state": ao.state, "why": "absent and expired"})
                elif not self.risk.st.emergency:
                    ao.to("TRANSMITTED", now, "absent at the broker: re-sent with the same key")
                    ao.attempts = 0
                    self._transmit(ao)
            if not any(o.state == "UNKNOWN" for o in self.book.orders.values()):
                self.risk.unblock("UNKNOWN_ORDER")
            if self.flatten_pending and not m["positions"]:
                self.flatten_pending = False
                self.audit.write(now, "EMERGENCY", {"flattened": True})
            self._equity(float(m["equity"]))
        elif t == "ACK":
            o = self.book.orders.get(m["key"]) or self.book.orders.get(self.sent.get(int(m["ref"]), ""))
            if o is not None:
                o.acked = True
                if m["status"] == "DUPLICATE":
                    self._request_positions()  # the broker already has it: learn its state from the snapshot
        elif t == "REJECT":
            o = self.book.orders.get(m["key"])
            if o is not None:
                o.acked = True
                o.to("REJECTED", now, f"EA reject {m['retcode']} {m['reason']}")
                self.audit.write(now, "ORDER", {"key": o.key, "state": o.state, "retcode": m["retcode"], "reason": m["reason"]})
        elif t == "TXN":
            if m["kind"] == "MODIFY":
                o = self.book.resolve(str(m.get("key") or ""), int(m["position"]) if m.get("position") else None)
                if o is not None:
                    o.sl, o.tp = str(m.get("sl", o.sl)), str(m.get("tp", o.tp))
                    self.audit.write(now, "ORDER", {"key": o.key, "modified": [o.sl, o.tp]})
                return
            o, eff = self.book.on_txn(m, now)
            if o is not None:
                o.acked = True
                slip = None
                if eff == "fill" and self.sym is not None:
                    sgn = 1 if o.side == "BUY" else -1
                    slip = round((float(m["price"]) - o.expected_price) * sgn / float(self.sym["point"]), 3)  # adverse > 0
                self.audit.write(now, "ORDER", {"key": o.key, "state": o.state, "effect": eff, "txn": m, "slippage_points": slip})
                if slip is not None and self.risk.on_fill(slip):
                    self.audit.write(now, "ALERT", {"kind": "SLIPPAGE", "key": o.key, "points": slip})
                if eff == "closed":
                    self.risk.on_closed(o.realized)
            else:
                self.audit.write(now, "ALERT", {"kind": "TXN_FOR_UNKNOWN_ORDER", "txn": m})

    def _equity(self, eq: float) -> None:
        breaches = self.risk.on_equity(eq, self.now())
        # latch once: flatten retries are driven by tick(); re-latching on every POSITIONS/HEARTBEAT made a request loop
        if breaches and not self.risk.st.emergency:
            self.emergency_stop(f"{breaches[0]} limit reached (equity {eq})")

    def _request_positions(self) -> None:
        self.last_reconcile_req = self.now()
        self._send("POSITIONS_REQ")

    # ------------------------------------------------------------------ control
    def emergency_stop(self, reason: str) -> None:
        first = not self.risk.st.emergency
        self.risk.latch(reason)
        self.flatten_pending = True
        if first:
            self.audit.write(self.now(), "EMERGENCY", {"reason": reason})
        self._send("FLATTEN_ALL", reason=reason[:120])
        self._request_positions()

    def reset_emergency(self, operator: str) -> None:
        self.risk.reset_emergency(operator)
        self.audit.write(self.now(), "EMERGENCY", {"reset_by": operator})
        self.save()

    def tick(self) -> None:
        """One bridge step. If state or audit cannot be written (disk full, permissions), trading is blocked
        (``PERSISTENCE_FAILED``): an order whose existence cannot be recorded must not be sent."""
        try:
            self._tick()
            self.risk.unblock("PERSISTENCE_FAILED")
        except OSError as e:
            self.risk.block("PERSISTENCE_FAILED", repr(e))
            log.critical("execution bridge cannot persist its state or audit log: %r; trading blocked", e)

    def _tick(self) -> None:
        now = self.now()
        conns = getattr(self.t, "connections", None)
        if conns is not None and conns != self.conn_seen:  # a (re)connected EA: handshake and reconcile before trading
            self.conn_seen = conns
            self.hello = None
            self.risk.block("NOT_RECONCILED", "EA (re)connected: reconcile before trading again")
            self.audit.write(now, "ALERT", {"kind": "EA_CONNECTED", "connections": conns})
            self._send("HELLO")
        for line in self.t.receive():
            try:
                m = self.ep.parse(line, now)
            except ProtocolError as e:
                self.audit.write(now, "REFUSED", {"reason": str(e), "line": line[:300]})
                continue
            self.audit.write(now, "IN", {"line": line.rstrip("\n")})
            self._on(m)
        if now - self.last_hb_out >= self.bs.heartbeat_ms:
            self.last_hb_out = now
            if self.hello is None:
                self._send("HELLO")
            else:
                self._send("HEARTBEAT")
        if (
            self.last_hb_in is not None
            and now - self.last_hb_in > self.bs.heartbeat_timeout_ms
            and "NO_HEARTBEAT" not in self.risk.st.blocked
        ):
            self.risk.block("NO_HEARTBEAT", f"no message from the EA for {now - self.last_hb_in} ms")
            self.audit.write(now, "ALERT", {"kind": "NO_HEARTBEAT", "silent_ms": now - self.last_hb_in})
            self.hello = None  # re-handshake when it comes back (new EA incarnation or reconnect)
            self.risk.block("NOT_RECONCILED", "EA lost: reconcile before trading again")
        for o in list(self.book.orders.values()):
            if (
                o.state == "TRANSMITTED"
                and not o.acked
                and o.last_sent_ms is not None
                and now - o.last_sent_ms >= self.bs.ack_timeout_ms
            ):
                if o.attempts <= self.bs.max_retries:
                    self.audit.write(now, "ORDER", {"key": o.key, "retry": o.attempts, "why": "no ACK"})
                    self._transmit(o)  # the same key: the EA refuses a second position for it
                else:
                    o.to("UNKNOWN", now, f"no ACK after {o.attempts} attempts")
                    self.risk.block("UNKNOWN_ORDER", o.key)
                    self.audit.write(now, "ALERT", {"kind": "UNKNOWN_ORDER", "key": o.key})
                    self._request_positions()
        if self.hello is not None and now - self.last_reconcile_req >= self.bs.reconcile_every_s * 1000:
            self._request_positions()
        if self.flatten_pending and now - self.last_reconcile_req >= self.bs.ack_timeout_ms:
            self._send("FLATTEN_ALL", reason=str(self.risk.st.emergency_reason or "emergency")[:120])
            self._request_positions()
        flag = self.root / "EMERGENCY_STOP"
        if flag.exists():
            reason = flag.read_text().strip() or "operator emergency stop"
            flag.unlink()
            self.emergency_stop(reason)
        reset = self.root / "EMERGENCY_RESET"
        if reset.exists():
            op = reset.read_text().strip()
            reset.unlink()
            if op:
                self.reset_emergency(op)
        self.save()

    def status(self) -> dict[str, Any]:
        return {"mode": self.mode, "session": self.ep.session, "connected": bool(getattr(self.t, "connected", False)),
                "account": None if self.hello is None else {k: self.hello[k] for k in ("account", "trade_mode", "server", "currency")},
                "market": self.market, "blocked": dict(self.risk.st.blocked), "emergency": self.risk.st.emergency,
                "emergency_reason": self.risk.st.emergency_reason,
                "orders": {s: sum(1 for o in self.book.orders.values() if o.state == s) for s in {o.state for o in self.book.orders.values()}},
                "open_positions": [o.key for o in self.book.open_positions()], "refused_messages": len(self.ep.refused),
                "last_reconcile": None if self.last_reconcile is None else {k: self.last_reconcile[k] for k in ("at", "clean")}}  # fmt: skip

    def close(self) -> None:
        self.save()
        self.audit.write(self.now(), "STOP", self.status())
        self.audit.close()
