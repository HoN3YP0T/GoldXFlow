"""Phase 12: execution simulation on broker quotes, risk limits and the cash ledger.

Event order (one simulation clock shared with the replay): broker ticks with time <= a signal's event time are applied
before that signal is evaluated; an order becomes active at ``event_time + latency`` and fills on the **first broker
tick at or after** that time; a position can exit from the next tick on.

Fills (documented in every report):
* market entry: buy at **ask**, sell at **bid**, plus slippage = ``slippage_points`` + ceil(``slippage_vol_mult`` x
  population sigma of the last ``slippage_vol_ticks`` broker mid changes before the fill), against the trader;
* stop loss: a market order when the bid (long) / ask (short) reaches the stop: filled at that quote plus slippage
  (gaps fill at the gapped price, not at the stop);
* take profit: a limit order, filled at the target price when the bid (long) / ask (short) reaches it;
* **stop/target ambiguity**: ticks decide the first touch. When the tick that would fill the target comes after a gap
  longer than ``max_tick_gap`` and the Binance price path during the gap (translated with the order's basis) reached
  the stop, the **stop** is assumed first (unfavourable) and counted as ``gap_assumed_stop``;
* broker rejection: a deterministic draw per order (seeded) below ``reject_rate``; stops-level rule: stop and target
  must be at least ``stops_level_points`` from the fill price, on the correct sides; otherwise the order is rejected;
* time exit after ``max_hold``; positions still open at the end of the data are closed at the last quote;
* commission per lot per side; swap per lot at each server-time rollover on Mon–Fri, triple on ``triple_swap_day``.

Risk (checked at the signal time, suppress only): sizing = floor(risk_per_trade x equity / (stop distance x contract
size) / volume_step) x volume_step, capped by volume_max and the exposure limit, refused below volume_min; maximum
concurrent positions; daily loss limit and maximum consecutive losses (no new entries for the rest of the UTC day);
maximum drawdown (no new entries for the rest of the run); session filter; a known basis and a fresh broker quote.

Money is exact: every cash flow is a ``Fraction`` in USD rounded once to integer cents (round half to even), and the
balance is the running sum of those cents, so the ledger reconciles exactly.
"""

from __future__ import annotations

import hashlib
import math
from fractions import Fraction
from typing import Any

import numpy as np
from numpy.typing import NDArray

from ..timeutil import DAY_MS
from .basis import BasisModel
from .settings import BacktestSettings, BrokerSpec

I64 = np.int64
INF = 2**62


def cents(x: Fraction) -> int:
    return int(round(x * 100))


class Simulator:
    def __init__(self, spec: BrokerSpec, bt: BacktestSettings, basis: BasisModel, rollovers: Any) -> None:
        self.spec, self.bt, self.basis, self.rollovers = spec, bt, basis, rollovers
        self.balance = cents(bt.starting_capital)
        self.peak = self.balance
        self.halted = False
        self.day = -1
        self.day_start = self.balance
        self.locked_day = -1
        self.lock_reason = ""
        self.consec = 0
        self.last: list[int] | None = None  # last broker tick [t, bid, ask]
        self.mids: list[int] = []  # recent mids x2
        self.orders: list[dict[str, Any]] = []  # accepted, waiting for a fill
        self.positions: list[dict[str, Any]] = []
        self.next_roll = -INF  # next swap rollover (UTC ms); set from the first tick
        self.n = 0
        self.out: dict[str, list[dict[str, Any]]] = {"orders": [], "fills": [], "trades": [], "cash": [], "equity": []}
        self.counts: dict[str, int] = {"gap_assumed_stop": 0, "late_signals": 0}
        self.path = {"t": np.zeros(0, I64), "lo": np.zeros(0, I64), "hi": np.zeros(0, I64)}  # Binance 1 s path

    # ------------------------------------------------------------------ helpers
    def _cash(self, t: int, kind: str, oid: str, amount: Fraction) -> int:
        c = cents(amount)
        self.balance += c
        self.out["cash"].append({"time": t, "kind": kind, "order_id": oid, "amount_cents": c, "balance_cents": self.balance})
        return c

    def _equity(self) -> int:
        if self.last is None:
            return self.balance
        _, bid, ask = self.last
        u = Fraction(0)
        for p in self.positions:
            px = bid if p["dir"] > 0 else ask
            u += Fraction((px - p["entry"]) * p["dir"]) * self.spec.point * self.spec.contract_size * Fraction(p["lots"])
        return self.balance + cents(u)

    def _mark(self, t: int, why: str) -> None:
        eq = self._equity()
        self.peak = max(self.peak, eq)
        if self.peak > 0 and Fraction(self.peak - eq, self.peak) >= self.bt.max_drawdown and not self.halted:
            self.halted = True
            self.lock_reason = "MAX_DRAWDOWN"
        self.out["equity"].append({"time": t, "balance_cents": self.balance, "equity_cents": eq, "event": why})

    def _new_day(self, t: int) -> None:
        d = t // DAY_MS
        if d != self.day:
            self.day = d
            self.day_start = self.balance
            self.consec = 0
            self._mark(d * DAY_MS, "DAY_START")

    def _slip(self) -> int:
        k = self.bt.slippage_vol_ticks
        extra = 0
        if self.bt.slippage_vol_mult > 0 and len(self.mids) > k:
            d = np.diff(np.array(self.mids[-(k + 1) :], I64)) / 2.0
            extra = math.ceil(float(self.bt.slippage_vol_mult) * float(d.std()))
        return self.bt.slippage_points + extra

    def _rolls(self, upto: int) -> None:
        """Apply every swap rollover at or before ``upto``."""
        while True:
            r = self.next_roll
            if r > upto:
                return
            mult = self.rollovers.multiplier(r)
            for p in self.positions:
                if p["entry_time"] < r and mult:
                    sw = (
                        (self.spec.swap_long_per_lot if p["dir"] > 0 else self.spec.swap_short_per_lot)
                        * Fraction(p["lots"])
                        * mult
                    )
                    p["swap"] += self._cash(r, "SWAP", p["oid"], sw)
            self.next_roll = self.rollovers.next_after(r)

    # ------------------------------------------------------------------ signals -> orders
    def signal(self, sig: dict[str, Any], sessions_open: list[str]) -> None:
        E = int(sig["event_time"])
        self._new_day(E)
        self.n += 1
        oid = f"o{self.n:06d}-{sig['signal_id']}"
        d = 1 if sig["direction"] == "LONG" else -1
        order = {"order_id": oid, "signal_id": sig["signal_id"], "strategy_id": sig["strategy_id"], "direction": sig["direction"],
                 "event_time": E, "submit_at": E + self.bt.latency_ms, "expiry_at": int(sig["expiry_at"]), "status": None,
                 "entry_ref_points": None, "sl_points": None, "tp_points": None, "lots": None, "basis_half": None, "risk_cents": None}  # fmt: skip
        reason = None
        bh, nb = self.basis.at(E)
        if sig.get("entry_type", "market") != "market":
            reason = "REJECTED_UNSUPPORTED_ENTRY_TYPE"  # limit/stop entries are not simulated in v1
        elif self.halted:
            reason = "REJECTED_MAX_DRAWDOWN"
        elif self.locked_day == E // DAY_MS:
            reason = f"REJECTED_{self.lock_reason}"
        elif self.bt.sessions and not set(sessions_open) & set(self.bt.sessions):
            reason = "REJECTED_SESSION"
        elif len(self.positions) + len(self.orders) >= self.bt.max_concurrent:
            reason = "REJECTED_MAX_CONCURRENT"
        elif self.last is None or E - self.last[0] > self.bt.max_tick_gap_ms:
            reason = "REJECTED_STALE_QUOTE"
        elif bh is None:
            reason = "REJECTED_NO_BASIS"
        if reason is None:
            assert bh is not None
            ent = self.basis.to_broker(int(sig["entry_price_ticks"]), bh)
            sl = self.basis.to_broker(int(sig["stop_loss_ticks"]), bh)
            tp = self.basis.to_broker(int(sig["take_profit_ticks"]), bh)
            per_lot = Fraction(abs(ent - sl)) * self.spec.point * self.spec.contract_size
            risk_amt = self.bt.risk_per_trade * Fraction(self._equity(), 100)
            step = self.spec.volume_step
            lots = (risk_amt / per_lot // step) * step if per_lot > 0 else Fraction(0)
            used = sum((Fraction(p["lots"]) for p in self.positions), Fraction(0)) + sum(
                (Fraction(o["lots"]) for o in self.orders), Fraction(0)
            )
            lots = min(lots, self.spec.volume_max, (self.bt.max_exposure_lots - used) // step * step)
            order.update(
                entry_ref_points=ent, sl_points=sl, tp_points=tp, basis_half=bh, lots=str(lots), risk_cents=cents(per_lot * lots)
            )
            if (sl - ent) * d >= 0 or (tp - ent) * d <= 0:
                reason = "REJECTED_INVALID_TRANSLATION"
            elif lots < self.spec.volume_min:
                reason = "REJECTED_SIZE_BELOW_MIN"
        if reason is not None:
            order["status"] = reason
            self.out["orders"].append(order)
            return
        order["dir"] = d
        self.orders.append(order)

    # ------------------------------------------------------------------ ticks
    def _fill(self, o: dict[str, Any], k: int, t: NDArray[Any], bid: NDArray[Any], ask: NDArray[Any]) -> None:
        d = o["dir"]
        u = int.from_bytes(hashlib.sha256(f"{self.bt.seed}:{o['signal_id']}".encode()).digest()[:8], "big") / 2**64
        slip = self._slip()
        px = int(ask[k]) + slip if d > 0 else int(bid[k]) - slip
        sl, tp, lvl = o["sl_points"], o["tp_points"], self.spec.stops_level_points
        status = "FILLED"
        if u < self.bt.reject_rate:
            status = "REJECTED_BROKER"
        elif not ((px - sl) * d >= max(lvl, 1) and (tp - px) * d >= max(lvl, 1)):
            status = "REJECTED_STOPS_LEVEL"
        o.update(status=status, fill_time=int(t[k]), fill_points=px if status == "FILLED" else None)
        self.out["orders"].append({k2: v for k2, v in o.items() if k2 != "dir"})
        if status != "FILLED":
            return
        lots = Fraction(o["lots"])
        pos = {"oid": o["order_id"], "signal_id": o["signal_id"], "strategy_id": o["strategy_id"], "dir": d, "lots": o["lots"],
               "entry_time": int(t[k]), "entry": px, "sl": sl, "tp": tp, "basis_half": o["basis_half"], "risk_cents": o["risk_cents"],
               "deadline": int(t[k]) + self.bt.max_hold_ms, "commission": 0, "swap": 0}  # fmt: skip
        pos["commission"] += self._cash(int(t[k]), "COMMISSION", o["order_id"], -self.spec.commission_per_lot_side * lots)
        self.out["fills"].append({"order_id": o["order_id"], "signal_id": o["signal_id"], "time": int(t[k]), "side": "BUY" if d > 0 else "SELL",
                                  "price_points": px, "quote_points": int(ask[k] if d > 0 else bid[k]), "lots": o["lots"], "reason": "ENTRY",
                                  "slippage_points": slip, "latency_ms": int(t[k]) - o["event_time"]})  # fmt: skip
        self.positions.append(pos)
        self._mark(int(t[k]), "ENTRY")

    def _exit(self, p: dict[str, Any], t_ms: int, px: int, quote: int, reason: str, slip: int) -> None:
        d = p["dir"]
        lots = Fraction(p["lots"])
        gross = Fraction((px - p["entry"]) * d) * self.spec.point * self.spec.contract_size * lots
        g = self._cash(t_ms, "PNL", p["oid"], gross)
        p["commission"] += self._cash(t_ms, "COMMISSION", p["oid"], -self.spec.commission_per_lot_side * lots)
        net = g + p["commission"] + p["swap"]
        self.out["fills"].append({"order_id": p["oid"], "signal_id": p["signal_id"], "time": t_ms, "side": "SELL" if d > 0 else "BUY",
                                  "price_points": px, "quote_points": quote, "lots": p["lots"], "reason": reason, "slippage_points": slip,
                                  "latency_ms": 0})  # fmt: skip
        self.out["trades"].append({"order_id": p["oid"], "signal_id": p["signal_id"], "strategy_id": p["strategy_id"],
                                   "direction": "LONG" if d > 0 else "SHORT", "lots": p["lots"], "entry_time": p["entry_time"],
                                   "entry_points": p["entry"], "exit_time": t_ms, "exit_points": px, "exit_reason": reason, "gross_cents": g,
                                   "commission_cents": p["commission"], "swap_cents": p["swap"], "net_cents": net, "risk_cents": p["risk_cents"],
                                   "r_multiple": (net / p["risk_cents"]) if p["risk_cents"] else None, "basis_half": p["basis_half"]})  # fmt: skip
        self.positions.remove(p)
        self.consec = self.consec + 1 if net < 0 else 0
        if self.consec >= self.bt.max_consecutive_losses:
            self.locked_day, self.lock_reason = t_ms // DAY_MS, "CONSECUTIVE_LOSSES"
        if self.day_start > 0 and Fraction(self.day_start - self.balance, self.day_start) >= self.bt.daily_loss_limit:
            self.locked_day, self.lock_reason = t_ms // DAY_MS, "DAILY_LOSS"
        self._mark(t_ms, "EXIT")

    def _exit_index(
        self, p: dict[str, Any], i0: int, t: NDArray[Any], bid: NDArray[Any], ask: NDArray[Any]
    ) -> tuple[int, str] | None:
        i0 = max(i0, int(np.searchsorted(t, p["entry_time"], "right")))  # never on the fill tick itself
        q = bid[i0:] if p["dir"] > 0 else ask[i0:]
        if not len(q):
            return None
        d = p["dir"]
        sl_hit = (q - p["sl"]) * d <= 0
        tp_hit = (q - p["tp"]) * d >= 0
        tm = t[i0:] >= p["deadline"]
        anyh = sl_hit | tp_hit | tm
        if not anyh.any():
            return None
        j = int(np.argmax(anyh))
        return i0 + j, ("SL" if sl_hit[j] else "TP" if tp_hit[j] else "MAX_HOLD")

    def _gap_stop(self, p: dict[str, Any], t0: int, t1: int) -> bool:
        """Did the Binance path between two broker ticks reach the stop (translated with the order's basis)?"""
        P = self.path
        i, j = int(np.searchsorted(P["t"], t0, "right")), int(np.searchsorted(P["t"], t1, "right"))
        if j <= i:
            return False
        bm = self.basis
        sl_half = 2 * p["sl"] * bm.kr
        if p["dir"] > 0:
            return bool(((2 * P["lo"][i:j] * bm.kb - p["basis_half"]) <= sl_half).any())
        return bool(((2 * P["hi"][i:j] * bm.kb - p["basis_half"]) >= sl_half).any())

    def run(
        self, t: NDArray[Any], bid: NDArray[Any], ask: NDArray[Any], signals: list[tuple[dict[str, Any], list[str]]], upto: int
    ) -> None:
        """Process broker ticks (time < upto) merged with signals (event_time <= upto) in time order."""
        i = 0
        sigs = sorted(signals, key=lambda x: (int(x[0]["event_time"]), x[0]["signal_id"]))
        si = 0
        n = len(t)
        if self.next_roll == -INF:
            self.next_roll = self.rollovers.next_after((int(t[0]) if n else upto) - 1)
        while True:
            cand: list[tuple[int, int, str, Any]] = []  # (time, priority, kind, payload)
            if si < len(sigs):
                E = int(sigs[si][0]["event_time"])
                if self.last is not None and E < self.last[0]:
                    self.counts["late_signals"] += 1
                cand.append((E, 1, "sig", None))
            for o in self.orders:
                k = int(np.searchsorted(t, o["submit_at"], "left"))
                k = max(k, i)
                if k < n and int(t[k]) < o["expiry_at"]:
                    cand.append((int(t[k]), 2, "fill", (o, k)))
                elif o["expiry_at"] <= upto:
                    cand.append((o["expiry_at"], 0, "expire", o))
            for p in self.positions:
                x = self._exit_index(p, i, t, bid, ask)
                if x is not None:
                    cand.append((int(t[x[0]]), 3, "exit", (p, x)))
            if self.positions and self.next_roll < upto:
                cand.append((self.next_roll, 0, "roll", None))
            if not cand:
                break
            tm, _, kind, pay = min(cand, key=lambda c: (c[0], c[1]))
            if kind == "sig":
                j = int(np.searchsorted(t, tm, "right"))
                self._advance_ticks(t, bid, ask, i, j)
                i = max(i, j)
                self._rolls(tm)
                self.signal(*sigs[si])
                si += 1
            elif kind == "expire":
                self._advance_ticks(t, bid, ask, i, int(np.searchsorted(t, tm, "left")))
                i = max(i, int(np.searchsorted(t, tm, "left")))
                pay["status"] = "EXPIRED"
                self.orders.remove(pay)
                self.out["orders"].append({k2: v for k2, v in pay.items() if k2 != "dir"})
            elif kind == "roll":
                self._advance_ticks(t, bid, ask, i, int(np.searchsorted(t, tm, "left")))
                i = max(i, int(np.searchsorted(t, tm, "left")))
                self._rolls(tm)
            elif kind == "fill":
                o, k = pay
                self._advance_ticks(t, bid, ask, i, k)
                i = k
                self._rolls(int(t[k]))
                self.orders.remove(o)
                self._new_day(int(t[k]))
                self._fill(o, k, t, bid, ask)
            else:
                p, (k, why) = pay
                prev = int(t[k - 1]) if k > 0 else (self.last[0] if self.last else int(t[k]))
                self._advance_ticks(t, bid, ask, i, k)
                i = k
                self._rolls(int(t[k]))
                self._new_day(int(t[k]))
                d = p["dir"]
                q = int(bid[k] if d > 0 else ask[k])
                slip = self._slip()
                if why == "TP" and int(t[k]) - prev > self.bt.max_tick_gap_ms and self._gap_stop(p, prev, int(t[k])):
                    self.counts["gap_assumed_stop"] += 1
                    self._exit(p, int(t[k]), p["sl"] - d * slip, q, "GAP_ASSUMED_STOP", slip)
                elif why == "TP":
                    self._exit(p, int(t[k]), p["tp"], q, "TP", 0)
                else:
                    self._exit(p, int(t[k]), q - d * slip, q, why, slip)
        self._advance_ticks(t, bid, ask, i, n)
        if self.positions:
            self._rolls(upto - 1)

    def _advance_ticks(self, t: NDArray[Any], bid: NDArray[Any], ask: NDArray[Any], i: int, j: int) -> None:
        if j > i:
            self.last = [int(t[j - 1]), int(bid[j - 1]), int(ask[j - 1])]
            k = self.bt.slippage_vol_ticks + 1
            self.mids = (self.mids + (bid[max(i, j - k) : j] + ask[max(i, j - k) : j]).tolist())[-k:]

    def close_all(self, reason: str) -> None:
        if self.last is None:
            return
        t, bid, ask = self.last
        for p in list(self.positions):
            q = bid if p["dir"] > 0 else ask
            self._exit(p, t, q, q, reason, 0)
        for o in list(self.orders):
            o["status"] = "EXPIRED_END_OF_DATA"
            self.orders.remove(o)
            self.out["orders"].append({k2: v for k2, v in o.items() if k2 != "dir"})

    # ------------------------------------------------------------------ state
    def state(self) -> dict[str, Any]:
        return {k: getattr(self, k) for k in ("balance", "peak", "halted", "day", "day_start", "locked_day", "lock_reason", "consec", "last",
                                              "mids", "orders", "positions", "next_roll", "n", "out", "counts")} | {
            "path": {k: v.tolist() for k, v in self.path.items()}}  # fmt: skip

    def load(self, s: dict[str, Any]) -> None:
        for k, v in s.items():
            if k == "path":
                self.path = {kk: np.asarray(vv, I64) for kk, vv in v.items()}
            else:
                setattr(self, k, v)
