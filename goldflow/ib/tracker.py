"""Phase 10: Initial Balance state machine for one IB instance (one definition on one local date).

Pure and deterministic: it is fed finalised bars in time order (``formation_bar``, ``acceptance_bar``,
``behaviour_bar``) and told how far each stream's time has advanced (``fire_ib``, ``fire_held``, ``fire_summary``). Every emitted row carries ``event_time`` (when its evidence is complete) and is produced only once
the caller's clock has reached that time, so nothing is back-dated.

Rules (docs/METRICS_IB_V1.md):
* Forming, [ib_start, ib_end): IB high/low = max/min of formation-bar highs/lows (= the trade extremes). A developing
  snapshot is produced at every formation-bar end. At ib_end: IB_FORMED (high, low, mid, range, extensions), or
  IB_EMPTY if nothing traded.
* After ib_end until session_end:
  - Breakout: a behaviour-interval bar close > IB high (BREAKOUT_UP) or < IB low (BREAKOUT_DOWN).
  - Failed break: after a breakout, a behaviour bar closes back on or inside the broken extreme with bar_end <=
    breakout time + failed_break_window -> FAILED_BREAK_*. Otherwise BREAKOUT_HELD_* at that deadline. A later
    close back inside after a held breakout is RETURN_INSIDE_*. Each side can break again afterwards.
  - Acceptance outside (the Phase 9 rule): a stretch of consecutive acceptance-interval closes beyond the extreme;
    ACCEPTED_* once the stretch lasts >= accept_time, or once the lots traded beyond the extreme during the stretch
    reach accept_vol_frac x the window volume so far. Once per stretch; a close back inside ends the stretch.
  - Range expansion: the first formation bar whose high (low) reaches IB high + k*range (IB low - k*range) gives
    RANGE_EXT_UP/DOWN for that k. Exact rational comparison.
  - Volume/delta are documented boolean filters on breakouts only (``delta_confirms``, ``volume_confirms``); they
    never gate an event.
* At session_end: SESSION_SUMMARY.
"""

from __future__ import annotations

from fractions import Fraction
from typing import Any

from .settings import IBSettings

SIDES = ("UP", "DOWN")


def _f(x: Fraction | None) -> float | None:
    return None if x is None else float(x)


class IBTracker:
    def __init__(
        self,
        name: str,
        label: str,
        ib_start: int,
        ib_end: int,
        session_end: int,
        st: IBSettings,
        beh_ms: int,
        labels: dict[str, Any],
    ) -> None:
        self.name, self.label = name, label
        self.ib_start, self.ib_end, self.session_end = ib_start, ib_end, session_end
        self.st = st
        self.beh_ms = beh_ms
        self.labels = labels  # holiday / weekend labels (fixed per instance)
        self.hi: int | None = None
        self.lo: int | None = None
        self.open: int | None = None
        self.vol = self.buy = self.sell = 0
        self.formed = False
        self.done = False
        self.sess_hi: int | None = None
        self.sess_lo: int | None = None
        self.last_close: int | None = None
        self.sess_vol = 0  # all lots from ib_start (the acceptance "window volume")
        self.ext_next = {"UP": 0, "DOWN": 0}
        self.bo: dict[str, dict[str, Any]] = {s: {"state": "inside", "t": None, "deadline": None} for s in SIDES}
        self.acc: dict[str, dict[str, Any]] = {s: {"start": None, "beyond": 0, "accepted": False} for s in SIDES}
        self.counts = {f"{e}_{s}": 0 for e in ("breakout", "failed", "held", "returned", "accepted") for s in SIDES}

    # ------------------------------------------------------------------ helpers
    @property
    def rng(self) -> int | None:
        return None if self.hi is None or self.lo is None else self.hi - self.lo

    def ib_fields(self) -> dict[str, Any]:
        r = self.rng
        out: dict[str, Any] = {
            "ib_high_ticks": self.hi,
            "ib_low_ticks": self.lo,
            "ib_mid_ticks": None if r is None else (self.hi + self.lo) / 2,  # type: ignore[operator]
            "ib_range_ticks": r,
        }
        for k in self.st.extensions:
            ks = _kname(k)
            out[f"ext_up_{ks}_ticks"] = None if r is None else _f(self.hi + k * r)  # type: ignore[operator]
            out[f"ext_dn_{ks}_ticks"] = None if r is None else _f(self.lo - k * r)  # type: ignore[operator]
        return out

    def _row(self, event: str, t: int, ev_start: int, ev_end: int, **kw: Any) -> dict[str, Any]:
        return {
            "ib": self.name,
            "label": self.label,
            "ib_start": self.ib_start,
            "ib_end": self.ib_end,
            "session_end": self.session_end,
            "event": event,
            "event_time": t,
            "evidence_start": ev_start,
            "evidence_end": ev_end,
            **self.labels,
            **kw,
        }

    # ------------------------------------------------------------------ time
    def fire_ib(self, t: int, inclusive: bool) -> list[dict[str, Any]]:
        """IB close once time reaches ib_end (after every formation bar ending <= ib_end was fed)."""
        if not self.formed and (self.ib_end <= t if inclusive else self.ib_end < t):
            return [self._close_ib()]
        return []

    def fire_held(self, t: int, inclusive: bool) -> list[dict[str, Any]]:
        """BREAKOUT_HELD_* for breakouts whose failed-break window ended (deadline < t, or <= t) without a return."""
        out: list[dict[str, Any]] = []
        if not self.formed or self.hi is None:
            return out
        for s in SIDES:
            b = self.bo[s]
            d = b["deadline"]
            if b["state"] == "broken" and d <= self.session_end and (d <= t if inclusive else d < t):
                b["state"] = "held"
                self.counts[f"held_{s}"] += 1
                out.append(
                    self._row(
                        f"BREAKOUT_HELD_{s}",
                        d,
                        b["t"],
                        d,
                        price_ticks=self.hi if s == "UP" else self.lo,
                        direction=s,
                        **self.ib_fields(),
                    )
                )
        return out

    def fire_summary(self, t: int) -> list[dict[str, Any]]:
        """SESSION_SUMMARY once every stream has reached session_end."""
        if not self.done and self.session_end <= t:
            return [self._summary()]
        return []

    def _close_ib(self) -> dict[str, Any]:
        self.formed = True
        if self.hi is None:
            return self._row("IB_EMPTY", self.ib_end, self.ib_start, self.ib_end, **self.ib_fields(), volume_lots=0)
        return self._row(
            "IB_FORMED",
            self.ib_end,
            self.ib_start,
            self.ib_end,
            **self.ib_fields(),
            open_ticks=self.open,
            volume_lots=self.vol,
            buy_lots=self.buy,
            sell_lots=self.sell,
            delta_lots=self.buy - self.sell,
        )

    def _summary(self) -> dict[str, Any]:
        self.done = True
        r = self.rng
        loc = None
        if self.last_close is not None and self.hi is not None and self.lo is not None:
            loc = "above" if self.last_close > self.hi else ("below" if self.last_close < self.lo else "inside")
        ext = {
            f"max_ext_{s.lower()}": (_kname(self.st.extensions[self.ext_next[s] - 1]) if self.ext_next[s] else None)
            for s in SIDES
        }
        ratio = None
        if r and self.sess_hi is not None and self.sess_lo is not None:
            ratio = (self.sess_hi - self.sess_lo) / r
        return self._row(
            "SESSION_SUMMARY",
            self.session_end,
            self.ib_start,
            self.session_end,
            **self.ib_fields(),
            session_high_ticks=self.sess_hi,
            session_low_ticks=self.sess_lo,
            close_ticks=self.last_close,
            close_location=loc,
            range_ratio=ratio,
            volume_lots=self.sess_vol,
            **ext,
            **self.counts,
            open_breakout_up=self.bo["UP"]["state"] == "broken",
            open_breakout_down=self.bo["DOWN"]["state"] == "broken",
        )

    # ------------------------------------------------------------------ bars
    def formation_bar(self, b: dict[str, Any]) -> list[dict[str, Any]]:
        """A finalised formation-interval bar (dict of scalars). Inside the IB it builds the IB; after it, it drives
        range expansion and the session extremes."""
        s, e = b["bar_start"], b["bar_end"]
        if s < self.ib_start or e > self.session_end:
            return []
        self.sess_hi = b["high"] if self.sess_hi is None else max(self.sess_hi, b["high"])
        self.sess_lo = b["low"] if self.sess_lo is None else min(self.sess_lo, b["low"])
        self.last_close = b["close"]
        self.sess_vol += b["volume"]
        if e <= self.ib_end:
            if self.open is None:
                self.open = b["open"]
            self.hi = b["high"] if self.hi is None else max(self.hi, b["high"])
            self.lo = b["low"] if self.lo is None else min(self.lo, b["low"])
            self.vol += b["volume"]
            self.buy += b["buy"]
            self.sell += b["sell"]
            return [
                self._row(
                    "IB_DEVELOPING",
                    e,
                    self.ib_start,
                    e,
                    **self.ib_fields(),
                    volume_lots=self.vol,
                    delta_lots=self.buy - self.sell,
                )
            ]
        out: list[dict[str, Any]] = []
        r = self.rng
        if self.hi is None or self.lo is None or not r:
            return out
        ks = self.st.extensions
        while self.ext_next["UP"] < len(ks) and b["high"] - self.hi >= ks[self.ext_next["UP"]] * r:
            k = ks[self.ext_next["UP"]]
            out.append(
                self._row(
                    "RANGE_EXT_UP", e, s, e, k=_kname(k), price_ticks=_f(self.hi + k * r), direction="UP", **self.ib_fields()
                )
            )
            self.ext_next["UP"] += 1
        while self.ext_next["DOWN"] < len(ks) and self.lo - b["low"] >= ks[self.ext_next["DOWN"]] * r:
            k = ks[self.ext_next["DOWN"]]
            out.append(
                self._row(
                    "RANGE_EXT_DOWN", e, s, e, k=_kname(k), price_ticks=_f(self.lo - k * r), direction="DOWN", **self.ib_fields()
                )
            )
            self.ext_next["DOWN"] += 1
        return out

    def acceptance_bar(self, b: dict[str, Any], beyond_up: int, beyond_dn: int) -> list[dict[str, Any]]:
        """A finalised acceptance-interval bar after the IB, with the lots it traded strictly above IB high
        (beyond_up) and strictly below IB low (beyond_dn)."""
        s, e = b["bar_start"], b["bar_end"]
        if s < self.ib_end or e > self.session_end or self.hi is None or self.lo is None:
            return []
        out = []
        st = self.st
        for side, beyond_close, lots, level in (
            ("UP", b["close"] > self.hi, beyond_up, self.hi),
            ("DOWN", b["close"] < self.lo, beyond_dn, self.lo),
        ):
            a = self.acc[side]
            if not beyond_close:
                a.update(start=None, beyond=0, accepted=False)
                continue
            if a["start"] is None:
                a.update(start=s, beyond=0, accepted=False)
            a["beyond"] += lots
            if a["accepted"]:
                continue
            by_time = e - a["start"] >= st.accept_time_ms
            f = st.accept_vol_frac
            by_vol = a["beyond"] * f.denominator >= f.numerator * self.sess_vol
            if by_time or by_vol:
                a["accepted"] = True
                self.counts[f"accepted_{side}"] += 1
                crit = "time+volume" if by_time and by_vol else ("time" if by_time else "volume")
                out.append(
                    self._row(
                        f"ACCEPTED_{'ABOVE' if side == 'UP' else 'BELOW'}",
                        e,
                        a["start"],
                        e,
                        price_ticks=level,
                        direction=side,
                        criterion=crit,
                        beyond_lots=a["beyond"],
                        window_lots=self.sess_vol,
                        **self.ib_fields(),
                    )
                )
        return out

    def behaviour_bar(self, b: dict[str, Any]) -> list[dict[str, Any]]:
        """A finalised behaviour-interval bar after the IB: breakouts, failed breaks and returns inside."""
        s, e = b["bar_start"], b["bar_end"]
        if s < self.ib_end or e > self.session_end or self.hi is None or self.lo is None:
            return []
        out = []
        for side, beyond, level in (("UP", b["close"] > self.hi, self.hi), ("DOWN", b["close"] < self.lo, self.lo)):
            st = self.bo[side]
            if st["state"] == "broken" and not beyond:
                if e <= st["deadline"]:
                    self.counts[f"failed_{side}"] += 1
                    out.append(
                        self._row(f"FAILED_BREAK_{side}", e, st["t"], e, price_ticks=level, direction=side, **self.ib_fields())
                    )
                st.update(state="inside", t=None, deadline=None)
            elif st["state"] == "held" and not beyond:
                self.counts[f"returned_{side}"] += 1
                out.append(self._row(f"RETURN_INSIDE_{side}", e, s, e, price_ticks=level, direction=side, **self.ib_fields()))
                st.update(state="inside", t=None, deadline=None)
            elif st["state"] == "inside" and beyond:
                st.update(state="broken", t=e, deadline=e + self.st.failed_window_ms)
                self.counts[f"breakout_{side}"] += 1
                nb = (self.ib_end - self.ib_start) // self.beh_ms
                f = self.st.breakout_vol_mult
                vol_ok = b["volume"] * nb * f.denominator >= f.numerator * self.vol
                delta = b["buy"] - b["sell"]
                out.append(
                    self._row(
                        f"BREAKOUT_{side}",
                        e,
                        s,
                        e,
                        price_ticks=level,
                        direction=side,
                        close_ticks=b["close"],
                        bar_volume_lots=b["volume"],
                        bar_delta_lots=delta,
                        delta_confirms=delta > 0 if side == "UP" else delta < 0,
                        volume_confirms=bool(vol_ok),
                        **self.ib_fields(),
                    )
                )
        return out

    # ------------------------------------------------------------------ state
    def get_state(self) -> dict[str, Any]:
        return {
            k: getattr(self, k)
            for k in (
                "hi",
                "lo",
                "open",
                "vol",
                "buy",
                "sell",
                "formed",
                "done",
                "sess_hi",
                "sess_lo",
                "last_close",
                "sess_vol",
                "ext_next",
                "bo",
                "acc",
                "counts",
            )
        }

    def set_state(self, s: dict[str, Any]) -> None:
        for k, v in s.items():
            setattr(self, k, v)


def _kname(k: Fraction) -> str:
    return str(k.numerator) if k.denominator == 1 else f"{float(k):g}" if Fraction(f"{float(k):g}") == k else str(k)
