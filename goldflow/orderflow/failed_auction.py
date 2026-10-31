"""Phase 9: failed auctions and post-event outcomes.

**Failed auction at a high** (the low is the mirror). All must hold:
1. The episode probed beyond a reference level by >= ``probe_bins`` bins (a new high beyond the level).
2. Participation at the probe was weak: volume traded beyond the level <= ``fa_vol_frac`` x the volume of the
   episode's bars, **or** responsive selling shows in delta (buy - sell beyond the level < 0).
3. Price closed back inside within ``fa_return_time`` of the probe start.
4. The return held: ``fa_hold_bars`` consecutive closes inside, with no trade beyond the probe extreme. The event is
   confirmed at the end of the last of those bars, never earlier. A wick alone is never enough: rule 4 needs bars
   after the return, and rule 2 needs evidence of weak participation.
The row records the probe start (``event_time``), the confirmation time, the invalidation level (the probe extreme)
and the evidence. ``Outcome`` then records what happened over the next N minutes, as review data that becomes
available only at confirmation + N.
"""

from __future__ import annotations

from fractions import Fraction
from typing import Any

from .auction import beyond


def failed_auction_evidence(snap: dict[str, Any], L: Fraction, side: int, st: Any) -> dict[str, Any] | None:
    """Rules 1-3 on a closed (returned, not accepted) episode. None if they do not hold."""
    if snap["accepted_at"] is not None or snap["ext"] is None:
        return None
    probe = (snap["ext"] * L.denominator - L.numerator) * side >= st.probe_bins * st.bin_ticks * L.denominator
    weak_vol = snap["vol_beyond"] * st.fa_vol_frac.denominator <= st.fa_vol_frac.numerator * snap["vol_total"]
    responsive = snap["delta_beyond"] * side < 0
    quick = snap["end"] - snap["start"] <= st.fa_return_ms
    if not (probe and (weak_vol or responsive) and quick):
        return None
    return {"weak_volume": bool(weak_vol), "responsive_delta": bool(responsive)}


class FailedAuctionHold:
    """Rule 4: the return must hold for ``fa_hold_bars`` closes inside without trading beyond the probe extreme."""

    __slots__ = ("level_id", "price", "side", "snap", "ev", "held", "need")

    def __init__(self, level_id: str, price: Fraction, side: int, snap: dict[str, Any], ev: dict[str, Any], need: int) -> None:
        self.level_id, self.price, self.side, self.snap, self.ev, self.held, self.need = level_id, price, side, snap, ev, 0, need

    def step(self, bar: dict[str, Any]) -> str | None:
        """'CONFIRMED', 'DROPPED' or None (still holding)."""
        if int(bar["bar_start"]) < self.snap["end"]:
            return None
        ext = int(bar["high_ticks"]) if self.side > 0 else int(bar["low_ticks"])
        if (ext - self.snap["ext"]) * self.side > 0 or beyond(int(bar["close_ticks"]), self.price, self.side):
            return "DROPPED"
        self.held += 1
        return "CONFIRMED" if self.held >= self.need else None

    def tree(self) -> list[Any]:
        return [
            self.level_id,
            [self.price.numerator, self.price.denominator],
            self.side,
            self.snap,
            self.ev,
            self.held,
            self.need,
        ]

    @classmethod
    def load(cls, x: list[Any]) -> "FailedAuctionHold":
        h = cls(x[0], Fraction(int(x[1][0]), int(x[1][1])), int(x[2]), dict(x[3]), dict(x[4]), int(x[6]))
        h.held = int(x[5])
        return h


class Outcome:
    """Post-event review metrics at each horizon: close change, best and worst excursion in the expected direction,
    and whether the invalidation price was reached. ``direction`` = expected move (+1 up, -1 down)."""

    __slots__ = ("event_id", "t0", "ref", "direction", "invalid", "horizons", "best", "worst", "last_close", "hit_invalid")

    def __init__(
        self, event_id: str, t0: int, ref: int, direction: int, invalid: Fraction | int | None, horizons_ms: list[int]
    ) -> None:
        self.event_id, self.t0, self.ref, self.direction, self.invalid = event_id, t0, ref, direction, invalid
        self.horizons = sorted(horizons_ms)
        self.best = self.worst = 0
        self.last_close = ref
        self.hit_invalid = False

    def step(self, bar: dict[str, Any], shift: int) -> list[dict[str, Any]]:
        """Feed one finalised bar (time order). First emits every horizon that ended before this bar starts, so a
        horizon never sees a later bar."""
        out = self.due(int(bar["bar_start"]), shift)
        if int(bar["bar_start"]) < self.t0 or not self.horizons:
            return out
        up = (int(bar["high_ticks"]) - self.ref) * self.direction
        dn = (int(bar["low_ticks"]) - self.ref) * self.direction
        self.best = max(self.best, up, dn)
        self.worst = min(self.worst, up, dn)
        self.last_close = int(bar["close_ticks"])
        if self.invalid is not None:
            ext = int(bar["high_ticks"]) if self.direction < 0 else int(bar["low_ticks"])
            if (ext - self.invalid) * -self.direction >= 0:
                self.hit_invalid = True
        return out

    def due(self, upto: int, shift: int) -> list[dict[str, Any]]:
        """Rows for every horizon that has fully elapsed (all bars ending by t0 + h are final)."""
        out = []
        while self.horizons and self.t0 + self.horizons[0] <= upto:
            h = self.horizons.pop(0)
            out.append({"event_id": self.event_id, "horizon_minutes": h // 60_000, "ref_price_ticks": self.ref,
                        "close_price_ticks": self.last_close, "move_ticks": (self.last_close - self.ref) * self.direction,
                        "mfe_ticks": self.best, "mae_ticks": self.worst, "invalidated": None if self.invalid is None else self.hit_invalid,
                        "available_at": self.t0 + h + shift})  # fmt: skip
        return out

    def tree(self) -> list[Any]:
        inv = None if self.invalid is None else [Fraction(self.invalid).numerator, Fraction(self.invalid).denominator]
        return [self.event_id, self.t0, self.ref, self.direction, inv, self.horizons, self.best, self.worst,
                self.last_close, self.hit_invalid]  # fmt: skip

    @classmethod
    def load(cls, x: list[Any]) -> "Outcome":
        inv = None if x[4] is None else Fraction(int(x[4][0]), int(x[4][1]))
        o = cls(x[0], int(x[1]), int(x[2]), int(x[3]), inv, [int(h) for h in x[5]])
        o.best, o.worst, o.last_close, o.hit_invalid = int(x[6]), int(x[7]), int(x[8]), bool(x[9])
        return o
