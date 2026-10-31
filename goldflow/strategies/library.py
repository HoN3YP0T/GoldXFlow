"""Phase 11: example research strategies. **None is assumed profitable**: each states a hypothesis for Phase 12–13 to
test out of sample. All prices are signal-venue ticks; entries are references for the execution layer.

Every strategy gathers evidence available at the trigger's time (``evidence_for``) and turns it into a signal with
the pure ``build``, so a signal can be re-derived from its stored evidence alone.
"""

from __future__ import annotations

import json
import math
from fractions import Fraction
from typing import Any

from ..processing.settings import parse_interval
from ..providers.base import Capability
from .base import AnalyticsState, Strategy
from .regime import score
from .signal import Evidence, TradingSignal

DQ = ("OK", "UNVERIFIED", "PARTIAL", "MISSING")


def _worst(*xs: str | None) -> str:
    v = [x for x in xs if x]
    return max(v, key=lambda s: DQ.index(s) if s in DQ else 3) if v else "OK"


def _ev(ev: tuple[Evidence, ...], metric: str) -> Evidence:
    return next(e for e in ev if e.metric == metric)


def _toward(price: Fraction, direction: int) -> int:
    """A target at a rational price, rounded toward the entry side (never beyond the level): LONG floor, SHORT ceil."""
    return math.floor(price) if direction > 0 else math.ceil(price)


def _regime_group(reg: dict[str, Any], prefer: tuple[str, ...]) -> float:
    st = reg.get("auction_state")
    return 1.0 if st in prefer else (0.5 if st in (None, "UNDETERMINED") else 0.25)


def _signal(s: Strategy, meta: dict[str, Any], direction: int, entry: int, stop: int, target: int, ev: tuple[Evidence, ...],
            dq: str, groups: dict[str, list[float]], weights: dict[str, float]) -> TradingSignal | None:  # fmt: skip
    if direction > 0 and not (stop < entry < target) or direction < 0 and not (target < entry < stop):
        return None
    if Fraction(abs(target - entry), abs(entry - stop)) < s.frac("min_rr"):
        return None
    sc, detail = score(groups, weights)
    return TradingSignal(
        s.id, s.version, meta["provider"], meta["instrument"], meta["event_time"], "LONG" if direction > 0 else "SHORT", "market",
        entry, stop, target, "1R", sc, ev, meta["event_time"] + parse_interval(s.params["expiry"]), stop,
        tuple(sorted(c.value for c in s.required_capabilities)), dq, meta["trigger_id"], detail,
    )  # fmt: skip


class FailedAuctionValueEdge(Strategy):
    id = "fa_value_edge"
    version = "1"
    hypothesis = (
        "A failed auction beyond the previous value area's edge (VAH/VAL), with responsive delta, marks a rejected "
        "attempt to leave value; price tends to rotate back toward the value area's POC."
    )
    required_inputs = frozenset({"auction", "orderflow"})
    triggers = frozenset({"auction.events"})
    defaults = {
        "level_types": ("VAH,VAL", "comma-separated Phase 9 level types that qualify"),
        "require_delta": (True, "the confirming 1m bar's delta (or the probe's responsive delta) must point back toward value"),
        "stop_buffer_ticks": (10, "stop = probe extreme + this many ticks"),
        "min_rr": ("1.0", "minimum reward:risk to emit (exact rational)"),
        "expiry": ("5m", "an unfilled signal is void after this"),
    }

    def evidence_for(self, table: str, row: dict[str, Any], state: AnalyticsState) -> tuple[Evidence, ...] | None:
        if row["event_type"] != "FAILED_AUCTION" or row["level_type"] not in self.params["level_types"].split(","):
            return None
        bar = state.bar_ending("1m", int(row["confirmed_at"]))
        wk = str(row["level_id"]).rsplit(":", 1)[0]
        poc = state.levels.get(f"{wk}:POC")
        if bar is None or poc is None:
            return None
        d = json.loads(row["detail"])
        direction = -1 if row["side"] == "ABOVE" else 1
        return (
            Evidence("auction.FAILED_AUCTION", {"side": row["side"], "level_id": row["level_id"], "level_type": row["level_type"],
                     "level_price_ticks": row["level_price_ticks"], "probe_extreme_ticks": row["probe_extreme_ticks"],
                     "confirmed_at": row["confirmed_at"], "weak_volume": d.get("weak_volume"), "responsive_delta": d.get("responsive_delta"),
                     "dq_status": row["dq_status"], "coverage": row["coverage"]},
                     "METRICS_AUCTION_V1 §4 rules 1-4", int(row["available_at"]), (row["event_id"],), True),
            Evidence("orderflow.confirm_bar", {"bar_start": bar["bar_start"], "close_ticks": bar["close_ticks"], "delta_lots": bar["delta_lots"],
                     "volume_lots": bar["volume_lots"], "dq_status": bar["dq_status"], "coverage": bar["coverage"]},
                     "delta points back toward value", int(bar["available_at"]), (f"orderflow.bars:1m:{bar['bar_start']}",),
                     int(bar["delta_lots"]) * direction > 0),
            Evidence("auction.target_level", {"level_id": poc["level_id"], "price_exact": poc["price_exact"]}, None, int(poc["available_at"]),
                     (poc["level_id"],), None),
            Evidence("regime", state.regime, None, int(row["available_at"]), (), None),
        )  # fmt: skip

    def build(self, evidence: tuple[Evidence, ...], meta: dict[str, Any]) -> TradingSignal | None:
        fa, bar, tg, rg = (
            _ev(evidence, m).value for m in ("auction.FAILED_AUCTION", "orderflow.confirm_bar", "auction.target_level", "regime")
        )
        direction = -1 if fa["side"] == "ABOVE" else 1
        delta_ok = int(bar["delta_lots"]) * direction > 0
        if self.params["require_delta"] and not (delta_ok or fa["responsive_delta"]):
            return None
        if fa["coverage"] != "FULL" or bar["coverage"] != "FULL":
            return None
        entry = int(bar["close_ticks"])
        stop = int(fa["probe_extreme_ticks"]) - direction * int(self.params["stop_buffer_ticks"])
        target = _toward(Fraction(tg["price_exact"]), direction)
        flow = min(1.0, abs(int(bar["delta_lots"])) / max(int(bar["volume_lots"]), 1) * 4) if delta_ok else 0.0
        return _signal(self, meta, direction, entry, stop, target, evidence, _worst(fa["dq_status"], bar["dq_status"]),
                       {"auction": [1.0], "flow": [flow, 1.0 if fa["responsive_delta"] else 0.0], "context": [_regime_group(rg, ("BALANCE", "TRANSITION"))]},
                       {"auction": 0.5, "flow": 0.3, "context": 0.2})  # fmt: skip


class AbsorptionAtLevel(Strategy):
    id = "absorption_level"
    version = "1"
    hypothesis = (
        "Confirmed absorption (heavy aggression that failed to move price, followed by the response) at a high-volume "
        "node or a VWAP band shows passive liquidity defending an accepted price; price tends to continue the response."
    )
    required_inputs = frozenset({"orderflow", "profile"})
    triggers = frozenset({"orderflow.events"})
    defaults = {
        "near_ticks": (20, "the absorption zone must be within this many ticks of an HVN or VWAP band"),
        "profile_bin": (10, "profile bin size whose developing trading-day snapshot supplies HVNs and bands"),
        "band_k": ("1,2", "VWAP bands (k sigma) that qualify"),
        "rr_target": ("2", "target = entry + rr_target x risk"),
        "stop_buffer_ticks": (5, "stop = absorption invalidation level + this many ticks"),
        "min_rr": ("1.5", "minimum reward:risk to emit"),
        "expiry": ("5m", "an unfilled signal is void after this"),
    }

    def evidence_for(self, table: str, row: dict[str, Any], state: AnalyticsState) -> tuple[Evidence, ...] | None:
        if row["event_type"] != "ABSORPTION" or row["status"] != "CONFIRMED":
            return None
        snap = state.profile_dev.get(f"trading_day:trading_day:{self.params['profile_bin']}")
        if snap is None:
            return None
        lo, hi = int(row["level_low_ticks"]), int(row["level_high_ticks"])
        cands = [("HVN", float(p)) for p in snap.get("hvn_prices", [])]
        for k in self.params["band_k"].split(","):
            for d in ("up", "dn"):
                v = snap.get(f"band_{d}_{k}_ticks")
                if v is not None:
                    cands.append((f"VWAP_{'+' if d == 'up' else '-'}{k}SD", float(v)))
        dist = [(0.0 if lo <= p <= hi else min(abs(lo - p), abs(hi - p)), name, p) for name, p in cands]
        near = sorted(x for x in dist if x[0] <= self.params["near_ticks"])
        if not near:
            return None
        dd, name, p = near[0]
        return (
            Evidence("orderflow.ABSORPTION", {k: row[k] for k in ("event_id", "side", "level_low_ticks", "level_high_ticks", "lots",
                     "resolution_price_ticks", "invalidate_ticks", "evidence_start", "evidence_end", "event_time", "dq_status", "coverage",
                     "unknown_share")}, "METRICS_ORDERFLOW_V1 §7.1, status CONFIRMED", int(row["available_at"]), (row["event_id"],), True),
            Evidence("profile.level", {"type": name, "price_ticks": p, "distance_ticks": dd, "snapshot_as_of": snap["as_of"],
                     "window_key": snap["window_key"]}, self.params["near_ticks"], int(snap["available_at"]),
                     (f"profile:{snap['window_key']}:{snap['as_of']}",), True),
            Evidence("regime", state.regime, None, int(row["available_at"]), (), None),
        )  # fmt: skip

    def build(self, evidence: tuple[Evidence, ...], meta: dict[str, Any]) -> TradingSignal | None:
        ab, lv, rg = (_ev(evidence, m).value for m in ("orderflow.ABSORPTION", "profile.level", "regime"))
        if ab["coverage"] != "FULL":
            return None
        direction = -1 if ab["side"] == "BUY" else 1  # buyers absorbed -> the response is down
        entry = int(ab["resolution_price_ticks"])
        stop = int(ab["invalidate_ticks"]) - direction * int(self.params["stop_buffer_ticks"])
        risk = abs(entry - stop)
        target = entry + direction * math.ceil(risk * self.frac("rr_target"))
        prox = 1.0 - min(1.0, float(lv["distance_ticks"]) / max(self.params["near_ticks"], 1))
        return _signal(self, meta, direction, entry, stop, target, evidence, ab["dq_status"],
                       {"orderflow": [1.0], "location": [prox], "context": [_regime_group(rg, ("BALANCE",))]},
                       {"orderflow": 0.5, "location": 0.3, "context": 0.2})  # fmt: skip


class IBFailedBreak(Strategy):
    id = "ib_failed_break"
    version = "1"
    hypothesis = (
        "A breakout from the Initial Balance that closes back inside within the failed-break window traps breakout "
        "traders; price tends to rotate back into the IB toward its midpoint."
    )
    required_inputs = frozenset({"ib", "orderflow"})
    triggers = frozenset({"ib.ib_events"})
    defaults = {
        "target": ("mid", "'mid' (IB midpoint) or 'opposite' (the other IB extreme)"),
        "stop_buffer_ticks": (10, "stop = extreme reached during the breakout + this many ticks"),
        "min_rr": ("1.0", "minimum reward:risk to emit"),
        "expiry": ("5m", "an unfilled signal is void after this"),
        "break_bar": ("5m", "the IB break interval (the breakout bar's length)"),
    }

    def evidence_for(self, table: str, row: dict[str, Any], state: AnalyticsState) -> tuple[Evidence, ...] | None:
        if not str(row["event_type"]).startswith("FAILED_BREAK_"):
            return None
        ib = state.ib_final.get(str(row["ib_key"]))
        d = json.loads(row["detail"])
        if ib is None or "breakout_at" not in d:
            return None
        a = int(d["breakout_at"]) - parse_interval(self.params["break_bar"])
        bars = state.bars_between("1m", a, int(row["event_time"]))
        if not bars:
            return None
        up = row["event_type"].endswith("UP")
        ext = max(int(b["high_ticks"]) for b in bars) if up else min(int(b["low_ticks"]) for b in bars)
        return (
            Evidence("ib.FAILED_BREAK", {k: row[k] for k in ("event_id", "event_type", "event_time", "price_ticks", "coverage")} | {"breakout_at": d["breakout_at"]},
                     "METRICS_IB_V1 §3", int(row["available_at"]), (row["event_id"],), True),
            Evidence("ib.final", {k: ib[k] for k in ("ib_key", "high_ticks", "low_ticks", "dq_status", "coverage")}, None, int(ib["available_at"]),
                     (str(ib["ib_key"]),), None),
            Evidence("orderflow.break_extreme", {"extreme_ticks": ext, "from": a, "to": int(row["event_time"]), "bars": len(bars)}, None,
                     max(int(b["available_at"]) for b in bars), tuple(f"orderflow.bars:1m:{b['bar_start']}" for b in bars), None),
            Evidence("regime", state.regime, None, int(row["available_at"]), (), None),
        )  # fmt: skip

    def build(self, evidence: tuple[Evidence, ...], meta: dict[str, Any]) -> TradingSignal | None:
        fb, ib, ex, rg = (_ev(evidence, m).value for m in ("ib.FAILED_BREAK", "ib.final", "orderflow.break_extreme", "regime"))
        if fb["coverage"] != "FULL" or ib["coverage"] != "FULL":
            return None
        direction = -1 if str(fb["event_type"]).endswith("UP") else 1
        entry = int(fb["price_ticks"])
        stop = int(ex["extreme_ticks"]) - direction * int(self.params["stop_buffer_ticks"])
        h, lo = int(ib["high_ticks"]), int(ib["low_ticks"])
        tp = Fraction(h + lo, 2) if self.params["target"] == "mid" else Fraction(lo if direction < 0 else h)
        target = _toward(tp, direction)
        return _signal(self, meta, direction, entry, stop, target, evidence, ib["dq_status"],
                       {"ib": [1.0], "context": [_regime_group(rg, ("BALANCE", "TRANSITION"))]}, {"ib": 0.7, "context": 0.3})  # fmt: skip


class DepthImbalanceExample(Strategy):
    """A placeholder that needs historical L2 depth. It exists to prove the engine refuses unsupported inputs: Binance
    does not publish historical L2 for this instrument, so starting it on history raises ``CapabilityUnavailable``."""

    id = "depth_example"
    hypothesis = "Requires price-level order-book history; not runnable on this dataset."
    required_capabilities = frozenset({Capability.HISTORICAL_TRADES, Capability.HISTORICAL_L2_DEPTH})
    triggers = frozenset()
    defaults = {"min_rr": ("1.0", "unused"), "expiry": ("5m", "unused")}

    def evidence_for(self, table: str, row: dict[str, Any], state: AnalyticsState) -> tuple[Evidence, ...] | None:
        return None

    def build(self, evidence: tuple[Evidence, ...], meta: dict[str, Any]) -> TradingSignal | None:
        return None


REGISTRY: dict[str, type[Strategy]] = {
    c.id: c for c in (FailedAuctionValueEdge, AbsorptionAtLevel, IBFailedBreak, DepthImbalanceExample)
}
