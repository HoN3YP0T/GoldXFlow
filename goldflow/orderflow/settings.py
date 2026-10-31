"""Phase 7 orderflow settings: defaults in code, optionally overridden by an ``orderflow`` block in the config.

Every threshold is a research parameter, **not** a tuned or validated value; Phase 13 decides which are useful.
The settings hash is stored in every checkpoint, so a resume under changed settings is refused.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass
from fractions import Fraction
from typing import Any

DEFAULT_ORDERFLOW: dict[str, Any] = {
    "version": 1,
    "cvd": {"intervals": ["1m"], "resets": ["utc_day", "trading_day", "session"]},
    "imbalance": {
        "interval": "1m",
        "bin_ticks": 10,
        "mode": "diagonal",  # diagonal | horizontal
        "ratio": "3.0",
        "min_lots": 1000,
        "zero_policy": "none",  # none: an empty comparison bin gives no imbalance | imbalance: it counts as one
        "stack_n": 3,
    },
    "big_trades": {
        "cluster_ms": 0,  # 0 = fills with the same exchange timestamp
        "sweep_bin_ticks": 10,  # consecutive fills may step at most one bin of this size in the aggressor's direction
        "abs_lots": None,
        "notional_usdt": None,
        "quantile": {"enabled": True, "q": "0.995", "lookback_hours": 4, "min_samples": 2000, "update_ms": 60_000},
        "time_of_day": {"enabled": False, "q": "0.995", "bucket_minutes": 30, "days": 20, "min_days": 5, "keep_top": 5000},
        "combine": "any",  # any | all (over the enabled criteria)
        "store_trade_ids_max": 10_000,
    },
    "absorption": {
        "interval": "5s",
        "window_bars": 12,
        "bin_ticks": 10,
        "tol_bins": 1,
        "abs_lots": None,
        "quantile": {"enabled": True, "q": "0.99", "lookback_hours": 4, "min_samples": 1000, "update_ms": 60_000},
        "max_progress_bins": 1,
        "min_orders": 3,
        "max_single_order_frac": "0.5",
        "confirm_bins": 3,
        "confirm_window_ms": 300_000,
        "break_bins": 2,
    },
    "exhaustion": {
        "interval": "1m",
        "bin_ticks": 10,
        "lookback_bars": 30,
        "k_pushes": 3,
        "confirm_bins": 3,
        "confirm_window_bars": 10,
    },
    "divergence": {
        "interval": "5m",
        "bin_ticks": 10,
        "swing_left": 3,
        "swing_right": 3,
        "max_pivot_gap_bars": 48,
        "oscillators": ["cvd", "delta"],
        "cvd_reset": "trading_day",
        "inval_bins": 1,
    },
}


def _merge(base: dict[str, Any], over: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for k, v in over.items():
        if k.startswith("_"):
            continue
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out


def frac(x: Any, name: str, lo: Fraction | None = None, hi: Fraction | None = None) -> Fraction:
    try:
        f = Fraction(str(x))
    except (ValueError, ZeroDivisionError) as e:
        raise ValueError(f"{name}: {x!r} is not a decimal number") from e
    if lo is not None and f < lo:
        raise ValueError(f"{name}: {x} < {lo}")
    if hi is not None and f > hi:
        raise ValueError(f"{name}: {x} > {hi}")
    return f


def _pos_int(d: dict[str, Any], k: str, name: str, allow_zero: bool = False, allow_none: bool = False) -> None:
    v = d.get(k)
    if v is None and allow_none:
        return
    if not isinstance(v, int) or isinstance(v, bool) or v < (0 if allow_zero else 1):
        raise ValueError(f"{name}.{k} must be an integer >= {0 if allow_zero else 1}, got {v!r}")


@dataclass(frozen=True)
class OrderflowSettings:
    raw: dict[str, Any]
    hash: str

    @classmethod
    def from_config(cls, cfg: Any, intervals: tuple[str, ...] | None = None) -> "OrderflowSettings":
        """Validate. `intervals` = the Phase 5 processing intervals (each analytic must use one of them)."""
        raw = _merge(DEFAULT_ORDERFLOW, cfg.raw.get("orderflow") or {})
        if intervals is None:
            from ..processing.settings import DEFAULT_PROCESSING

            src: Any = (cfg.raw.get("processing") or {}).get("intervals") or DEFAULT_PROCESSING["intervals"]
            intervals = tuple(str(x) for x in src)
        ivs = set(intervals)
        c = raw["cvd"]
        for iv in c["intervals"]:
            if iv not in ivs:
                raise ValueError(f"orderflow.cvd.intervals: {iv} is not a processing interval {sorted(ivs)}")
        for r in c["resets"]:
            if r not in ("utc_day", "trading_day", "trading_week", "session"):
                raise ValueError(f"orderflow.cvd.resets: unknown reset {r!r}")
        im = raw["imbalance"]
        if im["interval"] not in ivs:
            raise ValueError(f"orderflow.imbalance.interval {im['interval']} is not a processing interval")
        if im["mode"] not in ("diagonal", "horizontal"):
            raise ValueError("orderflow.imbalance.mode must be diagonal or horizontal")
        if im["zero_policy"] not in ("none", "imbalance"):
            raise ValueError("orderflow.imbalance.zero_policy must be none or imbalance")
        frac(im["ratio"], "orderflow.imbalance.ratio", lo=Fraction(1))
        for k in ("bin_ticks", "min_lots", "stack_n"):
            _pos_int(im, k, "orderflow.imbalance")
        bt = raw["big_trades"]
        _pos_int(bt, "cluster_ms", "orderflow.big_trades", allow_zero=True)
        _pos_int(bt, "sweep_bin_ticks", "orderflow.big_trades")
        _pos_int(bt, "abs_lots", "orderflow.big_trades", allow_none=True)
        _pos_int(bt, "store_trade_ids_max", "orderflow.big_trades")
        if bt["notional_usdt"] is not None:
            frac(bt["notional_usdt"], "orderflow.big_trades.notional_usdt", lo=Fraction(0))
        if bt["combine"] not in ("any", "all"):
            raise ValueError("orderflow.big_trades.combine must be any or all")
        q = bt["quantile"]
        frac(q["q"], "orderflow.big_trades.quantile.q", Fraction(0), Fraction(1))
        for k in ("lookback_hours", "min_samples", "update_ms"):
            _pos_int(q, k, "orderflow.big_trades.quantile")
        td = bt["time_of_day"]
        frac(td["q"], "orderflow.big_trades.time_of_day.q", Fraction(0), Fraction(1))
        for k in ("bucket_minutes", "days", "min_days", "keep_top"):
            _pos_int(td, k, "orderflow.big_trades.time_of_day")
        if 1440 % td["bucket_minutes"]:
            raise ValueError("orderflow.big_trades.time_of_day.bucket_minutes must divide a day")
        if td["min_days"] > td["days"]:
            raise ValueError("orderflow.big_trades.time_of_day.min_days must be <= days")
        enabled = [bt["abs_lots"] is not None, bt["notional_usdt"] is not None, q["enabled"], td["enabled"]]
        if not any(enabled):
            raise ValueError("orderflow.big_trades: enable at least one threshold")
        ab = raw["absorption"]
        if ab["interval"] not in ivs:
            raise ValueError(f"orderflow.absorption.interval {ab['interval']} is not a processing interval")
        for k in ("window_bars", "bin_ticks", "min_orders", "confirm_bins", "confirm_window_ms", "break_bins"):
            _pos_int(ab, k, "orderflow.absorption")
        for k in ("tol_bins", "max_progress_bins"):
            _pos_int(ab, k, "orderflow.absorption", allow_zero=True)
        _pos_int(ab, "abs_lots", "orderflow.absorption", allow_none=True)
        frac(ab["max_single_order_frac"], "orderflow.absorption.max_single_order_frac", Fraction(0), Fraction(1))
        aq = ab["quantile"]
        frac(aq["q"], "orderflow.absorption.quantile.q", Fraction(0), Fraction(1))
        for k in ("lookback_hours", "min_samples", "update_ms"):
            _pos_int(aq, k, "orderflow.absorption.quantile")
        if ab["abs_lots"] is None and not aq["enabled"]:
            raise ValueError("orderflow.absorption: set abs_lots or enable the quantile threshold")
        ex = raw["exhaustion"]
        if ex["interval"] not in ivs:
            raise ValueError(f"orderflow.exhaustion.interval {ex['interval']} is not a processing interval")
        for k in ("bin_ticks", "lookback_bars", "confirm_bins", "confirm_window_bars"):
            _pos_int(ex, k, "orderflow.exhaustion")
        if not isinstance(ex["k_pushes"], int) or ex["k_pushes"] < 2:
            raise ValueError("orderflow.exhaustion.k_pushes must be an integer >= 2")
        dv = raw["divergence"]
        if dv["interval"] not in ivs:
            raise ValueError(f"orderflow.divergence.interval {dv['interval']} is not a processing interval")
        for k in ("bin_ticks", "swing_left", "swing_right", "max_pivot_gap_bars"):
            _pos_int(dv, k, "orderflow.divergence")
        _pos_int(dv, "inval_bins", "orderflow.divergence", allow_zero=True)
        if not dv["oscillators"] or any(o not in ("cvd", "delta") for o in dv["oscillators"]):
            raise ValueError("orderflow.divergence.oscillators must be a non-empty subset of [cvd, delta]")
        if dv["cvd_reset"] not in ("utc_day", "trading_day", "trading_week"):
            raise ValueError("orderflow.divergence.cvd_reset must be utc_day, trading_day or trading_week")
        h = hashlib.sha256(json.dumps(raw, sort_keys=True, default=str).encode()).hexdigest()
        return cls(raw, h)

    def __getitem__(self, k: str) -> Any:
        return self.raw[k]
