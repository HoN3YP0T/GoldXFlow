"""Phase 7 settings: the ``orderflow`` config block (docs/METRICS_ORDERFLOW_V1.md), validated at startup.

Phase 1's ``settings.validate_config`` calls ``OrderflowSettings.from_config`` at ``Config.load``, so a bad value stops
the CLI with exit code 2. Every threshold that takes part in a decision is an exact rational (decimal string) or an
integer, never a float. The settings ``hash`` is stored in every engine checkpoint and in the derived dataset manifest,
so a resume under changed settings is refused and every event traces back to the parameters that produced it.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from fractions import Fraction
from typing import Any

from .settings import _frac

RESET_KINDS = ("utc_day", "trading_day", "trading_week", "session", "custom", "anchor")
DEFAULT_ORDERFLOW: dict[str, Any] = {
    "version": 1,
    "bar_intervals": ["1m", "5m"],
    "cvd": {"resets": ["utc_day", "trading_day", "session"], "anchors": []},
    "max_unknown_share": "0.05",
    "imbalance": {
        "interval": "1m",
        "bin_ticks": 10,
        "method": "diagonal",
        "ratio": "3",
        "min_lots": 2000,
        "zero_policy": "none",
        "stack_n": 3,
    },
    "big_trades": {
        "cluster_ms": 0,
        "max_step_ticks": 25,
        "combine": "any",
        "min_qty_lots": None,
        "min_notional_usdt": None,
        "rolling_quantile": {"enabled": True, "q": "0.995", "hours": 24, "min_samples": 2000},
        "time_of_day": {"enabled": False, "q": "0.995", "bucket": "30m", "lookback_days": 20, "min_samples": 200},
    },
    "absorption": {
        "enabled": True,
        "interval": "1m",
        "bin_ticks": 10,
        "tol_bins": 1,
        "min_zone_lots": None,
        "zone_quantile": "0.95",
        "quantile_hours": 24,
        "min_samples": 360,
        "max_progress_bins": 2,
        "min_active_seconds": 3,
        "max_second_share": "0.5",
        "confirm_bins": 3,
        "confirm_window": "5m",
    },
    "exhaustion": {
        "enabled": True,
        "interval": "1m",
        "bin_ticks": 10,
        "lookback_bars": 5,
        "pushes": 3,
        "max_span_bars": 30,
        "cvd_reset": "trading_day",
        "confirm_bins": 5,
        "inval_bins": 1,
        "confirm_window": "15m",
    },
    "divergence": {
        "enabled": True,
        "interval": "5m",
        "bin_ticks": 10,
        "swing_bars": 3,
        "measure": "cvd",
        "cvd_reset": "trading_day",
        "max_gap_bars": 48,
        "inval_bins": 3,
        "track_bars": 48,
    },
    "persist": True,
}
BLOCKS = ("cvd", "imbalance", "big_trades", "absorption", "exhaustion", "divergence")


@dataclass(frozen=True)
class ImbalanceParams:
    interval: str
    bin_ticks: int
    method: str  # diagonal | horizontal
    ratio: Fraction
    min_lots: int
    zero_policy: str  # none | any
    stack_n: int


@dataclass(frozen=True)
class QuantileParams:
    enabled: bool
    q: Fraction
    window_ms: int  # rolling: hours; time of day: bucket length
    lookback_days: int
    min_samples: int


@dataclass(frozen=True)
class BigTradeParams:
    cluster_ms: int
    max_step_ticks: int
    combine: str  # any | all
    min_qty_lots: int | None
    min_notional_tick_lots: int | None  # USDT x 1e5 (tick * lot)
    rolling: QuantileParams
    tod: QuantileParams


@dataclass(frozen=True)
class AbsorptionParams:
    enabled: bool
    interval: str
    bin_ticks: int
    tol_bins: int
    min_zone_lots: int | None
    zone_quantile: Fraction | None
    quantile_ms: int
    min_samples: int
    max_progress_bins: int
    min_active_seconds: int
    max_second_share: Fraction
    confirm_bins: int
    confirm_ms: int


@dataclass(frozen=True)
class ExhaustionParams:
    enabled: bool
    interval: str
    bin_ticks: int
    lookback_bars: int
    pushes: int
    max_span_bars: int
    cvd_reset: str
    confirm_bins: int
    inval_bins: int
    confirm_ms: int


@dataclass(frozen=True)
class DivergenceParams:
    enabled: bool
    interval: str
    bin_ticks: int
    swing_bars: int
    measure: str  # cvd | delta
    cvd_reset: str
    max_gap_bars: int
    inval_bins: int
    track_bars: int


def _int(v: Any, what: str, lo: int = 0) -> int:
    if isinstance(v, bool) or not isinstance(v, int):
        raise ValueError(f"{what}: expected an integer, got {v!r}")
    if v < lo:
        raise ValueError(f"{what} must be >= {lo}")
    return v


def _opt_int(v: Any, what: str, lo: int = 1) -> int | None:
    return None if v is None else _int(v, what, lo)


@dataclass(frozen=True)
class OrderflowSettings:
    version: int
    bar_intervals: tuple[str, ...]
    cvd_resets: tuple[str, ...]
    cvd_anchors: tuple[tuple[str, int], ...]
    max_unknown_share: Fraction
    imbalance: ImbalanceParams
    big: BigTradeParams
    absorption: AbsorptionParams
    exhaustion: ExhaustionParams
    divergence: DivergenceParams
    persist: bool
    raw: dict[str, Any]
    hash: str

    @property
    def all_intervals(self) -> tuple[str, ...]:
        """Every bar interval the engine builds flow bars for (outputs plus each detector's interval)."""
        xs = list(self.bar_intervals) + [self.imbalance.interval]
        for d in (self.absorption, self.exhaustion, self.divergence):
            if d.enabled:
                xs.append(d.interval)
        return tuple(dict.fromkeys(xs))

    @classmethod
    def from_config(cls, cfg: Any) -> "OrderflowSettings":
        from ..processing.settings import ProcessingSettings, parse_interval
        from ..timeutil import parse_iso, to_ms

        user = cfg.raw.get("orderflow") or {}
        raw: dict[str, Any] = {**DEFAULT_ORDERFLOW, **{k: v for k, v in user.items() if not k.startswith("_")}}
        for blk in BLOCKS:
            merged = {**DEFAULT_ORDERFLOW[blk], **(user.get(blk) or {})}
            raw[blk] = {k: v for k, v in merged.items() if not k.startswith("_")}
        bt = raw["big_trades"]
        for sub in ("rolling_quantile", "time_of_day"):
            merged = {**DEFAULT_ORDERFLOW["big_trades"][sub], **((user.get("big_trades") or {}).get(sub) or {})}
            bt[sub] = {k: v for k, v in merged.items() if not k.startswith("_")}
        unknown = sorted(set(raw) - set(DEFAULT_ORDERFLOW))
        if unknown:
            raise ValueError(f"orderflow: unknown keys {unknown}")

        proc = ProcessingSettings.from_config(cfg)
        if proc.footprint_bin_ticks != 1:
            raise ValueError(
                "orderflow needs processing.footprint_bin_ticks = 1 (exact 1-tick levels; detectors rebin themselves)"
            )

        def iv(name: Any, what: str) -> str:
            n = str(name)
            if n not in proc.interval_ms:
                raise ValueError(f"{what} {n!r} is not in processing.intervals {list(proc.intervals)}")
            return n

        def dur(v: Any, what: str) -> int:
            ms = parse_interval(str(v))
            if ms % proc.base_ms:
                raise ValueError(f"{what} must be a multiple of the base interval {proc.intervals[0]}")
            return ms

        bars = tuple(iv(x, "orderflow.bar_intervals entry") for x in raw["bar_intervals"])
        if not bars or len(set(bars)) != len(bars):
            raise ValueError("orderflow.bar_intervals must be a non-empty list of distinct intervals")

        cv = raw["cvd"]
        resets = tuple(str(x) for x in cv["resets"])
        bad = [r for r in resets if r not in RESET_KINDS]
        if bad or not resets or len(set(resets)) != len(resets):
            raise ValueError(f"orderflow.cvd.resets: unknown or duplicate kinds {bad}; allowed {list(RESET_KINDS)}")
        anchors = []
        for a in cv["anchors"]:
            name, start = str(a["name"]), to_ms(parse_iso(str(a["start"])))
            if start % proc.base_ms:
                raise ValueError(f"cvd anchor {name}: start must align to the base interval {proc.intervals[0]}")
            anchors.append((name, start))
        if len({a[0] for a in anchors}) != len(anchors):
            raise ValueError("orderflow.cvd.anchors: duplicate names")
        if anchors and "anchor" not in resets:
            raise ValueError("orderflow.cvd.anchors are set but 'anchor' is not in orderflow.cvd.resets")

        mus = _frac(raw["max_unknown_share"], "orderflow.max_unknown_share")
        if not (0 <= mus <= 1):
            raise ValueError("orderflow.max_unknown_share must be in [0, 1]")

        im = raw["imbalance"]
        imp = ImbalanceParams(
            iv(im["interval"], "orderflow.imbalance.interval"),
            _int(im["bin_ticks"], "imbalance.bin_ticks", 1),
            str(im["method"]),
            _frac(im["ratio"], "imbalance.ratio"),
            _int(im["min_lots"], "imbalance.min_lots", 1),
            str(im["zero_policy"]),
            _int(im["stack_n"], "imbalance.stack_n", 2),
        )
        if imp.method not in ("diagonal", "horizontal"):
            raise ValueError("imbalance.method must be 'diagonal' or 'horizontal'")
        if imp.ratio < 1:
            raise ValueError("imbalance.ratio must be >= 1")
        if imp.zero_policy not in ("none", "any"):
            raise ValueError("imbalance.zero_policy must be 'none' (no imbalance against an empty bin) or 'any'")

        def qp(d: dict[str, Any], what: str, rolling: bool) -> QuantileParams:
            q = _frac(d["q"], f"{what}.q")
            if not (0 < q < 1):
                raise ValueError(f"{what}.q must be in (0, 1)")
            if rolling:
                w = _int(d["hours"], f"{what}.hours", 1) * 3_600_000
                lb = 0
            else:
                w = dur(d["bucket"], f"{what}.bucket")
                if 86_400_000 % w or w % 60_000:
                    raise ValueError(f"{what}.bucket must be whole minutes dividing one day")
                lb = _int(d["lookback_days"], f"{what}.lookback_days", 1)
            return QuantileParams(bool(d["enabled"]), q, w, lb, _int(d["min_samples"], f"{what}.min_samples", 1))

        bp = BigTradeParams(
            _int(bt["cluster_ms"], "big_trades.cluster_ms", 0),
            _int(bt["max_step_ticks"], "big_trades.max_step_ticks", 0),
            str(bt["combine"]),
            _opt_int(bt["min_qty_lots"], "big_trades.min_qty_lots"),
            None
            if bt["min_notional_usdt"] is None
            else int(_frac(bt["min_notional_usdt"], "big_trades.min_notional_usdt") * 100_000),
            qp(bt["rolling_quantile"], "big_trades.rolling_quantile", True),
            qp(bt["time_of_day"], "big_trades.time_of_day", False),
        )
        if bp.combine not in ("any", "all"):
            raise ValueError("big_trades.combine must be 'any' or 'all'")
        if bp.min_notional_tick_lots is not None and bp.min_notional_tick_lots <= 0:
            raise ValueError("big_trades.min_notional_usdt must be > 0")
        if not (bp.min_qty_lots or bp.min_notional_tick_lots or bp.rolling.enabled or bp.tod.enabled):
            raise ValueError("big_trades: enable at least one threshold (min_qty_lots, min_notional_usdt, a quantile)")

        ab = raw["absorption"]
        zq = None if ab["zone_quantile"] is None else _frac(ab["zone_quantile"], "absorption.zone_quantile")
        ap = AbsorptionParams(
            bool(ab["enabled"]),
            iv(ab["interval"], "orderflow.absorption.interval"),
            _int(ab["bin_ticks"], "absorption.bin_ticks", 1),
            _int(ab["tol_bins"], "absorption.tol_bins", 0),
            _opt_int(ab["min_zone_lots"], "absorption.min_zone_lots"),
            zq,
            _int(ab["quantile_hours"], "absorption.quantile_hours", 1) * 3_600_000,
            _int(ab["min_samples"], "absorption.min_samples", 1),
            _int(ab["max_progress_bins"], "absorption.max_progress_bins", 0),
            _int(ab["min_active_seconds"], "absorption.min_active_seconds", 2),
            _frac(ab["max_second_share"], "absorption.max_second_share"),
            _int(ab["confirm_bins"], "absorption.confirm_bins", 1),
            dur(ab["confirm_window"], "absorption.confirm_window"),
        )
        if ap.enabled and ap.min_zone_lots is None and ap.zone_quantile is None:
            raise ValueError("absorption: set min_zone_lots and/or zone_quantile")
        if zq is not None and not (0 < zq < 1):
            raise ValueError("absorption.zone_quantile must be in (0, 1)")
        if not (0 < ap.max_second_share < 1):
            raise ValueError("absorption.max_second_share must be in (0, 1): a single burst must never be enough")

        ex = raw["exhaustion"]
        ep = ExhaustionParams(
            bool(ex["enabled"]),
            iv(ex["interval"], "orderflow.exhaustion.interval"),
            _int(ex["bin_ticks"], "exhaustion.bin_ticks", 1),
            _int(ex["lookback_bars"], "exhaustion.lookback_bars", 1),
            _int(ex["pushes"], "exhaustion.pushes", 2),
            _int(ex["max_span_bars"], "exhaustion.max_span_bars", 2),
            str(ex["cvd_reset"]),
            _int(ex["confirm_bins"], "exhaustion.confirm_bins", 1),
            _int(ex["inval_bins"], "exhaustion.inval_bins", 1),
            dur(ex["confirm_window"], "exhaustion.confirm_window"),
        )
        dv = raw["divergence"]
        dp = DivergenceParams(
            bool(dv["enabled"]),
            iv(dv["interval"], "orderflow.divergence.interval"),
            _int(dv["bin_ticks"], "divergence.bin_ticks", 1),
            _int(dv["swing_bars"], "divergence.swing_bars", 1),
            str(dv["measure"]),
            str(dv["cvd_reset"]),
            _int(dv["max_gap_bars"], "divergence.max_gap_bars", 2),
            _int(dv["inval_bins"], "divergence.inval_bins", 1),
            _int(dv["track_bars"], "divergence.track_bars", 1),
        )
        if dp.measure not in ("cvd", "delta"):
            raise ValueError("divergence.measure must be 'cvd' or 'delta'")
        for what, r in (("exhaustion.cvd_reset", ep.cvd_reset), ("divergence.cvd_reset", dp.cvd_reset)):
            if r not in resets or r in ("session", "custom", "anchor"):
                raise ValueError(
                    f"{what} {r!r} must be one of orderflow.cvd.resets and a continuous series "
                    "(utc_day, trading_day or trading_week)"
                )
        body = {"orderflow": raw, "processing_hash": proc.hash}
        h = hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()
        return cls(
            _int(raw["version"], "orderflow.version", 1),
            bars,
            resets,
            tuple(anchors),
            mus,
            imp,
            bp,
            ap,
            ep,
            dp,
            bool(raw["persist"]),
            raw,
            h,
        )
