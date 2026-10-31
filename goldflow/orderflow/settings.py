"""Phase 6 settings: the ``volume_profile`` config block (docs/METRICS_PROFILE_V1.md), validated at startup.

Phase 1's ``settings.validate_config`` calls ``ProfileSettings.from_config`` at ``Config.load``, so a bad value stops
the CLI with exit code 2. The settings ``hash`` is stored in every engine checkpoint and every persisted snapshot, so
a resume under changed settings is refused and every result can be traced to the parameters that produced it.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from fractions import Fraction
from typing import Any

from ..timeutil import parse_iso, to_ms

WINDOW_KINDS = ("utc_day", "trading_day", "trading_week", "session", "custom", "anchor")
DEFAULT_PROFILE: dict[str, Any] = {
    "version": 1,
    "bin_ticks": [10],
    "value_area": {"pct": "0.70", "method": "pair"},
    "nodes": {
        "smooth_bins": 5,
        "truncate_sigmas": 4,
        "kernel_scale_bits": 20,
        "prom_frac": "0.10",
        "hvn_min_vol_frac": "1.0",
        "lvn_max_vol_frac": "0.5",
        "min_separation_bins": 10,
        "width_rel_height": "0.5",
    },
    "vwap": {"band_k": [1, 2, 3], "variance": "population"},
    "snapshot_every": "1m",
    "windows": list(WINDOW_KINDS),
    "anchors": [],
    "persist": True,
}


def _frac(v: Any, what: str) -> Fraction:
    """Exact rational from a decimal string (or int). Floats are refused: 0.7 is not exactly representable."""
    if isinstance(v, bool) or isinstance(v, float):
        raise ValueError(f'{what}: give a decimal string such as "0.70" (floats are not exact)')
    try:
        return Fraction(str(v))
    except (ValueError, ZeroDivisionError):
        raise ValueError(f"{what}: {v!r} is not a number") from None


@dataclass(frozen=True)
class NodeParams:
    smooth_bins: int
    truncate_sigmas: int
    kernel_scale_bits: int
    prom_frac: Fraction
    hvn_min_vol_frac: Fraction
    lvn_max_vol_frac: Fraction
    min_separation_bins: int
    width_rel_height: Fraction


@dataclass(frozen=True)
class ProfileSettings:
    version: int
    bin_ticks: tuple[int, ...]
    va_pct: Fraction
    va_method: str  # "pair" (default, CBOT-style) | "single"
    nodes: NodeParams
    band_k: tuple[int, ...]
    snapshot_ms: int
    windows: tuple[str, ...]
    anchors: tuple[tuple[str, int], ...]  # (name, start_ms)
    persist: bool
    raw: dict[str, Any]
    hash: str

    @classmethod
    def from_config(cls, cfg: Any) -> "ProfileSettings":
        from ..processing.settings import ProcessingSettings, parse_interval

        user = cfg.raw.get("volume_profile") or {}
        raw: dict[str, Any] = {**DEFAULT_PROFILE, **{k: v for k, v in user.items() if not k.startswith("_")}}
        raw["value_area"] = {**DEFAULT_PROFILE["value_area"], **(user.get("value_area") or {})}
        raw["nodes"] = {**DEFAULT_PROFILE["nodes"], **(user.get("nodes") or {})}
        raw["vwap"] = {**DEFAULT_PROFILE["vwap"], **(user.get("vwap") or {})}
        for blk in ("value_area", "nodes", "vwap"):
            raw[blk] = {k: v for k, v in raw[blk].items() if not k.startswith("_")}

        bins = [int(b) for b in raw["bin_ticks"]]
        if not bins or any(b < 1 for b in bins) or len(set(bins)) != len(bins):
            raise ValueError("volume_profile.bin_ticks must be a non-empty list of distinct integers >= 1")
        va = raw["value_area"]
        pct = _frac(va["pct"], "volume_profile.value_area.pct")
        if not (0 < pct <= 1):
            raise ValueError("volume_profile.value_area.pct must be in (0, 1]")
        if va["method"] not in ("pair", "single"):
            raise ValueError("volume_profile.value_area.method must be 'pair' or 'single'")
        nd = raw["nodes"]
        npar = NodeParams(
            int(nd["smooth_bins"]),
            int(nd["truncate_sigmas"]),
            int(nd["kernel_scale_bits"]),
            _frac(nd["prom_frac"], "nodes.prom_frac"),
            _frac(nd["hvn_min_vol_frac"], "nodes.hvn_min_vol_frac"),
            _frac(nd["lvn_max_vol_frac"], "nodes.lvn_max_vol_frac"),
            int(nd["min_separation_bins"]),
            _frac(nd["width_rel_height"], "nodes.width_rel_height"),
        )
        if npar.smooth_bins < 1 or npar.truncate_sigmas < 1:
            raise ValueError("nodes.smooth_bins and nodes.truncate_sigmas must be >= 1")
        if not (8 <= npar.kernel_scale_bits <= 24):
            raise ValueError("nodes.kernel_scale_bits must be in [8, 24]")
        if not (0 <= npar.prom_frac <= 1) or npar.hvn_min_vol_frac < 0 or npar.lvn_max_vol_frac < 0:
            raise ValueError("nodes fractions must be >= 0 (prom_frac <= 1)")
        if npar.min_separation_bins < 1:
            raise ValueError("nodes.min_separation_bins must be >= 1")
        if not (0 < npar.width_rel_height <= 1):
            raise ValueError("nodes.width_rel_height must be in (0, 1]")
        vw = raw["vwap"]
        if vw["variance"] != "population":
            raise ValueError("volume_profile.vwap.variance: only 'population' (volume-weighted) is implemented")
        ks = tuple(int(k) for k in vw["band_k"])
        if any(k < 1 for k in ks) or len(set(ks)) != len(ks):
            raise ValueError("volume_profile.vwap.band_k must be distinct integers >= 1")
        windows = tuple(raw["windows"])
        bad = [w for w in windows if w not in WINDOW_KINDS]
        if bad or not windows:
            raise ValueError(f"volume_profile.windows: unknown kinds {bad}; allowed {list(WINDOW_KINDS)}")

        proc = ProcessingSettings.from_config(cfg)
        snap = parse_interval(str(raw["snapshot_every"]))
        if snap % proc.base_ms:
            raise ValueError(f"volume_profile.snapshot_every must be a multiple of the base interval {proc.intervals[0]}")
        if proc.footprint_bin_ticks != 1:
            raise ValueError(
                "volume_profile needs processing.footprint_bin_ticks = 1 (exact 1-tick levels; profiles rebin themselves)"
            )
        anchors = []
        for a in raw["anchors"]:
            name, start = str(a["name"]), to_ms(parse_iso(str(a["start"])))
            if start % proc.base_ms:
                raise ValueError(f"anchor {name}: start must align to the base interval {proc.intervals[0]}")
            anchors.append((name, start))
        if len({a[0] for a in anchors}) != len(anchors):
            raise ValueError("volume_profile.anchors: duplicate names")
        body = {"volume_profile": raw, "processing_hash": proc.hash}
        h = hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()
        return cls(
            int(raw["version"]),
            tuple(bins),
            pct,
            str(va["method"]),
            npar,
            ks,
            snap,
            windows,
            tuple(anchors),
            bool(raw["persist"]),
            raw,
            h,
        )
