"""Phase 9 settings: the ``auction`` config block (docs/METRICS_AUCTION_V1.md), validated at startup.

Phase 1's ``settings.validate_config`` calls ``AuctionSettings.from_config`` at ``Config.load``. Every threshold that
takes part in a decision is an integer or an exact rational (decimal string). The ``hash`` covers this block plus the
processing and profile settings it depends on, and guards checkpoints and the derived dataset.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from fractions import Fraction
from typing import Any

from .settings import _frac

LEVEL_TYPES = ("HIGH", "LOW", "POC", "VAH", "VAL", "VWAP", "VWAP_BAND", "HVN", "LVN")
DEFAULT_AUCTION: dict[str, Any] = {
    "version": 1,
    "interval": "1m",
    "bin_ticks": 10,
    "levels": {
        "sources": ["trading_day", "session", "ib"],
        "types": ["HIGH", "LOW", "POC", "VAH", "VAL", "VWAP"],
        "band_k": [1, 2],
        "user": [],
    },
    "acceptance": {"accept_time": "15m", "accept_vol_frac": "0.10"},
    "rejection": {"probe_bins": 3, "reject_time": "30m"},
    "failed_auction": {"fa_vol_frac": "0.25", "fa_return_time": "30m", "fa_hold_bars": 5, "outcome_minutes": [15, 30, 60]},
    "state": {"series": ["trading_day"], "every": "15m", "rot_n": 2, "ov_frac": "0.5", "poc_shift_bins": 5},
    "profile_shape": {
        "windows": ["trading_day", "session"],
        "poor_frac": "0.9",
        "excess_bins": 3,
        "tail_frac": "0.25",
        "excess_time": "5m",
        "single_print_frac": "0.1",
        "single_print_min_bins": 2,
    },
    "persist": True,
}
BLOCKS = ("levels", "acceptance", "rejection", "failed_auction", "state", "profile_shape")


@dataclass(frozen=True)
class AuctionSettings:
    version: int
    interval: str
    interval_ms: int
    bin_ticks: int
    sources: tuple[str, ...]
    level_types: tuple[str, ...]
    band_k: tuple[int, ...]
    user_levels: tuple[tuple[str, Fraction], ...]
    accept_ms: int
    accept_vol_frac: Fraction
    probe_bins: int
    reject_ms: int
    fa_vol_frac: Fraction
    fa_return_ms: int
    fa_hold_bars: int
    outcome_minutes: tuple[int, ...]
    state_series: tuple[str, ...]
    state_every_ms: int
    rot_n: int
    ov_frac: Fraction
    poc_shift_bins: int
    shape_windows: tuple[str, ...]
    poor_frac: Fraction
    excess_bins: int
    tail_frac: Fraction
    excess_ms: int
    sp_frac: Fraction
    sp_min_bins: int
    persist: bool
    raw: dict[str, Any]
    hash: str

    @classmethod
    def from_config(cls, cfg: Any) -> "AuctionSettings":
        from ..processing.settings import ProcessingSettings, parse_interval
        from .settings import ProfileSettings

        user = cfg.raw.get("auction") or {}
        raw: dict[str, Any] = {**DEFAULT_AUCTION, **{k: v for k, v in user.items() if not k.startswith("_")}}
        for blk in BLOCKS:
            merged = {**DEFAULT_AUCTION[blk], **(user.get(blk) or {})}
            raw[blk] = {k: v for k, v in merged.items() if not k.startswith("_")}
        unknown = sorted(set(raw) - set(DEFAULT_AUCTION))
        if unknown:
            raise ValueError(f"auction: unknown keys {unknown}")
        proc = ProcessingSettings.from_config(cfg)
        prof = ProfileSettings.from_config(cfg)

        def dur(v: Any, what: str) -> int:
            ms = parse_interval(str(v))
            if ms % proc.base_ms:
                raise ValueError(f"{what} must be a multiple of the base interval {proc.intervals[0]}")
            return ms

        def pint(v: Any, what: str, lo: int) -> int:
            if isinstance(v, bool) or not isinstance(v, int) or v < lo:
                raise ValueError(f"{what} must be an integer >= {lo}")
            return v

        iv = str(raw["interval"])
        if iv not in proc.interval_ms:
            raise ValueError(f"auction.interval {iv!r} is not in processing.intervals {list(proc.intervals)}")
        I = proc.interval_ms[iv]
        bt = pint(raw["bin_ticks"], "auction.bin_ticks", 1)
        if bt not in prof.bin_ticks:
            raise ValueError(f"auction.bin_ticks {bt} must be one of volume_profile.bin_ticks {list(prof.bin_ticks)}")
        lv = raw["levels"]
        sources = tuple(str(x) for x in lv["sources"])
        bad = [
            s for s in sources if (s.split(":")[0] not in prof.windows and s.split(":")[0] != "ib") or s.split(":")[0] == "anchor"
        ]
        if bad:
            raise ValueError(f"auction.levels.sources {bad}: each must be a volume_profile window kind (not anchor) or ib[:name]")
        ib_names = [s.split(":", 1)[1] for s in sources if s.startswith("ib:")]
        if any(s.startswith("ib") for s in sources):
            from .initial_balance import IBSettings

            known = {d.name for d in IBSettings.from_config(cfg).defs}
            if any(n not in known for n in ib_names):
                raise ValueError(f"auction.levels.sources: unknown IB definitions {[n for n in ib_names if n not in known]}")
        types = tuple(str(x) for x in lv["types"])
        if any(t not in LEVEL_TYPES for t in types) or len(set(types)) != len(types):
            raise ValueError(f"auction.levels.types must be distinct values of {list(LEVEL_TYPES)}")
        band_k = tuple(int(k) for k in lv["band_k"])
        if "VWAP_BAND" in types and any(k not in prof.band_k for k in band_k):
            raise ValueError(f"auction.levels.band_k {list(band_k)} must be in volume_profile.vwap.band_k {list(prof.band_k)}")
        users = []
        for u in lv["user"]:
            px = _frac(u["price"], f"auction.levels.user {u.get('name')}.price")  # USDT -> ticks, exact
            users.append((str(u["name"]), px / Fraction(proc.tick_size)))
        if len({u[0] for u in users}) != len(users):
            raise ValueError("auction.levels.user: duplicate names")
        ac, rj, fa, stb, ps = raw["acceptance"], raw["rejection"], raw["failed_auction"], raw["state"], raw["profile_shape"]
        accept_ms = dur(ac["accept_time"], "acceptance.accept_time")
        if accept_ms % I:
            raise ValueError("acceptance.accept_time must be a multiple of auction.interval (it counts bar closes)")
        st_series = tuple(str(x) for x in stb["series"])
        for s in st_series:
            if s.startswith("ib") or (s.split(":")[0] not in sources and s not in sources):
                raise ValueError(f"auction.state.series {s!r} must also be a level source")
            if not {"VAH", "VAL", "POC"} <= set(types):
                raise ValueError("auction.state needs VAH, VAL and POC in auction.levels.types")
        every = dur(stb["every"], "state.every")
        if every % prof.snapshot_ms:
            raise ValueError("auction.state.every must be a multiple of volume_profile.snapshot_every")
        outs = tuple(int(m) for m in fa["outcome_minutes"])
        if not outs or any(m < 1 for m in outs):
            raise ValueError("failed_auction.outcome_minutes must be positive minutes")
        shape_w = tuple(str(x) for x in ps["windows"])
        st = cls(
            pint(raw["version"], "auction.version", 1),
            iv,
            I,
            bt,
            sources,
            types,
            band_k,
            tuple(users),
            accept_ms,
            _frac(ac["accept_vol_frac"], "acceptance.accept_vol_frac"),
            pint(rj["probe_bins"], "rejection.probe_bins", 1),
            dur(rj["reject_time"], "rejection.reject_time"),
            _frac(fa["fa_vol_frac"], "failed_auction.fa_vol_frac"),
            dur(fa["fa_return_time"], "failed_auction.fa_return_time"),
            pint(fa["fa_hold_bars"], "failed_auction.fa_hold_bars", 1),
            outs,
            st_series,
            every,
            pint(stb["rot_n"], "state.rot_n", 1),
            _frac(stb["ov_frac"], "state.ov_frac"),
            pint(stb["poc_shift_bins"], "state.poc_shift_bins", 1),
            shape_w,
            _frac(ps["poor_frac"], "profile_shape.poor_frac"),
            pint(ps["excess_bins"], "profile_shape.excess_bins", 1),
            _frac(ps["tail_frac"], "profile_shape.tail_frac"),
            dur(ps["excess_time"], "profile_shape.excess_time"),
            _frac(ps["single_print_frac"], "profile_shape.single_print_frac"),
            pint(ps["single_print_min_bins"], "profile_shape.single_print_min_bins", 1),
            bool(raw["persist"]),
            raw,
            "",
        )
        for what, x in (("accept_vol_frac", st.accept_vol_frac), ("fa_vol_frac", st.fa_vol_frac), ("ov_frac", st.ov_frac),
                        ("poor_frac", st.poor_frac), ("tail_frac", st.tail_frac), ("single_print_frac", st.sp_frac)):  # fmt: skip
            if not (0 < x <= 1):
                raise ValueError(f"auction {what} must be in (0, 1]")
        if any(w.split(":")[0] not in ("utc_day", "trading_day", "trading_week", "session", "custom") for w in shape_w):
            raise ValueError("auction.profile_shape.windows must be Phase 5 window kinds")
        ib_hash = None
        if any(x.startswith("ib") for x in sources):
            from .initial_balance import IBSettings

            ib_hash = IBSettings.from_config(cfg).hash
        body = {"auction": raw, "processing_hash": proc.hash, "profile_hash": prof.hash, "ib_hash": ib_hash}
        h = hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()
        return cls(**{**st.__dict__, "hash": h})
