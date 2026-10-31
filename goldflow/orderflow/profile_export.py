"""Phase 6: chart-ready JSON for one window (``vprofile-export``).

The JSON holds the binned profile (price, lots, buy/sell), POC, value area, HVN/LVN nodes, VWAP with its bands, and
the developing series (one point per stored snapshot) so a chart can replay how the levels formed. Prices are given
both in integer ticks and in USDT, volumes in lots and in ounces.

Final windows read the stored profile (``levels.parquet``). For an ``as_of`` inside a window the profile is rebuilt
from the trades partitions over [covered_from, as_of), which equals the stored snapshot at that time.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.compute as pc

from .profile_rebuild import TradeSource, snapshot_from_trades
from .profile_settings import ProfileSettings
from .profile_store import profile_root, read_manifest, read_table
from .profile_verify import _rows


def export_window(cfg: Any, window_key: str, bin_ticks: int, as_of: int | None = None, out: Path | None = None) -> Path:
    ps = ProfileSettings.from_config(cfg)
    if bin_ticks not in ps.bin_ticks:
        raise ValueError(f"bin_ticks {bin_ticks} is not configured (volume_profile.bin_ticks = {list(ps.bin_ticks)})")
    man = read_manifest(cfg.data_root, cfg.symbol)
    tick, step = Decimal(cfg["instrument"]["tick_size"]), Decimal(cfg["instrument"]["step_size"])
    snaps: list[dict[str, Any]] = []
    nodes: list[dict[str, Any]] = []
    levels: list[dict[str, Any]] = []
    for d in sorted(man.get("dates", {})):

        def sel(t: Any) -> Any:
            return t.filter(pc.and_(pc.equal(t.column("window_key"), window_key), pc.equal(t.column("bin_ticks"), bin_ticks)))

        s = sel(read_table(cfg.data_root, cfg.symbol, "snapshots", d))
        if s.num_rows == 0:
            continue
        snaps += _rows(s)
        nodes += _rows(sel(read_table(cfg.data_root, cfg.symbol, "nodes", d)))
        levels += _rows(sel(read_table(cfg.data_root, cfg.symbol, "levels", d)))
    if not snaps:
        raise KeyError(f"no snapshots for {window_key} at bin_ticks={bin_ticks}")
    snaps.sort(key=lambda r: r["as_of"])
    if as_of is not None:
        snaps = [r for r in snaps if r["as_of"] <= as_of]
        if not snaps or snaps[-1]["as_of"] != as_of:
            raise KeyError(f"no snapshot of {window_key} exactly at as_of={as_of}; snapshots are every {ps.snapshot_ms} ms")
    last = snaps[-1]

    def usd(t: Any) -> float | None:
        return None if t is None else float(Decimal(t) * tick)

    def oz(q: int) -> float:
        return float(Decimal(q) * step)

    if last["is_final"] and as_of is None:
        prof = [
            {"price_ticks": r["price_ticks"], "lots": r["lots"], "buy_lots": r["buy_lots"], "sell_lots": r["sell_lots"]}
            for r in levels
        ]
        nd = [n for n in nodes if n["as_of"] == last["as_of"]]
    else:
        ref = snapshot_from_trades(cfg, TradeSource(cfg), last["covered_from"], last["as_of"], ps)[bin_ticks]
        p = ref["profile"]
        prof = [
            {"price_ticks": p.price(i), "lots": int(p.vol[i]), "buy_lots": int(p.buy[i]), "sell_lots": int(p.sell[i])}
            for i in range(p.n)
            if p.vol[i] > 0
        ]
        nd = ref["nodes"]
    for r in prof:
        r.update(price=usd(r["price_ticks"]), oz=oz(r["lots"]))
    bands = {k: {"up": usd(last[f"band_up_{k}_ticks"]), "dn": usd(last[f"band_dn_{k}_ticks"])} for k in ps.band_k}
    doc = {
        "window": {k: last[k] for k in ("window_key", "kind", "name", "label", "window_start", "window_end", "covered_from")},
        "as_of": last["as_of"],
        "available_at": last["available_at"],
        "is_final": last["is_final"],
        "bin_ticks": bin_ticks,
        "bin_usd": float(Decimal(bin_ticks) * tick),
        "coverage": last["coverage"],
        "dq_status": last["dq_status"],
        "units": {"price": "USDT (price_ticks * tick_size)", "oz": "lots * step_size", "bin": "price = lowest price in the bin"},
        "profile": prof,
        "poc": {"price": usd(last["poc_ticks"]), "lots": last["poc_lots"]},
        "value_area": {
            "val": usd(last["val_ticks"]),
            "vah": usd(last["vah_ticks"]),
            "lots": last["va_lots"],
            "target": last["va_target"],
            "method": ps.va_method,
        },
        "vwap": {
            "price": usd(last["vwap_ticks"]),
            "sigma_usd": None if last["sigma_ticks"] is None else last["sigma_ticks"] * float(tick),
            "bands": bands,
        },
        "nodes": [
            {
                "type": n["node_type"],
                "price": usd(n["price_ticks"]),
                "lots": n["lots"],
                "prominence_lots": n["prominence_lots"],
                "left": n["left_ticks"] * float(tick),
                "right": n["right_ticks"] * float(tick),
                "width_usd": n["width_ticks"] * float(tick),
            }
            for n in nd
        ],
        "developing": [
            {
                "as_of": r["as_of"],
                "poc": usd(r["poc_ticks"]),
                "val": usd(r["val_ticks"]),
                "vah": usd(r["vah_ticks"]),
                "vwap": usd(r["vwap_ticks"]),
                "total_lots": r["total_lots"],
                **{f"band_up_{k}": usd(r[f"band_up_{k}_ticks"]) for k in ps.band_k},
                **{f"band_dn_{k}": usd(r[f"band_dn_{k}_ticks"]) for k in ps.band_k},
            }
            for r in snaps
        ],
        "settings": {"profile_settings_hash": man.get("profile_settings_hash"), "spec": "docs/METRICS_PROFILE_V1.md"},
    }
    if out is None:
        safe = window_key.replace(":", "_")
        out = (
            profile_root(cfg.data_root, cfg.symbol) / "charts" / f"{safe}-b{bin_ticks}{'' if as_of is None else f'-{as_of}'}.json"
        )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=1))
    return out
