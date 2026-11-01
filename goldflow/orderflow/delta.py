"""Phase 7: delta per bar. ``delta = buy_lots - sell_lots`` from exchange-reported sides only.

A flow bar is the Phase 5 footprint bar of one interval plus:
* ``delta_high`` / ``delta_low``: extremes of the running delta inside the bar (base-interval resolution, the open of
  0 included, so delta_high >= 0 >= delta_low);
* ``unknown_lots`` and ``unknown_share = unknown / volume``: the side uncertainty, carried on every output.
``delta_pct`` (delta / volume) is reported as a float for display only; decisions never use it.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from .cvd import PathAgg

BAR_KEYS = ("bar_start", "bar_end", "available_at", "open_ticks", "high_ticks", "low_ticks", "close_ticks", "volume_lots",
            "buy_lots", "sell_lots", "unknown_lots", "delta_lots", "trade_count", "buy_count", "sell_count",
            "first_trade_id", "last_trade_id", "weekend", "dq_status", "coverage")  # fmt: skip


def flow_bars(interval: str, bars: dict[str, Any], path: PathAgg) -> list[dict[str, Any]]:
    """Rows for finalised Phase 5 bars of ``interval``, joined with their intra-bar delta path."""
    out = []
    cols = {k: np.asarray(bars[k]).tolist() for k in BAR_KEYS}
    for i in range(len(cols["bar_start"])):
        r = {k: cols[k][i] for k in BAR_KEYS}
        p = path.pop(int(r["bar_start"]))
        if p is None:
            raise RuntimeError(f"no delta path for {interval} bar {r['bar_start']}: base bars missing")
        if p[4] != r["delta_lots"]:
            raise RuntimeError(f"delta path close {p[4]} != bar delta {r['delta_lots']} at {r['bar_start']}")
        v = int(r["volume_lots"])
        r.update(
            interval=interval,
            delta_high=p[2],
            delta_low=p[3],
            unknown_share=(int(r["unknown_lots"]) / v) if v else 0.0,
            delta_pct=(int(r["delta_lots"]) / v) if v else 0.0,
        )
        out.append(r)
    return out
