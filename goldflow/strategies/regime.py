"""Phase 11: regime features (documented, no look-ahead). Context for scoring and review, never a trigger.

Built from rows already available at the dispatch clock:
* ``realized_vol_bps``: population standard deviation of the last ``vol_bars`` 5-minute log returns (closes of the
  Phase 7 5m flow bars), in basis points. None until enough bars exist.
* ``volume_ratio``: the last 5m bar's volume divided by the mean volume of the same 5-minute time-of-day bucket on the
  previous ``volume_lookback_days`` days that had a bar there. None without history.
* ``auction_state``: the latest Phase 9 state of the trading day (balance, imbalance, transition), meaning trend
  (value migrating) versus rotation.
* ``session``: the configured sessions open at the clock (from the Phase 5 calendar); ``weekend`` from the regular
  schedule.
"""

from __future__ import annotations

import math
from typing import Any

DAY = 86_400_000


class RegimeTracker:
    def __init__(self, vol_bars: int = 12, lookback_days: int = 20) -> None:
        self.vol_bars = vol_bars
        self.lookback = lookback_days
        self.closes: list[int] = []
        self.vol_hist: dict[str, list[list[int]]] = {}  # bucket -> [[day, volume], ...]
        self.last: dict[str, Any] = {}

    def on_bar(self, row: dict[str, Any]) -> None:
        if row["interval"] != "5m":
            return
        self.closes.append(int(row["close_ticks"]))
        del self.closes[: -(self.vol_bars + 1)]
        s = int(row["bar_start"])
        b, day = str((s % DAY) // 300_000), s // DAY
        hist = [x for x in self.vol_hist.get(b, []) if x[0] < day][-self.lookback :]
        ratio = (int(row["volume_lots"]) / (sum(x[1] for x in hist) / len(hist))) if hist and sum(x[1] for x in hist) else None
        self.vol_hist[b] = hist + [[day, int(row["volume_lots"])]]
        rv = None
        if len(self.closes) == self.vol_bars + 1:
            r = [math.log(self.closes[i + 1] / self.closes[i]) for i in range(self.vol_bars)]
            m = sum(r) / len(r)
            rv = math.sqrt(sum((x - m) ** 2 for x in r) / len(r)) * 1e4
        self.last = {"realized_vol_bps": rv, "volume_ratio": ratio, "as_of_bar": s}

    def snapshot(self, auction_state: dict[str, Any] | None, sessions: list[str], weekend: bool) -> dict[str, Any]:
        st = auction_state or {}
        return {
            **self.last,
            "auction_state": st.get("state"),
            "auction_state_as_of": st.get("as_of"),
            "sessions": sessions,
            "weekend": weekend,
        }

    def tree(self) -> dict[str, Any]:
        return {"closes": self.closes, "vol_hist": dict(sorted(self.vol_hist.items())), "last": self.last}

    def load(self, t: dict[str, Any]) -> None:
        self.closes = [int(x) for x in t["closes"]]
        self.vol_hist = {k: [list(x) for x in v] for k, v in t["vol_hist"].items()}
        self.last = dict(t["last"])


def score(groups: dict[str, list[float]], weights: dict[str, float]) -> tuple[float, dict[str, Any]]:
    """Setup score in [0, 1]: within a group of correlated evidence (for example delta and CVD) only the strongest item
    counts, so the same information is never counted twice; groups are combined with documented weights. It is **not**
    a probability: it becomes one only after calibration on validation data (Phase 13)."""
    tot = sum(weights.values())
    parts = {g: max(min(max(v), 1.0), 0.0) if v else 0.0 for g, v in groups.items()}
    s = sum(weights.get(g, 0.0) * x for g, x in parts.items()) / tot if tot else 0.0
    return round(s, 6), {"groups": parts, "weights": weights, "calibrated": False}
