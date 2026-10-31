"""Phase 5: aggressor-side classification.

Exchange-reported side (the only side that enters buy/sell volume):
  buyer_maker_flag = false -> BUY  (the buyer took liquidity)
  buyer_maker_flag = true  -> SELL (the buyer was the maker, so the seller hit the bid; Binance semantics)
  buyer_maker_flag = null  -> UNKNOWN
  side_evidence            = EXCHANGE_REPORTED, or UNKNOWN when the flag is absent

An inferred side (the tick rule) is optional and off by default. When it is enabled it only splits the volume of
UNKNOWN-side trades into separately labelled INFERRED columns. It is never added to exchange-reported buy/sell.
"""
from __future__ import annotations
from typing import Any
import numpy as np
from numpy.typing import NDArray
import pyarrow as pa
import pyarrow.compute as pc

BUY, SELL, UNKNOWN = np.int8(1), np.int8(-1), np.int8(0)
SIDE_NAMES = {1: "BUY", -1: "SELL", 0: "UNKNOWN"}
EVIDENCE_EXCHANGE, EVIDENCE_UNKNOWN = "EXCHANGE_REPORTED", "UNKNOWN"


def classify_buyer_maker(flag: pa.ChunkedArray | pa.Array) -> NDArray[Any]:
    """int8 side codes (1 BUY, -1 SELL, 0 UNKNOWN) from Binance ``is_buyer_maker``."""
    valid = pc.is_valid(flag).to_numpy(zero_copy_only=False)
    bm = pc.fill_null(flag, False).to_numpy(zero_copy_only=False)
    side = np.where(bm, SELL, BUY).astype(np.int8)
    side[~valid] = UNKNOWN
    return side


def side_codes_from_labels(labels: pa.ChunkedArray | pa.Array) -> NDArray[Any]:
    """int8 codes from a canonical ``aggressor_side`` column (dictionary or string). Used as a cross-check of the
    Phase 3 derived column against our own classification of the reported flag."""
    s = labels.cast(pa.string())
    buy = pc.fill_null(pc.equal(s, "BUY"), False).to_numpy(zero_copy_only=False)
    sell = pc.fill_null(pc.equal(s, "SELL"), False).to_numpy(zero_copy_only=False)
    return np.where(buy, BUY, np.where(sell, SELL, UNKNOWN)).astype(np.int8)


def tick_rule(price_ticks: NDArray[Any], prev_price: int | None, prev_dir: int) -> tuple[NDArray[Any], int | None, int]:
    """Classic tick rule over the trade sequence: uptick -> +1, downtick -> -1, zero tick -> previous direction.
    Returns (directions int8, last price, last direction). Direction 0 means "no prior price change yet".
    The state (last price, last direction) is carried across chunks so the result is chunk-independent."""
    n = len(price_ticks)
    if n == 0: return np.empty(0, np.int8), prev_price, prev_dir
    p = price_ticks.astype(np.int64)
    prev = np.empty(n, np.int64); prev[1:] = p[:-1]; prev[0] = p[0] if prev_price is None else prev_price
    d = np.sign(p - prev).astype(np.int8)
    # forward-fill zero ticks with the last non-zero direction (seeded by prev_dir)
    nz = d != 0
    idx = np.where(nz, np.arange(n), -1)
    np.maximum.accumulate(idx, out=idx)
    out = np.where(idx >= 0, d[np.maximum(idx, 0)], np.int8(prev_dir)).astype(np.int8)
    return out, int(p[-1]), int(out[-1])
