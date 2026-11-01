"""Phase 14: live individual trades (decision D3, option a: REST polling of ``/fapi/v1/trades``).

**Trade definition.** Live uses the same definition as the history: every individual trade, including RPI trades
(``isRPITrade``), exactly as in the data.binance.vision trades archive. RPI trades are counted (``rpi_trades``,
``rpi_lots``) so their share of volume is measured on live data. The aggTrade WebSocket, which excludes RPI trades
since 2025-12-31, is recorded for monitoring and cross-checks only, never used as the trade source.

**Normalisation.** A poll response is validated by the **same** Phase 3 trade validator (``normalize.validate_trades``):
tick and step multiples, quote notional, positive values, duplicates and sequence outliers. Accepted rows become a
``TradeEvent`` v1 table (``record_origin = LIVE_REST_TRADES``, ``receive_time_utc`` = local receive time). Rejected
rows are counted by reason and kept in the raw capture.

**Gaps.** XAUUSDT trade ids are increasing but **not contiguous** (Phase 3), so a skipped id is not proof of a missing
trade. A gap is *suspected* when a poll returns a full page (``limit`` trades) whose oldest new id is above the last id
already seen: more trades may have happened than one page holds. It is then backfilled with ``historicalTrades``
(API key, MARKET_DATA) from the last seen id; without a key, or if the backfill cannot reach the last seen id, the
interval is recorded as a gap and a ``GAP`` data-quality interval is opened over it, so dependent signals are blocked.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pyarrow as pa

from .. import schemas as S
from ..normalize import build_table, const_cols, null_ts, validate_trades

INF = 4_102_444_800_000  # 2100-01-01: "no bound" for the validator (must stay a printable time)


def trade_spec(cfg: Any) -> dict[str, Any]:
    from fractions import Fraction

    inst = cfg["instrument"]
    return {
        "onboard_ms": int(inst["onboard_ms"]),
        "inv_tick": int(1 / Fraction(inst["tick_size"])),
        "inv_step": int(1 / Fraction(inst["step_size"])),
    }


def normalize_rest_trades(rows: list[dict[str, Any]], recv_ms: int, cfg: Any) -> tuple[pa.Table, dict[str, int], dict[str, int]]:
    """REST trade objects -> (TradeEvent table of accepted rows, rejects by reason, RPI counters)."""
    n = len(rows)
    raw = {
        "id": np.array([int(r["id"]) for r in rows], np.int64),
        "price": np.array([float(r["price"]) for r in rows]),
        "qty": np.array([float(r["qty"]) for r in rows]),
        "quote_qty": np.array([float(r["quoteQty"]) for r in rows]),
        "time": np.array([int(r["time"]) for r in rows], np.int64),
        "is_buyer_maker": np.array([bool(r["isBuyerMaker"]) for r in rows], bool),
        "raw_row": np.arange(n, dtype=np.int64),
    }
    rej, der = validate_trades(raw, 0, INF, trade_spec(cfg), INF, None)
    ok = rej.ok
    reasons: dict[str, int] = {}
    for c in rej.code[~ok]:
        reasons[str(c)] = reasons.get(str(c), 0) + 1
    rpi = np.array([bool(r.get("isRPITrade", False)) for r in rows], bool)
    lots = der["quantity_lots"]
    counters = {
        "trades": int(ok.sum()),
        "rpi_trades": int((rpi & ok).sum()),
        "lots": int(lots[ok].sum()),
        "rpi_lots": int(lots[rpi & ok].sum()),
    }
    m = int(ok.sum())
    order = np.argsort(raw["id"][ok], kind="stable")
    bm = raw["is_buyer_maker"][ok][order]
    sym = cfg.symbol
    cols = {**const_cols(m, sym), "source_trade_id": raw["id"][ok][order], "aggregate_trade_id": pa.nulls(m, pa.int64()),
            "exchange_event_time_utc": raw["time"][ok][order], "transaction_time_utc": raw["time"][ok][order],
            "receive_time_utc": np.full(m, recv_ms, np.int64) if m else null_ts(0), "price": raw["price"][ok][order],
            "quantity": raw["qty"][ok][order], "quote_notional": raw["quote_qty"][ok][order], "buyer_maker_flag": bm,
            "aggressor_side": np.where(bm, "SELL", "BUY"), "side_evidence": np.full(m, "EXCHANGE_REPORTED"),
            "price_ticks": der["price_ticks"][ok][order], "quantity_lots": lots[ok][order], "record_origin": np.full(m, "LIVE_REST_TRADES"),
            "data_quality_status": np.full(m, "VALID"), "raw_file_id": np.zeros(m, np.int32), "raw_row": raw["raw_row"][ok][order]}  # fmt: skip
    return build_table(S.TRADE_SCHEMA, cols), reasons, counters


@dataclass
class GapTracker:
    """Which polled trades are new, and whether a poll may have missed trades (full page beyond the last seen id)."""

    limit: int
    last_id: int | None = None
    last_time: int | None = None
    id_skips: int = 0
    suspected_gaps: list[list[int]] = field(default_factory=list)

    def new_rows(self, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool]:
        rows = sorted(rows, key=lambda r: int(r["id"]))
        fresh = [r for r in rows if self.last_id is None or int(r["id"]) > self.last_id]
        gap = bool(self.last_id is not None and fresh and len(rows) >= self.limit and int(fresh[0]["id"]) > self.last_id + 1
                   and int(rows[0]["id"]) > self.last_id)  # fmt: skip
        if fresh and self.last_id is not None and int(fresh[0]["id"]) > self.last_id + 1 and not gap:
            self.id_skips += 1  # normal on this instrument: ids are not contiguous
        return fresh, gap

    def advance(self, rows: list[dict[str, Any]]) -> None:
        if rows:
            self.last_id = (
                max(int(r["id"]) for r in rows) if self.last_id is None else max(self.last_id, max(int(r["id"]) for r in rows))
            )
            self.last_time = max(int(r["time"]) for r in rows)

    def state(self) -> dict[str, Any]:
        return {
            "limit": self.limit,
            "last_id": self.last_id,
            "last_time": self.last_time,
            "id_skips": self.id_skips,
            "suspected_gaps": self.suspected_gaps,
        }

    @classmethod
    def load(cls, s: dict[str, Any]) -> "GapTracker":
        return cls(int(s["limit"]), s["last_id"], s["last_time"], int(s["id_skips"]), [list(x) for x in s["suspected_gaps"]])


def rest_payload(rows: list[dict[str, Any]]) -> str:
    return json.dumps(rows, separators=(",", ":"))
