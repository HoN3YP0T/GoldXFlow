"""Bitget ``XAUUSDT`` USDT-M perpetual (Phase 15). From the build host (2026-10-31) the REST instrument endpoint is
reachable, but the historical trade archive host (img.bitget.com) is not (proxy CONNECT 502). Historical trades are
therefore declared NOT_IMPLEMENTED and refused, instead of being approximated by the recent-only REST fills."""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import pyarrow as pa

from ..archive_venue import ArchiveVenueProvider
from ..base import Availability as A
from ..base import Capability, CapabilityInfo, CapabilitySet, InstrumentSpec
from ..registry import register


@register("bitget")
class BitgetUsdtFuturesProvider(ArchiveVenueProvider):
    provider_id = "bitget"
    venue_id = "bitget-usdt-futures"
    DISCOVERY_URL = "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES&symbol={symbol}"
    HEALTH_URL = "https://api.bitget.com/api/v2/public/time"

    def capabilities(self) -> CapabilitySet:
        return CapabilitySet({
            Capability.INSTRUMENT_DISCOVERY: CapabilityInfo(A.AVAILABLE, "REST /api/v2/mix/market/contracts"),
            Capability.HISTORICAL_TRADES: CapabilityInfo(A.NOT_IMPLEMENTED, "archive host img.bitget.com unreachable from the build host; REST fills cover recent days only"),
            Capability.HISTORICAL_AGGTRADES: CapabilityInfo(A.UNAVAILABLE, "not published"),
            Capability.HISTORICAL_KLINES: CapabilityInfo(A.NOT_IMPLEMENTED, "REST candles; not built"),
            Capability.HISTORICAL_L2_DEPTH: CapabilityInfo(A.UNAVAILABLE, "not published"),
            Capability.REALTIME_TRADES: CapabilityInfo(A.NOT_IMPLEMENTED, "WebSocket trade channel; not built"),
            Capability.REALTIME_L2_DEPTH: CapabilityInfo(A.NOT_IMPLEMENTED, "WebSocket books (up to 100 levels via REST); not built"),
            Capability.AGGRESSOR_SIDE: CapabilityInfo(A.EXCHANGE_REPORTED, "side = taker side"),
            Capability.TRADE_SEQUENCE: CapabilityInfo(A.NOT_CONTIGUOUS, "64-bit ids, not contiguous"),
        })  # fmt: skip

    def historical_trades(self, symbol: str, start_ms: int, end_ms: int) -> Iterator[pa.RecordBatch]:
        self.capabilities().require([Capability.HISTORICAL_TRADES], self.provider_id)
        return iter(())  # pragma: no cover - require() raises

    def _spec(self, j: Any, symbol: str) -> InstrumentSpec:
        d = j["data"][0]
        tick = str(Decimal(d["priceEndStep"]).scaleb(-int(d["pricePlace"])))  # pricePlace 2, priceEndStep 1 -> 0.01
        return InstrumentSpec(self.provider_id, self.venue_id, "usdt_perpetual", symbol, f"bitget:usdt_perpetual:{symbol}", d["symbolStatus"],
                              d["symbolType"], None, d["baseCoin"], d["quoteCoin"], tick, d["sizeMultiplier"], d.get("minTradeNum"),
                              d.get("minTradeUSDT"), "1", int(d["openTime"]) if d.get("openTime") else None, d)  # fmt: skip
