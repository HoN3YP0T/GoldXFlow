"""Bybit ``XAUUSDT`` linear perpetual (Phase 15). From the build host (2026-10-31) the REST API is geo-restricted
(HTTP 403, CloudFront), but the public trade archive ``public.bybit.com/trading`` is reachable (files from 2026-03-09).
Its trade ids are UUIDs, so completeness cannot be proven from the data: every imported day is UNVERIFIED."""

from __future__ import annotations

from typing import Any

from ..archive_venue import ArchiveVenueProvider
from ..base import Availability as A
from ..base import Capability, CapabilityInfo, CapabilitySet, InstrumentSpec
from ..registry import register


@register("bybit")
class BybitLinearProvider(ArchiveVenueProvider):
    provider_id = "bybit"
    venue_id = "bybit-linear"
    DISCOVERY_URL = "https://api.bybit.com/v5/market/instruments-info?category=linear&symbol={symbol}"
    HEALTH_URL = "https://api.bybit.com/v5/market/time"

    def capabilities(self) -> CapabilitySet:
        return CapabilitySet({
            Capability.INSTRUMENT_DISCOVERY: CapabilityInfo(A.GEO_RESTRICTED, "REST HTTP 403 from US hosts; spec declared in config, checked on the archive"),
            Capability.HISTORICAL_TRADES: CapabilityInfo(A.AVAILABLE, "public.bybit.com daily CSV (UTC days) with RPI flag; raw kept"),
            Capability.HISTORICAL_AGGTRADES: CapabilityInfo(A.UNAVAILABLE, "not published"),
            Capability.HISTORICAL_KLINES: CapabilityInfo(A.GEO_RESTRICTED, "REST only; no completeness reference reachable"),
            Capability.HISTORICAL_DEPTH_BANDS: CapabilityInfo(A.UNAVAILABLE, "not published"),
            Capability.HISTORICAL_L2_DEPTH: CapabilityInfo(A.NOT_IMPLEMENTED, "Bybit publishes order-book history for some symbols; not acquired (D5)"),
            Capability.HISTORICAL_BID_ASK: CapabilityInfo(A.UNAVAILABLE, "not published"),
            Capability.REALTIME_TRADES: CapabilityInfo(A.NOT_IMPLEMENTED, "publicTrade WebSocket; not built"),
            Capability.REALTIME_AGGTRADES: CapabilityInfo(A.UNAVAILABLE, "no aggregated-trade stream"),
            Capability.REALTIME_L2_DEPTH: CapabilityInfo(A.NOT_IMPLEMENTED, "orderbook WebSocket; not built"),
            Capability.AGGRESSOR_SIDE: CapabilityInfo(A.EXCHANGE_REPORTED, "side = taker side"),
            Capability.TRADE_SEQUENCE: CapabilityInfo(A.UNAVAILABLE, "trdMatchID is a UUID; source_trade_id is derived (day*1e7+row)"),
        })  # fmt: skip

    def _spec(self, j: Any, symbol: str) -> InstrumentSpec:
        d = j["result"]["list"][0]
        return InstrumentSpec(self.provider_id, self.venue_id, "usdt_perpetual", symbol, f"bybit:usdt_perpetual:{symbol}", d["status"],
                              d["contractType"], None, d["baseCoin"], d["quoteCoin"], d["priceFilter"]["tickSize"], d["lotSizeFilter"]["qtyStep"],
                              d["lotSizeFilter"].get("minOrderQty"), d["lotSizeFilter"].get("minNotionalValue"), "1",
                              int(d["launchTime"]) if d.get("launchTime") else None, d)  # fmt: skip
