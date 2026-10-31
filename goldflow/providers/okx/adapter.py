"""OKX ``XAU-USDT-SWAP`` (Phase 15). Verified from the build host on 2026-10-31: the REST instrument endpoint and the
daily trade archive are reachable. The archive's trade ids are contiguous (2026-10-23: 279,753 consecutive steps of 1),
so completeness is provable. Archive days are UTC+8."""

from __future__ import annotations

from typing import Any

from ..archive_venue import ArchiveVenueProvider
from ..base import Availability as A
from ..base import Capability, CapabilityInfo, CapabilitySet, InstrumentSpec
from ..registry import register


@register("okx")
class OkxSwapProvider(ArchiveVenueProvider):
    provider_id = "okx"
    venue_id = "okx-swap"
    DISCOVERY_URL = "https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId={symbol}"
    HEALTH_URL = "https://www.okx.com/api/v5/public/time"

    def capabilities(self) -> CapabilitySet:
        return CapabilitySet({
            Capability.INSTRUMENT_DISCOVERY: CapabilityInfo(A.AVAILABLE, "REST /api/v5/public/instruments"),
            Capability.HISTORICAL_TRADES: CapabilityInfo(A.AVAILABLE, "public daily archive (UTC+8 days), raw kept; source=1 marks RPI-like orders"),
            Capability.HISTORICAL_AGGTRADES: CapabilityInfo(A.UNAVAILABLE, "not published"),
            Capability.HISTORICAL_KLINES: CapabilityInfo(A.NOT_IMPLEMENTED, "REST candles exist; completeness comes from contiguous ids instead"),
            Capability.HISTORICAL_DEPTH_BANDS: CapabilityInfo(A.UNAVAILABLE, "not published"),
            Capability.HISTORICAL_L2_DEPTH: CapabilityInfo(A.NOT_IMPLEMENTED, "OKX sells order-book history; not acquired (decision D5)"),
            Capability.HISTORICAL_BID_ASK: CapabilityInfo(A.UNAVAILABLE, "not published"),
            Capability.REALTIME_TRADES: CapabilityInfo(A.NOT_IMPLEMENTED, "WebSocket trades channel; not built"),
            Capability.REALTIME_AGGTRADES: CapabilityInfo(A.UNAVAILABLE, "no aggregated-trade stream"),
            Capability.REALTIME_L2_DEPTH: CapabilityInfo(A.NOT_IMPLEMENTED, "books channel (400 levels; 5,000 via REST); not built"),
            Capability.AGGRESSOR_SIDE: CapabilityInfo(A.EXCHANGE_REPORTED, "side = taker side"),
            Capability.TRADE_SEQUENCE: CapabilityInfo(A.AVAILABLE, "contiguous trade ids: gaps are provable"),
        })  # fmt: skip

    def _spec(self, j: Any, symbol: str) -> InstrumentSpec:
        d = j["data"][0]
        return InstrumentSpec(self.provider_id, self.venue_id, "usdt_swap", symbol, f"okx:usdt_swap:{symbol}", d["state"], d["ctType"] + "_swap",
                              "COMMODITY" if d.get("ctValCcy") == "XAU" else None, d.get("ctValCcy", ""), d.get("settleCcy", ""), d["tickSz"],
                              d["lotSz"], d.get("minSz"), None, d["ctVal"], int(d["listTime"]) if d.get("listTime") else None, d)  # fmt: skip
