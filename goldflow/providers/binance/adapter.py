"""Binance USD-M futures adapter.

Wraps the existing, verified Phase 2 components (`rest_client.BinanceFuturesREST`, `vision_archive.VisionArchive`)
without changing their behaviour. Acquisition at scale still runs through `goldflow.acquire` (resumable, checksummed,
catalogued); this adapter is the contract-level entry point other modules use.
"""

from __future__ import annotations

import tempfile
import time
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa

from ...rest_client import BinanceFuturesREST, GeoBlocked
from ...timeutil import from_ms
from ...vision_archive import VisionArchive, archive_key
from ..base import (
    Availability,
    Capability,
    CapabilityInfo,
    CapabilitySet,
    HealthStatus,
    InstrumentSpec,
    Provider,
    ProviderUnavailable,
)
from ..registry import register

A = Availability


@register("binance")
class BinanceUsdmProvider(Provider):
    provider_id = "binance"
    venue_id = "binance-futures-usdm"

    def __init__(
        self, rest: BinanceFuturesREST, archive: VisionArchive, market: str = "futures_um", cache_dir: Path | None = None
    ):
        self.rest, self.archive, self.market = rest, archive, market
        self.cache_dir = cache_dir

    @classmethod
    def from_config(cls, cfg: Any) -> "BinanceUsdmProvider":
        from ...acquire import make_archive, make_rest  # same construction as Phase 2 (no behaviour change)

        return cls(
            make_rest(cfg), make_archive(cfg), cfg["market"], cfg.data_root / "cache" if hasattr(cfg, "data_root") else None
        )

    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(
            {
                Capability.INSTRUMENT_DISCOVERY: CapabilityInfo(A.AVAILABLE, "REST exchangeInfo; HTTP 451 from US hosts"),
                Capability.HISTORICAL_TRADES: CapabilityInfo(A.AVAILABLE, "data.binance.vision daily/monthly trades, SHA-256"),
                Capability.HISTORICAL_AGGTRADES: CapabilityInfo(A.AVAILABLE, "archive; REST only for the recent 2 days (-4166)"),
                Capability.HISTORICAL_KLINES: CapabilityInfo(A.AVAILABLE, "archive klines; used as completeness reference"),
                Capability.HISTORICAL_DEPTH_BANDS: CapabilityInfo(
                    A.AVAILABLE_COARSE, "bookDepth: +-0.2%, +-1..5% cumulative, ~30 s"
                ),
                Capability.HISTORICAL_L2_DEPTH: CapabilityInfo(
                    A.UNAVAILABLE, "not published for USD-M; record live (Ph8/14) or buy"
                ),
                Capability.HISTORICAL_BID_ASK: CapabilityInfo(A.UNAVAILABLE, "bookTicker not published for XAUUSDT"),
                Capability.REALTIME_TRADES: CapabilityInfo(
                    A.NOT_IMPLEMENTED, "Phase 14 (REST /fapi/v1/trades polling; RPI policy)"
                ),
                Capability.REALTIME_AGGTRADES: CapabilityInfo(A.NOT_IMPLEMENTED, "Phase 14 (WebSocket aggTrade; excludes RPI)"),
                Capability.REALTIME_L2_DEPTH: CapabilityInfo(
                    A.AVAILABLE, "Phase 8 recorder: depth@100ms + /fapi/v1/depth, U/u/pu checked; non-US host only (451)"
                ),
                Capability.AGGRESSOR_SIDE: CapabilityInfo(A.EXCHANGE_REPORTED, "is_buyer_maker: true => seller aggressor"),
                Capability.TRADE_SEQUENCE: CapabilityInfo(
                    A.NOT_CONTIGUOUS, "ids skip unused values; completeness via kline counts"
                ),
            }
        )

    def discover_instrument(self, symbol: str) -> InstrumentSpec:
        try:
            s = self.rest.symbol_spec(symbol)
        except GeoBlocked as e:
            raise ProviderUnavailable(str(e)) from e
        return InstrumentSpec(
            provider_id=self.provider_id,
            venue_id=self.venue_id,
            market_type="usdm_perpetual",
            symbol=symbol,
            instrument_id=f"binance:usdm_perpetual:{symbol}",
            status=s["status"],
            contract_type=s["contract_type"],
            underlying_type=s.get("underlying_type"),
            base_asset=s["base_asset"],
            quote_asset=s["quote_asset"],
            tick_size=s["tick_size"],
            step_size=s["step_size"],
            min_qty=s.get("min_qty"),
            min_notional=s.get("min_notional"),
            onboard_ms=s.get("onboard_date_ms"),
            raw=s,
        )

    def historical_trade_keys(self, symbol: str, start_ms: int, end_ms: int) -> list[str]:
        """Daily archive keys covering [start, end) that are actually published (listed, never assumed)."""
        listing = self.archive.list_prefix(archive_key(self.market, "daily", "trades", symbol, "X").rsplit("/", 1)[0] + "/")
        d, last = from_ms(start_ms).date(), from_ms(end_ms - 1).date()
        keys = []
        while d <= last:
            k = archive_key(self.market, "daily", "trades", symbol, str(d))
            if k in listing:
                keys.append(k)
            d += timedelta(days=1)
        return keys

    def historical_trades(self, symbol: str, start_ms: int, end_ms: int) -> Iterator[pa.RecordBatch]:
        """Stream raw-schema trade batches for [start, end) from verified daily archive files (downloaded to a
        cache and checksum-verified). For bulk acquisition use `goldflow.cli acquire`, which also catalogues."""
        from ...integrity import _batches

        cache = self.cache_dir or Path(tempfile.mkdtemp(prefix="goldflow-cache-"))
        for key in self.historical_trade_keys(symbol, start_ms, end_ms):
            dest = cache / key.rsplit("/", 1)[-1]
            expected = self.archive.fetch_checksum(key)
            if not dest.exists():
                _, sha, _ = self.archive.download(key, dest, None)
                if sha != expected:
                    raise ProviderUnavailable(f"checksum mismatch for {key}")
            for b in _batches(dest, "trades"):
                t = b.column("time").to_numpy()
                m = (t >= start_ms) & (t < end_ms)
                if m.any():
                    yield b.filter(pa.array(m))

    def health(self) -> HealthStatus:
        now = int(time.time() * 1000)
        try:
            self.rest.ping()
            return HealthStatus("OK", f"REST {self.rest.base} reachable", now)
        except GeoBlocked as e:
            return HealthStatus("DOWN", f"geo-blocked: {e}", now)
        except Exception as e:  # noqa: BLE001 - health must never raise
            return HealthStatus("DOWN", repr(e), now)

    def close(self) -> None:
        self.rest.s.close()
        self.archive.s.close()
