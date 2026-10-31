"""Deterministic synthetic provider for tests ONLY (refused outside the 'test' profile)."""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import numpy as np
import pyarrow as pa

from ..base import Availability, Capability, CapabilityInfo, CapabilitySet, HealthStatus, InstrumentSpec, Provider
from ..registry import register

A = Availability


@register("dummy")
class DummyProvider(Provider):
    provider_id = "dummy"
    venue_id = "dummy-synthetic"
    synthetic = True

    def __init__(self, seed: int = 7, trades_per_hour: int = 600, base_price: float = 4000.0):
        self.seed, self.rate, self.base = seed, trades_per_hour, base_price

    @classmethod
    def from_config(cls, cfg: Any) -> "DummyProvider":
        d = (cfg.raw if hasattr(cfg, "raw") else cfg).get("dummy", {})
        return cls(d.get("seed", 7), d.get("trades_per_hour", 600), d.get("base_price", 4000.0))

    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(
            {
                Capability.INSTRUMENT_DISCOVERY: CapabilityInfo(A.AVAILABLE, "synthetic"),
                Capability.HISTORICAL_TRADES: CapabilityInfo(A.AVAILABLE, "synthetic, deterministic per seed"),
                Capability.AGGRESSOR_SIDE: CapabilityInfo(A.EXCHANGE_REPORTED, "synthetic"),
                Capability.TRADE_SEQUENCE: CapabilityInfo(A.AVAILABLE, "contiguous synthetic ids"),
                Capability.HISTORICAL_L2_DEPTH: CapabilityInfo(A.UNAVAILABLE, "synthetic provider has no book"),
            }
        )

    def discover_instrument(self, symbol: str) -> InstrumentSpec:
        return InstrumentSpec(
            self.provider_id,
            self.venue_id,
            "synthetic",
            symbol,
            f"dummy:synthetic:{symbol}",
            "TRADING",
            "SYNTHETIC",
            None,
            "XAU",
            "USDT",
            "0.01",
            "0.001",
            "0.001",
            "5",
            "1",
            0,
            {"synthetic": True},
        )

    def historical_trades(self, symbol: str, start_ms: int, end_ms: int) -> Iterator[pa.RecordBatch]:
        """Random-walk trades in the raw Binance trades schema. Same seed + range => identical output."""
        hour = 3_600_000
        first_hour = start_ms // hour
        next_id = 1
        for h in range(first_hour, (end_ms - 1) // hour + 1):
            rng = np.random.default_rng([self.seed, h])
            ts = np.sort(rng.integers(h * hour, (h + 1) * hour, self.rate)).astype(np.int64)
            px = np.round(self.base + np.cumsum(rng.normal(0, 0.05, self.rate)) + rng.normal(0, 0.5), 2)
            qty = np.round(rng.exponential(0.2, self.rate) + 0.001, 3)
            bm = rng.random(self.rate) < 0.5
            ids = np.arange(h * self.rate + 1, (h + 1) * self.rate + 1, dtype=np.int64)
            m = (ts >= start_ms) & (ts < end_ms)
            if m.any():
                yield pa.record_batch(
                    {
                        "id": ids[m],
                        "price": px[m],
                        "qty": qty[m],
                        "quote_qty": np.round(px[m] * qty[m], 8),
                        "time": ts[m],
                        "is_buyer_maker": bm[m],
                    }
                )
            next_id += self.rate

    def health(self) -> HealthStatus:
        return HealthStatus("OK", "synthetic", int(time.time() * 1000))
