"""Provider contract (Phase 1).

A provider is one data source (an exchange, a futures data vendor, a broker feed). Adapters never pretend to support
data they don't have: each declares a `CapabilitySet`, and callers use `require()` so an unsupported request fails
loudly with `CapabilityUnavailable` instead of returning empty or substitute data.

Venue separation rule: one provider instance = one venue. Nothing in this contract merges data across providers.
"""

from __future__ import annotations

import abc
from collections.abc import AsyncIterator, Iterable, Iterator
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import pyarrow as pa


class Capability(str, Enum):
    INSTRUMENT_DISCOVERY = "instrument_discovery"
    HISTORICAL_TRADES = "historical_trades"  # individual executions
    HISTORICAL_AGGTRADES = "historical_aggtrades"  # exchange-aggregated executions
    HISTORICAL_KLINES = "historical_klines"
    HISTORICAL_DEPTH_BANDS = "historical_depth_bands"  # coarse cumulative depth (not L2)
    HISTORICAL_L2_DEPTH = "historical_l2_depth"  # price-level snapshots + incremental updates
    HISTORICAL_BID_ASK = "historical_bid_ask"  # top-of-book quote history
    REALTIME_TRADES = "realtime_trades"
    REALTIME_AGGTRADES = "realtime_aggtrades"
    REALTIME_L2_DEPTH = "realtime_l2_depth"
    AGGRESSOR_SIDE = "aggressor_side"
    TRADE_SEQUENCE = "trade_sequence"  # contiguous trade ids usable for gap detection


class Availability(str, Enum):
    AVAILABLE = "AVAILABLE"
    AVAILABLE_COARSE = "AVAILABLE_COARSE"  # exists but at a resolution that limits use
    EXCHANGE_REPORTED = "EXCHANGE_REPORTED"  # (aggressor side) reported by the venue, not inferred
    NOT_CONTIGUOUS = "NOT_CONTIGUOUS"  # (trade sequence) ids exist but skip values
    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"  # venue offers it, adapter does not yet (phase noted)
    REQUIRES_CREDENTIALS = "REQUIRES_CREDENTIALS"
    GEO_RESTRICTED = "GEO_RESTRICTED"
    UNAVAILABLE = "UNAVAILABLE"  # venue does not provide it

    @property
    def usable(self) -> bool:
        return self in (
            Availability.AVAILABLE,
            Availability.AVAILABLE_COARSE,
            Availability.EXCHANGE_REPORTED,
            Availability.NOT_CONTIGUOUS,
        )


@dataclass(frozen=True)
class CapabilityInfo:
    availability: Availability
    note: str = ""


class CapabilityUnavailable(RuntimeError):
    """A requested data type is not supported by this provider (or not for this interval)."""


class ProviderUnavailable(RuntimeError):
    """The provider cannot be reached or refuses service (e.g. HTTP 451 geo-block)."""


class ProviderNotAllowed(RuntimeError):
    """The provider may not be used in the current profile (e.g. synthetic data outside tests)."""


@dataclass(frozen=True)
class CapabilitySet:
    entries: dict[Capability, CapabilityInfo]

    def get(self, cap: Capability) -> CapabilityInfo:
        return self.entries.get(cap, CapabilityInfo(Availability.UNAVAILABLE, "not declared"))

    def supports(self, cap: Capability) -> bool:
        return self.get(cap).availability.usable

    def require(self, caps: Iterable[Capability], provider_id: str = "") -> None:
        missing = [f"{c.value} ({self.get(c).availability.value}: {self.get(c).note})" for c in caps if not self.supports(c)]
        if missing:
            raise CapabilityUnavailable(f"provider {provider_id or '?'} lacks: " + "; ".join(missing))

    def as_dict(self) -> dict[str, dict[str, str]]:
        return {c.value: {"availability": i.availability.value, "note": i.note} for c, i in self.entries.items()}


@dataclass(frozen=True)
class InstrumentSpec:
    provider_id: str
    venue_id: str
    market_type: str
    symbol: str
    instrument_id: str
    status: str
    contract_type: str
    underlying_type: str | None
    base_asset: str
    quote_asset: str
    tick_size: str
    step_size: str
    min_qty: str | None = None
    min_notional: str | None = None
    contract_multiplier: str = "1"
    onboard_ms: int | None = None
    raw: dict[str, Any] = field(default_factory=dict, compare=False)


@dataclass(frozen=True)
class RawEvent:
    """A payload exactly as received, before any transformation (persisted before normalisation)."""

    provider_id: str
    stream: str
    receive_time_ms: int
    payload: dict[str, Any]


@dataclass(frozen=True)
class HealthStatus:
    state: str  # OK | DEGRADED | DOWN
    detail: str
    checked_ms: int


class Provider(abc.ABC):
    """Contract every market-data adapter implements."""

    provider_id: str = "abstract"
    venue_id: str = "abstract"
    synthetic: bool = False  # synthetic providers are refused outside the test profile

    @abc.abstractmethod
    def capabilities(self) -> CapabilitySet:
        """Declared, honest capability set."""

    @abc.abstractmethod
    def discover_instrument(self, symbol: str) -> InstrumentSpec:
        """Query the venue for the instrument's live specification (never hard-coded)."""

    @abc.abstractmethod
    def historical_trades(self, symbol: str, start_ms: int, end_ms: int) -> Iterator[pa.RecordBatch]:
        """Individual trades with availability in [start_ms, end_ms), as raw-schema record batches, oldest first."""

    def historical_depth(self, symbol: str, start_ms: int, end_ms: int) -> Iterator[pa.RecordBatch]:
        """Price-level L2 history. Default: unsupported."""
        self.capabilities().require([Capability.HISTORICAL_L2_DEPTH], self.provider_id)
        raise NotImplementedError  # pragma: no cover

    async def subscribe(self, streams: list[str]) -> AsyncIterator[RawEvent]:
        """Real-time streams (Phase 14). Default: unsupported."""
        self.capabilities().require([Capability.REALTIME_TRADES], self.provider_id)
        raise NotImplementedError  # pragma: no cover
        yield  # pragma: no cover  (makes this an async generator)

    @abc.abstractmethod
    def health(self) -> HealthStatus:
        """Cheap reachability/health probe."""

    def close(self) -> None:
        """Release network resources."""

    def __enter__(self) -> "Provider":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
