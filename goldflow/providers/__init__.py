"""Market-data providers. Every adapter implements `base.Provider` and declares its capabilities honestly."""

from .base import (
    Availability,
    Capability,
    CapabilityInfo,
    CapabilitySet,
    CapabilityUnavailable,
    HealthStatus,
    InstrumentSpec,
    Provider,
    ProviderNotAllowed,
    ProviderUnavailable,
    RawEvent,
)
from .registry import available_providers, create_provider, register

__all__ = [
    "Availability",
    "Capability",
    "CapabilityInfo",
    "CapabilitySet",
    "CapabilityUnavailable",
    "HealthStatus",
    "InstrumentSpec",
    "Provider",
    "ProviderNotAllowed",
    "ProviderUnavailable",
    "RawEvent",
    "available_providers",
    "create_provider",
    "register",
]
