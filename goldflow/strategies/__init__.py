"""Phase 11: strategy interface and signal generation (docs/SIGNALS_V1.md)."""

from .base import AnalyticsState, Strategy
from .engine import SignalStore, StrategyEngine, build_strategies, strategy_settings
from .library import REGISTRY, AbsorptionAtLevel, FailedAuctionValueEdge, IBFailedBreak
from .signal import Evidence, TradingSignal

__all__ = [
    "AbsorptionAtLevel",
    "AnalyticsState",
    "Evidence",
    "FailedAuctionValueEdge",
    "IBFailedBreak",
    "REGISTRY",
    "SignalStore",
    "Strategy",
    "StrategyEngine",
    "TradingSignal",
    "build_strategies",
    "strategy_settings",
]
