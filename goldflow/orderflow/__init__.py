"""Orderflow analytics.

Phase 6 (docs/METRICS_PROFILE_V1.md): Volume Profile, POC, Value Area, HVN/LVN and VWAP with bands, as one Phase 5
``ProcessorSink`` (``ProfileEngine``) plus pure functions (``volume_profile``, ``vwap``) and a Parquet snapshot store.

Phase 7 (docs/METRICS_ORDERFLOW_V1.md): transaction-based orderflow analytics.

Delta and CVD, footprint imbalance and stacked imbalance, taker-order reconstruction and Big Trades, absorption,
exhaustion and price/CVD/delta divergence. Everything runs as one Phase 5 ``ProcessorSink`` (``OrderflowAnalytics``),
so replay and live share the same code.
"""

from .big_trades import BigTradeDetector, TakerOrderBuilder
from .cvd import ResetCVD
from .engine import OrderflowAnalytics
from .imbalance import compute_imbalances
from .profile_engine import ProfileEngine
from .profile_settings import ProfileSettings
from .profile_store import ProfileStore
from .profile_store import read_manifest as read_profile_manifest
from .profile_store import read_table as read_profile_table
from .rolling import DailyBucketBaseline, RollingQuantile
from .settings import DEFAULT_ORDERFLOW, OrderflowSettings
from .store import OrderflowStore, orderflow_root
from .volume_profile import BinnedProfile, bin_dense, bin_sparse, find_nodes, poc_index, profile_metrics, value_area
from .vwap import VwapSums, sums_from_levels, vwap_stats

__all__ = [
    "BinnedProfile",
    "ProfileEngine",
    "ProfileSettings",
    "ProfileStore",
    "VwapSums",
    "bin_dense",
    "bin_sparse",
    "find_nodes",
    "poc_index",
    "profile_metrics",
    "read_profile_manifest",
    "read_profile_table",
    "sums_from_levels",
    "value_area",
    "vwap_stats",
    "DEFAULT_ORDERFLOW",
    "BigTradeDetector",
    "DailyBucketBaseline",
    "OrderflowAnalytics",
    "OrderflowSettings",
    "OrderflowStore",
    "ResetCVD",
    "RollingQuantile",
    "TakerOrderBuilder",
    "compute_imbalances",
    "orderflow_root",
]
