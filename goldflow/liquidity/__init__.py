"""Phase 8: price-level order-book reconstruction and liquidity analytics (live-recorded data only).

There is no historical price-level L2 for the Phase 2 dataset (2026-04-30 .. 2026-10-30): depth capability is ABSENT
there. Depth history accumulates only from the live recorder in this package. See docs/METRICS_LIQUIDITY_V1.md.
"""

from .book import (
    BookKey,
    BookStatus,
    DepthSnapshot,
    DepthUpdate,
    LevelChange,
    LocalOrderBook,
    MergedBookError,
    Outcome,
    ValidityInterval,
    parse_depth_update,
    parse_snapshot,
)
from .capture import CaptureRecord, CaptureWriter, capture_files, capture_root, iter_capture
from .coverage import DepthCapability, DepthCoverage, DepthUnavailable, require_valid_depth
from .driver import BookDriver, DriverConfig, replay_capture
from .metrics import BookMetrics, ChangeKind, NearTouchFlow, book_metrics, classify_changes
from .registry import BookRegistry, assert_same_book
from .tracker import LiquidityTracker
from .units import UnitError, UnitScale

__all__ = [
    "BookDriver",
    "BookKey",
    "BookMetrics",
    "BookRegistry",
    "BookStatus",
    "CaptureRecord",
    "CaptureWriter",
    "ChangeKind",
    "DepthCapability",
    "DepthCoverage",
    "DepthSnapshot",
    "DepthUnavailable",
    "DepthUpdate",
    "DriverConfig",
    "LevelChange",
    "LiquidityTracker",
    "LocalOrderBook",
    "MergedBookError",
    "NearTouchFlow",
    "Outcome",
    "UnitError",
    "UnitScale",
    "ValidityInterval",
    "assert_same_book",
    "book_metrics",
    "capture_files",
    "capture_root",
    "classify_changes",
    "iter_capture",
    "parse_depth_update",
    "parse_snapshot",
    "replay_capture",
    "require_valid_depth",
]
