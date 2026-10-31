"""Phase 5: trade processing, side classification and price-level aggregation (see docs/PROCESSING_V1.md)."""
from .trade_processor import TradeProcessor, ProcessorSink
from .footprint_store import FootprintWriter, read_footprint, read_manifest
from .sessions import SessionCalendar
from .settings import ProcessingSettings

__all__ = ["TradeProcessor", "ProcessorSink", "FootprintWriter", "read_footprint", "read_manifest", "SessionCalendar",
           "ProcessingSettings"]
