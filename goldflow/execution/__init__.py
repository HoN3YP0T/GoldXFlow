"""Phase 16: MT5 execution bridge and risk controls (docs/EXECUTION_V1.md). Live trading is off by default."""

from .audit import AuditBroken, AuditLog
from .bridge import ExecutionBridge
from .gate import evidence, record_gate
from .orders import Order, OrderBook
from .protocol import Endpoint, ProtocolError, hmac_test_vector, idempotency_key
from .reconcile import reconcile
from .risk import Decision, RiskEngine
from .settings import BridgeSettings, RiskSettings
from .symbol import check_symbol, snap
from .transport import FileQueue, Pipe, TcpServer

__all__ = [
    "AuditBroken",
    "AuditLog",
    "BridgeSettings",
    "Decision",
    "Endpoint",
    "ExecutionBridge",
    "FileQueue",
    "Order",
    "OrderBook",
    "Pipe",
    "ProtocolError",
    "RiskEngine",
    "RiskSettings",
    "TcpServer",
    "check_symbol",
    "evidence",
    "hmac_test_vector",
    "idempotency_key",
    "reconcile",
    "record_gate",
    "snap",
]
