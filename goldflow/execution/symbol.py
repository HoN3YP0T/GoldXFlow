"""Phase 16: broker instrument validation at start-up.

The EA reports the broker's ``SymbolInfo``. It must match the ``broker`` block, the contract specification you copied
from MT5 (decision D2), which Phase 12 backtests were run with. Any difference **refuses to start**, because the
backtests would no longer describe the live contract. The checks are:
- digits;
- contract size;
- volume min, step and max;
- stops level;
- trade mode (FULL);
- tick size = point (a 2-digit XAUUSD has tick 0.01);
- the account currency, compared on the HELLO message.

The Binance XAUUSDT → broker symbol mapping is the Phase 15 mapping record's ``execution_instrument``. Signal prices
are translated by keeping the signal's distances from its entry and applying them to the broker price at send time.
Phase 12 does the same through its basis model: the basis is the broker price minus the Binance price at that moment.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any


class SymbolMismatch(RuntimeError):
    pass


def check_symbol(info: dict[str, Any], broker: dict[str, Any], hello: dict[str, Any] | None = None) -> list[str]:
    bad = []

    def eq(name: str, got: Any, want: Any) -> None:
        if Decimal(str(got)) != Decimal(str(want)):
            bad.append(f"{name}: broker reports {got}, broker block says {want}")

    if info.get("symbol") != broker.get("symbol"):
        bad.append(f"symbol: broker reports {info.get('symbol')!r}, broker block says {broker.get('symbol')!r}")
    eq("digits", info["digits"], broker["digits"])
    eq("contract_size", info["contract_size"], broker["contract_size"])
    eq("volume_min", info["volume_min"], broker["volume_min"])
    eq("volume_step", info["volume_step"], broker["volume_step"])
    eq("volume_max", info["volume_max"], broker["volume_max"])
    eq("stops_level", info["stops_level"], broker.get("stops_level_points", 0))
    if Decimal(str(info["tick_size"])) != Decimal(str(info["point"])):
        bad.append(f"tick_size {info['tick_size']} != point {info['point']}: prices would not be snapped correctly")
    if info.get("trade_mode") != "FULL":
        bad.append(f"trade_mode {info.get('trade_mode')}: trading is not fully allowed on this symbol")
    if hello is not None and broker.get("account_currency") and hello.get("currency") != broker["account_currency"]:
        bad.append(f"account currency {hello.get('currency')} != {broker['account_currency']}")
    return bad


def snap(price: float, tick: float, digits: int) -> str:
    """Price snapped to the broker tick, as exact text with the symbol's digits."""
    t = Decimal(str(tick))
    p = (Decimal(str(price)) / t).to_integral_value() * t
    return f"{p:.{digits}f}"
