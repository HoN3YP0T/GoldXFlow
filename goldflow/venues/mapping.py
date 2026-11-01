"""Phase 15: instrument mapping records and per-venue configurations.

A mapping record says what one venue's instrument *is*:
- its source instrument (provider, venue, market type, symbol, instrument id);
- its contract terms (quote and settlement currency, tick size, quantity step, quantity unit, and ounces per quantity
  unit);
- its trading session and price basis;
- the execution instrument it may be traded through (your MT5 broker symbol; decision D2);
- where its specification came from.

Every venue keeps its own identity: data of different venues never share a partition, an instrument id, a catalog or a
processor. A cross-venue quantity is only ever produced by converting each venue's quantity to troy ounces with that
venue's ``oz_per_qty_unit``, and it is labelled as a cross-venue aggregate (``aggregate.py``).
"""

from __future__ import annotations

import copy
import os
from dataclasses import asdict, dataclass
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any

from ..config import Config
from ..schemas import VENUES, instrument_id

DEFAULT_VENUES: dict[str, dict[str, Any]] = {
    "binance": {
        "symbol": "XAUUSDT",
        "quote": "USDT",
        "settle": "USDT",
        "tick_size": "0.01",
        "step_size": "0.001",
        "qty_unit": "XAU",
        "oz_per_qty_unit": "1",
        "session": "24/7 TradFi perpetual (Binance trading-session schedule applies)",
        "price_basis": "USDT per troy ounce; perpetual on spot gold (underlying COMMODITY)",
        "spec_source": "REST exchangeInfo (Phase 2 discovery, data/state/instrument_discovery.json)",
        "archive_day_utc_offset_h": 0,
        "onboard_ms": None,
    },
    "okx": {
        "symbol": "XAU-USDT-SWAP",
        "quote": "USDT",
        "settle": "USDT",
        "tick_size": "0.1",
        "step_size": "1",
        "qty_unit": "contract",
        "oz_per_qty_unit": "0.001",
        "session": "24/7 perpetual swap",
        "price_basis": "USDT per troy ounce; linear swap, 1 contract = 0.001 XAU",
        "spec_source": "REST /api/v5/public/instruments (2026-10-31): tickSz 0.1, lotSz 1, ctVal 0.001 XAU, linear",
        "archive_day_utc_offset_h": 8,
        "onboard_ms": 1744192800563,
    },
    "bybit": {
        "symbol": "XAUUSDT",
        "quote": "USDT",
        "settle": "USDT",
        "tick_size": "0.01",
        "step_size": "0.001",
        "qty_unit": "XAU",
        "oz_per_qty_unit": "1",
        "session": "24/7 linear perpetual",
        "price_basis": "USDT per troy ounce; linear perpetual",
        "spec_source": "DECLARED: REST instruments-info is geo-restricted from the build host (HTTP 403); checked on the "
        "archive (2026-10-23: every price a multiple of 0.01, every size of 0.001). Re-verify on a reachable host.",
        "archive_day_utc_offset_h": 0,
        "onboard_ms": 1773014400000,
    },
    "bitget": {
        "symbol": "XAUUSDT",
        "quote": "USDT",
        "settle": "USDT",
        "tick_size": "0.01",
        "step_size": "0.01",
        "qty_unit": "XAU",
        "oz_per_qty_unit": "1",
        "session": "24/7 USDT-M perpetual",
        "price_basis": "USDT per troy ounce; USDT-M perpetual",
        "spec_source": "REST /api/v2/mix/market/contracts (2026-10-31): pricePlace 2, sizeMultiplier 0.01",
        "archive_day_utc_offset_h": 0,
        "onboard_ms": 1765533622558,
    },
}


@dataclass(frozen=True)
class InstrumentMapping:
    provider: str
    venue_id: str
    market_type: str
    symbol: str
    instrument_id: str
    quote: str
    settle: str
    tick_size: str
    step_size: str
    qty_unit: str
    oz_per_qty_unit: str
    session: str
    price_basis: str
    spec_source: str
    archive_day_utc_offset_h: int
    onboard_ms: int | None
    execution_instrument: str | None

    @property
    def tick(self) -> Fraction:
        return Fraction(self.tick_size)

    @property
    def step(self) -> Fraction:
        return Fraction(self.step_size)

    @property
    def oz_per_lot(self) -> Fraction:
        """Troy ounces represented by one quantity step (one ``quantity_lots`` unit)."""
        return self.step * Fraction(self.oz_per_qty_unit)

    def row(self) -> dict[str, Any]:
        d = asdict(self)
        f = self.oz_per_lot
        d["oz_per_lot"] = str(Decimal(f.numerator) / Decimal(f.denominator))
        return d


def venue_settings(cfg: Any) -> dict[str, dict[str, Any]]:
    user = cfg.raw.get("venues") or {}
    out: dict[str, dict[str, Any]] = {}
    for p, base in DEFAULT_VENUES.items():
        u = {k: v for k, v in (user.get(p) or {}).items() if not k.startswith("_")}
        bad = sorted(set(u) - set(base))
        if bad:
            raise ValueError(f"venues.{p}: unknown keys {bad}")
        out[p] = {**base, **u}
    extra = sorted(k for k in user if not k.startswith("_") and k not in DEFAULT_VENUES and k != "basis")
    if extra:
        raise ValueError(f"venues: unknown venues {extra} (known: {sorted(DEFAULT_VENUES)})")
    for p, v in out.items():
        for k in ("tick_size", "step_size", "oz_per_qty_unit"):
            if Fraction(str(v[k])) <= 0:
                raise ValueError(f"venues.{p}.{k} must be > 0")
    return out


def basis_settings(cfg: Any) -> dict[str, int]:
    b = {
        "grid_ms": 1000,
        "max_age_ms": 5000,
        **{k: v for k, v in ((cfg.raw.get("venues") or {}).get("basis") or {}).items() if not k.startswith("_")},
    }
    if set(b) - {"grid_ms", "max_age_ms"} or int(b["grid_ms"]) < 100 or int(b["max_age_ms"]) < int(b["grid_ms"]):
        raise ValueError("venues.basis: keys grid_ms (>= 100) and max_age_ms (>= grid_ms)")
    return {k: int(v) for k, v in b.items()}


def mappings(cfg: Any) -> dict[str, InstrumentMapping]:
    broker = cfg.raw.get("broker") or {}
    exec_sym = broker.get("symbol") if broker.get("name") else None
    out = {}
    for p, v in venue_settings(cfg).items():
        venue, market = VENUES[p]
        out[p] = InstrumentMapping(p, venue, market, v["symbol"], instrument_id(v["symbol"], p), v["quote"], v["settle"],
                                   str(v["tick_size"]), str(v["step_size"]), v["qty_unit"], str(v["oz_per_qty_unit"]), v["session"],
                                   v["price_basis"], v["spec_source"], int(v["archive_day_utc_offset_h"]), v["onboard_ms"], exec_sym)  # fmt: skip
    return out


def venue_config(cfg: Any, provider: str) -> Any:
    """A configuration for one venue's data, derived from the main one: same analytics settings, the venue's own
    instrument, normalised partitions (``provider=<venue>``) and its own catalog (so quality events never mix).

    Tick-denominated analytics parameters (profile bins, imbalance thresholds) are inherited as they are: a venue with a
    different tick size needs them reviewed before its results are compared with Binance's."""
    if provider == "binance":
        return cfg
    m = mappings(cfg)[provider]
    raw = copy.deepcopy(cfg.raw)
    raw["source"], raw["symbol"] = provider, m.symbol
    raw["instrument"] = {**raw["instrument"], "tick_size": m.tick_size, "step_size": m.step_size,
                         "onboard_ms": int(m.onboard_ms or 0), "symbol": m.symbol}  # fmt: skip
    raw["paths"] = {**raw["paths"], "state_db": str(Path(raw["paths"]["data_root"]) / "state" / "venues" / f"{provider}.sqlite")}
    raw["replay"] = copy.deepcopy(raw["replay"])
    for k, s in raw["replay"]["streams"].items():
        s["enabled"] = k == "trades"  # only individual trades are imported for other venues
    return VenueConfig(raw, cfg.base_dir)


class VenueConfig(Config):
    """Config of one venue's data. Its catalog sits next to the main one, also when GOLDFLOW_STATE_DB moves that."""

    @property
    def state_db(self) -> Path:
        env = os.environ.get("GOLDFLOW_STATE_DB")
        if env:
            return Path(env).resolve().parent / "venues" / f"{self.raw['source']}.sqlite"
        return Path((self.base_dir / self.raw["paths"]["state_db"]).resolve())
