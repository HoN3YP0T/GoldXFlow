"""Phase 12 settings: the ``broker`` block (execution venue, decision D2) and the ``backtest`` block (execution model,
risk limits). Validated at ``Config.load``. Every amount is an exact decimal string.

The broker block describes **your** MT5 broker's XAUUSD. Until it is filled in (``name`` is null) a backtest refuses to
run: fills must be on broker prices and costs, never on Binance's.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from fractions import Fraction
from typing import Any
from zoneinfo import ZoneInfo

from ..processing.settings import parse_interval

DAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
DEFAULT_BROKER: dict[str, Any] = {
    "name": None,
    "symbol": "XAUUSD",
    "digits": 2,
    "contract_size": "100",
    "volume_min": "0.01",
    "volume_step": "0.01",
    "volume_max": "50",
    "commission_per_lot_side": "0",
    "swap_long_per_lot": "0",
    "swap_short_per_lot": "0",
    "swap_time": "00:00",
    "triple_swap_day": "Wed",
    "stops_level_points": 0,
    "server_tz": None,
    "account_currency": "USD",
}
DEFAULT_BACKTEST: dict[str, Any] = {
    "version": 1,
    "starting_capital": "10000",
    "latency_ms": 250,
    "slippage_points": 2,
    "slippage_vol_mult": "0",
    "slippage_vol_ticks": 20,
    "reject_rate": "0",
    "max_tick_gap": "60s",
    "max_hold": "24h",
    "seed": 1,
    "basis": {"window": "5m", "min_samples": 30},
    "risk": {
        "risk_per_trade": "0.005",
        "max_concurrent_positions": 1,
        "max_exposure_lots": "5",
        "daily_loss_limit": "0.02",
        "max_drawdown": "0.08",
        "max_consecutive_losses": 4,
        "sessions": [],
    },  # fmt: skip
}


def _frac(v: Any, what: str) -> Fraction:
    if isinstance(v, (bool, float)):
        raise ValueError(f"{what}: give a decimal string (floats are not exact)")
    try:
        return Fraction(str(v))
    except (ValueError, ZeroDivisionError):
        raise ValueError(f"{what}: {v!r} is not a number") from None


@dataclass(frozen=True)
class BrokerSpec:
    name: str
    symbol: str
    digits: int
    contract_size: Fraction  # ounces per lot
    volume_min: Fraction
    volume_step: Fraction
    volume_max: Fraction
    commission_per_lot_side: Fraction  # account currency, charged on entry and on exit
    swap_long_per_lot: Fraction  # per lot per rollover (negative = charge)
    swap_short_per_lot: Fraction
    swap_time_min: int  # server-time minute of the daily rollover
    triple_swap_day: int  # weekday index of the triple rollover
    stops_level_points: int
    server_tz: str
    account_currency: str

    @property
    def point(self) -> Fraction:
        return Fraction(1, 10**self.digits)

    def as_dict(self) -> dict[str, Any]:
        return {k: (str(v) if isinstance(v, Fraction) else v) for k, v in self.__dict__.items()}

    @classmethod
    def from_raw(cls, raw: dict[str, Any] | None) -> "BrokerSpec | None":
        b = {**DEFAULT_BROKER, **{k: v for k, v in (raw or {}).items() if not k.startswith("_")}}
        bad = sorted(set(b) - set(DEFAULT_BROKER))
        if bad:
            raise ValueError(f"broker: unknown keys {bad}")
        if b["name"] is None:
            return None
        if not b["server_tz"]:
            raise ValueError(
                "broker.server_tz is required: MT5 tick times are broker server time (an IANA zone such as 'Europe/Athens')"
            )
        ZoneInfo(str(b["server_tz"]))
        h, m = (int(x) for x in str(b["swap_time"]).split(":"))
        spec = cls(str(b["name"]), str(b["symbol"]), int(b["digits"]), _frac(b["contract_size"], "broker.contract_size"),
                   _frac(b["volume_min"], "broker.volume_min"), _frac(b["volume_step"], "broker.volume_step"),
                   _frac(b["volume_max"], "broker.volume_max"), _frac(b["commission_per_lot_side"], "broker.commission_per_lot_side"),
                   _frac(b["swap_long_per_lot"], "broker.swap_long_per_lot"), _frac(b["swap_short_per_lot"], "broker.swap_short_per_lot"),
                   h * 60 + m, DAYS.index(str(b["triple_swap_day"])), int(b["stops_level_points"]), str(b["server_tz"]),
                   str(b["account_currency"]))  # fmt: skip
        if (
            not (0 <= spec.digits <= 5)
            or spec.contract_size <= 0
            or spec.volume_step <= 0
            or not (0 < spec.volume_min <= spec.volume_max)
        ):
            raise ValueError("broker: digits in [0, 5]; contract_size, volume_step > 0; 0 < volume_min <= volume_max")
        if spec.volume_min % spec.volume_step or spec.commission_per_lot_side < 0 or spec.stops_level_points < 0:
            raise ValueError("broker: volume_min must be a multiple of volume_step; commission and stops level >= 0")
        if spec.account_currency != "USD":
            raise ValueError("broker.account_currency: only USD accounts are modelled (XAUUSD P&L is in USD)")
        return spec


@dataclass(frozen=True)
class BacktestSettings:
    starting_capital: Fraction
    latency_ms: int
    slippage_points: int
    slippage_vol_mult: Fraction
    slippage_vol_ticks: int
    reject_rate: Fraction
    max_tick_gap_ms: int
    max_hold_ms: int
    seed: int
    basis_window_ms: int
    basis_min_samples: int
    risk_per_trade: Fraction
    max_concurrent: int
    max_exposure_lots: Fraction
    daily_loss_limit: Fraction
    max_drawdown: Fraction
    max_consecutive_losses: int
    sessions: tuple[str, ...]
    raw: dict[str, Any]
    hash: str

    @classmethod
    def from_config(cls, cfg: Any) -> "BacktestSettings":
        user = cfg.raw.get("backtest") or {}
        raw = {**DEFAULT_BACKTEST, **{k: v for k, v in user.items() if not k.startswith("_")}}
        for blk in ("basis", "risk"):
            raw[blk] = {k: v for k, v in {**DEFAULT_BACKTEST[blk], **(user.get(blk) or {})}.items() if not k.startswith("_")}
        bad = sorted(set(raw) - set(DEFAULT_BACKTEST))
        if bad:
            raise ValueError(f"backtest: unknown keys {bad}")
        rk, bs = raw["risk"], raw["basis"]

        def i(v: Any, what: str, lo: int = 0) -> int:
            if isinstance(v, bool) or not isinstance(v, int) or v < lo:
                raise ValueError(f"backtest {what} must be an integer >= {lo}")
            return v

        st = cls(_frac(raw["starting_capital"], "starting_capital"), i(raw["latency_ms"], "latency_ms"), i(raw["slippage_points"], "slippage_points"),
                 _frac(raw["slippage_vol_mult"], "slippage_vol_mult"), i(raw["slippage_vol_ticks"], "slippage_vol_ticks", 2),
                 _frac(raw["reject_rate"], "reject_rate"), parse_interval(str(raw["max_tick_gap"])), parse_interval(str(raw["max_hold"])),
                 i(raw["seed"], "seed"), parse_interval(str(bs["window"])), i(bs["min_samples"], "basis.min_samples", 1),
                 _frac(rk["risk_per_trade"], "risk.risk_per_trade"), i(rk["max_concurrent_positions"], "risk.max_concurrent_positions", 1),
                 _frac(rk["max_exposure_lots"], "risk.max_exposure_lots"), _frac(rk["daily_loss_limit"], "risk.daily_loss_limit"),
                 _frac(rk["max_drawdown"], "risk.max_drawdown"), i(rk["max_consecutive_losses"], "risk.max_consecutive_losses", 1),
                 tuple(str(x) for x in rk["sessions"]), raw, "")  # fmt: skip
        if st.starting_capital <= 0 or not (0 <= st.reject_rate < 1) or not (0 < st.risk_per_trade <= Fraction(1, 10)):
            raise ValueError("backtest: starting_capital > 0, reject_rate in [0, 1), risk_per_trade in (0, 0.1]")
        if (
            not (0 < st.daily_loss_limit < 1)
            or not (0 < st.max_drawdown < 1)
            or st.max_exposure_lots <= 0
            or st.slippage_vol_mult < 0
        ):
            raise ValueError("backtest.risk: limits must be fractions in (0, 1); exposure > 0; slippage_vol_mult >= 0")
        broker = BrokerSpec.from_raw(cfg.raw.get("broker"))
        body = {"backtest": raw, "broker": broker.as_dict() if broker else None}
        h = hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()
        return cls(**{**st.__dict__, "hash": h})
