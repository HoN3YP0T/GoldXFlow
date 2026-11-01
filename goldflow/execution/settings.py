"""Phase 16: bridge and risk settings (config blocks ``bridge`` and ``risk``).

The bridge secret is **never** in the config. It is read from the file named by ``bridge.secret_file``, which the EA
reads too (MT5 Common Files folder), or from the environment variable ``bridge.secret_env``. It must be at least 32
bytes.

Risk limits are decision **D7**. Their **defaults come from the Phase 12 ``backtest.risk`` block**, so live trading
uses the limits the backtests were run with (PROPOSED: 0.5 % per trade, 2 % daily loss, 8 % drawdown, 1 position,
4 consecutive losses). The ``risk`` block may make a limit **stricter**, never looser: a looser live limit would trade
outside what was tested, and is refused.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

DEFAULT_BRIDGE: dict[str, Any] = {
    "transport": "tcp",
    "host": "127.0.0.1",
    "port": 47011,
    "file_queue_dir": None,
    "secret_file": None,
    "secret_env": "GOLDFLOW_BRIDGE_SECRET",
    "magic": 20261031,
    "deviation_points": 30,
    "ack_timeout_ms": 3000,
    "max_retries": 3,
    "heartbeat_ms": 2000,
    "heartbeat_timeout_ms": 10000,
    "max_skew_ms": 5000,
    "reconcile_every_s": 30,
}
DEFAULT_RISK: dict[str, Any] = {
    "risk_per_trade_pct": "0.5",
    "max_daily_loss_pct": "2",
    "max_drawdown_pct": "8",
    "max_positions": 1,
    "max_exposure_lots": "1.00",
    "max_consecutive_losses": 4,
    "max_spread_points": 60,
    "max_slippage_points": 50,
    "max_avg_slippage_points": 25,
    "slippage_window": 20,
    "sessions": "any",
    "block_on_dq": True,
    "day_reset_utc": "00:00",
}


@dataclass(frozen=True)
class BridgeSettings:
    transport: str
    host: str
    port: int
    file_queue_dir: str | None
    secret_file: str | None
    secret_env: str
    magic: int
    deviation_points: int
    ack_timeout_ms: int
    max_retries: int
    heartbeat_ms: int
    heartbeat_timeout_ms: int
    max_skew_ms: int
    reconcile_every_s: int

    @classmethod
    def from_config(cls, cfg: Any) -> "BridgeSettings":
        raw = _merge("bridge", DEFAULT_BRIDGE, cfg.raw.get("bridge"))
        if raw["transport"] not in ("tcp", "file"):
            raise ValueError("bridge.transport: tcp | file")
        if raw["host"] not in ("127.0.0.1", "localhost", "::1"):
            raise ValueError("bridge.host must be a loopback address (the channel is local by design)")
        if raw["transport"] == "file" and not raw["file_queue_dir"]:
            raise ValueError("bridge.file_queue_dir is required for the file transport (MT5 Common Files\\goldflow)")
        for k in ("ack_timeout_ms", "heartbeat_ms", "heartbeat_timeout_ms", "max_skew_ms", "reconcile_every_s"):
            if int(raw[k]) <= 0:
                raise ValueError(f"bridge.{k} must be > 0")
        if int(raw["heartbeat_timeout_ms"]) < 2 * int(raw["heartbeat_ms"]) or not (0 <= int(raw["max_retries"]) <= 10):
            raise ValueError("bridge: heartbeat_timeout_ms >= 2 x heartbeat_ms, max_retries in [0, 10]")
        return cls(**{k: raw[k] for k in DEFAULT_BRIDGE})

    def secret(self, base: Path | None = None) -> bytes:
        s: bytes | None = None
        if self.secret_file:
            p = Path(self.secret_file)
            p = p if p.is_absolute() or base is None else base / p
            if p.exists():
                s = p.read_bytes().strip()
        if s is None and os.environ.get(self.secret_env):
            s = os.environ[self.secret_env].encode()
        if s is None or len(s) < 32:
            raise ValueError(
                f"bridge secret missing or shorter than 32 bytes (file {self.secret_file!r} or env {self.secret_env})"
            )
        return s


@dataclass(frozen=True)
class RiskSettings:
    risk_per_trade_pct: Decimal
    max_daily_loss_pct: Decimal
    max_drawdown_pct: Decimal
    max_positions: int
    max_exposure_lots: Decimal
    max_consecutive_losses: int
    max_spread_points: int
    max_slippage_points: int
    max_avg_slippage_points: int
    slippage_window: int
    sessions: tuple[str, ...] | str
    block_on_dq: bool
    day_reset_utc: str

    @classmethod
    def from_config(cls, cfg: Any) -> "RiskSettings":
        bt = ((cfg.raw.get("backtest") or {}).get("risk")) or {}
        tested: dict[str, Any] = {}
        if bt:
            tested = {"risk_per_trade_pct": Decimal(str(bt["risk_per_trade"])) * 100, "max_daily_loss_pct": Decimal(str(bt["daily_loss_limit"])) * 100,
                      "max_drawdown_pct": Decimal(str(bt["max_drawdown"])) * 100, "max_positions": int(bt["max_concurrent_positions"]),
                      "max_exposure_lots": Decimal(str(bt["max_exposure_lots"])), "max_consecutive_losses": int(bt["max_consecutive_losses"])}  # fmt: skip
            if bt.get("sessions"):
                tested["sessions"] = list(bt["sessions"])
        raw = _merge("risk", {**DEFAULT_RISK, **tested}, cfg.raw.get("risk"))
        for k, v in tested.items():
            if k != "sessions" and Decimal(str(raw[k])) > Decimal(str(v)):
                raise ValueError(
                    f"risk.{k} = {raw[k]} is looser than the backtested backtest.risk value {v}; live limits may only be stricter"
                )
        pct = {k: Decimal(str(raw[k])) for k in ("risk_per_trade_pct", "max_daily_loss_pct", "max_drawdown_pct")}
        if not (Decimal("0") < pct["risk_per_trade_pct"] <= Decimal("2")):
            raise ValueError("risk.risk_per_trade_pct must be in (0, 2]")
        if not (pct["risk_per_trade_pct"] <= pct["max_daily_loss_pct"] <= pct["max_drawdown_pct"] <= Decimal("50")):
            raise ValueError("risk: risk_per_trade_pct <= max_daily_loss_pct <= max_drawdown_pct <= 50")
        if int(raw["max_positions"]) < 1 or Decimal(str(raw["max_exposure_lots"])) <= 0 or int(raw["max_consecutive_losses"]) < 1:
            raise ValueError("risk: max_positions >= 1, max_exposure_lots > 0, max_consecutive_losses >= 1")
        if raw["block_on_dq"] is not True:
            raise ValueError("risk.block_on_dq cannot be disabled: no order may be sent while a data-quality interval is open")
        ses = raw["sessions"] if raw["sessions"] == "any" else tuple(raw["sessions"])
        if ses != "any":
            known = {x["name"] for x in (cfg.raw.get("sessions") or {}).get("sessions", [])}
            bad = sorted(set(ses) - known) if known else []
            if bad:
                raise ValueError(f"risk.sessions: unknown sessions {bad}")
        h, m = str(raw["day_reset_utc"]).split(":")
        if not (0 <= int(h) < 24 and 0 <= int(m) < 60):
            raise ValueError("risk.day_reset_utc: HH:MM")
        return cls(pct["risk_per_trade_pct"], pct["max_daily_loss_pct"], pct["max_drawdown_pct"], int(raw["max_positions"]),
                   Decimal(str(raw["max_exposure_lots"])), int(raw["max_consecutive_losses"]), int(raw["max_spread_points"]),
                   int(raw["max_slippage_points"]), int(raw["max_avg_slippage_points"]), int(raw["slippage_window"]), ses,
                   True, str(raw["day_reset_utc"]))  # fmt: skip


def _merge(name: str, default: dict[str, Any], user: dict[str, Any] | None) -> dict[str, Any]:
    u = {k: v for k, v in (user or {}).items() if not k.startswith("_")}
    bad = sorted(set(u) - set(default))
    if bad:
        raise ValueError(f"{name}: unknown keys {bad}")
    return {**default, **u}
