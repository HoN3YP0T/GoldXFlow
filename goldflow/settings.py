"""Validated configuration (Phase 1).

`validate_config(raw)` checks every block of the JSON config with pydantic models plus cross-field rules and raises
`ConfigError` listing *all* problems at once. `Config.load()` calls it, so an invalid or conflicting configuration
stops the program at startup with a clear message instead of failing deep inside a run.

Defaults for blocks that are absent from the JSON file (profile/execution) live here, not in the file, so adding
them does not change the config hash recorded by existing datasets and checkpoints.
"""

from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

DAY_MS = 86_400_000


class ConfigError(ValueError):
    """Raised when the configuration is invalid; the message lists every problem found."""

    def __init__(self, problems: list[str]):
        self.problems = problems
        super().__init__("invalid configuration:\n  - " + "\n  - ".join(problems))


class Profile(str, Enum):
    RESEARCH = "research"  # history, analytics, backtests; no order routing
    TEST = "test"  # automated tests; synthetic providers allowed
    LIVE = "live"  # order routing; locked until the Phase 16 demo gate (M7) is recorded


class _Strict(BaseModel):
    # Unknown keys are allowed (documentation keys such as "_doc"/"why" exist in the file) but typed keys are checked.
    model_config = ConfigDict(extra="allow", frozen=True)


def _decimal_str(v: str, name: str) -> str:
    try:
        d = Decimal(v)
    except (InvalidOperation, TypeError) as e:
        raise ValueError(f"{name} must be a decimal string, got {v!r}") from e
    if d <= 0:
        raise ValueError(f"{name} must be > 0, got {v!r}")
    return v


class RangeCfg(_Strict):
    months: int = Field(ge=1, le=36)
    start: str | None = None
    end: str | None = None


class DatasetCfg(_Strict):
    monthly: bool
    daily: bool
    rest_tail: bool = False
    role: Literal["primary", "secondary", "cross_check", "depth_snapshot", "context"]
    prefer_daily: bool = False

    @model_validator(mode="after")
    def _granularity(self) -> "DatasetCfg":
        if not (self.monthly or self.daily):
            raise ValueError("at least one of monthly/daily must be true")
        if self.prefer_daily and not self.daily:
            raise ValueError("prefer_daily requires daily=true")
        return self


class PathsCfg(_Strict):
    data_root: str
    state_db: str

    @field_validator("data_root", "state_db")
    @classmethod
    def _relative(cls, v: str) -> str:
        if Path(v).is_absolute() or ".." in Path(v).parts:
            raise ValueError(f"path must be relative to the project root without '..': {v!r}")
        return v


class ArchiveCfg(_Strict):
    base_url: str
    list_url: str
    max_workers: int = Field(ge=1, le=32)
    max_attempts: int = Field(ge=1, le=20)
    backoff_base_s: float = Field(gt=0, le=60)
    backoff_cap_s: float = Field(gt=0, le=600)
    timeout_s: float = Field(gt=0, le=600)
    chunk_bytes: int = Field(ge=4096, le=64 << 20)

    @field_validator("base_url", "list_url")
    @classmethod
    def _https(cls, v: str) -> str:
        if not v.startswith("https://"):
            raise ValueError(f"must be an https URL: {v!r}")
        return v


class RestCfg(_Strict):
    base_url: str
    weight_limit_per_min: int = Field(ge=1)
    weight_budget_fraction: float = Field(gt=0, le=1)
    max_attempts: int = Field(ge=1, le=20)
    backoff_base_s: float = Field(gt=0)
    backoff_cap_s: float = Field(gt=0)
    timeout_s: float = Field(gt=0, le=300)
    api_key_env: str

    @field_validator("base_url")
    @classmethod
    def _https(cls, v: str) -> str:
        if not v.startswith("https://"):
            raise ValueError(f"must be an https URL: {v!r}")
        return v

    @field_validator("api_key_env")
    @classmethod
    def _env_name(cls, v: str) -> str:
        if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", v):
            raise ValueError("api_key_env must be an environment variable NAME (the key itself never goes in the config)")
        return v


class InstrumentCfg(_Strict):
    tick_size: str
    step_size: str
    onboard_ms: int = Field(gt=1_500_000_000_000)

    @field_validator("tick_size", "step_size")
    @classmethod
    def _dec(cls, v: str, info: Any) -> str:
        return _decimal_str(v, info.field_name)


class StreamCfg(_Strict):
    enabled: bool
    time_col: str
    time_offset_ms: int = Field(ge=0)
    latency_ms: int = Field(ge=0)
    rank: int = Field(ge=1, le=126)

    @model_validator(mode="after")
    def _shift(self) -> "StreamCfg":
        if self.time_offset_ms + self.latency_ms >= DAY_MS:
            raise ValueError("time_offset_ms + latency_ms must be < 1 day")
        return self


class ReplayCfg(_Strict):
    streams: dict[str, StreamCfg]
    quality_events: bool
    max_events_per_chunk: int = Field(ge=1, le=10_000_000)
    checkpoint_every_events: int = Field(ge=1)
    paced_chunk_sim_ms: int = Field(ge=1)


class DepthCfg(_Strict):
    """Phase 8 live price-level book recorder. Defaults live here (not in the JSON file) so the config hash is unchanged.

    Endpoints are configurable because Binance has changed WebSocket routing before: re-verify them on the recording host.
    The RPI stream/endpoint names are unverified and therefore disabled by default."""

    ws_base_url: str = "wss://fstream.binance.com/stream"
    stream: str = "{symbol}@depth@100ms"
    snapshot_path: str = "/fapi/v1/depth"
    snapshot_limit: Literal[5, 10, 20, 50, 100, 500, 1000] = 1000
    snapshot_min_interval_s: float = Field(default=2.0, ge=0, le=600)
    rpi_enabled: bool = False
    rpi_stream: str = "{symbol}@rpiDepth@500ms"
    rpi_snapshot_path: str = "/fapi/v1/rpiDepth"
    max_buffer_events: int = Field(default=20_000, ge=10, le=10_000_000)
    invalidate_on_crossed_book: bool = True
    reconnect_base_s: float = Field(default=1.0, gt=0, le=60)
    reconnect_cap_s: float = Field(default=60.0, gt=0, le=3600)
    max_connection_s: float = Field(default=23 * 3600 + 50 * 60, ge=60, le=24 * 3600)
    flush_every_s: float = Field(default=1.0, ge=0, le=60)
    status_every_s: float = Field(default=60.0, gt=0)
    metrics_cadence_ms: int = Field(default=1_000, ge=100)
    flow_window_ms: int = Field(default=60_000, ge=1_000)
    touch_ticks: int = Field(default=10, ge=0)
    tick_bands: list[int] = Field(default_factory=lambda: [10, 50, 100, 500])
    pct_bands: list[str] = Field(default_factory=lambda: ["0.1", "0.25", "0.5", "1.0"])
    top_k: int = Field(default=5, ge=1)

    @field_validator("ws_base_url")
    @classmethod
    def _wss(cls, v: str) -> str:
        if not v.startswith("wss://"):
            raise ValueError(f"must be a wss:// URL: {v!r}")
        return v.rstrip("/")

    @field_validator("stream", "rpi_stream")
    @classmethod
    def _stream(cls, v: str) -> str:
        if "{symbol}" not in v:
            raise ValueError("stream must contain the {symbol} placeholder")
        return v

    @field_validator("snapshot_path", "rpi_snapshot_path")
    @classmethod
    def _path(cls, v: str) -> str:
        if not v.startswith("/fapi/"):
            raise ValueError(f"must be a /fapi/ REST path: {v!r}")
        return v

    @field_validator("tick_bands")
    @classmethod
    def _bands(cls, v: list[int]) -> list[int]:
        if any(x <= 0 for x in v):
            raise ValueError("tick bands must be > 0")
        return v

    @field_validator("pct_bands")
    @classmethod
    def _pct(cls, v: list[str]) -> list[str]:
        for x in v:
            try:
                d = Decimal(x)
            except InvalidOperation as e:
                raise ValueError(f"pct band {x!r} is not a decimal string") from e
            if not (0 < d <= 50):
                raise ValueError(f"pct band {x!r} must be in (0, 50]")
        return v

    @model_validator(mode="after")
    def _backoff(self) -> "DepthCfg":
        if self.reconnect_cap_s < self.reconnect_base_s:
            raise ValueError("reconnect_cap_s must be >= reconnect_base_s")
        return self

    def streams(self, symbol: str) -> dict[str, str]:
        """Combined-stream name -> book kind."""
        out = {self.stream.format(symbol=symbol.lower()): "regular"}
        if self.rpi_enabled:
            out[self.rpi_stream.format(symbol=symbol.lower())] = "rpi"
        return out

    def url(self, symbol: str) -> str:
        return f"{self.ws_base_url}?streams=" + "/".join(self.streams(symbol))


class ExecutionCfg(_Strict):
    """Order routing. Off by default; the live profile additionally requires the recorded M7 demo gate."""

    live_trading_enabled: bool = False


class Settings(_Strict):
    source: Literal["binance", "dummy"]
    market: Literal["futures_um", "futures_cm", "spot"]
    symbol: str = Field(pattern=r"^[A-Z0-9]{3,20}$")
    expected_contract_type: str
    expected_underlying_type: str
    range: RangeCfg
    datasets: dict[str, DatasetCfg]
    paths: PathsCfg
    archive: ArchiveCfg
    rest: RestCfg
    instrument: InstrumentCfg
    replay: ReplayCfg
    execution: ExecutionCfg = ExecutionCfg()
    depth: DepthCfg = DepthCfg()

    @model_validator(mode="after")
    def _cross(self) -> "Settings":
        problems: list[str] = []
        known = {"trades", "aggTrades", "bookDepth", "metrics"}
        for ds in self.datasets:
            if ds not in known and not re.fullmatch(r"klines_(1m|3m|5m|15m|30m|1h|2h|4h|6h|8h|12h|1d)", ds):
                problems.append(f"datasets.{ds}: unknown dataset name")
        roles = [d.role for d in self.datasets.values()]
        if roles.count("primary") != 1:
            problems.append(f"datasets: exactly one dataset must have role 'primary' (found {roles.count('primary')})")
        if self.datasets.get("trades") and self.datasets["trades"].role != "primary":
            problems.append("datasets.trades must be the primary dataset (individual executions are the source of truth)")
        ranks = [s.rank for s in self.replay.streams.values() if s.enabled]
        if len(set(ranks)) != len(ranks):
            problems.append(f"replay.streams: enabled stream ranks must be unique, got {sorted(ranks)}")
        # time columns must exist in the canonical schema of the stream
        try:
            from .schemas import SCHEMAS

            for name, s in self.replay.streams.items():
                sch = SCHEMAS.get(name)
                if sch is None:
                    problems.append(f"replay.streams.{name}: no canonical schema with this name")
                elif s.time_col not in sch.names:
                    problems.append(f"replay.streams.{name}.time_col {s.time_col!r} is not a column of {name}")
        except ImportError:  # pragma: no cover - schemas always importable inside the package
            pass
        if self.archive.backoff_cap_s < self.archive.backoff_base_s:
            problems.append("archive.backoff_cap_s must be >= archive.backoff_base_s")
        if self.rest.backoff_cap_s < self.rest.backoff_base_s:
            problems.append("rest.backoff_cap_s must be >= rest.backoff_base_s")
        if problems:
            raise ValueError("; ".join(problems))
        return self


def validate_config(raw: dict[str, Any], base_dir: Path | None = None, check_discovery: bool = True) -> Settings:
    """Validate a raw config dict. Optionally cross-checks the instrument spec against the last live discovery
    (data/state/instrument_discovery.json) so a tick/step change on the exchange is caught before data is processed."""
    try:
        s = Settings.model_validate(raw)
    except ValidationError as e:
        problems = []
        for err in e.errors():
            loc = ".".join(str(p) for p in err["loc"]) or "(root)"
            problems.append(f"{loc}: {err['msg']}")
        raise ConfigError(problems) from None
    if raw.get("processing") is not None or raw.get("sessions") is not None:
        # Phase 5 blocks: intervals, bins, session calendar (zoneinfo, DST gaps), alignment rules
        from .config import Config
        from .processing.settings import ProcessingSettings

        try:
            ProcessingSettings.from_config(Config(raw, base_dir or Path(".")))
        except (ValueError, KeyError) as e:
            raise ConfigError([f"processing/sessions: {e}"]) from None
    if raw.get("volume_profile") is not None:
        # Phase 6 block: bins, value-area rule, node parameters, VWAP bands, snapshot cadence, anchors
        from .config import Config
        from .orderflow.settings import ProfileSettings

        try:
            ProfileSettings.from_config(Config(raw, base_dir or Path(".")))
        except (ValueError, KeyError, TypeError) as e:
            raise ConfigError([f"volume_profile: {e}"]) from None
    if check_discovery and base_dir is not None:
        disc = base_dir / s.paths.data_root / "state" / "instrument_discovery.json"
        if disc.exists():
            try:
                spec = json.loads(disc.read_text()).get("rest", {}).get("spec") or {}
            except (OSError, ValueError):
                spec = {}
            problems = []
            if spec.get("tick_size") and Decimal(spec["tick_size"]) != Decimal(s.instrument.tick_size):
                problems.append(f"instrument.tick_size {s.instrument.tick_size} != exchange {spec['tick_size']}")
            if spec.get("step_size") and Decimal(spec["step_size"]) != Decimal(s.instrument.step_size):
                problems.append(f"instrument.step_size {s.instrument.step_size} != exchange {spec['step_size']}")
            if spec.get("onboard_date_ms") and int(spec["onboard_date_ms"]) != s.instrument.onboard_ms:
                problems.append(f"instrument.onboard_ms {s.instrument.onboard_ms} != exchange {spec['onboard_date_ms']}")
            if problems:
                raise ConfigError(problems + ["(spec drift: re-run `discover` and review before processing data)"])
    return s


def config_hash(raw: dict[str, Any]) -> str:
    """Stable hash of the configuration file content (same definition the Phase 3 catalog records)."""
    return hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()
