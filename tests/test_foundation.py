"""Phase 1 tests: validated settings, profiles/live lock, provider contract and registry, logging, metrics, migrations."""

from __future__ import annotations

import copy
import json
import logging
import shutil
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from goldflow import cli
from goldflow.config import Config
from goldflow.migrations import MIGRATIONS, Migration, MigrationError, apply_migrations, current_version
from goldflow.observability import JsonFormatter, Metrics, set_correlation_id
from goldflow.profiles import LiveTradingLocked, assert_profile_allowed, resolve_profile, trading_mode
from goldflow.providers import (
    Availability,
    Capability,
    CapabilityUnavailable,
    ProviderNotAllowed,
    ProviderUnavailable,
    available_providers,
    create_provider,
    register,
)
from goldflow.providers.base import Provider
from goldflow.providers.binance import BinanceUsdmProvider
from goldflow.rest_client import GeoBlocked
from goldflow.settings import ConfigError, Profile, config_hash, validate_config

ROOT = Path(__file__).resolve().parents[1]
RAW = json.loads((ROOT / "config/binance_xauusdt.json").read_text())


def bad(**mut: object) -> dict:  # type: ignore[type-arg]
    r = copy.deepcopy(RAW)
    for path, v in mut.items():
        node = r
        keys = path.split("__")
        for k in keys[:-1]:
            node = node[k]
        node[keys[-1]] = v
    return r


# ------------------------------------------------------------------ settings
def test_repo_config_is_valid_and_hash_is_stable() -> None:
    s = validate_config(RAW)
    assert s.symbol == "XAUUSDT" and s.execution.live_trading_enabled is False
    assert config_hash(RAW) == config_hash(json.loads(json.dumps(RAW)))
    assert "execution" not in RAW  # defaults live in code, so existing dataset/checkpoint hashes are unchanged


def test_field_errors_are_all_reported_together() -> None:
    with pytest.raises(ConfigError) as e:
        validate_config(
            bad(
                rest__weight_budget_fraction=1.5,
                instrument__tick_size="abc",
                replay__streams__trades__latency_ms=-5,
                archive__base_url="http://x",
            )
        )
    msg = str(e.value)
    for frag in ("weight_budget_fraction", "tick_size", "latency_ms", "base_url"):
        assert frag in msg
    assert len(e.value.problems) == 4


@pytest.mark.parametrize(
    "mut,frag",
    [
        ({"replay__streams__metrics__rank": 50}, "ranks must be unique"),
        ({"datasets__aggTrades__role": "primary"}, "exactly one dataset must have role 'primary'"),
        ({"replay__streams__trades__time_col": "nope"}, "is not a column of trades"),
        ({"datasets__aggTrades__daily": False, "datasets__aggTrades__prefer_daily": True}, "prefer_daily requires daily"),
        ({"rest__api_key_env": "abcd1234secret"}, "environment variable NAME"),
        ({"paths__data_root": "/abs/data"}, "relative"),
        ({"replay__streams__klines_1m__time_offset_ms": 86_400_000}, "< 1 day"),
    ],
)
def test_conflicting_settings_rejected(mut: dict, frag: str) -> None:  # type: ignore[type-arg]
    with pytest.raises(ConfigError, match=frag.replace("(", r"\(").replace(")", r"\)")):
        validate_config(bad(**mut))


def test_spec_drift_against_last_discovery_is_rejected(tmp_path: Path) -> None:
    (tmp_path / "data" / "state").mkdir(parents=True)
    (tmp_path / "data" / "state" / "instrument_discovery.json").write_text(
        json.dumps(
            {"rest": {"spec": {"tick_size": "0.1", "step_size": "0.001", "onboard_date_ms": RAW["instrument"]["onboard_ms"]}}}
        )
    )
    with pytest.raises(ConfigError, match="tick_size 0.01 != exchange 0.1"):
        validate_config(RAW, tmp_path)
    validate_config(RAW, tmp_path, check_discovery=False)


# ------------------------------------------------------------------ profiles / live lock
def test_profile_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GOLDFLOW_PROFILE", raising=False)
    assert resolve_profile() is Profile.RESEARCH
    monkeypatch.setenv("GOLDFLOW_PROFILE", "test")
    assert resolve_profile() is Profile.TEST and resolve_profile("LIVE") is Profile.LIVE
    with pytest.raises(ValueError, match="unknown profile"):
        resolve_profile("prod")
    assert trading_mode(Profile.RESEARCH).startswith("DISABLED")


def test_live_is_locked_until_enabled_and_gate_recorded(tmp_path: Path) -> None:
    s = validate_config(RAW)
    assert_profile_allowed(Profile.RESEARCH, s, tmp_path)
    with pytest.raises(LiveTradingLocked, match="live_trading_enabled is false.*M7"):
        assert_profile_allowed(Profile.LIVE, s, tmp_path)
    on = validate_config(bad(execution={"live_trading_enabled": True}))
    with pytest.raises(LiveTradingLocked, match="M7"):
        assert_profile_allowed(Profile.LIVE, on, tmp_path)
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "live_gates.json").write_text(json.dumps({"M7_demo_passed": True}))
    with pytest.raises(LiveTradingLocked):  # approval fields missing
        assert_profile_allowed(Profile.LIVE, on, tmp_path)
    (tmp_path / "state" / "live_gates.json").write_text(
        json.dumps({"M7_demo_passed": True, "approved_by": "operator", "approved_utc": "2027-01-01T00:00:00Z"})
    )
    assert_profile_allowed(Profile.LIVE, on, tmp_path)


# ------------------------------------------------------------------ CLI: starts validated, non-trading
def _project(tmp: Path, source: str = "dummy") -> Path:
    shutil.copytree(ROOT / "config", tmp / "config")
    c = json.loads((tmp / "config/binance_xauusdt.json").read_text())
    c["source"] = source
    (tmp / "config/binance_xauusdt.json").write_text(json.dumps(c))
    return tmp / "config/binance_xauusdt.json"


def test_cli_status_validates_and_reports_non_trading(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    cfgp = _project(tmp_path)
    assert cli.main(["status", "--config", str(cfgp), "--profile", "test"]) == 0
    out = capsys.readouterr().out
    st = json.loads(out[out.index("{") :])
    assert st["config"] == "VALID" and st["trading_mode"].startswith("DISABLED") and st["schema_migration_version"] == 1
    assert st["provider_health"]["state"] == "OK" and st["capabilities"]["historical_l2_depth"] == "UNAVAILABLE"
    log = (tmp_path / "data/state/goldflow.jsonl").read_text().strip().splitlines()
    rec = json.loads(log[-1])
    assert rec["profile"] == "test" and rec["correlation_id"].startswith("status-") and rec["event_id"].endswith(":1")


def test_cli_refuses_invalid_config_and_live(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    cfgp = _project(tmp_path)
    assert cli.main(["status", "--config", str(cfgp), "--profile", "live"]) == 2
    assert "live profile refused" in capsys.readouterr().err
    c = json.loads(cfgp.read_text())
    c["replay"]["streams"]["trades"]["rank"] = 10
    cfgp.write_text(json.dumps(c))
    assert cli.main(["status", "--config", str(cfgp)]) == 2
    assert "ranks must be unique" in capsys.readouterr().err


def test_cli_refuses_synthetic_provider_outside_test(tmp_path: Path) -> None:
    cfgp = _project(tmp_path)
    with pytest.raises(ProviderNotAllowed):
        cli.main(["status", "--config", str(cfgp), "--profile", "research"])


# ------------------------------------------------------------------ providers
def test_registry_and_synthetic_rule() -> None:
    assert {"binance", "dummy"} <= set(available_providers())
    cfg = Config(copy.deepcopy(RAW), ROOT)
    with pytest.raises(ProviderNotAllowed, match="only allowed in the 'test' profile"):
        create_provider(cfg, Profile.RESEARCH, "dummy")
    with pytest.raises(KeyError, match="unknown provider"):
        create_provider(cfg, Profile.TEST, "nope")
    with pytest.raises(ValueError, match="already registered"):
        register("binance")(type("X", (BinanceUsdmProvider,), {}))


def test_dummy_provider_is_deterministic_and_raw_schema() -> None:
    cfg = Config(copy.deepcopy(RAW), ROOT)
    p1, p2 = create_provider(cfg, Profile.TEST, "dummy"), create_provider(cfg, Profile.TEST, "dummy")
    b1 = list(p1.historical_trades("XAUUSDT", 1_000_000, 8_000_000))
    b2 = list(p2.historical_trades("XAUUSDT", 1_000_000, 8_000_000))
    assert [b.to_pydict() for b in b1] == [b.to_pydict() for b in b2] and sum(b.num_rows for b in b1) > 0
    assert b1[0].schema.names == ["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"]
    t = [x for b in b1 for x in b.column("time").to_pylist()]
    assert min(t) >= 1_000_000 and max(t) < 8_000_000 and p1.synthetic


class _FakeRest:
    base = "https://fapi.example"

    def __init__(self, geo: bool = False):
        self.geo = geo

    def symbol_spec(self, symbol: str) -> dict:  # type: ignore[type-arg]
        if self.geo:
            raise GeoBlocked("HTTP 451", 451)
        return {
            "symbol": symbol,
            "status": "TRADING",
            "contract_type": "TRADIFI_PERPETUAL",
            "underlying_type": "COMMODITY",
            "base_asset": "XAU",
            "quote_asset": "USDT",
            "tick_size": "0.01",
            "step_size": "0.001",
            "min_qty": "0.001",
            "min_notional": "5",
            "onboard_date_ms": 1765440300000,
        }

    def ping(self) -> dict:  # type: ignore[type-arg]
        if self.geo:
            raise GeoBlocked("HTTP 451", 451)
        return {}


class _FakeArchive:
    def __init__(self, keys: list[str]):
        self.keys = keys

    def list_prefix(self, prefix: str) -> dict:  # type: ignore[type-arg]
        return {k: None for k in self.keys if k.startswith(prefix)}


def test_binance_adapter_contract_without_network() -> None:
    from goldflow.vision_archive import archive_key

    keys = [archive_key("futures_um", "daily", "trades", "XAUUSDT", d) for d in ("2026-06-01", "2026-06-03")]
    p = BinanceUsdmProvider(_FakeRest(), _FakeArchive(keys))  # type: ignore[arg-type]
    spec = p.discover_instrument("XAUUSDT")
    assert spec.tick_size == "0.01" and spec.instrument_id == "binance:usdm_perpetual:XAUUSDT"
    caps = p.capabilities()
    assert caps.get(Capability.HISTORICAL_L2_DEPTH).availability is Availability.UNAVAILABLE
    assert caps.get(Capability.AGGRESSOR_SIDE).availability is Availability.EXCHANGE_REPORTED
    with pytest.raises(CapabilityUnavailable, match="historical_l2_depth"):
        list(p.historical_depth("XAUUSDT", 0, 1))
    # only published days are returned (2026-06-02 missing upstream -> not invented)
    got = p.historical_trade_keys("XAUUSDT", 1780272000000, 1780272000000 + 3 * 86_400_000)
    assert got == keys
    assert p.health().state == "OK"
    g = BinanceUsdmProvider(_FakeRest(geo=True), _FakeArchive([]))  # type: ignore[arg-type]
    with pytest.raises(ProviderUnavailable, match="451"):
        g.discover_instrument("XAUUSDT")
    assert g.health().state == "DOWN"


def test_contract_is_abstract() -> None:
    with pytest.raises(TypeError):
        Provider()  # type: ignore[abstract]


# ------------------------------------------------------------------ logging / metrics
def test_json_log_records_are_structured() -> None:
    set_correlation_id("run-x")
    rec = logging.LogRecord("goldflow.acquire", logging.INFO, __file__, 1, "downloaded %s", ("f.zip",), None)
    rec.unit = "archive:trades:daily:2026-06-01"
    a = json.loads(JsonFormatter().format(rec))
    b = json.loads(JsonFormatter().format(rec))
    assert a["message"] == "downloaded f.zip" and a["component"] == "goldflow.acquire" and a["severity"] == "INFO"
    assert a["correlation_id"] == "run-x" and a["unit"] == "archive:trades:daily:2026-06-01"
    assert a["ts"].endswith("Z") and a["event_id"] != b["event_id"]


def test_metrics_registry() -> None:
    m = Metrics()
    m.inc("events", 3, stream="trades")
    m.inc("events", 2, stream="trades")
    m.gauge("queue_depth", 7)
    m.observe("latency_s", 0.2)
    m.observe("latency_s", 0.4)
    s = m.snapshot()
    assert s["counters"] == [{"name": "events", "labels": {"stream": "trades"}, "value": 5.0}]
    assert s["gauges"][0]["value"] == 7 and s["timings"][0]["count"] == 2 and s["timings"][0]["max_s"] == 0.4


# ------------------------------------------------------------------ migrations
def test_migrations_apply_once_and_detect_tampering(tmp_path: Path) -> None:
    db = sqlite3.connect(tmp_path / "x.sqlite", isolation_level=None)
    assert apply_migrations(db) == [1] and apply_migrations(db) == [] and current_version(db) == 1
    m2 = MIGRATIONS + [Migration(2, "sqlite", "add example table", "CREATE TABLE example(x INTEGER)")]
    assert apply_migrations(db, m2) == [2]
    assert db.execute("SELECT name FROM sqlite_master WHERE name='example'").fetchone()
    tampered = MIGRATIONS + [Migration(2, "sqlite", "add example table", "CREATE TABLE example(y TEXT)")]
    with pytest.raises(MigrationError, match="modified after being applied"):
        apply_migrations(db, tampered)
    with pytest.raises(MigrationError, match="unknown to this code"):
        apply_migrations(db, MIGRATIONS)
    with pytest.raises(MigrationError, match="without gaps"):
        apply_migrations(sqlite3.connect(":memory:"), MIGRATIONS + [Migration(3, "sqlite", "gap")])
