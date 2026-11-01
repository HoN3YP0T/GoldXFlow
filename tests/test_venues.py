"""Phase 15 tests: venue identity (Binance unchanged), mapping records and venue configs, OKX import (UTC+8 archive
days, contiguous-id completeness, PARTIAL hours, UNVERIFIED and MISSING days, immutable raw files), Bybit import (text
timestamps to ms, derived ids, RPI counts, UNVERIFIED), the Phase 3 validator on venue data, the unchanged stack on a
venue's data (reconciled; another venue's trades refused), basis and the labelled ounce aggregate on hand-computed
data, honest capabilities, and the providers report. All archive files here are SYNTHETIC, built in the test."""

import copy, gzip, io, json, os, sys, zipfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_processing import MS, REAL_CFG  # noqa: E402

from goldflow import schemas as S  # noqa: E402
from goldflow.config import Config  # noqa: E402
from goldflow.normalize import const_cols, part_path  # noqa: E402
from goldflow.processing import TradeProcessor  # noqa: E402
from goldflow.processing.settings import ProcessingSettings  # noqa: E402
from goldflow.providers import CapabilityUnavailable, create_provider  # noqa: E402
from goldflow.settings import ConfigError, validate_config  # noqa: E402
from goldflow.venues import AGG_LABEL, basis, mappings, providers_report, venue_config, venue_import, venue_run, volume_oz  # noqa: E402
from goldflow.venues.archives import _ms_from_seconds_text, archive_name  # noqa: E402
from goldflow.venues.ingest import catalog  # noqa: E402

H = 3_600_000


def cfg_at(tmp):
    raw = copy.deepcopy(REAL_CFG)
    raw["paths"] = {"data_root": "data", "state_db": "data/state/acquisition.sqlite"}
    (tmp / "data" / "state").mkdir(parents=True, exist_ok=True)
    return Config(raw, tmp)


def okx_zip(day, rows):
    buf = io.StringIO()
    buf.write("instrument_name,trade_id,side,price,size,created_time,source\n")
    for i, side, px, sz, t, src in rows:
        buf.write(f"XAU-USDT-SWAP,{i},{side},{px},{sz},{t},{src}\n")
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        z.writestr(f"XAU-USDT-SWAP-trades-{day}.csv", buf.getvalue())
    return out.getvalue()


def bybit_gz(rows):
    buf = io.StringIO()
    buf.write("timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional,RPI\n")
    for k, (ts, side, sz, px, rpi) in enumerate(rows):
        buf.write(f"{ts},XAUUSDT,{side},{sz},{px},ZeroPlusTick,uuid-{k},0,{sz},{float(sz) * float(px)!r},{rpi}\n")
    return gzip.compress(buf.getvalue().encode())


class Fetch:
    def __init__(self, files):
        self.files, self.calls = files, []

    def __call__(self, url):
        self.calls.append(url)
        return (self.files[url], {"etag": "x"}) if url in self.files else None


def okx_archive(t0, t1, step_ms=60_000, gap=None, start_id=1000):
    """SYNTHETIC OKX trades every step_ms in [t0, t1), split into UTC+8 archive days; ids contiguous except `gap`."""
    ts = np.arange(t0, t1, step_ms, dtype=np.int64)
    rng = np.random.default_rng(3)
    px = 40000 + np.cumsum(rng.integers(-2, 3, len(ts)))  # ticks of 0.1
    files, i = {}, start_id
    for k, t in enumerate(ts.tolist()):
        if gap and gap[0] <= t < gap[1]:
            i += 1
            continue
        day = str(np.datetime64(t + 8 * H, "ms").astype("datetime64[D]"))
        files.setdefault(day, []).append(
            (i, "buy" if k % 3 else "sell", f"{px[k] / 10:.1f}", str(1 + k % 7), t, "1" if k % 10 == 0 else "0")
        )
        i += 1
    return {archive_name("okx", "XAU-USDT-SWAP", d)[0]: okx_zip(d, r) for d, r in files.items()}, files


# ============================================================================ identity, mapping, venue config
def test_identity_is_per_venue_and_binance_is_unchanged(tmp_path):
    assert S.instrument_id("XAUUSDT") == "binance:usdm_perpetual:XAUUSDT"
    assert S.instrument_id("XAU-USDT-SWAP", "okx") == "okx:usdt_swap:XAU-USDT-SWAP"
    assert "provider=binance/market=usdm_perpetual" in str(part_path(tmp_path, "XAUUSDT", "trades", "2026-10-23"))
    assert "provider=bybit/market=usdt_perpetual" in str(part_path(tmp_path, "XAUUSDT", "trades", "2026-10-23", provider="bybit"))
    c = const_cols(2, "XAUUSDT", "bybit")
    assert c["provider_id"].to_pylist() == ["bybit"] * 2 and c["venue_id"].to_pylist() == ["bybit-linear"] * 2
    cfg = cfg_at(tmp_path)
    b, y = ProcessingSettings.from_config(cfg), ProcessingSettings.from_config(venue_config(cfg, "bybit"))
    assert b.instrument_id == "binance:usdm_perpetual:XAUUSDT" and y.instrument_id == "bybit:usdt_perpetual:XAUUSDT"
    assert b.hash != y.hash  # same symbol, tick and step, but another venue's state can never be restored here


def test_mappings_and_venue_config(tmp_path, monkeypatch):
    cfg = cfg_at(tmp_path)
    m = mappings(cfg)
    assert str(m["okx"].oz_per_lot) == "1/1000" and m["okx"].tick_size == "0.1" and m["okx"].archive_day_utc_offset_h == 8
    assert m["bybit"].row()["oz_per_lot"] == "0.001" and m["bitget"].oz_per_lot == pytest.approx(0.01)
    v = venue_config(cfg, "okx")
    assert (v["source"], v.symbol, v["instrument"]["tick_size"]) == ("okx", "XAU-USDT-SWAP", "0.1")
    assert v.state_db.name == "okx.sqlite" and v.state_db.parent.name == "venues" and v.data_root == cfg.data_root
    assert [k for k, s in v["replay"]["streams"].items() if s["enabled"]] == ["trades"]
    monkeypatch.setenv("GOLDFLOW_STATE_DB", str(tmp_path / "elsewhere" / "acq.sqlite"))
    assert venue_config(cfg, "okx").state_db == (tmp_path / "elsewhere" / "venues" / "okx.sqlite").resolve()
    for bad, msg in (
        ({"okx": {"tick": "0.1"}}, "unknown keys"),
        ({"kraken": {}}, "unknown venues"),
        ({"okx": {"tick_size": "0"}}, "> 0"),
        ({"basis": {"grid_ms": 10}}, "grid_ms"),
    ):
        r = copy.deepcopy(REAL_CFG)
        r["venues"].update(bad)
        with pytest.raises(ConfigError, match=msg):
            validate_config(r, None, check_discovery=False)


# ============================================================================ OKX import
def test_okx_import_utc8_days_completeness_and_raw_files(tmp_path):
    cfg = cfg_at(tmp_path)
    t0, t1 = MS("2026-10-22T16:00:00.000Z"), MS("2026-10-25T16:00:00.000Z")
    gap = (MS("2026-10-24T05:10:00.000Z"), MS("2026-10-24T05:13:00.000Z"))  # 3 trades missing (ids skipped)
    files, by_file = okx_archive(t0, t1, gap=gap)
    f = Fetch(files)
    out = venue_import(cfg, "okx", ["2026-10-23", "2026-10-24", "2026-10-25", "2026-10-26"], fetch=f)
    d = out["days"]
    assert d["2026-10-23"]["status"] == "COMPLETE" and d["2026-10-23"]["trades"] == 1440
    assert (
        d["2026-10-24"]["status"] == "PARTIAL" and d["2026-10-24"]["trades"] == 1437 and d["2026-10-24"]["id_breaks"][0][2] == 3
    )
    assert d["2026-10-25"]["status"] == "UNVERIFIED"  # archive day 26 (UTC+8) not published
    assert d["2026-10-26"]["status"] == "MISSING" and d["2026-10-26"]["trades"] == 0
    vc = venue_config(cfg, "okx")
    db = catalog(vc)
    assert db.execute("SELECT hour_utc, status FROM hour_status").fetchall() == [("2026-10-24T05:00:00Z", "PARTIAL")]
    db.close()
    t = pq.read_table(part_path(cfg.data_root, "XAU-USDT-SWAP", "trades", "2026-10-23", provider="okx"))
    assert set(t.column("instrument_id").to_pylist()) == {"okx:usdt_swap:XAU-USDT-SWAP"} and set(
        t.column("venue_id").to_pylist()
    ) == {"okx-swap"}
    ts = t.column("exchange_event_time_utc").cast(pa.int64()).to_numpy()
    assert ts.min() == MS("2026-10-23T00:00:00.000Z") and ts.max() < MS(
        "2026-10-24T00:00:00.000Z"
    )  # UTC day from two UTC+8 files
    rows = [
        r
        for day_rows in by_file.values()
        for r in day_rows
        if MS("2026-10-23T00:00:00.000Z") <= r[4] < MS("2026-10-24T00:00:00.000Z")
    ]
    assert t.column("price_ticks").to_pylist() == [round(float(r[2]) * 10) for r in rows]  # tick 0.1
    assert t.column("quantity_lots").to_pylist() == [int(r[3]) for r in rows]  # contracts = lots of 0.001 oz
    assert t.column("aggressor_side").to_pylist() == [("SELL" if r[1] == "sell" else "BUY") for r in rows]
    assert d["2026-10-23"]["rpi_trades"] == sum(1 for r in rows if r[5] == "1")
    assert d["2026-10-23"]["oz"] == pytest.approx(sum(int(r[3]) for r in rows) / 1000)
    # raw files are kept exactly, never fetched twice, and a changed raw file is refused
    raw = sorted((cfg.data_root / "raw" / "okx").rglob("*.zip"))
    assert len(raw) == 3 and all(json.loads(Path(str(p) + ".meta.json").read_text())["size"] == p.stat().st_size for p in raw)
    n = len(f.calls)
    out2 = venue_import(cfg, "okx", ["2026-10-23"], fetch=f)
    assert len(f.calls) == n + 0 and out2["days"]["2026-10-23"]["logical_digest"] == d["2026-10-23"]["logical_digest"]
    raw[0].write_bytes(raw[0].read_bytes() + b"x")
    with pytest.raises(RuntimeError, match="immutable"):
        venue_import(cfg, "okx", ["2026-10-23"], fetch=f)


def test_okx_rejects_off_tick_prices_and_a_wrong_instrument(tmp_path):
    cfg = cfg_at(tmp_path)
    d0 = MS("2026-10-23T00:00:00.000Z")
    rows = [
        (1, "buy", "4000.1", "2", d0 + 1000, "0"),
        (2, "buy", "4000.15", "2", d0 + 2000, "0"),
        (3, "sell", "4000.2", "1", d0 + 3000, "0"),
    ]
    files = {
        archive_name("okx", "XAU-USDT-SWAP", "2026-10-23")[0]: okx_zip("2026-10-23", rows),
        archive_name("okx", "XAU-USDT-SWAP", "2026-10-24")[0]: okx_zip(
            "2026-10-24", [(4, "buy", "4000.2", "1", d0 + 20 * H, "0")]
        ),
    }
    out = venue_import(cfg, "okx", ["2026-10-23"], fetch=Fetch(files))
    day = out["days"]["2026-10-23"]
    assert (
        day["trades"] == 3 and day["rejected"] == 1 and day["status"] == "PARTIAL"
    )  # 2 + 1 from the next UTC+8 file; the rejected id leaves a hole
    bad = {}
    z = io.BytesIO()
    with zipfile.ZipFile(z, "w") as zz:
        zz.writestr(
            "f.csv", "instrument_name,trade_id,side,price,size,created_time,source\nBTC-USDT-SWAP,1,buy,1.0,1,1792713601000,0\n"
        )
    bad[archive_name("okx", "XAU-USDT-SWAP", "2026-10-23")[0]] = z.getvalue()
    with pytest.raises(ValueError, match="instruments"):
        venue_import(cfg_at(tmp_path / "b"), "okx", ["2026-10-23"], fetch=Fetch(bad))


# ============================================================================ Bybit import
def test_bybit_text_timestamps_derived_ids_rpi_and_unverified(tmp_path):
    assert _ms_from_seconds_text(pa.array(["1792713600.26", "1792713600.0372", "1792713600", "1792713600.9999"])).tolist() == [
        1792713600260,
        1792713600037,
        1792713600000,
        1792713600999,
    ]
    cfg = cfg_at(tmp_path)
    d0 = MS("2026-10-23T00:00:00.000Z") // 1000
    rows = [(f"{d0 + k}.{k % 10}", "Buy" if k % 2 else "Sell", "0.004", "4032.62", k % 5 == 0) for k in range(200)]
    rows.append((f"{d0 + 300}.5", "Buy", "0.004", "4032.625", 0))  # off tick: rejected
    out = venue_import(
        cfg,
        "bybit",
        ["2026-10-23"],
        fetch=Fetch(
            {archive_name("bybit", "XAUUSDT", "2026-10-23")[0]: bybit_gz([(a, b, c, d, int(e)) for a, b, c, d, e in rows])}
        ),
    )
    day = out["days"]["2026-10-23"]
    assert day["status"] == "UNVERIFIED" and day["trades"] == 200 and day["rejected"] == 1 and day["rpi_trades"] == 40
    t = pq.read_table(part_path(cfg.data_root, "XAUUSDT", "trades", "2026-10-23", provider="bybit"))
    ids = np.array(t.column("source_trade_id").to_pylist())
    assert (np.diff(ids) > 0).all() and ids[0] == 20749 * 10**7
    assert t.column("exchange_event_time_utc").cast(pa.int64()).to_pylist()[:3] == [
        d0 * 1000,
        (d0 + 1) * 1000 + 100,
        (d0 + 2) * 1000 + 200,
    ]
    assert t.column("aggressor_side").to_pylist()[:2] == ["SELL", "BUY"]


# ============================================================================ the unchanged stack on a venue
def test_venue_run_uses_the_unchanged_stack_and_reconciles(tmp_path):
    cfg = cfg_at(tmp_path)
    files, _ = okx_archive(MS("2026-10-22T16:00:00.000Z"), MS("2026-10-25T16:00:00.000Z"), step_ms=20_000)
    venue_import(cfg, "okx", ["2026-10-23", "2026-10-24"], fetch=Fetch(files))
    out = venue_run(cfg, "okx", "t1")
    assert (
        out["status"] == "completed" and out["reconciliation"]["equal"] and out["reconciliation"]["partition_trades"] == 2 * 4320
    )
    assert (
        out["instrument_id"] == "okx:usdt_swap:XAU-USDT-SWAP"
        and out["tables"]["bars_1m"] == 2880
        and out["tables"]["profile_final"] > 0
    )
    assert (Path(out["output"]) / "bars_1m.parquet").exists() and "provider=okx" in out["output"]
    assert out["oz"] == pytest.approx(out["reconciliation"]["bar_lots"] / 1000)
    # a processor for this venue refuses Binance trades: markets are never merged
    proc = TradeProcessor(venue_config(cfg, "okx"), sinks=[])
    n = 3
    tb = pa.table(
        {
            **const_cols(n, "XAUUSDT"),
            "exchange_event_time_utc": pa.array([1, 2, 3], pa.int64()),
            "price_ticks": [1, 2, 3],
            "quantity_lots": [1, 1, 1],
            "source_trade_id": [1, 2, 3],
            "buyer_maker_flag": [True, False, True],
            "aggressor_side": ["SELL", "BUY", "SELL"],
            "record_origin": ["ARCHIVE_TRADES"] * 3,
        }
    )
    with pytest.raises(ValueError, match="markets are never merged"):
        proc._columns(tb)


# ============================================================================ cross-venue series
def _venue_table(tmp, cfg, provider, sym, times, ticks, lots):
    from goldflow.normalize import build_table, write_atomic

    n = len(times)
    cols = {
        **const_cols(n, sym, provider),
        "source_trade_id": np.arange(n),
        "aggregate_trade_id": pa.nulls(n, pa.int64()),
        "exchange_event_time_utc": np.array(times, np.int64),
        "transaction_time_utc": np.array(times, np.int64),
        "receive_time_utc": pa.nulls(n, S.TS),
        "price": np.zeros(n),
        "quantity": np.zeros(n),
        "quote_notional": np.zeros(n),
        "buyer_maker_flag": np.zeros(n, bool),
        "aggressor_side": np.full(n, "BUY"),
        "side_evidence": np.full(n, "EXCHANGE_REPORTED"),
        "price_ticks": np.array(ticks, np.int64),
        "quantity_lots": np.array(lots, np.int64),
        "record_origin": np.full(n, "ARCHIVE_TRADES"),
        "data_quality_status": np.full(n, "VALID"),
        "raw_file_id": np.zeros(n, np.int32),
        "raw_row": np.arange(n),
    }
    write_atomic(build_table(S.TRADE_SCHEMA, cols), part_path(cfg.data_root, sym, "trades", "2026-10-23", provider=provider))
    vc = venue_config(cfg, provider)
    db = catalog(vc) if provider != "binance" else __import__("sqlite3").connect(str(vc.state_db))
    db.executescript(
        "CREATE TABLE IF NOT EXISTS day_status(dataset TEXT, date TEXT, status TEXT, evidence_json TEXT);"
        "CREATE TABLE IF NOT EXISTS hour_status(dataset TEXT, hour_utc TEXT, status TEXT, detail_json TEXT);"
    )
    db.execute(
        "INSERT INTO day_status VALUES('trades','2026-10-23',?, '{}')", ("UNVERIFIED" if provider == "bybit" else "COMPLETE",)
    )
    db.commit()
    db.close()


def test_basis_exact_asof_with_staleness_and_labelled_volume(tmp_path):
    cfg = cfg_at(tmp_path)
    d0 = MS("2026-10-23T00:00:00.000Z")
    _venue_table(
        tmp_path,
        cfg,
        "binance",
        "XAUUSDT",
        [d0 + 500, d0 + 1000, d0 + 1500, d0 + 60_500],
        [400010, 400020, 400030, 400040],
        [5, 7, 9, 11],
    )
    _venue_table(tmp_path, cfg, "okx", "XAU-USDT-SWAP", [d0 + 900, d0 + 2100], [40001, 40003], [1000, 3])  # tick 0.1
    _venue_table(tmp_path, cfg, "bybit", "XAUUSDT", [d0 + 100], [400000], [2])
    r = basis(cfg, "binance", "okx", ["2026-10-23"])
    t = pq.read_table(
        cfg.data_root / "derived" / "cross_venue" / "basis" / "pair=binance-okx" / "date=2026-10-23" / "basis.parquet"
    )
    g = t.column("grid_time").cast(pa.int64()).to_pylist()
    # g=1000: binance last before 1000 is 400010 (t=500; t=1000 is not strictly before), okx 40001*10 -> basis 0
    # g=2000: binance 400030 (t=1500), okx 400010 (t=900) -> +20 units (0.20 USDT); g=3000: okx 2100 -> 400030 -> 0
    assert g[:3] == [d0 + 1000, d0 + 2000, d0 + 3000] and t.column("basis_units").to_pylist()[:3] == [0, 20, 0]
    assert t.column("age_b_ms").to_pylist()[:3] == [100, 1100, 900]
    assert g[-1] == d0 + 6000  # binance last trade 1500 + max age 5 s: no value after 6.5 s
    assert r["unit_usdt"] == "0.01" and r["points"] == 6 and t.schema.metadata[b"goldflow.unit_usdt"] == b"0.01"
    v = volume_oz(cfg, ["binance", "okx", "bybit"], ["2026-10-23"])
    assert v["oz_by_venue"] == {"binance": pytest.approx(0.032), "okx": pytest.approx(1.003), "bybit": pytest.approx(0.002)}
    assert v["total_oz"] == pytest.approx(1.037) and v["minutes_all_complete"] == 0 and v["label"] == AGG_LABEL
    vt = pq.read_table(
        cfg.data_root / "derived" / "cross_venue" / "volume_oz" / "interval=1m" / "date=2026-10-23" / "volume_oz.parquet"
    )
    assert vt.schema.metadata[b"goldflow.label"].decode() == AGG_LABEL
    assert (
        vt.column("binance_oz_units").to_pylist()[:2] == [21, 11] and vt.column("total_oz_units").to_pylist()[0] == 21 + 1003 + 2
    )
    assert vt.column("bybit_status").to_pylist()[0] == "UNVERIFIED" and vt.column("all_complete").to_pylist()[0] is False


# ============================================================================ capabilities and report
def test_capabilities_are_honest_and_report_lists_every_provider(tmp_path):
    cfg = cfg_at(tmp_path)
    with pytest.raises(CapabilityUnavailable, match="historical_trades"):
        list(create_provider(cfg, provider_id="bitget").historical_trades("XAUUSDT", 0, 1))
    with pytest.raises(CapabilityUnavailable, match="instrument_discovery"):
        create_provider(cfg, provider_id="bybit").discover_instrument("XAUUSDT")
    files, _ = okx_archive(MS("2026-10-22T16:00:00.000Z"), MS("2026-10-24T16:00:00.000Z"))
    venue_import(cfg, "okx", ["2026-10-23"], fetch=Fetch(files))
    rep = providers_report(cfg, probe=False)
    assert set(rep["providers"]) == {"binance", "okx", "bybit", "bitget"}
    ok = rep["providers"]["okx"]
    assert (
        ok["coverage"]["by_status"] == {"COMPLETE": 1}
        and ok["coverage"]["trades"] == 1440
        and ok["capabilities"]["trade_sequence"]["availability"] == "AVAILABLE"
    )
    md = (cfg.data_root / "reports" / "providers.md").read_text()
    assert "okx:usdt_swap:XAU-USDT-SWAP" in md and "GEO_RESTRICTED" in md and "nothing below is merged" in md
    assert os.path.exists(cfg.data_root / "reports" / "providers.json")


def test_phase3_raw_registry_ignores_other_venues_raw_files(tmp_path):
    """Regression (found in Phase 17): Phase 3 registered every zip under data/raw and refused to run once OKX raw
    archives existed. It must register only its own provider's tree, and still flag a stray zip inside it."""
    from goldflow.catalog import Catalog
    from goldflow.checkpoint import Checkpoint
    cfg = cfg_at(tmp_path)
    Checkpoint(cfg.state_db)
    files, _ = okx_archive(MS("2026-10-22T16:00:00.000Z"), MS("2026-10-24T16:00:00.000Z"))
    venue_import(cfg, "okx", ["2026-10-23"], fetch=Fetch(files))
    assert list((cfg.data_root / "raw" / "okx").rglob("*.zip"))
    cat = Catalog(cfg)
    assert cat.register_raw(make_readonly=False)["unaccounted"] == []
    stray = cfg.data_root / "raw" / "binance" / "futures_um" / "XAUUSDT" / "trades" / "daily" / "stray.zip"
    stray.parent.mkdir(parents=True)
    stray.write_bytes(b"PK")
    assert cat.register_raw(make_readonly=False)["unaccounted"] == [str(stray)]
