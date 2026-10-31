"""Phase 14 tests: the live trade normaliser (Phase 3 validator, RPI counting), gap suspicion, merge_dq closing an
open interval, feed health (STALE opens with an open end, signals blocked, then closed), the live runner driven by a
fake REST endpoint and a fake clock (checkpoint and restart recovery, gaps with and without backfill, late trades,
poll errors, geo-blocking), raw capture and its replay, and historical/live parity on a synthetic dataset, including a
negative control showing that the harness does detect a difference."""
import copy, json, sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_processing import MS, REAL_CFG, make_dataset  # noqa: E402

from goldflow import schemas as S  # noqa: E402
from goldflow.config import Config  # noqa: E402
from goldflow.live import (OPEN_END, FeedHealth, GapTracker, LiveBus, LiveRunner, archive_polls, capture_polls,  # noqa: E402
                           live_settings, normalize_rest_trades, run_parity)
from goldflow.normalize import part_path  # noqa: E402
from goldflow.processing import TradeProcessor  # noqa: E402
from goldflow.processing.trade_processor import merge_dq  # noqa: E402
from goldflow.rest_client import GeoBlocked  # noqa: E402
from goldflow.settings import ConfigError, validate_config  # noqa: E402
from goldflow.strategies import StrategyEngine  # noqa: E402

T0 = MS("2026-10-23T10:00:00.000Z")


def row(i, t, px="4100.00", qty="0.010", bm=False, rpi=False):
    return {"id": i, "price": px, "qty": qty, "quoteQty": repr(float(px) * float(qty)), "time": t, "isBuyerMaker": bm, "isRPITrade": rpi}


def rest_ready(cfg):
    """Make the synthetic dataset representable as REST trades: no null sides, no trades recovered from aggTrades."""
    for p in sorted(Path(cfg.data_root).rglob("data_type=trades/date=*/part-000.parquet")):
        t = pq.read_table(p)
        bm = pa.array([bool(x) for x in t.column("buyer_maker_flag").fill_null(False).to_pylist()], pa.bool_())
        cols = {"buyer_maker_flag": bm, "aggressor_side": pa.array(np.where(bm.to_numpy(zero_copy_only=False), "SELL", "BUY")),
                "side_evidence": pa.array(["EXCHANGE_REPORTED"] * t.num_rows), "record_origin": pa.array(["ARCHIVE_TRADES"] * t.num_rows),
                "data_quality_status": pa.array(["VALID"] * t.num_rows)}
        for k, v in cols.items():
            i = t.schema.get_field_index(k)
            t = t.set_column(i, t.schema.field(k), v.cast(t.schema.field(k).type))
        pq.write_table(t, p)
    return cfg


# ============================================================================ normaliser, gaps, merge_dq
def test_normalize_rest_trades_uses_the_phase3_validator(tmp_path):
    cfg = Config(copy.deepcopy(REAL_CFG), tmp_path)
    rows = [row(10, T0), row(12, T0 + 5, bm=True, rpi=True, qty="0.250"), row(13, T0 + 9, px="4100.005"),
            row(12, T0 + 5, bm=True, rpi=True, qty="0.250"), row(14, T0 + 9, qty="0.020"), row(14, T0 + 9, qty="0.030")]
    tbl, rej, cnt = normalize_rest_trades(rows, T0 + 300, cfg)
    assert tbl.schema.equals(S.TRADE_SCHEMA, check_metadata=False) and tbl.num_rows == 2   # exact repeat kept once
    assert tbl.column("source_trade_id").to_pylist() == [10, 12] and tbl.column("price_ticks").to_pylist() == [410000, 410000]
    assert tbl.column("quantity_lots").to_pylist() == [10, 250] and tbl.column("aggressor_side").to_pylist() == ["BUY", "SELL"]
    assert set(tbl.column("record_origin").to_pylist()) == {"LIVE_REST_TRADES"} and tbl.column("receive_time_utc").cast(pa.int64()).to_pylist() == [T0 + 300] * 2
    assert rej["PRICE_NOT_TICK_MULTIPLE"] == 1 and rej["DUPLICATE_CONFLICT"] == 2      # conflicting repeats: both refused
    assert cnt == {"trades": 2, "rpi_trades": 1, "lots": 260, "rpi_lots": 250}
    empty, rej0, _ = normalize_rest_trades([], T0, cfg)
    assert empty.num_rows == 0 and rej0 == {}


def test_gap_tracker_skips_seen_ids_and_suspects_full_pages():
    g = GapTracker(limit=3)
    fresh, gap = g.new_rows([row(1, 1), row(3, 2), row(4, 3)])
    assert [r["id"] for r in fresh] == [1, 3, 4] and not gap
    g.advance(fresh)
    fresh, gap = g.new_rows([row(4, 3), row(6, 4)])                    # overlap + non-contiguous id: normal, not a gap
    assert [r["id"] for r in fresh] == [6] and not gap and g.id_skips == 1
    g.advance(fresh)
    fresh, gap = g.new_rows([row(9, 5), row(10, 6), row(11, 7)])        # full page, nothing overlapping: maybe missed trades
    assert gap and [r["id"] for r in fresh] == [9, 10, 11]
    fresh, gap = g.new_rows([row(6, 4), row(7, 5), row(8, 6)])          # full page that overlaps the last seen id: complete
    assert not gap
    assert GapTracker.load(json.loads(json.dumps(g.state()))) == g


def test_merge_dq_closes_an_open_interval():
    dq = []
    assert merge_dq(dq, 100, OPEN_END, "STALE") and dq == [[100, OPEN_END, "STALE"]]
    assert merge_dq(dq, 100, 250, "STALE") and dq == [[100, 250, "STALE"]]           # same start+status: end replaced
    assert not merge_dq(dq, 100, 250, "STALE")
    assert merge_dq(dq, 100, 300, "GAP") and len(dq) == 2


def test_feed_health_opens_and_closes_stale_intervals():
    h = FeedHealth({"trades": 1000, "markPrice": 500}, {"trades": "trades", "markPrice": "monitor"})
    assert h.check(10_000) == []                                         # never seen: nothing to judge yet
    h.seen("trades", 10_000); h.seen("markPrice", 10_000)
    assert h.check(10_600) == [("monitor", "STALE", 10_000, OPEN_END)]
    assert h.check(10_700) == []                                         # already open
    assert h.check(11_200) == [("trades", "STALE", 10_000, OPEN_END)]
    assert h.seen("trades", 12_000) == [("trades", "STALE", 10_000, 12_000)]
    st = h.status(12_000)
    assert st["trades"]["stale"] is False and st["markPrice"]["stale"] is True
    h2 = FeedHealth(h.limits, h.datasets); h2.load(json.loads(json.dumps(h.state())))
    assert h2.open == h.open and h2.last == h.last


def test_live_settings_are_validated(tmp_path):
    raw = copy.deepcopy(REAL_CFG)
    assert live_settings(Config(raw, tmp_path))["trade_source"] == "rest_poll"
    for bad, msg in (({"trade_source": "aggTrade"}, "rest_poll"), ({"poll_ms": 50}, "poll_ms"), ({"nope": 1}, "unknown keys"),
                     ({"grace_ms": 100}, "grace_ms")):
        r = copy.deepcopy(REAL_CFG); r["live"].update(bad)
        with pytest.raises(ConfigError, match=msg):
            validate_config(r, None, check_discovery=False)


# ============================================================================ the bus feeds the replay consumers
def test_bus_delivers_trades_and_stale_blocks_signals(tmp_path):
    cfg = Config(copy.deepcopy(REAL_CFG), tmp_path)
    strat = StrategyEngine(cfg, [], {})
    proc = TradeProcessor(cfg, sinks=[strat])
    bus = LiveBus([proc], grace_ms=3000)
    bus.start(T0)
    tbl, _, _ = normalize_rest_trades([row(i, T0 + 100 * i) for i in range(1, 30)], T0 + 3500, cfg)
    bus.push_trades(tbl)
    bus.advance(T0 + 3500)
    assert proc.stats["trades_processed"] == 29 and proc.clock == T0 + 500
    bus.advance(T0 + 3400)
    assert proc.clock == T0 + 500                                        # the clock never moves back
    bus.push_quality("trades", "STALE", T0 + 4000, OPEN_END)
    bus.advance(T0 + 9000)
    assert [T0 + 4000, OPEN_END, "STALE"] in proc.dq and [T0 + 4000, OPEN_END, "STALE"] in strat.dq
    bus.push_quality("trades", "STALE", T0 + 4000, T0 + 8000)
    bus.advance(T0 + 9500)
    assert [T0 + 4000, T0 + 8000, "STALE"] in strat.dq and not any(x[1] == OPEN_END for x in strat.dq)
    bus.advance(T0 + 2 * 3_600_000)
    bars = proc.final_upto
    assert all(v is not None for v in bars.values())


# ============================================================================ runner with a fake REST endpoint and clock
class FakeBinance:
    """Serves the synthetic dataset's trades as /fapi/v1/trades (latest `limit` published by now - delay) and
    /fapi/v1/historicalTrades (from an id)."""

    def __init__(self, cfg, clock, delay_ms=200):
        t = pq.read_table(part_path(cfg.data_root, cfg.symbol, "trades", "2026-10-23"))
        self.rows = [row(int(i), int(ts), repr(p), repr(q), bool(b)) for i, ts, p, q, b in zip(
            t.column("source_trade_id").to_pylist(), t.column("exchange_event_time_utc").cast(pa.int64()).to_pylist(),
            t.column("price").to_pylist(), t.column("quantity").to_pylist(), t.column("buyer_maker_flag").to_pylist())]
        self.ids = np.array([r["id"] for r in self.rows]); self.ts = np.array([r["time"] for r in self.rows])
        self.clock, self.delay, self.fail, self.geo, self.calls, self.hide = clock, delay_ms, 0, False, [], set()

    def get(self, path, params, raw=False):
        self.calls.append(path)
        if self.geo:
            raise GeoBlocked("HTTP 451")
        if self.fail:
            self.fail -= 1
            raise ConnectionError("reset")
        if path == "/fapi/v1/trades":
            n = int(np.searchsorted(self.ts, self.clock[0] - self.delay, "right"))
            out = [r for r in self.rows[max(0, n - params["limit"]):n]]
        else:
            k = int(np.searchsorted(self.ids, params["fromId"]))
            out = [r for r in self.rows[k:k + params["limit"]] if r["time"] <= self.clock[0] - self.delay]
        return json.dumps([r for r in out if r["id"] not in self.hide])


def live_cfg(tmp_path, **live):
    cfg = rest_ready(make_dataset(tmp_path, n_per_day=20000, partial_hour=None))
    cfg.raw["live"].update({"poll_ms": 5000, "grace_ms": 5000, "rest_limit": 500, "checkpoint_s": 60, **live})
    return cfg


def make_runner(cfg, clock, fake, name="L", key=True):
    proc = TradeProcessor(cfg, sinks=[])
    r = LiveRunner(cfg, [proc], name, rest_get=fake.get, now=lambda: clock[0])
    r.has_key = key
    return r, proc


def drive(r, clock, until, step=5000):
    while clock[0] < until:
        clock[0] += step
        r.step(clock[0])


def test_runner_end_to_end_checkpoint_restart_and_capture(tmp_path):
    cfg = live_cfg(tmp_path)
    clock = [T0]
    fake = FakeBinance(cfg, clock)
    r, proc = make_runner(cfg, clock, fake)
    r.start()
    drive(r, clock, T0 + 600_000)
    n_ref = int(((fake.ts >= T0 - 5000) & (fake.ts <= clock[0] - 200)).sum())
    first_poll_page = 500
    assert r.counters["polls"] == 120 and r.counters["gaps"] == 0 and r.counters["late_trades"] == 0
    assert proc.stats["trades_processed"] >= n_ref - first_poll_page and r.counters["checkpoints"] >= 9
    r.stop()
    # restart: checkpoint restored, RECOVERY opened over the outage, trades since the last durable id backfilled
    last_id = r.tracker.last_id
    clock[0] += 3_600_000                                                # 1 hour down: more trades than one page
    r2, proc2 = make_runner(cfg, clock, fake)
    r2.start()
    assert r2.status == "recovering" and r2.tracker.last_id == last_id
    assert proc2.stats["trades_processed"] == proc.stats["trades_processed"]
    r2.step(clock[0] + 5000); clock[0] += 5000
    assert r2.status == "running"
    rec = [x for x in proc2.dq if x[2] == "RECOVERY"]
    assert len(rec) == 1 and rec[0][1] != OPEN_END                       # opened, then closed when caught up
    got = proc2.stats["trades_processed"] - proc.stats["trades_processed"]
    expect = int(((fake.ids > last_id) & (fake.ts <= clock[0] - 200)).sum())
    assert got == expect and r2.counters["backfill_pages"] >= 1 and r2.counters["gaps"] == 0
    r2.stop()
    # every poll response was captured raw, first
    polls = capture_polls(cfg)
    assert len(polls) == r2.counters["polls"] + r2.counters["backfill_pages"]
    assert all(isinstance(p[1], list) for p in polls)


def test_runner_gap_without_key_poll_errors_late_trades_and_geoblock(tmp_path):
    cfg = live_cfg(tmp_path)
    clock = [T0]
    fake = FakeBinance(cfg, clock)
    r, proc = make_runner(cfg, clock, fake, key=False)
    r.start()
    drive(r, clock, T0 + 60_000)
    fake.fail = 3                                                        # three failed polls: counted, no data invented
    drive(r, clock, T0 + 75_000)
    assert r.counters["poll_errors"] == 3
    fake.fail = 4                                                        # 20 s without a poll answer: the feed is stale
    drive(r, clock, T0 + 95_000)
    stale = [x for x in proc.dq if x[2] == "STALE"]
    assert stale and stale[0][1] == OPEN_END
    drive(r, clock, T0 + 100_000)
    assert [x for x in proc.dq if x[2] == "STALE"][0][1] != OPEN_END     # closed when the feed answered again
    # a burst larger than one page with no API key: recorded as a GAP, not silently skipped
    clock[0] += 3_600_000
    r.step(clock[0])
    assert r.counters["gaps"] == 1 and [x for x in proc.dq if x[2] == "GAP"] and r.tracker.suspected_gaps
    # a trade that arrives after its 1 s bar is final: counted and marked LATE
    drive(r, clock, clock[0] + 30_000)
    hidden = [x for x in fake.rows if clock[0] + 1_000 < x["time"] < clock[0] + 4_000][:1]
    assert hidden
    fake.hide = {hidden[0]["id"]}
    drive(r, clock, clock[0] + 30_000)
    fake.hide = set()
    # re-published late (published now, newest id, but its trade time is 25 s old): insert it where it is published
    k = int(np.searchsorted(fake.ts, clock[0] - 200, "right"))
    fake.rows.insert(k, {**hidden[0], "id": int(fake.ids.max()) + 1})
    fake.ts = np.insert(fake.ts, k, clock[0] - 200); fake.ids = np.insert(fake.ids, k, fake.ids.max() + 1)
    clock[0] += 1
    r.step(clock[0])
    assert r.counters["late_trades"] == 1 and [x for x in proc.dq if x[2] == "LATE"]
    assert sum(proc.late_after_bar_final.values()) >= 1
    fake.geo = True
    with pytest.raises(GeoBlocked):
        r.step(clock[0] + 5000)


# ============================================================================ parity
def test_parity_on_synthetic_dataset_and_negative_control(tmp_path):
    cfg = rest_ready(make_dataset(tmp_path, n_per_day=6000))           # with a PARTIAL hour: quality reaches both paths
    s, e = MS("2026-10-23T00:00:00.000Z"), MS("2026-10-25T00:00:00.000Z")
    out = run_parity(cfg, s, e, report=True)
    assert out["all_equal"], {k: v for k, v in out["tables"].items() if not v["equal"]}
    assert out["trades"] == 12000 and out["rejected"] == 0 and not any(out["late_after_bar_final"].values())
    assert {"phase5.bars.1s", "phase5.windows", "phase6.snapshots", "phase7.cvd", "phase10.ib", "phase9.levels"} <= set(out["tables"])
    assert (Path(cfg.data_root) / "reports" / "parity_report.md").exists()
    # negative control: polls delayed beyond the grace period make trades late, and the harness reports the difference
    bad = run_parity(cfg, s, e, delay_ms=6000, grace_ms=1000, report=False)
    assert not bad["all_equal"] and sum(bad["late_after_bar_final"].values()) > 0
    polls = archive_polls(cfg, s, s + 3_600_000, 750, 200)
    rt = [p[0] for p in polls]
    assert rt == sorted(rt) and all(len(p) <= 1000 for _, p in polls)
    assert all(t - r["time"] >= 200 and t % 750 == 0 for t, p in polls for r in p)  # published >= delay before each poll
    # archived trades that a REST poll could not have produced (unknown side, recovered from aggTrades) are refused
    raw_cfg = make_dataset(tmp_path / "raw")
    with pytest.raises(ValueError, match="cannot be represented"):
        archive_polls(raw_cfg, s, e, 750, 200)
