"""Phase 8: order-book reconstruction, recorder, capture, coverage gating and liquidity metrics."""

from __future__ import annotations

import asyncio
import contextlib
import gzip
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from depth_sim import STREAM, SimExchange, px, ws

from goldflow.liquidity import (
    BookDriver,
    BookKey,
    BookRegistry,
    BookStatus,
    CaptureWriter,
    ChangeKind,
    DepthCapability,
    DepthCoverage,
    DepthUnavailable,
    DriverConfig,
    LevelChange,
    LiquidityTracker,
    LocalOrderBook,
    MergedBookError,
    NearTouchFlow,
    Outcome,
    UnitError,
    UnitScale,
    assert_same_book,
    book_metrics,
    capture_files,
    classify_changes,
    iter_capture,
    parse_depth_update,
    parse_snapshot,
    replay_capture,
)
from goldflow.liquidity.coverage import Interval
from goldflow.liquidity.dataset import DepthBookConsumer, apply_depth_rows, normalize_capture, write_validity
from goldflow.liquidity.recorder import DepthRecorder, RecorderConfig, SnapshotRefused

P, Q = UnitScale("0.01"), UnitScale("0.001")
KEY = BookKey("binance", "binance-futures-usdm", "usdm_perpetual", "XAUUSDT")
DCFG = DriverConfig("XAUUSDT", "0.01", "0.001", {STREAM: "regular"})


def upd(ev: dict, ref: object = None):
    return parse_depth_update(ev, P, Q, ref)


def snap(s: dict, limit: int | None = None, ref: object = None):
    return parse_snapshot(s, P, Q, ref, limit)


def synced(ex: SimExchange, book: LocalOrderBook, n_before: int = 3) -> None:
    for _ in range(n_before):
        book.on_update(upd(ex.step()))
    book.on_snapshot(snap(ex.snapshot()))
    book.on_update(upd(ex.step()))


def assert_truth(book: LocalOrderBook, ex: SimExchange) -> None:
    b, a = ex.truth()
    assert book.bids == b and book.asks == a


# ------------------------------------------------------------------------------------------------- units
def test_units_exact_and_refusals():
    assert P.to_units("4012.34") == 401234 and P.to_units("4012.340") == 401234 and P.to_units("4012") == 401200
    assert Q.to_units("0.001") == 1 and Q.to_units("12.500") == 12500 and Q.to_units("0") == 0
    assert P.to_units("4.01234e3") == 401234
    for bad in ("4012.345", "-1.00", "abc", "", "1.2.3", "1e-3"):
        with pytest.raises(UnitError):
            P.to_units(bad)
    assert UnitScale("0.05").to_units("1.15") == 23
    with pytest.raises(UnitError):
        UnitScale("0.05").to_units("1.12")
    assert P.from_float(4012.34) == 401234 and P.to_str(401234) == "4012.34" and Q.to_str(12500) == "12.500"
    with pytest.raises(UnitError):
        P.from_float(4012.345)


# ------------------------------------------------------------------------------------------------- sync procedure
def test_documented_sync_procedure_rebuilds_the_true_book():
    ex = SimExchange(1)
    book = LocalOrderBook(KEY)
    buffered = [upd(ex.step(), ref=f"b{i}") for i in range(5)]
    for e in buffered:
        assert book.on_update(e) == [] and book.status is BookStatus.NO_BOOK
    s = ex.snapshot()
    book.on_snapshot(snap(s, ref="snap"))
    # every buffered event had u <= L; the one with u == L bridges (U <= L <= u), the rest are dropped
    outs = {o.ref: o.outcome for o in book.drain_outcomes()}
    assert outs["b4"] is Outcome.APPLIED and outs["snap"] is Outcome.SNAPSHOT_APPLIED
    assert all(outs[f"b{i}"] is Outcome.DROPPED_BEFORE_SNAPSHOT for i in range(4))
    assert book.status is BookStatus.VALID
    for _ in range(300):
        book.on_update(upd(ex.step()))
        assert book.is_valid
    assert_truth(book, ex)
    assert book.intervals[0].end_ms is None and book.known_valid_until() == book.last_event_ms + 1


def test_snapshot_older_than_buffer_is_rejected_then_a_new_one_bridges():
    ex = SimExchange(2)
    book = LocalOrderBook(KEY)
    old = ex.snapshot()
    ex.step()  # this event is never received
    for _ in range(3):
        book.on_update(upd(ex.step()))
    book.on_snapshot(snap(old, ref="old"))
    assert book.status is BookStatus.NO_BOOK
    assert {o.ref: o.outcome for o in book.drain_outcomes()}["old"] is Outcome.SNAPSHOT_REJECTED
    book.on_snapshot(snap(ex.snapshot()))
    book.on_update(upd(ex.step()))
    assert book.is_valid
    assert_truth(book, ex)


def test_sequence_gap_invalidates_and_book_is_rebuilt():
    ex = SimExchange(3)
    book = LocalOrderBook(KEY)
    synced(ex, book)
    for _ in range(50):
        book.on_update(upd(ex.step()))
    last_e = book.last_event_ms
    ex.step()  # lost update -> next event's pu != last u
    gap = upd(ex.step(), ref="gap")
    book.on_update(gap)
    assert book.status is BookStatus.NO_BOOK and book.bids == {} and book.asks == {}
    assert book.invalid_reason.startswith("SEQUENCE_GAP")
    iv = book.intervals[0]
    assert iv.end_ms == last_e + 1 and iv.end_reason.startswith("SEQUENCE_GAP")
    assert book.counters.invalidations == 1 and list(book.buffer) == [gap]  # kept: it may bridge the next snapshot
    # metrics refuse while invalid
    with pytest.raises(DepthUnavailable):
        book_metrics(book)
    for _ in range(2):
        book.on_update(upd(ex.step()))
    book.on_snapshot(snap(ex.snapshot()))
    book.on_update(upd(ex.step()))
    assert book.is_valid and len(book.intervals) == 2
    for _ in range(100):
        book.on_update(upd(ex.step()))
    assert_truth(book, ex)


def test_zero_quantity_removes_level_and_duplicates_are_ignored():
    ex = SimExchange(4)
    book = LocalOrderBook(KEY)
    synced(ex, book)
    p = max(book.bids)
    ev = {
        "e": "depthUpdate",
        "E": ex.t + 1,
        "T": ex.t,
        "s": "XAUUSDT",
        "U": book.last_update_id + 1,
        "u": book.last_update_id + 1,
        "pu": book.last_update_id,
        "b": [[px(p), "0.000"]],
        "a": [],
    }
    ch = book.on_update(upd(ev))
    assert p not in book.bids and ch == [LevelChange("BID", p, ch[0].old_lots, 0)]
    before = book.state_digest()
    book.on_update(upd(ev, ref="dup"))
    assert book.state_digest() == before and book.counters.duplicates == 1


def test_crossed_book_and_invalid_payload_and_disconnect_invalidate():
    ex = SimExchange(5)
    book = LocalOrderBook(KEY)
    synced(ex, book)
    u = book.last_update_id
    best_ask = min(book.asks)
    ev = {
        "e": "depthUpdate",
        "E": ex.t + 1,
        "s": "XAUUSDT",
        "U": u + 1,
        "u": u + 1,
        "pu": u,
        "b": [[px(best_ask), "1.000"]],
        "a": [],
    }
    book.on_update(upd(ev))
    assert book.status is BookStatus.NO_BOOK and book.invalid_reason.startswith("CROSSED_BOOK")
    book2 = LocalOrderBook(KEY)
    synced(SimExchange(5), book2)
    book2.on_invalid_payload("bad")
    assert book2.status is BookStatus.NO_BOOK
    book3 = LocalOrderBook(KEY)
    ex3 = SimExchange(6)
    synced(ex3, book3)
    book3.on_update(upd(ex3.step()))
    book3.on_disconnect("test")
    assert book3.status is BookStatus.NO_BOOK and book3.intervals[-1].end_reason == "CONNECTION_LOST test"
    with pytest.raises(UnitError):
        upd({**ex3.step(), "b": [["4000.005", "1.000"]]})


def test_books_are_never_merged():
    reg = BookRegistry()
    a = reg.book(KEY)
    rpi = reg.book(BookKey(KEY.provider_id, KEY.venue_id, KEY.market_type, KEY.symbol, "rpi"))
    okx = reg.book(BookKey("okx", "okx-swap", "usdt_swap", "XAU-USDT-SWAP"))
    assert len({id(a), id(rpi), id(okx)}) == 3 and reg.book(KEY) is a and len(reg) == 3
    ev = SimExchange(7, symbol="PAXGUSDT").step()
    with pytest.raises(MergedBookError):
        a.on_update(upd(ev))
    with pytest.raises(MergedBookError):
        assert_same_book(KEY, okx.key)
    with pytest.raises(ValueError):
        reg.book(BookKey("binance", "v", "m", "XAUUSDT", "merged"))


# ------------------------------------------------------------------------------------------------- metrics
def _small_book() -> LocalOrderBook:
    b = LocalOrderBook(KEY)
    b.on_update(upd({"e": "depthUpdate", "E": 1000, "s": "XAUUSDT", "U": 5, "u": 6, "pu": 4, "b": [], "a": []}))
    b.on_snapshot(
        snap(
            {
                "lastUpdateId": 6,
                "E": 1001,
                "bids": [["100.00", "1.000"], ["99.99", "2.000"], ["99.90", "4.000"]],
                "asks": [["100.02", "3.000"], ["100.05", "1.000"], ["100.50", "8.000"]],
            },
            limit=1000,
        )
    )
    assert b.is_valid
    return b


def test_book_metrics_hand_computed():
    m = book_metrics(_small_book(), tick_bands=(5,), pct_bands=("0.25",), top_k=1)
    assert (m.best_bid_ticks, m.best_ask_ticks, m.spread_ticks, m.mid_x2_ticks) == (10000, 10002, 2, 20002)
    assert m.touch_imbalance == (1000 - 3000) / 4000
    t5 = m.bands[0]  # mid 100.01 +- 0.05: bids >= 99.96 (1 + 2), asks <= 100.06 (3 + 1)
    assert t5.band == "ticks:5" and t5.bid.lots == 3000 and t5.ask.lots == 4000 and t5.imbalance == -1000 / 7000
    assert t5.bid.topk_share == 2 / 3 and t5.ask.topk_share == 3 / 4
    p = m.bands[1]  # 0.25 % of 100.01 = 0.250025 -> bids >= 99.759975, asks <= 100.260025
    assert p.bid.lots == 7000 and p.ask.lots == 4000


def test_band_beyond_snapshot_depth_is_incomplete_not_extrapolated():
    b = LocalOrderBook(KEY)
    b.on_update(upd({"e": "depthUpdate", "E": 1000, "s": "XAUUSDT", "U": 5, "u": 6, "pu": 4, "b": [], "a": []}))
    b.on_snapshot(
        snap(
            {
                "lastUpdateId": 6,
                "E": 1001,
                "bids": [["100.00", "1.000"], ["99.99", "2.000"]],
                "asks": [["100.02", "3.000"], ["100.03", "1.000"]],
            },
            limit=2,
        )
    )
    m = book_metrics(b, tick_bands=(1, 50), pct_bands=(), top_k=1)
    assert m.bands[0].bid.lots == 1000 and m.bands[0].imbalance is not None
    assert m.bands[1].bid.lots is None and m.bands[1].ask.lots is None and m.bands[1].imbalance is None


def test_metrics_refuse_invalid_or_syncing_book():
    b = LocalOrderBook(KEY)
    with pytest.raises(DepthUnavailable):
        book_metrics(b)
    b.on_update(upd({"e": "depthUpdate", "E": 1000, "s": "XAUUSDT", "U": 8, "u": 9, "pu": 7, "b": [], "a": []}))
    b.on_snapshot(snap({"lastUpdateId": 10, "bids": [["1.00", "1.000"]], "asks": [["1.01", "1.000"]]}))
    assert b.status is BookStatus.SYNCING
    with pytest.raises(DepthUnavailable):
        book_metrics(b)


def test_change_classification_and_near_touch_flow():
    ch = [LevelChange("BID", 100, 5, 2), LevelChange("ASK", 105, 0, 4), LevelChange("ASK", 106, 9, 0)]
    out = classify_changes(ch, {("BID", 100): 2})
    kinds = [(c.side, c.price_ticks, c.kind, c.lots) for c in out]
    assert kinds == [
        ("BID", 100, ChangeKind.REDUCTION_WITH_TRADE, 2),
        ("BID", 100, ChangeKind.REDUCTION_UNATTRIBUTED, 1),
        ("ASK", 105, ChangeKind.ADDITION, 4),
        ("ASK", 106, ChangeKind.REDUCTION_UNATTRIBUTED, 9),
    ]
    assert all(c.kind is ChangeKind.REDUCTION_UNATTRIBUTED for c in classify_changes([LevelChange("BID", 1, 3, 1)]))
    f = NearTouchFlow(window_ms=1000, ticks=1)
    f.update(0, out, best_bid=100, best_ask=105)
    assert f.value(500) is None  # window not yet inside the valid run
    assert f.value(1000) is None or f.value(1000) == {"bid_add": 0, "bid_red": 0, "ask_add": 0, "ask_red": 0}
    f2 = NearTouchFlow(window_ms=1000, ticks=1)
    f2.update(0, [], None, None)
    f2.update(1500, out, 100, 105)
    assert f2.value(1500) == {"bid_add": 0, "bid_red": 3, "ask_add": 4, "ask_red": 9}
    assert f2.value(2600) == {"bid_add": 0, "bid_red": 0, "ask_add": 0, "ask_red": 0}
    f2.reset()
    assert f2.value(5000) is None


# ------------------------------------------------------------------------------------------------- coverage
def test_capability_labelled_per_interval():
    cov = DepthCoverage(KEY, [Interval(1000, 2000), Interval(3000, 4000)])
    assert cov.capability(1000, 2000) is DepthCapability.VALID
    assert cov.capability(1500, 2500) is DepthCapability.PARTIAL
    assert cov.capability(2000, 3000) is DepthCapability.ABSENT
    assert cov.capability(1500, 3500) is DepthCapability.PARTIAL  # a gap inside the window is never VALID
    hist = (1_777_507_200_000, 1_793_318_400_000)  # 2026-04-30 .. 2026-10-30: no historical L2
    assert cov.capability(*hist) is DepthCapability.PARTIAL or cov.capability(*hist) is DepthCapability.ABSENT
    assert DepthCoverage(KEY, []).capability(*hist) is DepthCapability.ABSENT
    with pytest.raises(DepthUnavailable):
        cov.require_valid(1500, 2500, "absorption-at-depth signal")
    cov.require_valid(3100, 3900)
    ex = SimExchange(8)
    book = LocalOrderBook(KEY)
    synced(ex, book)
    live = DepthCoverage.from_book(book)
    t0 = book.intervals[0].start_ms
    assert live.capability(t0, book.last_event_ms + 1) is DepthCapability.VALID
    assert live.capability(t0, book.last_event_ms + 2) is DepthCapability.PARTIAL  # not proven beyond the last event
    book.on_disconnect("x")
    assert DepthCoverage.from_book(book).capability(book.last_event_ms + 1, book.last_event_ms + 100) is DepthCapability.ABSENT


# ------------------------------------------------------------------------------------------------- capture
def test_capture_roundtrip_and_markers(tmp_path):
    w = CaptureWriter(tmp_path, session="20261101T000000000Z-aaaaaa")
    w.write("conn", 1_788_000_000_000, "", {"event": "OPEN"})
    w.write("ws", 1_788_000_000_100, '{"x":1}')
    w.write("ws", 1_788_003_600_100, '{"x":2}')  # next hour -> rotation
    w.close()
    files = capture_files(tmp_path)
    assert len(files) == 2
    recs = list(iter_capture(files))
    assert [(r.kind, r.seq, r.raw) for r in recs] == [("conn", 1, ""), ("ws", 2, '{"x":1}'), ("ws", 3, '{"x":2}')]
    # truncated tail (crash mid-write) -> complete lines, then a CAPTURE_TRUNCATED marker
    data = files[1].read_bytes()
    files[1].write_bytes(data[: len(data) // 2])
    recs = list(iter_capture(files))
    assert recs[-1].synthetic_marker and recs[-1].meta["event"] == "CAPTURE_TRUNCATED"
    # second session -> boundary marker; a missing line -> seq-gap marker
    w2 = CaptureWriter(tmp_path, session="20261101T020000000Z-bbbbbb")
    w2.write("ws", 1_788_007_200_000, "a")
    w2.seq += 1
    w2.write("ws", 1_788_007_200_100, "b")
    w2.close()
    ev = [r.meta.get("event") for r in iter_capture(capture_files(tmp_path)) if r.synthetic_marker]
    assert "SESSION_BOUNDARY" in ev and "CAPTURE_SEQ_GAP" in ev
    with gzip.open(files[0], "rt") as fh:
        line = json.loads(fh.readline())
    assert set(line) == {"v", "session", "seq", "kind", "recv_ms", "raw", "meta"}


# ------------------------------------------------------------------------------------------------- live recorder
class FakeWs:
    def __init__(self, feed: "FakeFeed"):
        self.feed = feed

    async def recv(self) -> str:
        await asyncio.sleep(0.0005)  # let snapshot tasks complete
        return self.feed.next_message()


class FakeFeed:
    """Messages from a SimExchange; drops one event (gap) and breaks the connection on schedule."""

    def __init__(self, ex: SimExchange, drop_at: tuple[int, ...] = (), break_at: tuple[int, ...] = (), total: int = 400):
        self.ex, self.drop_at, self.break_at, self.total = ex, set(drop_at), set(break_at), total
        self.n = 0
        self.stop: callable = lambda: None
        self.pending: list[dict] = []  # published by the exchange but not yet delivered (WebSocket lag)

    def lagged_snapshot(self, lag: int):
        """Snapshot fetcher: the exchange publishes `lag` events that reach us only after the snapshot response."""

        def fetch(kind: str):
            self.pending.extend(self.ex.step() for _ in range(lag))
            return 200, json.dumps(self.ex.snapshot(1000))

        return fetch

    def next_message(self) -> str:
        self.n += 1
        if self.n >= self.total:
            self.stop()
        if self.pending and self.n not in self.break_at:
            return ws(self.pending.pop(0))
        if self.n in self.break_at:
            self.pending.clear()
            for _ in range(3):
                self.ex.step()  # events published while we are disconnected
            raise ConnectionResetError("simulated disconnect")
        ev = self.ex.step()
        if self.n in self.drop_at:
            ev = self.ex.step()  # the previous event was lost in transit
        return ws(ev) if self.n % 50 else json.dumps(ev)  # mix enveloped and bare messages

    @contextlib.asynccontextmanager
    async def connect(self, url: str):
        yield FakeWs(self)


def _run_recorder(tmp_path: Path, ex: SimExchange, feed: FakeFeed, fetch=None, tracker=None):
    writer = CaptureWriter(tmp_path / "cap", flush_every_s=0.0)
    drv = BookDriver(DCFG, listeners=[tracker] if tracker else [])
    rcfg = RecorderConfig(
        url="wss://example.invalid/stream?streams=" + STREAM,
        snapshot_min_interval_s=0.0,
        reconnect_base_s=0.001,
        reconnect_cap_s=0.002,
        status_every_s=1e9,
        snapshot_limit=1000,
    )
    rec = DepthRecorder(rcfg, drv, writer, fetch or (lambda kind: (200, json.dumps(ex.snapshot(1000)))), feed.connect)
    feed.stop = rec.stop
    summary = asyncio.run(rec.run(duration_s=30))
    writer.close()
    return rec, drv, summary, writer


def test_live_recorder_gap_reconnect_and_replay_digest_equality(tmp_path):
    ex = SimExchange(9)
    feed = FakeFeed(ex, drop_at=(120,), break_at=(250,), total=400)
    samples = []
    tracker = LiquidityTracker(cadence_ms=1000, sink=samples.append)
    rec, drv, summary, writer = _run_recorder(tmp_path, ex, feed, fetch=feed.lagged_snapshot(3), tracker=tracker)
    assert summary.status == "stopped" and summary.connections == 2 and summary.reconnects == 1
    book = drv.books["regular"]
    assert book.counters.invalidations >= 2 and len(book.intervals) >= 2  # the gap and the disconnect
    reasons = " ".join(iv.end_reason or "" for iv in book.intervals)
    assert "SEQUENCE_GAP" in reasons and "CONNECTION_LOST" in reasons
    assert book.counters.snapshots_applied >= 2
    assert book.counters.dropped_before_snapshot >= 2  # lagged events already contained in the snapshot
    # samples only from a valid book, stamped with book time
    assert samples and all(s["book_time_ms"] >= book.intervals[0].start_ms for s in samples)
    # replay of the recorded capture through the same driver -> identical book state digest and intervals
    files = capture_files(tmp_path / "cap")
    rep = replay_capture(DCFG, iter_capture(files))
    assert rep.digests() == summary.extra["digests"]
    assert [iv.as_dict() for iv in rep.books["regular"].intervals] == [iv.as_dict() for iv in book.intervals]
    # normalise to DepthEvent v1 Parquet, then rebuild from rows in exchange-time order (the Phase 4 replay order)
    out = tmp_path / "norm"
    res = normalize_capture(DCFG, lambda: iter_capture(files), out)
    assert res["digests"] == summary.extra["digests"]
    parts = sorted(out.glob("data_type=depth_l2/date=*/part-000.parquet"))
    assert parts
    t = pa.concat_tables([pq.read_table(p) for p in parts])
    assert t.schema.metadata[b"goldflow.schema"] == b"DepthEvent"
    assert set(t.column("snapshot_or_delta").cast(pa.string()).to_pylist()) >= {"SNAPSHOT", "DELTA", "CONNECTION_LOST"}
    pbook = LocalOrderBook(KEY)
    for p in parts:
        apply_depth_rows(pbook, pq.read_table(p), P, Q)
    pbook.finish()
    assert pbook.state_digest() == summary.extra["digests"]["regular"]
    assert [(i.start_ms, i.end_ms) for i in pbook.intervals] == [(i.start_ms, i.end_ms) for i in book.intervals]
    # every applied row really was applied in the live run
    counts = res["outcome_counts"]["regular"]
    assert counts["APPLIED"] == book.counters.applied
    paths = write_validity(res["intervals"], DCFG, tmp_path / "derived")
    assert pq.read_table(paths[0]).num_rows == len(book.intervals)


def test_recorder_stops_on_geo_block(tmp_path):
    ex = SimExchange(10)
    feed = FakeFeed(ex, total=10_000)

    def refused(kind: str):
        raise SnapshotRefused("HTTP 451 from https://fapi.binance.com")

    rec, drv, summary, _ = _run_recorder(tmp_path, ex, feed, fetch=refused)
    assert summary.status == "geo_blocked" and "451" in summary.last_error
    assert drv.books["regular"].status is BookStatus.NO_BOOK  # never a book without a snapshot


def test_book_checkpoint_restore_equals_uninterrupted():
    ex1, ex2 = SimExchange(11), SimExchange(11)
    a, b = LocalOrderBook(KEY), LocalOrderBook(KEY)
    synced(ex1, a)
    synced(ex2, b)
    evs = [ex1.step() for _ in range(200)]
    for _ in range(200):
        ex2.step()
    for e in evs[:77]:
        a.on_update(upd(e))
        b.on_update(upd(e))
    c = LocalOrderBook(KEY)
    c.set_state(json.loads(json.dumps(b.get_state())))
    for e in evs[77:]:
        a.on_update(upd(e))
        c.on_update(upd(e))
    assert a.state_digest() == c.state_digest() and a.get_state() == c.get_state()


def test_phase4_replay_consumer_rebuilds_book(tmp_path):
    ex = SimExchange(12)
    w = CaptureWriter(tmp_path / "cap", flush_every_s=0.0)
    drv = BookDriver(DCFG)
    t = 1_788_000_000_000
    for i in range(400):
        if i == 3:
            r = w.write("snapshot", t + i, json.dumps(ex.snapshot()), {"book": "regular", "limit": 1000, "http_status": 200})
        else:
            r = w.write("ws", t + i, ws(ex.step()))
        drv.feed(r)
    w.close()
    drv.finish()
    out = tmp_path / "norm"
    normalize_capture(DCFG, lambda: iter_capture(capture_files(tmp_path / "cap")), out)
    table = pq.read_table(next(out.glob("data_type=depth_l2/date=*/part-000.parquet")))

    class Chunk:  # the slice of EventChunk the consumer uses
        tables = {"depth_l2": table}

        def stream_table(self, name):
            return self.tables[name]

    cons = DepthBookConsumer(DCFG)
    cons.on_chunk(Chunk())
    assert cons.book.state_digest() == drv.digests()["regular"] and cons.book.is_valid
    st = cons.get_state()
    cons.on_seek(0)
    assert not cons.book.is_valid
    cons.set_state(st)
    assert cons.book.is_valid


# ------------------------------------------------------------------------------------------------- settings and CLI
ROOT = Path(__file__).resolve().parents[1]
RAW = json.loads((ROOT / "config/binance_xauusdt.json").read_text())


def test_depth_settings_defaults_and_validation():
    from goldflow.settings import ConfigError, config_hash, validate_config

    s = validate_config(RAW)
    assert config_hash(RAW).startswith("f0b71abc")  # defaults live in code: the config file and its hash are unchanged
    d = s.depth
    assert d.snapshot_limit == 1000 and not d.rpi_enabled
    assert d.url("XAUUSDT") == "wss://fstream.binance.com/stream?streams=xauusdt@depth@100ms"
    assert validate_config({**RAW, "depth": {"rpi_enabled": True}}).depth.streams("XAUUSDT") == {
        "xauusdt@depth@100ms": "regular",
        "xauusdt@rpiDepth@500ms": "rpi",
    }
    for bad in (
        {"ws_base_url": "ws://x"},
        {"snapshot_limit": 2000},
        {"stream": "depth@100ms"},
        {"pct_bands": ["0"]},
        {"reconnect_base_s": 10, "reconnect_cap_s": 1},
        {"snapshot_path": "/api/v3/depth"},
    ):
        with pytest.raises(ConfigError):
            validate_config({**RAW, "depth": bad})


def test_cli_depth_replay_and_normalize(tmp_path, capsys):
    import shutil

    from goldflow import cli
    from goldflow.config import Config
    from goldflow.liquidity.capture import capture_root

    shutil.copytree(ROOT / "config", tmp_path / "config")
    cfgp = tmp_path / "config" / "binance_xauusdt.json"
    assert cli.main(["depth-replay", "--config", str(cfgp)]) == 1  # no capture yet: says so, does not invent one
    cfg = Config.load(cfgp)
    ex = SimExchange(13)
    w = CaptureWriter(capture_root(cfg.data_root, cfg.market, cfg.symbol), flush_every_s=0.0)
    t = 1_788_000_000_000
    w.write("conn", t, "", {"event": "OPEN"})
    for i in range(1, 300):
        if i == 4:
            w.write("snapshot", t + i, json.dumps(ex.snapshot()), {"book": "regular", "limit": 1000, "http_status": 200})
        else:
            w.write("ws", t + i, ws(ex.step()))
    w.close()
    capsys.readouterr()
    assert cli.main(["depth-replay", "--config", str(cfgp)]) == 0
    text = capsys.readouterr().out
    out = json.loads(text[text.index("\n{") + 1 :])
    assert out["books"]["regular"]["coverage"]["valid_intervals"] == 1 and out["metric_samples"] > 0
    assert cli.main(["depth-normalize", "--config", str(cfgp)]) == 0
    rep = json.loads((cfg.data_root / "reports" / "depth_coverage.json").read_text())
    assert rep["coverage"]["regular"]["valid_intervals"] == 1 and rep["outcome_counts"]["regular"]["APPLIED"] > 0
    assert list((cfg.data_root / "normalized").glob("**/data_type=depth_l2/date=*/part-000.parquet"))
    assert rep["digests"] == out["digests"]  # normalisation and capture replay rebuild the same book


def test_band_metrics_equal_bruteforce_reference():
    """The bisect/prefix-sum implementation equals a direct exact (Fraction) evaluation on random books."""
    import math
    from fractions import Fraction

    incomplete = 0
    for seed in range(25):
        ex = SimExchange(100 + seed, levels=300)
        book = LocalOrderBook(KEY)
        synced(ex, book)
        for _ in range(seed * 7):
            book.on_update(upd(ex.step()))
        limit = 1000 if seed % 2 else 120
        if seed % 2 == 0:  # truncated snapshot: some bands must report None
            book = LocalOrderBook(KEY)
            book.on_update(upd(ex.step()))
            book.on_snapshot(snap(ex.snapshot(limit), limit=limit))
            book.on_update(upd(ex.step()))
        m = book_metrics(book, tick_bands=(1, 7, 50, 250), pct_bands=("0.01", "0.1", "0.37"), top_k=3)
        mid2 = m.mid_x2_ticks
        for band in m.bands:
            kind, v = band.band.split(":")
            lo2, hi2 = (
                (Fraction(mid2 - 2 * int(v)), Fraction(mid2 + 2 * int(v)))
                if kind == "ticks"
                else (mid2 * (1 - Fraction(v) / 100), mid2 * (1 + Fraction(v) / 100))
            )
            bq = [q for p, q in book.bids.items() if 2 * p >= lo2]
            aq = [q for p, q in book.asks.items() if 2 * p <= hi2]
            # a band is complete iff every tick it contains is inside the snapshot-known range
            if book.known_bid_floor is not None and book.known_bid_floor > math.ceil(lo2 / 2):
                assert band.bid.lots is None
                incomplete += 1
            else:
                assert band.bid.lots == sum(bq) and band.bid.levels == len(bq)
                assert band.bid.topk_share == (sum(sorted(bq)[-3:]) / sum(bq) if bq else None)
            if book.known_ask_cap is not None and book.known_ask_cap < math.floor(hi2 / 2):
                assert band.ask.lots is None
            else:
                assert band.ask.lots == sum(aq) and band.ask.levels == len(aq)
    assert incomplete > 0  # the truncated-snapshot branch was exercised
