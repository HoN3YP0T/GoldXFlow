"""Capture -> canonical DepthEvent v1 Parquet, validity intervals, and the Phase 4 replay consumer (Phase 8).

Normalisation runs the capture through the same `BookDriver` the live recorder uses (pass 1: final outcome of every
record), then writes one row per record that touched a book (pass 2). Partitions:

  data/normalized/provider=binance/market=usdm_perpetual/instrument=<SYM>/data_type=depth_l2/date=D/part-000.parquet
  data_type=depth_l2_rpi for the RPI book (never mixed into depth_l2)

Row values of `snapshot_or_delta`: SNAPSHOT, DELTA, CONNECTION_LOST (a connection/capture boundary that invalidated the
book) and INVALID_PAYLOAD (an unparseable stream message that invalidated the book). `sequence_validity` is the
record's final outcome (APPLIED, DROPPED_BEFORE_SNAPSHOT, DUPLICATE, DISCARDED, SNAPSHOT_APPLIED, SNAPSHOT_REJECTED,
INVALID_PAYLOAD, CONNECTION_LOST). Boundary rows are stamped one millisecond after the last exchange time seen for that
book, so ordering by exchange time keeps them where they happened.

Reference columns: `raw_file_id` indexes the partition metadata list `goldflow.capture_sessions`; `raw_row` is the
record's `seq` in that session's capture.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ..replay import Consumer, EventChunk
from ..schemas import DEPTH_SCHEMA, SCHEMA_VERSION
from .book import DepthSnapshot, DepthUpdate, LocalOrderBook, Outcome
from .capture import CaptureRecord
from .driver import BookDriver, DriverConfig
from .units import UnitError, UnitScale

DATA_TYPES = {"regular": "depth_l2", "rpi": "depth_l2_rpi"}
APPLIED = {Outcome.APPLIED.value, Outcome.SNAPSHOT_APPLIED.value}


def _date(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d")


def _route(cfg: DriverConfig, msg: Any) -> str | None:
    if not isinstance(msg, dict):
        return None
    if "stream" in msg:
        return cfg.streams.get(str(msg["stream"]))
    return "regular" if msg.get("e") == "depthUpdate" else None


def _levels(raw: Any) -> list[dict[str, float]]:
    try:
        return [{"price": float(p), "quantity": float(q)} for p, q, *_ in raw]
    except (TypeError, ValueError):
        return []


def normalize_capture(
    cfg: DriverConfig, records_factory: Callable[[], Iterable[CaptureRecord]], out_root: Path
) -> dict[str, Any]:
    """Write DepthEvent v1 partitions and validity intervals. `records_factory` must return the capture records in
    order each time it is called (it is read twice). Returns a summary with per-book digests and intervals."""
    drv = BookDriver(cfg, collect_outcomes=True)
    drv.feed_all(records_factory())
    drv.finish()
    outcomes = {k: v.outcome.value for k, v in drv.outcomes.items()}

    rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    sessions: dict[tuple[str, str], list[str]] = {}
    last_e: dict[str, int] = {}
    counts: dict[str, dict[str, int]] = {}

    def add(kind: str, rec: CaptureRecord, row: dict[str, Any]) -> None:
        e = row["exchange_event_time_utc"]
        key = (kind, _date(e))
        sl = sessions.setdefault(key, [])
        if rec.session not in sl:
            sl.append(rec.session)
        row["raw_file_id"] = sl.index(rec.session)
        row["raw_row"] = rec.seq
        row["_order"] = len(rows.get(key, []))
        rows.setdefault(key, []).append(row)
        c = counts.setdefault(kind, {})
        c[row["sequence_validity"]] = c.get(row["sequence_validity"], 0) + 1

    def boundary_time(kind: str, rec: CaptureRecord) -> int:
        return last_e[kind] + 1 if kind in last_e else rec.recv_ms

    for rec in records_factory():
        for kind in drv.books:
            out = outcomes.get((kind, rec.ref))
            if out is None:
                continue
            base: dict[str, Any] = {
                "transaction_time_utc": None,
                "first_update_id": None,
                "final_update_id": None,
                "previous_update_id": None,
                "bids": [],
                "asks": [],
                "sequence_validity": out,
                "data_quality_status": "VALID" if out in APPLIED else "NOT_APPLIED",
            }
            if rec.kind == "conn":
                base.update(exchange_event_time_utc=boundary_time(kind, rec), snapshot_or_delta="CONNECTION_LOST")
                add(kind, rec, base)
                continue
            try:
                msg = json.loads(rec.raw)
            except ValueError:
                msg = None
            if rec.kind == "snapshot":
                m = msg if isinstance(msg, dict) else {}
                e = int(m["E"]) if m.get("E") is not None else rec.recv_ms
                base.update(
                    exchange_event_time_utc=e,
                    transaction_time_utc=int(m["T"]) if m.get("T") is not None else None,
                    final_update_id=int(m["lastUpdateId"]) if m.get("lastUpdateId") is not None else None,
                    bids=_levels(m.get("bids", [])),
                    asks=_levels(m.get("asks", [])),
                    snapshot_or_delta="SNAPSHOT",
                    _limit=rec.meta.get("limit"),
                )
                if out != Outcome.INVALID_PAYLOAD.value:
                    last_e[kind] = max(last_e.get(kind, e), e)
                add(kind, rec, base)
                continue
            d = None
            if isinstance(msg, dict) and _route(cfg, msg) == kind:
                d = msg.get("data", msg) if "stream" in msg else msg
            if out == Outcome.INVALID_PAYLOAD.value or d is None:
                base.update(exchange_event_time_utc=boundary_time(kind, rec), snapshot_or_delta="INVALID_PAYLOAD")
                add(kind, rec, base)
                continue
            e = int(d["E"])
            base.update(
                exchange_event_time_utc=e,
                transaction_time_utc=int(d["T"]) if d.get("T") is not None else None,
                first_update_id=int(d["U"]),
                final_update_id=int(d["u"]),
                previous_update_id=int(d["pu"]) if d.get("pu") is not None else None,
                bids=_levels(d.get("b", [])),
                asks=_levels(d.get("a", [])),
                snapshot_or_delta="DELTA",
            )
            last_e[kind] = max(last_e.get(kind, e), e)
            add(kind, rec, base)

    written = []
    for (kind, day), rs in sorted(rows.items()):
        rs.sort(key=lambda r: (r["exchange_event_time_utc"], r["_order"]))
        path = out_root / f"data_type={DATA_TYPES[kind]}" / f"date={day}" / "part-000.parquet"
        _write_partition(cfg, kind, rs, sessions[(kind, day)], path)
        written.append({"book": kind, "date": day, "rows": len(rs), "path": str(path)})

    intervals = {k: [iv.as_dict() for iv in b.intervals] for k, b in drv.books.items()}
    return {
        "digests": drv.digests(),
        "partitions": written,
        "outcome_counts": counts,
        "intervals": intervals,
        "stats": dict(drv.stats.__dict__),
        "counters": {k: dict(b.counters.__dict__) for k, b in drv.books.items()},
    }


def _write_partition(cfg: DriverConfig, kind: str, rs: list[dict[str, Any]], sess: list[str], path: Path) -> None:
    n = len(rs)
    inst = cfg.key(kind).instrument_id

    def const(v: str) -> pa.Array:
        return pa.array([v] * n, pa.string()).dictionary_encode().cast(pa.dictionary(pa.int8(), pa.string()))

    def dict_col(name: str) -> pa.Array:
        return pa.array([r[name] for r in rs], pa.string()).dictionary_encode().cast(pa.dictionary(pa.int8(), pa.string()))

    ts = pa.timestamp("ms", tz="UTC")
    cols: dict[str, pa.Array] = {
        "schema_version": pa.array([SCHEMA_VERSION] * n, pa.int16()),
        "provider_id": const(cfg.provider_id),
        "venue_id": const(cfg.venue_id),
        "market_type": const(cfg.market_type),
        "instrument_id": const(inst),
        "exchange_event_time_utc": pa.array([r["exchange_event_time_utc"] for r in rs], pa.int64()).cast(ts),
        "transaction_time_utc": pa.array([r["transaction_time_utc"] for r in rs], pa.int64()).cast(ts),
        "first_update_id": pa.array([r["first_update_id"] for r in rs], pa.int64()),
        "final_update_id": pa.array([r["final_update_id"] for r in rs], pa.int64()),
        "previous_update_id": pa.array([r["previous_update_id"] for r in rs], pa.int64()),
        "bids": pa.array([r["bids"] for r in rs], DEPTH_SCHEMA.field("bids").type),
        "asks": pa.array([r["asks"] for r in rs], DEPTH_SCHEMA.field("asks").type),
        "snapshot_or_delta": dict_col("snapshot_or_delta"),
        "sequence_validity": dict_col("sequence_validity"),
        "data_quality_status": dict_col("data_quality_status"),
        "raw_file_id": pa.array([r["raw_file_id"] for r in rs], pa.int32()),
        "raw_row": pa.array([r["raw_row"] for r in rs], pa.int64()),
    }
    md = dict(DEPTH_SCHEMA.metadata or {})
    md[b"goldflow.capture_sessions"] = json.dumps(sess).encode()
    md[b"goldflow.book_kind"] = kind.encode()
    limits = {f"{r['raw_file_id']}:{r['raw_row']}": r["_limit"] for r in rs if r.get("_limit") is not None}
    md[b"goldflow.snapshot_limits"] = json.dumps(limits).encode()
    t = pa.Table.from_arrays([cols[f.name] for f in DEPTH_SCHEMA], schema=DEPTH_SCHEMA.with_metadata(md))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    pq.write_table(t, tmp, compression="zstd")
    os.replace(tmp, path)


def write_validity(intervals: dict[str, list[dict[str, Any]]], cfg: DriverConfig, out_root: Path) -> list[Path]:
    """Validity intervals per book: data/derived/.../data_type=depth_validity/book=<kind>/intervals.parquet."""
    paths = []
    for kind, ivs in sorted(intervals.items()):
        p = out_root / "data_type=depth_validity" / f"book={kind}" / "intervals.parquet"
        p.parent.mkdir(parents=True, exist_ok=True)
        t = pa.Table.from_pylist(
            [{"book": cfg.key(kind).label, **iv} for iv in ivs],
            schema=pa.schema(
                [
                    ("book", pa.string()),
                    ("start_ms", pa.int64()),
                    ("end_ms", pa.int64()),
                    ("snapshot_update_id", pa.int64()),
                    ("start_update_id", pa.int64()),
                    ("last_update_id", pa.int64()),
                    ("events_applied", pa.int64()),
                    ("end_reason", pa.string()),
                ]
            ),
        )
        tmp = p.with_suffix(".parquet.tmp")
        pq.write_table(t, tmp)
        os.replace(tmp, p)
        paths.append(p)
    return paths


# ------------------------------------------------------------------------------------------------- replay consumer
def _row_levels(levels: list[dict[str, float]] | None, prices: UnitScale, qtys: UnitScale) -> tuple[tuple[int, int], ...]:
    return tuple((prices.from_float(x["price"]), qtys.from_float(x["quantity"])) for x in (levels or []))


def apply_depth_rows(book: LocalOrderBook, table: pa.Table, prices: UnitScale, qtys: UnitScale) -> None:
    """Feed DepthEvent v1 rows (in replay order) to a book. Uses only reported columns plus the boundary markers;
    the stored `sequence_validity` labels are not trusted: the book re-derives every decision."""
    if table.num_rows == 0:
        return
    e = table.column("exchange_event_time_utc").cast(pa.int64()).to_pylist()
    tt = table.column("transaction_time_utc").cast(pa.int64()).to_pylist()
    kind = table.column("snapshot_or_delta").cast(pa.string()).to_pylist()
    val = table.column("sequence_validity").cast(pa.string()).to_pylist()
    U = table.column("first_update_id").to_pylist()
    u = table.column("final_update_id").to_pylist()
    pu = table.column("previous_update_id").to_pylist()
    bids = table.column("bids").to_pylist()
    asks = table.column("asks").to_pylist()
    sess = table.column("raw_file_id").to_pylist()
    rows = table.column("raw_row").to_pylist()
    limits = json.loads((table.schema.metadata or {}).get(b"goldflow.snapshot_limits", b"{}"))
    for i in range(table.num_rows):
        ref = f"{sess[i]}:{rows[i]}"
        k = kind[i]
        try:
            if k == "DELTA":
                b, a = _row_levels(bids[i], prices, qtys), _row_levels(asks[i], prices, qtys)
                book.on_update(DepthUpdate(e[i], tt[i], U[i], u[i], pu[i], b, a, None, ref))
            elif k == "SNAPSHOT":
                if val[i] == Outcome.INVALID_PAYLOAD.value or u[i] is None:
                    continue
                b, a = _row_levels(bids[i], prices, qtys), _row_levels(asks[i], prices, qtys)
                book.on_snapshot(DepthSnapshot(u[i], e[i], tt[i], b, a, ref, limits.get(ref)))
            elif k == "CONNECTION_LOST":
                book.on_disconnect("CONNECTION_LOST row", ref)
            elif k == "INVALID_PAYLOAD":
                book.on_invalid_payload("INVALID_PAYLOAD row", ref)
        except UnitError as ex:
            book.on_invalid_payload(str(ex), ref)
    book.drain_outcomes()


class DepthBookConsumer(Consumer):
    """Phase 4 replay consumer that rebuilds a book from the `depth_l2` (or `depth_l2_rpi`) stream.

    Enabling the stream in `replay.streams` (time_col `exchange_event_time_utc`) changes the replay config hash, so
    it is left off until recorded data exists. `on_seek` invalidates the book: continuity across a seek is unknown."""

    def __init__(self, cfg: DriverConfig, stream: str = "depth_l2", kind: str = "regular"):
        self.name = f"depth_book_{kind}"
        self.stream = stream
        self.book = LocalOrderBook(cfg.key(kind), max_buffer=cfg.max_buffer, invalidate_on_cross=cfg.invalidate_on_cross)
        self.prices, self.qtys = UnitScale(cfg.tick_size), UnitScale(cfg.step_size)

    def on_chunk(self, chunk: EventChunk) -> None:
        if self.stream not in chunk.tables:
            return
        apply_depth_rows(self.book, chunk.stream_table(self.stream), self.prices, self.qtys)

    def on_seek(self, t_ms: int) -> None:
        self.book.on_disconnect("SEEK")
        self.book.drain_outcomes()

    def get_state(self) -> dict[str, Any]:
        return self.book.get_state()

    def set_state(self, state: dict[str, Any]) -> None:
        self.book.set_state(state)
