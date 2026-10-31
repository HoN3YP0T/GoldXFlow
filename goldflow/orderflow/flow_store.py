"""Phase 7: derived orderflow dataset (Parquet) and manifest.

Layout, one set of files per UTC date:
  data/derived/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/orderflow/date=YYYY-MM-DD/<table>.parquet
  .../orderflow/_manifest.json      per-date row counts, logical digests, SHA-256s, settings hashes, source version

Tables: bars, cvd, imbalances, stacked, big_trades, events (see docs/METRICS_ORDERFLOW_V1.md §8).
A row belongs to the UTC date of ``available_at - 1 ms`` (the moment it became known), so every row of a date has
been produced once the processor clock has passed the end of that date. Between replay checkpoints rows are
buffered; at each checkpoint the buffer is staged under ``date=D/_staging/`` and listed in the checkpoint, so a crash
loses nothing and a resume continues exactly (same mechanism as the Phase 5 and Phase 6 writers).
"""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .. import schemas as S
from ..fsutil import safe_remove
from ..normalize import logical_digest, write_atomic
from ..replay import ReplayGuardError
from ..timeutil import DAY_MS

SCHEMA_VERSION = 1
TS = S.TS
I64, F64, STR, BOOL = pa.int64(), pa.float64(), pa.string(), pa.bool_()
LIST_I64 = pa.list_(pa.int64())
COMMON = [("dq_status", STR), ("coverage", STR)]

FIELDS: dict[str, list[tuple[str, Any]]] = {
    "bars": [("interval", STR), ("bar_start", TS), ("bar_end", TS), ("available_at", TS), ("open_ticks", I64),
             ("high_ticks", I64), ("low_ticks", I64), ("close_ticks", I64), ("volume_lots", I64), ("buy_lots", I64),
             ("sell_lots", I64), ("unknown_lots", I64), ("delta_lots", I64), ("delta_high", I64), ("delta_low", I64),
             ("trade_count", I64), ("buy_count", I64), ("sell_count", I64), ("first_trade_id", I64),
             ("last_trade_id", I64), ("unknown_share", F64), ("delta_pct", F64), ("weekend", BOOL)] + COMMON,
    "cvd": [("interval", STR), ("bar_start", TS), ("bar_end", TS), ("available_at", TS), ("series", STR),
            ("series_key", STR), ("series_start", TS), ("series_end", TS), ("straddles", BOOL), ("cvd_open", I64), ("cvd_high", I64),
            ("cvd_low", I64), ("cvd_close", I64), ("coverage", STR)],
    "imbalances": [("interval", STR), ("bar_start", TS), ("available_at", TS), ("method", STR), ("side", STR),
                   ("bin_ticks", I64), ("price_ticks", I64), ("compare_price_ticks", I64), ("lots", I64),
                   ("opposite_lots", I64), ("unknown_lots", I64), ("ratio", F64), ("first_trade_id", I64),
                   ("last_trade_id", I64), ("bar_unknown_share", F64)] + COMMON,
    "stacked": [("interval", STR), ("bar_start", TS), ("available_at", TS), ("method", STR), ("side", STR),
                ("bin_ticks", I64), ("low_price_ticks", I64), ("high_price_ticks", I64), ("n_bins", I64), ("lots", I64),
                ("first_trade_id", I64), ("last_trade_id", I64)] + COMMON,
    "big_trades": [("first_time", TS), ("last_time", TS), ("available_at", TS), ("side", STR), ("n_fills", I64),
                   ("lots", I64), ("notional_tick_lots", I64), ("vwap_ticks", F64), ("first_price_ticks", I64),
                   ("last_price_ticks", I64), ("min_price_ticks", I64), ("max_price_ticks", I64),
                   ("first_trade_id", I64), ("last_trade_id", I64), ("trade_ids", LIST_I64), ("pass_qty", BOOL),
                   ("pass_notional", BOOL), ("pass_rolling", BOOL), ("pass_time_of_day", BOOL),
                   ("rolling_threshold_lots", I64), ("rolling_samples", I64), ("tod_threshold_lots", I64),
                   ("tod_samples", I64), ("weekend", BOOL)] + COMMON,
    "events": [("event_id", STR), ("event_type", STR), ("status", STR), ("side", STR), ("interval", STR),
               ("bin_ticks", I64), ("evidence_start", TS), ("evidence_end", TS), ("detected_at", TS), ("event_time", TS),
               ("available_at", TS), ("level_low_ticks", I64), ("level_high_ticks", I64), ("reference_ticks", I64),
               ("confirm_ticks", I64), ("invalidate_ticks", I64), ("deadline", TS), ("resolution_price_ticks", I64),
               ("reason", STR), ("lots", I64), ("first_trade_id", I64), ("last_trade_id", I64), ("unknown_share", F64),
               ("dq_status", STR), ("coverage", STR), ("detail", STR)],
}  # fmt: skip
SORT: dict[str, list[tuple[str, str]]] = {
    "bars": [("interval", "ascending"), ("bar_start", "ascending")],
    "cvd": [("interval", "ascending"), ("bar_start", "ascending"), ("series_key", "ascending")],
    "imbalances": [("interval", "ascending"), ("bar_start", "ascending"), ("side", "ascending"), ("price_ticks", "ascending")],
    "stacked": [("interval", "ascending"), ("bar_start", "ascending"), ("side", "ascending"), ("low_price_ticks", "ascending")],
    "big_trades": [("first_time", "ascending"), ("first_trade_id", "ascending")],
    "events": [("available_at", "ascending"), ("event_id", "ascending"), ("status", "ascending")],
}
TABLES = tuple(FIELDS)
DOC = {
    "units": "prices in ticks (tick_size USDT); volumes in lots (step_size oz); notional in tick*lots (USDT = x 1e-5)",
    "sides": "buy/sell from the exchange-reported aggressor only; unknown_lots are never assigned to a side",
    "date": "a row is stored under the UTC date of available_at - 1 ms (the moment it became known)",
    "available_at": "the earliest simulated time at which the row may be used (no look-ahead)",
    "bins": "price_ticks of a bin is its lowest price: bin * bin_ticks",
    "trace": "every row carries first/last source trade ids; big_trades carry every fill id; events state their "
    "trade selection rule in docs/METRICS_ORDERFLOW_V1.md",
    "spec": "docs/METRICS_ORDERFLOW_V1.md",
}


def orderflow_root(data_root: Path, symbol: str) -> Path:
    return (
        Path(data_root)
        / "derived"
        / f"provider={S.PROVIDER_ID}"
        / f"market={S.MARKET_TYPE}"
        / f"instrument={symbol}"
        / "orderflow"
    )


def row_date(available_at_ms: int) -> str:
    return str(date.fromordinal(date(1970, 1, 1).toordinal() + (int(available_at_ms) - 1) // DAY_MS))


def _day_end(d: str) -> int:
    return (date.fromisoformat(d).toordinal() - date(1970, 1, 1).toordinal() + 1) * DAY_MS


def to_table(table: str, rows: list[dict[str, Any]], fields_map: dict[str, list[tuple[str, Any]]] | None = None) -> pa.Table:
    cols = []
    fields = (fields_map or FIELDS)[table]
    for name, typ in fields:
        vals = [r.get(name) for r in rows]
        if pa.types.is_timestamp(typ):
            cols.append(pa.array(vals, I64).cast(typ))
        else:
            cols.append(pa.array(vals, typ))
    return pa.Table.from_arrays(cols, schema=pa.schema([pa.field(n, t) for n, t in fields]))


class DerivedStore:
    """Dated Parquet dataset staged at replay checkpoints. Subclasses set the dataset name, tables, sort keys and the
    manifest keys that must match for a dataset to be extended rather than replaced."""

    NAME = "orderflow"
    SCHEMA = "Orderflow"
    FIELDS: dict[str, list[tuple[str, Any]]] = FIELDS
    SORT: dict[str, list[tuple[str, str]]] = SORT
    DOC: dict[str, str] = DOC
    MATCH_KEYS: tuple[str, ...] = ("orderflow_settings_hash", "processing_settings_hash", "source_dataset_version")

    def __init__(self, data_root: Path, symbol: str, replace: bool = False) -> None:
        self.base = orderflow_root(data_root, symbol).with_name(self.NAME)
        self.replace = replace
        self.tables = tuple(self.FIELDS)
        self.buf: dict[str, dict[str, list[dict[str, Any]]]] = {t: {} for t in self.tables}
        self.staged: dict[str, list[str]] = {}
        self.written: dict[str, Any] = {}
        self.meta: dict[str, Any] = {}
        self.done_this_run: set[str] = set()
        self.seq = 0

    def start(self, meta: dict[str, Any], resumed: bool) -> None:
        self.meta = meta
        mp = self.base / "_manifest.json"
        if mp.exists():
            m = json.loads(mp.read_text())
            same = all(m.get(k) == meta[k] for k in self.MATCH_KEYS)
            if not same and not self.replace:
                raise ReplayGuardError(f"{mp} was built with different settings or source data; run a fresh (non-resumed) run")
            if resumed:
                return
            if same and not self.replace:
                self.written = m.get("dates", {})
                return
        if not resumed:
            self.written = {}
            self._write_manifest()

    def add(self, table: str, rows: list[dict[str, Any]]) -> None:
        for r in rows:
            d = row_date(int(r["available_at"]))
            if d in self.done_this_run:
                raise RuntimeError(f"{self.NAME} {table} rows for {d} arrived after that date was written as complete")
            self.buf[table].setdefault(d, []).append(r)

    def flush_complete(self, upto_ms: int, force: bool = False) -> None:
        dates = sorted({d for t in self.tables for d in self.buf[t]} | set(self.staged))
        for d in dates:
            end = _day_end(d)
            if force or end <= upto_ms:
                self._write_date(d, complete=end <= upto_ms)

    def discard_unwritten(self) -> None:
        self.buf = {t: {} for t in self.tables}
        for d in list(self.staged):
            self._clear_staging(d)
        self.staged = {}

    def get_state(self) -> dict[str, Any]:
        dates = sorted({d for t in self.tables for d in self.buf[t]})
        for d in dates:
            rows = {t: self.buf[t].pop(d, []) for t in self.tables}
            if not any(rows.values()):
                continue
            stem = f"{len(self.staged.get(d, []))}"
            sd = self._ddir(d) / "_staging"
            for t in self.tables:
                write_atomic(to_table(t, rows[t], self.FIELDS), sd / f"{t}-{stem}.parquet")
            self.staged.setdefault(d, []).append(stem)
        return {"staged": self.staged, "written": self.written, "done": sorted(self.done_this_run)}

    def set_state(self, s: dict[str, Any]) -> None:
        if not s:
            return
        self.staged = {d: list(v) for d, v in s.get("staged", {}).items()}
        self.written = s.get("written", {})
        self.done_this_run = set(s.get("done", []))
        self.buf = {t: {} for t in self.tables}

    def _ddir(self, d: str) -> Path:
        return self.base / f"date={d}"

    def _clear_staging(self, d: str) -> None:
        sd = self._ddir(d) / "_staging"
        if sd.exists():
            for f in sorted(sd.iterdir()):
                safe_remove(f)

    def _write_date(self, d: str, complete: bool) -> None:
        parts: dict[str, list[pa.Table]] = {t: [] for t in self.tables}
        for stem in self.staged.get(d, []):
            for t in self.tables:
                parts[t].append(pq.read_table(self._ddir(d) / "_staging" / f"{t}-{stem}.parquet"))
        for t in self.tables:
            rows = self.buf[t].pop(d, [])
            if rows:
                parts[t].append(to_table(t, rows, self.FIELDS))
        if not any(p.num_rows for ps in parts.values() for p in ps):
            self._clear_staging(d)
            self.staged.pop(d, None)
            return
        meta = {
            "goldflow.schema": self.SCHEMA,
            "goldflow.schema_version": str(SCHEMA_VERSION),
            "goldflow.date": d,
            "goldflow.date_complete": str(complete),
            "goldflow.doc": json.dumps(DOC),
            **{f"goldflow.{k}": str(v) for k, v in self.meta.items()},
        }
        entry: dict[str, Any] = {"date_complete": complete}
        for t in self.tables:
            tbl = (
                pa.concat_tables([x.replace_schema_metadata(None) for x in parts[t]])
                if parts[t]
                else to_table(t, [], self.FIELDS)
            )
            tbl = tbl.sort_by(self.SORT[t]).replace_schema_metadata({**meta, "goldflow.table": t})
            sha = write_atomic(tbl, self._ddir(d) / f"{t}.parquet")
            entry[t] = {"rows": tbl.num_rows, "logical_digest": logical_digest(tbl), "sha256": sha}
        self._clear_staging(d)
        self.staged.pop(d, None)
        if complete:
            self.done_this_run.add(d)
        self.written[d] = entry
        self._write_manifest()

    def _write_manifest(self) -> None:
        m = {
            "schema": "Orderflow",
            "schema_version": SCHEMA_VERSION,
            **self.meta,
            "doc": DOC,
            "dates": dict(sorted(self.written.items())),
        }
        p = self.base / "_manifest.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + ".tmp")
        tmp.write_text(json.dumps(m, indent=1, default=str))
        os.replace(tmp, p)


class OrderflowStore(DerivedStore):
    """Phase 7 dataset: bars, cvd, imbalances, stacked, big_trades, events."""


def read_manifest(data_root: Path, symbol: str) -> dict[str, Any]:
    p = orderflow_root(data_root, symbol) / "_manifest.json"
    return json.loads(p.read_text()) if p.exists() else {}


def read_table(data_root: Path, symbol: str, table: str, d: str) -> pa.Table:
    return pq.read_table(orderflow_root(data_root, symbol) / f"date={d}" / f"{table}.parquet")
