"""Phase 10: derived Initial Balance dataset in Parquet.

Layout:
  data/derived/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/initial_balance/date=YYYY-MM-DD/events.parquet
                                                                                                  .../developing.parquet
                                                                                                  .../sessions.parquet
  .../initial_balance/_manifest.json      per-date row counts and digests, settings hashes, source dataset version

A row belongs to the UTC date of ``event_time - 1 ms``. Rows are buffered in memory, written to ``date=D/_staging/``
at every replay checkpoint (so a crash loses nothing and a resume continues exactly) and merged into the date's files
once every row of that date exists.
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
from .tracker import _kname

TS, I64, F64, STR, BOOL = S.TS, pa.int64(), pa.float64(), pa.string(), pa.bool_()
KEY = [
    ("ib", STR),
    ("label", STR),
    ("ib_start", TS),
    ("ib_end", TS),
    ("session_end", TS),
    ("event", STR),
    ("event_time", TS),
    ("available_at", TS),
    ("evidence_start", TS),
    ("evidence_end", TS),
]
LABELS = [
    ("holiday", STR),
    ("holiday_status", STR),
    ("gold_closed", BOOL),
    ("weekend", BOOL),
    ("dq_status", STR),
    ("coverage", STR),
]
IBF = [("ib_high_ticks", I64), ("ib_low_ticks", I64), ("ib_mid_ticks", F64), ("ib_range_ticks", I64)]


def table_fields(extensions: Any) -> dict[str, list[tuple[str, Any]]]:
    ext = []
    for k in extensions:
        ext += [(f"ext_up_{_kname(k)}_ticks", F64), (f"ext_dn_{_kname(k)}_ticks", F64)]
    counts = [(f"{e}_{s}", I64) for e in ("breakout", "failed", "held", "returned", "accepted") for s in ("UP", "DOWN")]
    return {
        "events": KEY
        + [
            ("direction", STR),
            ("price_ticks", F64),
            ("k", STR),
            ("criterion", STR),
            ("close_ticks", I64),
            ("bar_volume_lots", I64),
            ("bar_delta_lots", I64),
            ("delta_confirms", BOOL),
            ("volume_confirms", BOOL),
            ("beyond_lots", I64),
            ("window_lots", I64),
            ("open_ticks", I64),
            ("volume_lots", I64),
            ("buy_lots", I64),
            ("sell_lots", I64),
            ("delta_lots", I64),
        ]
        + IBF
        + ext
        + LABELS,
        "developing": KEY + IBF + ext + [("volume_lots", I64), ("delta_lots", I64)] + LABELS,
        "sessions": KEY
        + IBF
        + ext
        + [
            ("session_high_ticks", I64),
            ("session_low_ticks", I64),
            ("close_ticks", I64),
            ("close_location", STR),
            ("range_ratio", F64),
            ("volume_lots", I64),
            ("max_ext_up", STR),
            ("max_ext_down", STR),
            ("open_breakout_up", BOOL),
            ("open_breakout_down", BOOL),
        ]
        + counts
        + LABELS,
    }


def ib_root(data_root: Path, symbol: str) -> Path:
    return (
        Path(data_root)
        / "derived"
        / f"provider={S.PROVIDER_ID}"
        / f"market={S.MARKET_TYPE}"
        / f"instrument={symbol}"
        / "initial_balance"
    )


def row_date(t_ms: int) -> str:
    return str(date.fromordinal(date(1970, 1, 1).toordinal() + (t_ms - 1) // DAY_MS))


def to_table(fields: list[tuple[str, Any]], rows: list[dict[str, Any]]) -> pa.Table:
    cols = []
    for name, typ in fields:
        vals = [r.get(name) for r in rows]
        cols.append(pa.array(vals, I64).cast(typ) if pa.types.is_timestamp(typ) else pa.array(vals, typ))
    return pa.Table.from_arrays(cols, schema=pa.schema([pa.field(n, t) for n, t in fields]))


class IBStore:
    TABLES = ("events", "developing", "sessions")

    def __init__(self, data_root: Path, symbol: str, replace: bool = False) -> None:
        self.base = ib_root(data_root, symbol)
        self.replace = replace
        self.fields: dict[str, list[tuple[str, Any]]] = {}
        self.buf: dict[str, dict[str, list[dict[str, Any]]]] = {t: {} for t in self.TABLES}
        self.staged: dict[str, list[str]] = {}
        self.written: dict[str, Any] = {}
        self.meta: dict[str, Any] = {}
        self.done_this_run: set[str] = set()

    def start(self, meta: dict[str, Any], extensions: Any, resumed: bool) -> None:
        self.fields = table_fields(extensions)
        self.meta = meta
        mp = self.base / "_manifest.json"
        if mp.exists():
            m = json.loads(mp.read_text())
            same = all(m.get(k) == meta[k] for k in ("ib_settings_hash", "processing_settings_hash", "source_dataset_version"))
            if not same and not self.replace:
                raise ReplayGuardError(
                    f"{mp} was built with different settings or source data; rerun with a fresh "
                    "(non-resumed) ib run to replace it"
                )
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
            d = row_date(int(r["event_time"]))
            if d in self.done_this_run:
                raise RuntimeError(f"IB rows for {d} arrived after that date was written as complete")
            self.buf[table].setdefault(d, []).append(r)

    def flush_complete(self, upto_ms: int, force: bool = False) -> None:
        for d in sorted({d for t in self.TABLES for d in self.buf[t]} | set(self.staged)):
            end = (date.fromisoformat(d).toordinal() - date(1970, 1, 1).toordinal() + 1) * DAY_MS
            if force or end <= upto_ms:
                self._write_date(d, complete=end <= upto_ms)

    def discard_unwritten(self) -> None:
        self.buf = {t: {} for t in self.TABLES}
        for d in list(self.staged):
            self._clear_staging(d)
        self.staged = {}

    def get_state(self) -> dict[str, Any]:
        for d in sorted({d for t in self.TABLES for d in self.buf[t]}):
            rows = {t: self.buf[t].pop(d, []) for t in self.TABLES}
            stem = f"{len(self.staged.get(d, []))}"
            for t in self.TABLES:
                write_atomic(to_table(self.fields[t], rows[t]), self.base / f"date={d}" / "_staging" / f"{t}-{stem}.parquet")
            self.staged.setdefault(d, []).append(stem)
        return {"staged": self.staged, "written": self.written, "done": sorted(self.done_this_run)}

    def set_state(self, s: dict[str, Any]) -> None:
        if s:
            self.staged = {d: list(v) for d, v in s.get("staged", {}).items()}
            self.written = s.get("written", {})
            self.done_this_run = set(s.get("done", []))
            self.buf = {t: {} for t in self.TABLES}

    def _clear_staging(self, d: str) -> None:
        sd = self.base / f"date={d}" / "_staging"
        if sd.exists():
            for f in sorted(sd.iterdir()):
                safe_remove(f)

    def _write_date(self, d: str, complete: bool) -> None:
        dd = self.base / f"date={d}"
        entry: dict[str, Any] = {"date_complete": complete}
        meta = {
            "goldflow.schema": "InitialBalance",
            "goldflow.date": d,
            "goldflow.date_complete": str(complete),
            **{f"goldflow.{k}": str(v) for k, v in self.meta.items()},
        }
        for t in self.TABLES:
            parts = [pq.read_table(dd / "_staging" / f"{t}-{stem}.parquet") for stem in self.staged.get(d, [])]
            rows = self.buf[t].pop(d, [])
            if rows:
                parts.append(to_table(self.fields[t], rows))
            tbl = pa.concat_tables([p.replace_schema_metadata(None) for p in parts]) if parts else to_table(self.fields[t], [])
            tbl = tbl.sort_by(
                [
                    ("event_time", "ascending"),
                    ("ib", "ascending"),
                    ("event", "ascending"),
                    ("price_ticks" if t == "events" else "ib_start", "ascending"),
                ]
            )
            tbl = tbl.replace_schema_metadata({**meta, "goldflow.table": t})
            sha = write_atomic(tbl, dd / f"{t}.parquet")
            entry[t] = {"rows": tbl.num_rows, "logical_digest": logical_digest(tbl), "sha256": sha}
        self._clear_staging(d)
        self.staged.pop(d, None)
        if complete:
            self.done_this_run.add(d)
        self.written[d] = entry
        self._write_manifest()

    def _write_manifest(self) -> None:
        m = {
            "schema": "InitialBalance events/developing/sessions",
            **self.meta,
            "spec": "docs/METRICS_IB_V1.md",
            "dates": dict(sorted(self.written.items())),
        }
        p = self.base / "_manifest.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + ".tmp")
        tmp.write_text(json.dumps(m, indent=1, default=str))
        os.replace(tmp, p)


def read_manifest(data_root: Path, symbol: str) -> dict[str, Any]:
    p = ib_root(data_root, symbol) / "_manifest.json"
    return json.loads(p.read_text()) if p.exists() else {}


def read_table(data_root: Path, symbol: str, table: str, d: str) -> pa.Table:
    return pq.read_table(ib_root(data_root, symbol) / f"date={d}" / f"{table}.parquet")
