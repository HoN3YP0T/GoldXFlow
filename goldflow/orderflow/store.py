"""Phase 6: derived profile dataset (snapshots, nodes, final-window profiles) in Parquet.

Layout (one set of files per UTC date):
  data/derived/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/profile/date=YYYY-MM-DD/snapshots.parquet
                                                                                              .../nodes.parquet
                                                                                              .../levels.parquet
  .../profile/_manifest.json     per-date row counts and digests, settings hashes, source dataset version

A row belongs to the UTC date of ``as_of - 1 ms``, the last millisecond it covers, so the 00:00 snapshot that closes a
day is stored with that day. A date is written once every snapshot belonging to it has been produced. Between replay
checkpoints rows are buffered in memory; at every checkpoint (``get_state``) the buffer is written to ``date=D/_staging/`` and the
checkpoint records the staged files, so a crash loses nothing and a resume continues exactly (same as the Phase 5
footprint writer).
"""

from __future__ import annotations

import json
import os
from decimal import Decimal
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
DEC = pa.decimal128(38, 0)

KEY_FIELDS = [
    ("window_key", STR),
    ("kind", STR),
    ("name", STR),
    ("label", STR),
    ("window_start", TS),
    ("window_end", TS),
    ("covered_from", TS),
    ("as_of", TS),
    ("available_at", TS),
    ("is_final", BOOL),
    ("bin_ticks", I64),
]


def snapshot_fields(band_k: tuple[int, ...]) -> list[tuple[str, Any]]:
    bands = []
    for k in band_k:
        bands += [(f"band_up_{k}_ticks", F64), (f"band_dn_{k}_ticks", F64)]
    return (
        KEY_FIELDS
        + [
            ("coverage", STR),
            ("dq_status", STR),
            ("total_lots", I64),
            ("buy_lots", I64),
            ("sell_lots", I64),
            ("low_ticks", I64),
            ("high_ticks", I64),
            ("n_bins", I64),
            ("low_bin_ticks", I64),
            ("high_bin_ticks", I64),
            ("poc_ticks", I64),
            ("poc_lots", I64),
            ("val_ticks", I64),
            ("vah_ticks", I64),
            ("va_lots", I64),
            ("va_bins", I64),
            ("va_steps", I64),
            ("va_target", STR),
            ("hvn_count", I64),
            ("lvn_count", I64),
            ("sum_q", I64),
            ("sum_qp", I64),
            ("sum_qp2", DEC),
            ("vwap_ticks", F64),
            ("sigma_ticks", F64),
        ]
        + bands
    )


NODE_FIELDS = KEY_FIELDS + [
    ("node_type", STR),
    ("bin", I64),
    ("price_ticks", I64),
    ("lots", I64),
    ("smoothed_lots", F64),
    ("prominence_lots", F64),
    ("left_ticks", F64),
    ("right_ticks", F64),
    ("width_ticks", F64),
]
LEVEL_FIELDS = KEY_FIELDS + [("bin", I64), ("price_ticks", I64), ("lots", I64), ("buy_lots", I64), ("sell_lots", I64)]
DOC = {
    "units": "prices in ticks (tick_size USDT); volumes in lots (step_size oz); sum_qp in tick*lots, sum_qp2 in tick^2*lots",
    "bins": "bin b holds ticks [b*bin_ticks, (b+1)*bin_ticks-1] and is reported at its lowest price b*bin_ticks",
    "as_of": "the snapshot contains exactly the trades with exchange time < as_of; available_at = as_of + trade shift",
    "date": "a row is stored under the UTC date of as_of - 1 ms (the 00:00 snapshot closing a day belongs to that day)",
    "is_final": "true for the snapshot at the window end (emitted after the window was finalised by the processor)",
    "window_end": "null for anchored windows (open-ended)",
    "covered_from": "first trade time the snapshot can contain: window_start, or later when processing began inside the window "
    "(coverage PARTIAL_START)",
    "vwap": "vwap = sum_qp/sum_q; sigma^2 = (sum_q*sum_qp2 - sum_qp^2)/sum_q^2 (volume-weighted population variance)",
    "levels": "levels.parquet holds the binned profile of final snapshots only (rebuild developing ones from trades)",
    "spec": "docs/METRICS_PROFILE_V1.md",
}


def profile_root(data_root: Path, symbol: str) -> Path:
    return (
        Path(data_root) / "derived" / f"provider={S.PROVIDER_ID}" / f"market={S.MARKET_TYPE}" / f"instrument={symbol}" / "profile"
    )


def _to_table(fields: list[tuple[str, Any]], rows: list[dict[str, Any]]) -> pa.Table:
    cols = []
    for name, typ in fields:
        vals = [r.get(name) for r in rows]
        if typ == DEC:
            cols.append(pa.array([None if v is None else Decimal(v) for v in vals], DEC))
        elif pa.types.is_timestamp(typ):
            cols.append(pa.array(vals, I64).cast(typ))
        else:
            cols.append(pa.array(vals, typ))
    return pa.Table.from_arrays(cols, schema=pa.schema([pa.field(n, t) for n, t in fields]))


SORT = [("as_of", "ascending"), ("window_key", "ascending"), ("bin_ticks", "ascending")]


class ProfileStore:
    TABLES = ("snapshots", "nodes", "levels")

    def __init__(self, data_root: Path, symbol: str, replace: bool = False) -> None:
        self.base = profile_root(data_root, symbol)
        self.replace = replace
        self.fields: dict[str, list[tuple[str, Any]]] = {}
        self.buf: dict[str, dict[str, list[dict[str, Any]]]] = {t: {} for t in self.TABLES}
        self.staged: dict[str, list[str]] = {}  # date -> stems
        self.written: dict[str, Any] = {}
        self.meta: dict[str, Any] = {}
        self.done_this_run: set[str] = set()

    # ------------------------------------------------------------------ lifecycle
    def start(self, meta: dict[str, Any], band_k: tuple[int, ...], resumed: bool) -> None:
        self.fields = {"snapshots": snapshot_fields(band_k), "nodes": NODE_FIELDS, "levels": LEVEL_FIELDS}
        self.meta = meta
        mp = self.base / "_manifest.json"
        if mp.exists():
            m = json.loads(mp.read_text())
            same = all(
                m.get(k) == meta[k] for k in ("profile_settings_hash", "processing_settings_hash", "source_dataset_version")
            )
            if not same and not self.replace:
                raise ReplayGuardError(
                    f"{mp} was built with different settings or source data; rerun with a fresh "
                    "(non-resumed) vprofile run to replace it"
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
            d = row_date(int(r["as_of"]))
            if d in self.done_this_run:
                raise RuntimeError(f"profile rows for {d} arrived after that date was written as complete")
            self.buf[table].setdefault(d, []).append(r)

    def flush_complete(self, upto_ms: int, force: bool = False) -> None:
        dates = sorted({d for t in self.TABLES for d in self.buf[t]} | set(self.staged))
        for d in dates:
            end = (int(_day_index(d)) + 1) * DAY_MS
            if force or end <= upto_ms:
                self._write_date(d, complete=end <= upto_ms)

    def discard_unwritten(self) -> None:
        for t in self.TABLES:
            self.buf[t] = {}
        for d in list(self.staged):
            self._clear_staging(d)
        self.staged = {}

    # ------------------------------------------------------------------ checkpoint state = staged files
    def get_state(self) -> dict[str, Any]:
        dates = sorted({d for t in self.TABLES for d in self.buf[t]})
        for d in dates:
            rows = {t: self.buf[t].pop(d, []) for t in self.TABLES}
            if not any(rows.values()):
                continue
            snaps = rows["snapshots"] or rows["nodes"] or rows["levels"]
            stem = f"{int(snaps[0]['as_of'])}-{int(snaps[-1]['as_of'])}-{len(self.staged.get(d, []))}"
            sd = self._ddir(d) / "_staging"
            for t in self.TABLES:
                write_atomic(_to_table(self.fields[t], rows[t]), sd / f"{t}-{stem}.parquet")
            self.staged.setdefault(d, []).append(stem)
        return {"staged": self.staged, "written": self.written, "done": sorted(self.done_this_run)}

    def set_state(self, s: dict[str, Any]) -> None:
        if not s:
            return
        self.staged = {d: list(v) for d, v in s.get("staged", {}).items()}
        self.written = s.get("written", {})
        self.done_this_run = set(s.get("done", []))
        self.buf = {t: {} for t in self.TABLES}

    # ------------------------------------------------------------------ writing
    def _ddir(self, d: str) -> Path:
        return self.base / f"date={d}"

    def _clear_staging(self, d: str) -> None:
        sd = self._ddir(d) / "_staging"
        if sd.exists():
            for f in sorted(sd.iterdir()):
                safe_remove(f)

    def _write_date(self, d: str, complete: bool) -> None:
        parts: dict[str, list[pa.Table]] = {t: [] for t in self.TABLES}
        for stem in self.staged.get(d, []):
            for t in self.TABLES:
                parts[t].append(pq.read_table(self._ddir(d) / "_staging" / f"{t}-{stem}.parquet"))
        for t in self.TABLES:
            rows = self.buf[t].pop(d, [])
            if rows:
                parts[t].append(_to_table(self.fields[t], rows))
        if not any(parts.values()):
            return
        meta = {
            "goldflow.schema": "ProfileSnapshot",
            "goldflow.schema_version": str(SCHEMA_VERSION),
            "goldflow.date": d,
            "goldflow.date_complete": str(complete),
            "goldflow.doc": json.dumps(DOC),
            **{f"goldflow.{k}": str(v) for k, v in self.meta.items()},
        }
        entry: dict[str, Any] = {"date_complete": complete}
        for t in self.TABLES:
            tbl = (
                pa.concat_tables([x.replace_schema_metadata(None) for x in parts[t]])
                if parts[t]
                else _to_table(self.fields[t], [])
            )
            keys = SORT + ([("node_type", "ascending"), ("price_ticks", "ascending")] if t == "nodes" else [])
            keys += [("price_ticks", "ascending")] if t == "levels" else []
            tbl = tbl.sort_by(keys).replace_schema_metadata({**meta, "goldflow.table": t})
            sha = write_atomic(tbl, self._ddir(d) / f"{t}.parquet")
            entry[t] = {"rows": tbl.num_rows, "logical_digest": logical_digest(tbl), "sha256": sha}
        snaps = pq.read_table(self._ddir(d) / "snapshots.parquet", columns=["is_final", "kind"])
        fin = snaps.filter(snaps.column("is_final"))
        entry["final_snapshots"] = fin.num_rows
        entry["developing_snapshots"] = snaps.num_rows - fin.num_rows
        self._clear_staging(d)
        self.staged.pop(d, None)
        if complete:
            self.done_this_run.add(d)
        self.written[d] = entry
        self._write_manifest()

    def _write_manifest(self) -> None:
        m = {
            "schema": "ProfileSnapshot/ProfileNode/ProfileLevel",
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


def _day_index(d: str) -> int:
    from datetime import date

    return date.fromisoformat(d).toordinal() - date(1970, 1, 1).toordinal()


def row_date(as_of_ms: int) -> str:
    """Partition date of a row: the UTC date of the last millisecond the snapshot covers (as_of - 1 ms)."""
    from datetime import date

    return str(date.fromordinal(date(1970, 1, 1).toordinal() + (as_of_ms - 1) // DAY_MS))


def read_manifest(data_root: Path, symbol: str) -> dict[str, Any]:
    p = profile_root(data_root, symbol) / "_manifest.json"
    return json.loads(p.read_text()) if p.exists() else {}


def read_table(data_root: Path, symbol: str, table: str, d: str) -> pa.Table:
    return pq.read_table(profile_root(data_root, symbol) / f"date={d}" / f"{table}.parquet")
