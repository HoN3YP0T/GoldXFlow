"""Phase 7 event catalogue storage: crash-safe parts during a run, date partitions plus manifest at the end.

  data/derived/provider=binance/market=usdm_perpetual/instrument=<SYM>/orderflow/
      events/date=D/part-000.parquet       all detected events and their status transitions (date of event time)
      imbalances/date=D/part-000.parquet   single-level footprint imbalances (date of bar start)
      cvd/date=D/part-000.parquet          CVD series per interval and reset (date of bar start)
      _manifest.json                       counts, logical digests, settings hashes, source dataset version
      _parts/<table>/part-NNNNNN.parquet   during a run only: flushed at every replay checkpoint

A checkpoint records the parts written so far. A resumed run deletes parts written after its checkpoint, so nothing
is duplicated or lost.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ..normalize import logical_digest, write_atomic

TS = pa.timestamp("ms", tz="UTC")
EVENT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("status", pa.string()),
        ("parent_id", pa.string()),
        ("side", pa.string()),
        ("interval", pa.string()),
        ("bin_ticks", pa.int64()),
        ("evidence_start_utc", TS),
        ("evidence_end_utc", TS),
        ("event_time_utc", TS),
        ("ref_price_ticks", pa.int64()),
        ("value_lots", pa.int64()),
        ("threshold_lots", pa.int64()),
        ("unknown_lots", pa.int64()),
        ("first_trade_id", pa.int64()),
        ("last_trade_id", pa.int64()),
        ("n_trades", pa.int64()),
        ("trade_ids", pa.list_(pa.int64())),
        ("sessions", pa.string()),
        ("weekend", pa.bool_()),
        ("dq_status", pa.string()),
        ("coverage", pa.string()),
        ("details", pa.string()),
    ]
)
IMB_SCHEMA = pa.schema(
    [
        ("bar_start_utc", TS),
        ("bar_end_utc", TS),
        ("available_at_utc", TS),
        ("interval", pa.string()),
        ("mode", pa.string()),
        ("bin_ticks", pa.int64()),
        ("bin", pa.int64()),
        ("price_ticks", pa.int64()),
        ("side", pa.string()),
        ("lots", pa.int64()),
        ("opp_lots", pa.int64()),
        ("unknown_lots", pa.int64()),
        ("first_trade_id", pa.int64()),
        ("last_trade_id", pa.int64()),
        ("stacked", pa.bool_()),
        ("weekend", pa.bool_()),
        ("dq_status", pa.string()),
        ("coverage", pa.string()),
    ]
)
CVD_SCHEMA = pa.schema(
    [
        ("interval", pa.string()),
        ("series", pa.string()),
        ("instance", pa.string()),
        ("bar_start_utc", TS),
        ("bar_end_utc", TS),
        ("available_at_utc", TS),
        ("bar_delta_lots", pa.int64()),
        ("bar_unknown_lots", pa.int64()),
        ("cvd_lots", pa.int64()),
        ("cum_buy_lots", pa.int64()),
        ("cum_sell_lots", pa.int64()),
        ("cum_unknown_lots", pa.int64()),
        ("coverage", pa.string()),
        ("dq_status", pa.string()),
        ("weekend", pa.bool_()),
    ]
)
SCHEMAS = {"events": EVENT_SCHEMA, "imbalances": IMB_SCHEMA, "cvd": CVD_SCHEMA}
DATE_COL = {"events": "event_time_utc", "imbalances": "bar_start_utc", "cvd": "bar_start_utc"}
SORT = {
    "events": [("event_time_utc", "ascending"), ("event_id", "ascending")],
    "imbalances": [("bar_start_utc", "ascending"), ("interval", "ascending"), ("bin", "ascending"), ("side", "ascending")],
    "cvd": [("bar_start_utc", "ascending"), ("interval", "ascending"), ("series", "ascending")],
}
MS_RENAME = {
    "evidence_start_ms": "evidence_start_utc",
    "evidence_end_ms": "evidence_end_utc",
    "event_time_ms": "event_time_utc",
    "bar_start": "bar_start_utc",
    "bar_end": "bar_end_utc",
    "available_at": "available_at_utc",
}


def orderflow_root(data_root: Path, symbol: str) -> Path:
    return Path(data_root) / "derived" / "provider=binance" / "market=usdm_perpetual" / f"instrument={symbol}" / "orderflow"


def to_table(name: str, cols: dict[str, Any]) -> pa.Table:
    sch = SCHEMAS[name]
    cols = {MS_RENAME.get(k, k): v for k, v in cols.items()}
    arrs = []
    for f in sch:
        v = cols[f.name]
        if pa.types.is_timestamp(f.type):
            arrs.append(pa.array(np.asarray(v, np.int64), pa.int64()).cast(f.type))
        elif isinstance(v, np.ndarray) and v.dtype != object:
            arrs.append(pa.array(v, f.type))
        else:
            arrs.append(pa.array(list(v), f.type))
    return pa.Table.from_arrays(arrs, schema=sch)


def read_partitions(d: Path, name: str) -> pa.Table:
    """Read a finalised table (all date partitions) without adding hive partition columns."""
    files = sorted(Path(d).glob("date=*/part-000.parquet"))
    if not files:
        return SCHEMAS[name].empty_table()
    return pa.concat_tables([pq.read_table(f) for f in files]).sort_by(SORT[name])


def rows_to_cols(rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    keys = list(rows[0]) if rows else []
    return {k: [r[k] for r in rows] for k in keys}


class OrderflowStore:
    def __init__(self, root: Path | None):
        self.root = Path(root) if root is not None else None  # None: keep everything in memory (tests)
        self.buf: dict[str, list[pa.Table]] = {k: [] for k in SCHEMAS}
        self.parts: dict[str, list[str]] = {k: [] for k in SCHEMAS}
        self.mem: dict[str, list[pa.Table]] = {k: [] for k in SCHEMAS}
        self.seq = 0
        self.finalized: dict[str, pa.Table] = {}

    def start(self, resumed: bool) -> None:
        if self.root is None:
            return
        pdir = self.root / "_parts"
        if not resumed:
            shutil.rmtree(pdir, ignore_errors=True)
            return
        keep = {p for v in self.parts.values() for p in v}
        if pdir.exists():
            for f in pdir.rglob("*.parquet"):
                if str(f.relative_to(pdir)) not in keep:
                    f.unlink()  # written after the checkpoint we resume from

    def add(self, name: str, cols: dict[str, Any]) -> None:
        n = len(next(iter(cols.values()))) if cols else 0
        if n:
            self.buf[name].append(to_table(name, cols))

    def flush(self) -> None:
        for name, tabs in self.buf.items():
            if not tabs:
                continue
            t = pa.concat_tables(tabs)
            self.buf[name] = []
            if self.root is None:
                self.mem[name].append(t)
                continue
            self.seq += 1
            rel = f"{name}/part-{self.seq:06d}.parquet"
            write_atomic(t, self.root / "_parts" / rel)
            self.parts[name].append(rel)

    def table(self, name: str) -> pa.Table:
        """Everything recorded so far for `name` (parts, memory and unflushed buffers), sorted."""
        if name in self.finalized:
            return self.finalized[name] if self.root is None else read_partitions(self.root / name, name)
        tabs = list(self.mem[name])
        if self.root is not None:
            tabs += [pq.read_table(self.root / "_parts" / p) for p in self.parts[name]]
        tabs += self.buf[name]
        t = pa.concat_tables(tabs) if tabs else SCHEMAS[name].empty_table()
        return t.sort_by(SORT[name])

    def finalize(self, meta: dict[str, Any]) -> dict[str, Any]:
        """Write date partitions and the manifest; remove the parts. Returns the manifest."""
        self.flush()
        man: dict[str, Any] = {**meta, "tables": {}}
        for name in SCHEMAS:
            t = self.table(name)
            self.finalized[name] = t if self.root is None else SCHEMAS[name].empty_table()
            man["tables"][name] = {"rows": t.num_rows, "logical_digest": logical_digest(t), "dates": {}}
            if self.root is None:
                continue
            shutil.rmtree(self.root / name, ignore_errors=True)
            if t.num_rows == 0:
                continue
            day = pc.cast(pc.divide(pc.cast(t.column(DATE_COL[name]), pa.int64()), 86_400_000), pa.int64()).to_numpy()
            for d in np.unique(day):
                sub = t.filter(pa.array(day == d))
                ds = str(np.datetime64(int(d), "D"))
                write_atomic(sub, self.root / name / f"date={ds}" / "part-000.parquet")
                man["tables"][name]["dates"][ds] = {"rows": sub.num_rows, "logical_digest": logical_digest(sub)}
        if self.root is not None:
            self.root.mkdir(parents=True, exist_ok=True)
            (self.root / "_manifest.json").write_text(json.dumps(man, indent=1, sort_keys=True))
            shutil.rmtree(self.root / "_parts", ignore_errors=True)
            self.parts = {k: [] for k in SCHEMAS}
        return man

    def get_state(self) -> dict[str, Any]:
        self.flush()
        return {"parts": self.parts, "seq": self.seq}

    def set_state(self, s: dict[str, Any]) -> None:
        if s:
            self.parts = {k: list(v) for k, v in s["parts"].items()}
            self.seq = int(s["seq"])
