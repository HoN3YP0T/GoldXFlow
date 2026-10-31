"""Phase 5: derived footprint dataset (finalised bars and their per-price-level footprint) in Parquet.

Layout (one pair of files per interval and UTC date of bar start):
  data/derived/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/footprint/interval=1m/date=YYYY-MM-DD/bars.parquet
                                                                                                   .../levels.parquet
  .../footprint/interval=1m/_manifest.json    per-date counts, totals, digests, settings hash, source dataset version

A date's files are written once every bar starting on that date is final (no developing bars are persisted). Between
checkpoints, finalised bars are buffered in memory. At every replay checkpoint (``get_state``) the buffer is flushed
to ``date=D/_staging/`` and the checkpoint records which staged files belong to it. A crash therefore loses nothing:
a resume reads the recorded staged files, and stray files from after the last checkpoint are ignored and removed.
"""
from __future__ import annotations
from typing import Any
import json, os
from pathlib import Path
import numpy as np
from numpy.typing import NDArray
import pyarrow as pa
import pyarrow.parquet as pq

from .. import schemas as S
from ..fsutil import safe_remove
from ..normalize import logical_digest, write_atomic
from ..replay import ReplayGuardError
from ..timeutil import DAY_MS
from .trade_processor import ProcessorSink

TS = S.TS
SCHEMA_VERSION = 1
BAR_FIELDS = [("bar_start", TS), ("bar_end", TS), ("available_at", TS), ("open_ticks", pa.int64()), ("high_ticks", pa.int64()),
              ("low_ticks", pa.int64()), ("close_ticks", pa.int64()), ("volume_lots", pa.int64()), ("buy_lots", pa.int64()),
              ("sell_lots", pa.int64()), ("unknown_lots", pa.int64()), ("delta_lots", pa.int64()), ("trade_count", pa.int64()),
              ("buy_count", pa.int64()), ("sell_count", pa.int64()), ("recovered_count", pa.int64()),
              ("notional_tick_lots", pa.int64()), ("inferred_buy_lots", pa.int64()), ("inferred_sell_lots", pa.int64()),
              ("first_trade_time", TS), ("last_trade_time", TS), ("first_trade_id", pa.int64()), ("last_trade_id", pa.int64()),
              ("n_levels_1tick", pa.int64()), ("weekend", pa.bool_()), ("dq_status", S.DICT), ("coverage", S.DICT),
              ("is_final", pa.bool_())]
LEVEL_FIELDS = [("bar_start", TS), ("level", pa.int64()), ("buy_lots", pa.int64()), ("sell_lots", pa.int64()),
                ("unknown_lots", pa.int64()), ("trade_count", pa.int64()), ("buy_count", pa.int64()), ("sell_count", pa.int64()),
                ("notional_tick_lots", pa.int64()), ("inferred_buy_lots", pa.int64()), ("inferred_sell_lots", pa.int64()),
                ("first_trade_time", TS), ("last_trade_time", TS), ("first_trade_id", pa.int64()), ("last_trade_id", pa.int64())]
INFERRED = ("inferred_buy_lots", "inferred_sell_lots")
DOC = {
    "units": "price in ticks (tick_size USDT), volume in lots (step_size oz), notional in tick*lots "
             "(USDT = notional_tick_lots * tick_size * step_size)",
    "side": "buy/sell = exchange-reported aggressor side (is_buyer_maker false/true); unknown is never reassigned",
    "inferred": "inferred_* = tick-rule split of UNKNOWN-side volume only; null when inferred_side is disabled",
    "finalisation": "a bar is written only after the clock passed bar_end + trade availability shift (available_at)",
    "level": "levels.level = price_ticks // footprint_bin_ticks; the bin's lowest price is level * footprint_bin_ticks ticks",
    "coverage": "FULL | PARTIAL_START (processing began after bar_start) | FILTERED (replay used date/session filters)",
    "dq_status": "worst Phase 3 data-quality status overlapping the bar for the trades dataset: OK|UNVERIFIED|PARTIAL|MISSING",
}


def footprint_root(data_root: Path, symbol: str) -> Path:
    return Path(data_root) / "derived" / f"provider={S.PROVIDER_ID}" / f"market={S.MARKET_TYPE}" / f"instrument={symbol}" / "footprint"


def _table(fields: list[tuple[str, Any]], cols: dict[str, Any], meta: dict[str, Any] | None, inferred: bool) -> pa.Table:
    arrs = []
    for name, typ in fields:
        v = cols[name]
        if name in INFERRED and not inferred: arrs.append(pa.nulls(len(cols["bar_start"]), pa.int64())); continue
        if typ == S.DICT: arrs.append(pa.array(np.asarray(v, dtype=object), pa.string()).dictionary_encode().cast(S.DICT))
        elif pa.types.is_timestamp(typ): arrs.append(pa.array(np.asarray(v, np.int64), pa.int64()).cast(typ))
        else: arrs.append(pa.array(np.asarray(v), typ))
    return pa.Table.from_arrays(arrs, schema=pa.schema([pa.field(n, t) for n, t in fields], metadata=meta))


def _concat_cols(parts: list[dict[str, Any]]) -> dict[str, Any]:
    return {k: np.concatenate([p[k] for p in parts]) for k in parts[0]} if parts else {}


def _col(t: pa.Table, c: str) -> NDArray[Any]:
    return np.asarray(t.column(c).to_numpy())


def _select(cols: dict[str, Any], m: NDArray[Any]) -> dict[str, Any]: return {k: v[m] for k, v in cols.items()}


class FootprintWriter(ProcessorSink):
    name = "footprint_writer"

    def __init__(self, data_root: Path, symbol: str, intervals: list[str] | None = None, replace: bool = False) -> None:
        self.base = footprint_root(data_root, symbol); self.symbol = symbol
        self.intervals: list[str] | None = list(intervals) if intervals is not None else None
        self.replace = replace
        self.buf: dict[str, Any] = {}       # interval -> date -> [(bars, levels)]
        self.staged: dict[str, Any] = {}    # interval -> date -> [stage file stem]
        self.written: dict[str, Any] = {}   # interval -> date -> manifest entry
        self.meta: dict[str, Any] = {}
        self.inferred = False
        self.done_this_run: set[str] = set()   # "interval|date" written as complete by this run (a repeat is a bug)

    # ------------------------------------------------------------------ lifecycle
    def on_start(self, ctx: dict[str, Any], proc: Any) -> None:
        st = proc.st
        if self.intervals is None: self.intervals = list(st.persist_intervals)
        self.inferred = st.inferred_enabled
        self.meta = {"processing_settings_hash": st.hash, "source_dataset_version": ctx.get("dataset_version"),
                     "footprint_bin_ticks": st.footprint_bin_ticks, "tick_size": st.tick_size, "step_size": st.step_size,
                     "instrument_id": st.instrument_id, "processing_version": st.version, "trade_shift_ms": st.trade_shift_ms}
        for iv in self.intervals:
            mp = self._dir(iv) / "_manifest.json"
            if mp.exists():
                m = json.loads(mp.read_text())
                same = all(m.get(k) == self.meta[k] for k in ("processing_settings_hash", "source_dataset_version", "footprint_bin_ticks"))
                if not same and not self.replace:
                    raise ReplayGuardError(f"{mp} was built with different settings or source data; rerun with a fresh "
                                           "(non-resumed) process run to replace it")
                if ctx.get("resumed"): continue                       # the checkpoint carries `written`
                if same and not self.replace: self.written[iv] = m.get("dates", {})   # extend the existing dataset
                else: self.written[iv] = {}; self._write_manifest(iv)

    def _dir(self, iv: str) -> Path: return self.base / f"interval={iv}"
    def _ddir(self, iv: str, d: str) -> Path: return self._dir(iv) / f"date={d}"

    def on_bars(self, interval: str, bars: dict[str, Any], levels: dict[str, Any], final_upto_ms: int) -> None:
        if interval not in (self.intervals or []): return
        day = bars["bar_start"] // DAY_MS; lday = levels["bar_start"] // DAY_MS
        for dn in np.unique(day):
            d = str(np.datetime64(int(dn), "D"))
            if f"{interval}|{d}" in self.done_this_run:
                raise RuntimeError(f"bars for {interval} {d} arrived after that date was written as complete")
            self.buf.setdefault(interval, {}).setdefault(d, []).append((_select(bars, day == dn), _select(levels, lday == dn)))
        self._flush_complete(interval, final_upto_ms)

    def _flush_complete(self, iv: str, final_upto_ms: int, force: bool = False) -> None:
        dates = sorted(set(self.buf.get(iv, {})) | set(self.staged.get(iv, {})))
        for d in dates:
            d_end = (int(np.datetime64(d, "D").astype(np.int64)) + 1) * DAY_MS
            if force or d_end <= final_upto_ms:
                self._write_date(iv, d, complete=d_end <= final_upto_ms)

    def on_seek(self, t_ms: int) -> None:
        # bars of a date interrupted by a seek would be incomplete: discard unwritten ones and their staged files
        for iv in list(self.buf): self.buf[iv] = {}
        for iv, dd in self.staged.items():
            for d in dd: self._clear_staging(iv, d)
            self.staged[iv] = {}

    def on_stop(self, ctx: dict[str, Any], proc: Any) -> None:
        if ctx.get("status") == "completed":
            for iv in self.intervals or []:
                self._flush_complete(iv, proc.final_upto.get(iv) or 0, force=True)

    # ------------------------------------------------------------------ state (checkpoint) = staged files
    def get_state(self) -> dict[str, Any]:
        for iv, dd in self.buf.items():
            for d, parts in dd.items():
                if not parts: continue
                b = _concat_cols([p[0] for p in parts]); lv = _concat_cols([p[1] for p in parts])
                stem = f"{int(b['bar_start'][0])}-{int(b['bar_start'][-1])}"
                sd = self._ddir(iv, d) / "_staging"
                write_atomic(_table(BAR_FIELDS, b, None, self.inferred), sd / f"bars-{stem}.parquet")
                write_atomic(_table(LEVEL_FIELDS, lv, None, self.inferred), sd / f"levels-{stem}.parquet")
                self.staged.setdefault(iv, {}).setdefault(d, []).append(stem)
            self.buf[iv] = {}
        return {"staged": self.staged, "written": self.written, "intervals": self.intervals, "done": sorted(self.done_this_run)}

    def set_state(self, s: dict[str, Any]) -> None:
        if not s: return
        self.staged = {iv: {d: list(v) for d, v in dd.items()} for iv, dd in s.get("staged", {}).items()}
        self.written = s.get("written", {}); self.buf = {}; self.done_this_run = set(s.get("done", []))
        if s.get("intervals") is not None: self.intervals = s["intervals"]

    # ------------------------------------------------------------------ writing
    def _clear_staging(self, iv: str, d: str) -> None:
        sd = self._ddir(iv, d) / "_staging"
        if sd.exists():
            for f in sorted(sd.iterdir()): safe_remove(f)

    def _write_date(self, iv: str, d: str, complete: bool) -> None:
        bparts, lparts = [], []
        for stem in self.staged.get(iv, {}).get(d, []):
            sd = self._ddir(iv, d) / "_staging"
            bparts.append(pq.read_table(sd / f"bars-{stem}.parquet")); lparts.append(pq.read_table(sd / f"levels-{stem}.parquet"))
        mem = self.buf.get(iv, {}).pop(d, [])
        if mem:
            bparts.append(_table(BAR_FIELDS, _concat_cols([p[0] for p in mem]), None, self.inferred))
            lparts.append(_table(LEVEL_FIELDS, _concat_cols([p[1] for p in mem]), None, self.inferred))
        if not bparts: return
        meta = {"goldflow.schema": "FootprintBar", "goldflow.schema_version": str(SCHEMA_VERSION), "goldflow.interval": iv,
                "goldflow.date": d, "goldflow.date_complete": str(complete), "goldflow.doc": json.dumps(DOC),
                **{f"goldflow.{k}": str(v) for k, v in self.meta.items()}}
        bars = pa.concat_tables([t.replace_schema_metadata(None) for t in bparts]).sort_by("bar_start")
        levels = pa.concat_tables([t.replace_schema_metadata(None) for t in lparts]).sort_by([("bar_start", "ascending"), ("level", "ascending")])
        bs = bars.column("bar_start").cast(pa.int64()).to_numpy()
        if len(bs) > 1 and not (np.diff(bs) > 0).all(): raise RuntimeError(f"duplicate bars for {iv} {d}: processor emitted a bar twice")
        bars = bars.replace_schema_metadata(meta)
        levels = levels.replace_schema_metadata({**meta, "goldflow.schema": "FootprintLevel"})
        dd = self._ddir(iv, d)
        b_sha = write_atomic(bars, dd / "bars.parquet"); l_sha = write_atomic(levels, dd / "levels.parquet")
        self._clear_staging(iv, d); self.staged.get(iv, {}).pop(d, None)
        if complete: self.done_this_run.add(f"{iv}|{d}")
        cov = dict(zip(*np.unique(np.asarray(bars.column("coverage").cast(pa.string()).to_pylist()), return_counts=True)))
        dq = dict(zip(*np.unique(np.asarray(bars.column("dq_status").cast(pa.string()).to_pylist()), return_counts=True)))
        self.written.setdefault(iv, {})[d] = {
            "bars": bars.num_rows, "level_rows": levels.num_rows, "date_complete": complete,
            "volume_lots": int(_col(bars, "volume_lots").sum()), "buy_lots": int(_col(bars, "buy_lots").sum()),
            "sell_lots": int(_col(bars, "sell_lots").sum()), "unknown_lots": int(_col(bars, "unknown_lots").sum()),
            "trade_count": int(_col(bars, "trade_count").sum()), "recovered_count": int(_col(bars, "recovered_count").sum()),
            "first_trade_id": int(_col(bars, "first_trade_id").min()), "last_trade_id": int(_col(bars, "last_trade_id").max()),
            "weekend_bars": int(_col(bars, "weekend").sum()),
            "coverage": {str(k): int(v) for k, v in cov.items()}, "dq_status": {str(k): int(v) for k, v in dq.items()},
            "bars_logical_digest": logical_digest(bars), "levels_logical_digest": logical_digest(levels),
            "bars_sha256": b_sha, "levels_sha256": l_sha}
        self._write_manifest(iv)

    def _write_manifest(self, iv: str) -> None:
        m = {"schema": "FootprintBar/FootprintLevel", "schema_version": SCHEMA_VERSION, "interval": iv, **self.meta,
             "doc": DOC, "dates": dict(sorted(self.written.get(iv, {}).items()))}
        p = self._dir(iv) / "_manifest.json"; p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + ".tmp"); tmp.write_text(json.dumps(m, indent=1)); os.replace(tmp, p)


def read_footprint(data_root: Path, symbol: str, interval: str, d: str) -> tuple[pa.Table, pa.Table]:
    dd = footprint_root(data_root, symbol) / f"interval={interval}" / f"date={d}"
    return pq.read_table(dd / "bars.parquet"), pq.read_table(dd / "levels.parquet")


def read_manifest(data_root: Path, symbol: str, interval: str) -> dict[str, Any]:
    p = footprint_root(data_root, symbol) / f"interval={interval}" / "_manifest.json"
    return json.loads(p.read_text()) if p.exists() else {}
