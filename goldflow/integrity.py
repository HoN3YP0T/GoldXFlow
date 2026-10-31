"""Streaming, memory-bounded verification of raw archive files.

Reads the CSV inside each zip in pyarrow batches (never the whole file in memory) and
computes: record count, id/time bounds, id gaps, duplicate ids, backwards timestamps,
invalid values, coverage vs the requested interval, and a per-minute trade aggregate
used later to cross-check trades against Binance's own 1m klines.
"""
from __future__ import annotations
import gzip, io, json, zipfile
from dataclasses import dataclass, field, asdict
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv

SCHEMAS = {
    "trades": (["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"],
               {"id": pa.int64(), "price": pa.float64(), "qty": pa.float64(), "quote_qty": pa.float64(),
                "time": pa.int64(), "is_buyer_maker": pa.bool_()}),
    "aggTrades": (["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id", "transact_time", "is_buyer_maker"],
                  {"agg_trade_id": pa.int64(), "price": pa.float64(), "quantity": pa.float64(), "first_trade_id": pa.int64(),
                   "last_trade_id": pa.int64(), "transact_time": pa.int64(), "is_buyer_maker": pa.bool_()}),
    "klines": (["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume", "count",
                "taker_buy_volume", "taker_buy_quote_volume", "ignore"],
               {"open_time": pa.int64(), "volume": pa.float64(), "count": pa.int64(), "taker_buy_volume": pa.float64(),
                "close_time": pa.int64(), "high": pa.float64(), "low": pa.float64()}),
    "bookDepth": (["timestamp", "percentage", "depth", "notional"],
                  {"timestamp": pa.timestamp("s"), "percentage": pa.float64(), "depth": pa.float64(), "notional": pa.float64()}),
    "metrics": (["create_time", "symbol", "sum_open_interest", "sum_open_interest_value", "count_toptrader_long_short_ratio",
                 "sum_toptrader_long_short_ratio", "count_long_short_ratio", "sum_taker_long_short_vol_ratio"],
                {"create_time": pa.timestamp("s")}),
}
TIME_GAP_FLAG_MS = 300_000   # flag any no-trade interval longer than 5 minutes

@dataclass
class VerifyResult:
    ok: bool
    records: int = 0
    first_id: int | None = None
    last_id: int | None = None
    first_ts: int | None = None
    last_ts: int | None = None
    id_gap_count: int = 0
    id_gap_ids: int = 0
    dup_ids: int = 0
    ts_backwards: int = 0
    invalid_values: int = 0
    out_of_interval: int = 0
    max_time_gap_ms: int = 0
    time_gaps: list = field(default_factory=list)       # [start_ms, end_ms] no-data stretches > threshold
    id_gaps: list = field(default_factory=list)         # [first_missing, last_missing, ts_before, ts_after]
    extra: dict = field(default_factory=dict)
    problems: list = field(default_factory=list)

    def summary(self) -> dict:
        d = asdict(self); d.pop("id_gaps"); d["time_gaps"] = d["time_gaps"][:50]; return d

def _kind(dataset: str) -> str:
    return "klines" if dataset.startswith("klines_") else dataset

def _open_member(zpath: Path):
    zf = zipfile.ZipFile(zpath)
    names = zf.namelist()
    if len(names) != 1 or not names[0].endswith(".csv"):
        raise ValueError(f"unexpected zip members {names}")
    test = zf.testzip()
    if test is not None:
        raise ValueError(f"zip CRC failure in member {test}")
    return zf, names[0]

def _batches(zpath: Path, dataset: str):
    kind = _kind(dataset)
    cols, types = SCHEMAS[kind]
    zf, name = _open_member(zpath)
    with zf.open(name) as fh:
        first = fh.readline()
    has_header = not first[:1].isdigit()
    if has_header:
        hdr = first.decode().strip().split(",")
        if hdr != cols:
            raise ValueError(f"header mismatch: {hdr} != {cols}")
    fh = zf.open(name)
    ro = pacsv.ReadOptions(column_names=cols, skip_rows=1 if has_header else 0, block_size=16 << 20)
    co = pacsv.ConvertOptions(column_types=types, true_values=["true", "True"], false_values=["false", "False"],
                              timestamp_parsers=["%Y-%m-%d %H:%M:%S"])
    reader = pacsv.open_csv(fh, read_options=ro, convert_options=co)
    try:
        for b in reader:
            yield b
    finally:
        fh.close(); zf.close()

def verify_file(zpath: Path, dataset: str, start_ms: int, end_ms: int, minute_out: Path | None = None) -> VerifyResult:
    kind = _kind(dataset)
    try:
        if kind == "trades": return _verify_trades(zpath, start_ms, end_ms, minute_out)
        if kind == "aggTrades": return _verify_agg(zpath, start_ms, end_ms)
        if kind == "klines": return _verify_klines(zpath, start_ms, end_ms)
        if kind in ("bookDepth", "metrics"): return _verify_snapshots(zpath, dataset, start_ms, end_ms)
    except Exception as e:  # corrupted / unparsable file is a hard failure, never skipped
        return VerifyResult(ok=False, problems=[f"parse error: {e!r}"])
    raise ValueError(dataset)

# ---------------------------------------------------------------- trades
def _id_stream_check(r: VerifyResult, ids: np.ndarray, ts: np.ndarray, prev_id, prev_ts):
    if prev_id is not None:
        ids = np.concatenate(([prev_id], ids)); ts = np.concatenate(([prev_ts], ts))
    d = np.diff(ids)
    r.dup_ids += int((d == 0).sum())
    back = d < 0
    if back.any(): r.problems.append(f"{int(back.sum())} id decreases"); r.dup_ids += int(back.sum())
    gi = np.nonzero(d > 1)[0]
    for i in gi:
        r.id_gaps.append([int(ids[i] + 1), int(ids[i + 1] - 1), int(ts[i]), int(ts[i + 1])])
    r.id_gap_count += len(gi); r.id_gap_ids += int((d[gi] - 1).sum())
    dt = np.diff(ts)
    r.ts_backwards += int((dt < 0).sum())
    if len(dt):
        m = int(dt.max()); r.max_time_gap_ms = max(r.max_time_gap_ms, m)
        for i in np.nonzero(dt > TIME_GAP_FLAG_MS)[0]:
            r.time_gaps.append([int(ts[i]), int(ts[i + 1])])

def _verify_trades(zpath, start_ms, end_ms, minute_out):
    r = VerifyResult(ok=True)
    prev_id = prev_ts = None
    n_min = (end_ms - start_ms) // 60_000 + 1
    cnt = np.zeros(n_min, np.int64); vol = np.zeros(n_min); bvol = np.zeros(n_min)
    qty_sum = 0.0; quote_bad = 0; buyer_maker = 0
    for b in _batches(zpath, "trades"):
        ids = b.column("id").to_numpy(); ts = b.column("time").to_numpy()
        px = b.column("price").to_numpy(); q = b.column("qty").to_numpy(); qq = b.column("quote_qty").to_numpy()
        ibm = b.column("is_buyer_maker").to_numpy(zero_copy_only=False)
        if b.num_rows == 0: continue
        if r.first_id is None: r.first_id, r.first_ts = int(ids[0]), int(ts[0])
        _id_stream_check(r, ids, ts, prev_id, prev_ts)
        prev_id, prev_ts = int(ids[-1]), int(ts[-1])
        r.records += b.num_rows
        r.invalid_values += int(((px <= 0) | (q <= 0) | ~np.isfinite(px) | ~np.isfinite(q)).sum())
        quote_bad += int((np.abs(px * q - qq) > np.maximum(1e-6, 1e-6 * qq) + 1e-5).sum())
        inside = (ts >= start_ms) & (ts < end_ms)
        r.out_of_interval += int((~inside).sum())
        mi = (ts[inside] - start_ms) // 60_000
        np.add.at(cnt, mi, 1); np.add.at(vol, mi, q[inside]); np.add.at(bvol, mi, np.where(ibm[inside], 0.0, q[inside]))
        qty_sum += float(q.sum()); buyer_maker += int(ibm.sum())
    r.last_id, r.last_ts = prev_id, prev_ts
    r.extra = {"volume_base": round(qty_sum, 6), "is_buyer_maker_true": buyer_maker, "quote_qty_mismatch": quote_bad}
    if minute_out is not None:
        nz = np.nonzero(cnt)[0]
        np.savez_compressed(minute_out, minute_ms=start_ms + nz * 60_000, count=cnt[nz], volume=vol[nz], taker_buy_volume=bvol[nz])
    _finalise(r, start_ms, end_ms)
    return r

def _verify_agg(zpath, start_ms, end_ms):
    r = VerifyResult(ok=True)
    prev_id = prev_ts = None; prev_last_tid = None
    covered = 0; tid_gaps = 0; tid_gap_ids = 0; tid_overlap = 0; first_tid = None
    for b in _batches(zpath, "aggTrades"):
        ids = b.column("agg_trade_id").to_numpy(); ts = b.column("transact_time").to_numpy()
        f = b.column("first_trade_id").to_numpy(); l = b.column("last_trade_id").to_numpy()
        q = b.column("quantity").to_numpy(); px = b.column("price").to_numpy()
        if b.num_rows == 0: continue
        if r.first_id is None: r.first_id, r.first_ts, first_tid = int(ids[0]), int(ts[0]), int(f[0])
        _id_stream_check(r, ids, ts, prev_id, prev_ts)
        prev_id, prev_ts = int(ids[-1]), int(ts[-1])
        r.records += b.num_rows
        r.invalid_values += int(((px <= 0) | (q <= 0) | (l < f)).sum())
        covered += int((l - f + 1).sum())
        ff = f if prev_last_tid is None else f  # trade-id continuity between consecutive agg rows
        pl = np.concatenate(([prev_last_tid], l[:-1])) if prev_last_tid is not None else l[:-1]
        fc = ff if prev_last_tid is not None else ff[1:]
        dd = fc - pl
        tid_gaps += int((dd > 1).sum()); tid_gap_ids += int((dd[dd > 1] - 1).sum()); tid_overlap += int((dd < 1).sum())
        prev_last_tid = int(l[-1])
        r.out_of_interval += int(((ts < start_ms) | (ts >= end_ms)).sum())
    r.last_id, r.last_ts = prev_id, prev_ts
    r.extra = {"trade_ids_covered": covered, "first_trade_id": first_tid, "last_trade_id": prev_last_tid,
               "trade_id_gaps": tid_gaps, "trade_id_gap_ids": tid_gap_ids, "trade_id_overlaps": tid_overlap}
    _finalise(r, start_ms, end_ms)
    return r

def _verify_klines(zpath, start_ms, end_ms):
    r = VerifyResult(ok=True)
    opens = []; cnt = 0; vol = 0.0
    for b in _batches(zpath, "klines_1m"):
        ot = b.column("open_time").to_numpy(); opens.append(ot)
        cnt += int(b.column("count").to_numpy().sum()); vol += float(b.column("volume").to_numpy().sum())
        r.records += b.num_rows
    ot = np.concatenate(opens) if opens else np.array([], np.int64)
    if len(ot):
        r.first_ts, r.last_ts = int(ot[0]), int(ot[-1])
        expected = np.arange(start_ms, end_ms, 60_000)
        missing = np.setdiff1d(expected, ot)
        r.extra = {"missing_minutes": int(len(missing)), "trade_count_sum": cnt, "volume_sum": round(vol, 6),
                   "dup_minutes": int(len(ot) - len(np.unique(ot)))}
        if len(missing): r.time_gaps = [[int(m), int(m) + 60_000] for m in missing[:200]]
        r.ts_backwards = int((np.diff(ot) < 0).sum())
    _finalise(r, start_ms, end_ms, check_edges=False)
    if r.extra.get("missing_minutes"): r.problems.append(f"{r.extra['missing_minutes']} missing 1m klines")
    return r

def _verify_snapshots(zpath, dataset, start_ms, end_ms):
    """bookDepth (~30s depth-band snapshots) and metrics (5-min OI/ratios). Files may be unsorted."""
    r = VerifyResult(ok=True)
    tcol = "timestamp" if dataset == "bookDepth" else "create_time"
    allts = []
    for b in _batches(zpath, dataset):
        t = b.column(tcol).cast(pa.int64()).to_numpy() * 1000
        allts.append(t); r.records += b.num_rows
    ts = np.unique(np.concatenate(allts)) if allts else np.array([], np.int64)
    if len(ts):
        r.first_ts, r.last_ts = int(ts[0]), int(ts[-1])
        dt = np.diff(np.concatenate(([start_ms], ts, [end_ms])))
        r.max_time_gap_ms = int(dt.max())
        thr = 120_000 if dataset == "bookDepth" else 600_000
        for i in np.nonzero(dt > thr)[0]:
            a = start_ms if i == 0 else int(ts[i - 1]); bnd = end_ms if i == len(ts) else int(ts[i])
            r.time_gaps.append([a, bnd])
        r.out_of_interval = int(((ts < start_ms) | (ts > end_ms)).sum())
        r.extra = {"distinct_timestamps": int(len(ts)), "gap_threshold_ms": thr}
    _finalise(r, start_ms, end_ms, check_edges=False)
    return r

def _finalise(r: VerifyResult, start_ms, end_ms, check_edges=True):
    if r.records == 0: r.problems.append("no records")
    if r.dup_ids: r.problems.append(f"{r.dup_ids} duplicate/decreasing ids")
    if r.ts_backwards: r.problems.append(f"{r.ts_backwards} backwards timestamps")
    if r.invalid_values: r.problems.append(f"{r.invalid_values} invalid values")
    if r.out_of_interval: r.problems.append(f"{r.out_of_interval} records outside file interval")
    # id gaps and time gaps are *findings* recorded as gaps, not file-corruption failures
    r.ok = not r.problems

def write_id_gaps(path: Path, gaps: list) -> None:
    with gzip.open(path, "wt") as f:
        for g in gaps: f.write(json.dumps(g) + "\n")
