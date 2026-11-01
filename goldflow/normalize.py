"""Phase 3: validation, quarantine and normalisation of the immutable raw archive.

Pipeline per UTC date D (days are independent, so they run in parallel worker processes):
  raw daily files for D (re-hashed against the registry first; never modified)
    -> parse (typed; on failure a string fallback isolates unparseable rows as PARSE_ERROR)
    -> record-level validation with reason codes (first failing rule wins)
    -> id/time sequence-outlier detection, then duplicate resolution
    -> valid rows -> canonical schema (constants, reported, derived, raw reference)
    -> trades only: recovery of single-trade aggTrades absent from the trades file
    -> completeness evidence: 1m-klines trade count/volume per minute and aggTrades quantity per row
    -> day + hour status: COMPLETE | PARTIAL | UNVERIFIED (MISSING when no source file exists)
    -> atomic Parquet write (tmp + rename), quarantine Parquet, catalog rows
Partitions: data/normalized/provider=<p>/market=<m>/instrument=<s>/data_type=<d>/date=<YYYY-MM-DD>/part-000.parquet
Nothing is loaded beyond one day of one dataset per worker.
"""
from __future__ import annotations
import hashlib, io, json, os, time, zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from . import schemas as S
from .fsutil import safe_remove
from .integrity import SCHEMAS as RAW_SCHEMAS
from .timeutil import DAY_MS, day_start_ms, from_ms, iso

HOUR_MS = 3_600_000
DATASETS = ["klines_1m", "trades", "aggTrades", "bookDepth", "metrics"]
BANDS = {-5.0, -4.0, -3.0, -2.0, -1.0, -0.2, 0.2, 1.0, 2.0, 3.0, 4.0, 5.0}   # verified: 12 bands in every snapshot

# ============================================================================ raw reading
def read_raw(path: Path, dataset: str) -> tuple[dict, list]:
    """Return ({column: numpy array}, parse_quarantine_rows). raw_row = 0-based data-row index in the CSV."""
    kind = "klines" if dataset.startswith("klines_") else dataset
    cols, types = RAW_SCHEMAS[kind]
    with zipfile.ZipFile(path) as zf:
        names = zf.namelist()
        if len(names) != 1: raise ValueError(f"{path.name}: unexpected members {names}")
        data = zf.read(names[0])
    first = data[:data.find(b"\n")]
    has_header = not first[:1].isdigit()
    if has_header and first.decode().strip().split(",") != cols:
        raise ValueError(f"{path.name}: header mismatch {first!r}")
    ro = pacsv.ReadOptions(column_names=cols, skip_rows=1 if has_header else 0)
    co = pacsv.ConvertOptions(column_types=types, true_values=["true", "True"], false_values=["false", "False"],
                              timestamp_parsers=["%Y-%m-%d %H:%M:%S"])
    try:
        t = pacsv.read_csv(io.BytesIO(data), read_options=ro, convert_options=co)
        return _to_numpy(t, types), []
    except pa.ArrowInvalid:
        return _read_raw_fallback(data, cols, types, has_header)

def _to_numpy(t: pa.Table, types: dict) -> dict:
    out = {}
    for c in t.column_names:
        col = t.column(c).combine_chunks()
        if pa.types.is_timestamp(col.type): col = col.cast(pa.int64())
        out[c] = col.to_numpy(zero_copy_only=False)
    out["raw_row"] = np.arange(t.num_rows, dtype=np.int64)
    return out

def _read_raw_fallback(data: bytes, cols: list, types: dict, has_header: bool) -> tuple[dict, list]:
    """Row-isolating parse: read everything as text, convert per column, unparseable rows -> PARSE_ERROR."""
    ro = pacsv.ReadOptions(column_names=cols, skip_rows=1 if has_header else 0)
    co = pacsv.ConvertOptions(column_types={c: pa.string() for c in cols})
    po = pacsv.ParseOptions(invalid_row_handler=lambda r: "skip")   # wrong column count -> handled below
    lines = data.split(b"\n")[1 if has_header else 0:]
    lines = [l for l in lines if l.strip()]
    good_rows, bad = [], []
    for i, l in enumerate(lines):
        parts = l.decode(errors="replace").strip().split(",")
        if len(parts) != len(cols):
            bad.append({"raw_row": i, "reason_code": "PARSE_ERROR", "reason_detail": f"{len(parts)} fields",
                        "raw_fields_json": json.dumps(parts)})
        else:
            good_rows.append((i, parts))
    out = {c: [] for c in cols}; rows = []
    for i, parts in good_rows:
        vals, ok = {}, True
        for c, v in zip(cols, parts):
            ty = types.get(c)
            try:
                if ty is None: vals[c] = v
                elif pa.types.is_boolean(ty):
                    if v not in ("true", "false", "True", "False"): raise ValueError(v)
                    vals[c] = v in ("true", "True")
                elif pa.types.is_integer(ty): vals[c] = int(v)
                elif pa.types.is_floating(ty): vals[c] = float(v)
                elif pa.types.is_timestamp(ty):
                    vals[c] = int(np.datetime64(v.replace(" ", "T"), "s").astype(np.int64))
                else: vals[c] = v
            except Exception:
                ok = False; break
        if ok:
            for c in cols: out[c].append(vals[c])
            rows.append(i)
        else:
            bad.append({"raw_row": i, "reason_code": "PARSE_ERROR", "reason_detail": f"bad value in column {c}",
                        "raw_fields_json": json.dumps(parts)})
    res = {c: np.array(v) if v else np.array([]) for c, v in out.items()}
    for c, ty in types.items():
        if pa.types.is_integer(ty) or pa.types.is_timestamp(ty): res[c] = res[c].astype(np.int64)
        elif pa.types.is_floating(ty): res[c] = res[c].astype(np.float64)
        elif pa.types.is_boolean(ty): res[c] = res[c].astype(bool)
    res["raw_row"] = np.array(rows, dtype=np.int64)
    return res, bad

# ============================================================================ validation helpers
class Rejects:
    """Collects (row index -> reason) with first-reason-wins semantics."""
    def __init__(self, n: int):
        self.code = np.full(n, "", dtype=object); self.detail = np.full(n, "", dtype=object)
    def mark(self, mask: np.ndarray, code: str, detail: str = ""):
        m = mask & (self.code == "")
        self.code[m] = code
        if detail: self.detail[m] = detail
    @property
    def ok(self) -> np.ndarray: return self.code == ""

def multiple_of(values: np.ndarray, inv_unit: int) -> tuple[np.ndarray, np.ndarray]:
    """(units, exact_mask): values expressed as integer multiples of 1/inv_unit, exactness to 1e-6 unit."""
    scaled = values * inv_unit
    units = np.rint(scaled)
    return units.astype(np.int64), np.abs(scaled - units) <= 1e-6

def sequence_outliers(ts: np.ndarray, max_iter: int = 20) -> tuple[np.ndarray, np.ndarray]:
    """Given timestamps of records already in id order, return (outlier_mask, ambiguous_mask).

    For a violation ts[i] > ts[i+1]: record i is the outlier if ts[i-1] <= ts[i+1] (it alone is too late);
    record i+1 is the outlier if ts[i] <= ts[i+2] (it alone is too early). If both or neither hold, the
    pair is ambiguous. Iterates until monotone; leftover violations are ambiguous.
    """
    n = len(ts); out = np.zeros(n, bool); amb = np.zeros(n, bool)
    for _ in range(max_iter):
        alive = np.nonzero(~out & ~amb)[0]
        t = ts[alive]
        v = np.nonzero(t[:-1] > t[1:])[0]
        if not len(v): return out, amb
        prev = np.where(v > 0, t[np.maximum(v - 1, 0)], np.iinfo(np.int64).min)
        nxt2 = np.where(v + 2 < len(t), t[np.minimum(v + 2, len(t) - 1)], np.iinfo(np.int64).max)
        late = prev <= t[v + 1]; early = t[v] <= nxt2
        out[alive[v[late & ~early]]] = True
        out[alive[v[early & ~late] + 1]] = True
        both = ~(late ^ early)
        amb[alive[v[both]]] = True; amb[alive[v[both] + 1]] = True
    alive = np.nonzero(~out & ~amb)[0]; t = ts[alive]
    v = np.nonzero(t[:-1] > t[1:])[0]
    amb[alive[v]] = True; amb[alive[v + 1]] = True
    return out, amb

def resolve_duplicates(ids: np.ndarray, payload_cols: list, rej: Rejects, alive: np.ndarray) -> None:
    """Among alive rows: identical copies -> keep first (file order), mark rest DUPLICATE_EXACT;
    differing payloads under one id -> mark every copy DUPLICATE_CONFLICT."""
    idx = np.nonzero(alive)[0]
    if not len(idx): return
    order = idx[np.argsort(ids[idx], kind="stable")]
    sid = ids[order]
    dup_start = np.nonzero(np.diff(sid) == 0)[0]
    if not len(dup_start): return
    groups = {}
    for p in dup_start:
        groups.setdefault(int(sid[p]), set()).update((int(order[p]), int(order[p + 1])))
    for gid, members in groups.items():
        m = sorted(members)
        ref = tuple(c[m[0]] for c in payload_cols)
        same = all(tuple(c[j] for c in payload_cols) == ref for j in m[1:])
        mask = np.zeros(len(ids), bool)
        if same:
            mask[m[1:]] = True; rej.mark(mask, "DUPLICATE_EXACT", f"copy of raw_row {m[0]}")
        else:
            mask[m] = True; rej.mark(mask, "DUPLICATE_CONFLICT", f"{len(m)} differing copies of id {gid}")

def const_cols(n: int, symbol: str, provider: str = S.PROVIDER_ID) -> dict:
    def d(v): return pa.DictionaryArray.from_arrays(pa.array(np.zeros(n, np.int8)), pa.array([v]))
    venue, market = S.VENUES[provider]
    return {"schema_version": pa.array(np.full(n, S.SCHEMA_VERSION, np.int16)), "provider_id": d(provider),
            "venue_id": d(venue), "market_type": d(market), "instrument_id": d(S.instrument_id(symbol, provider))}

def dict_col(values: np.ndarray) -> pa.Array:
    return pa.array(values.astype(object), pa.string()).dictionary_encode().cast(S.DICT)

def build_table(schema: pa.Schema, cols: dict) -> pa.Table:
    arrs = []
    for f in schema:
        v = cols[f.name]
        if isinstance(v, (pa.Array, pa.ChunkedArray)): a = v if v.type == f.type else v.cast(f.type)
        elif f.type == S.DICT: a = dict_col(np.asarray(v))
        elif pa.types.is_timestamp(f.type): a = pa.array(np.asarray(v, dtype="int64"), pa.int64(), mask=None if not hasattr(v, "mask") else v.mask).cast(f.type)
        else: a = pa.array(v, f.type)
        arrs.append(a)
    return pa.Table.from_arrays(arrs, schema=schema)

def null_ts(n: int) -> pa.Array: return pa.nulls(n, S.TS)

def quarantine_rows(dataset: str, rej: Rejects, raw: dict, raw_file_id: int, id_col: str | None, ts_col: str | None,
                    extra: list | None = None, ts_scale: int = 1) -> list[dict]:
    rows = []
    for i in np.nonzero(~rej.ok)[0]:
        rec = {k: (v[i].item() if hasattr(v[i], "item") else v[i]) for k, v in raw.items() if k != "raw_row"}
        rows.append({"dataset": dataset, "reason_code": rej.code[i], "reason_detail": rej.detail[i],
                     "raw_file_id": raw_file_id, "raw_row": int(raw["raw_row"][i]),
                     "source_id": int(raw[id_col][i]) if id_col else None,
                     "event_time_ms": int(raw[ts_col][i]) * ts_scale if ts_col else None,
                     "raw_fields_json": json.dumps(rec, default=str, sort_keys=True)})
    for b in extra or []:
        rows.append({"dataset": dataset, "reason_code": b["reason_code"], "reason_detail": b["reason_detail"],
                     "raw_file_id": raw_file_id, "raw_row": b["raw_row"], "source_id": None, "event_time_ms": None,
                     "raw_fields_json": b["raw_fields_json"]})
    return rows

def price_plausibility(rej: Rejects, px: np.ndarray, ts: np.ndarray, d_start: int, kref: dict | None):
    """Trade prices must lie inside Binance's own 1m kline [low, high] for their minute (klines are built from
    the same trades). Without klines for the minute: within 10% of the day's median price. Catches rows from
    another instrument, which is exactly the contamination found in XAUUSDT-trades-2026-06.zip (monthly)."""
    if kref is not None:
        mi = np.clip((ts - d_start) // 60_000, 0, 1439).astype(np.int64)
        have = kref["have"][mi]
        lo, hi = kref["low_ticks"][mi], kref["high_ticks"][mi]
        pt = np.rint(px * kref["inv_tick"]).astype(np.int64)
        rej.mark(have & ((pt < lo) | (pt > hi)), "PRICE_OUTSIDE_KLINE_RANGE")
        need_fallback = ~have
    else:
        need_fallback = np.ones(len(px), bool)
    if need_fallback.any():
        cand = px[rej.ok & np.isfinite(px) & (px > 0)]
        if len(cand):
            med = float(np.median(cand))
            rej.mark(need_fallback & (np.abs(px / med - 1.0) > 0.10), "PRICE_IMPLAUSIBLE", f"day median {med:.2f}")

def common_checks(rej: Rejects, ts: np.ndarray, d_start: int, d_end: int, spec: dict, acq_end: int):
    rej.mark(ts < spec["onboard_ms"], "TS_BEFORE_LISTING")
    rej.mark(ts >= acq_end, "TS_AFTER_ACQUISITION")
    rej.mark((ts < d_start) | (ts >= d_end), "TS_OUTSIDE_SOURCE_INTERVAL", f"file interval [{iso(d_start)}, {iso(d_end)})")

# ============================================================================ per-dataset validators
def validate_trades(raw: dict, d_start: int, d_end: int, spec: dict, acq_end: int, kref: dict | None = None) -> tuple[Rejects, dict]:
    n = len(raw["id"]); rej = Rejects(n)
    px, q, qq, ts = raw["price"], raw["qty"], raw["quote_qty"], raw["time"]
    rej.mark(~np.isfinite(px) | (px <= 0), "NONPOSITIVE_PRICE")
    rej.mark(~np.isfinite(q) | (q <= 0), "NONPOSITIVE_QUANTITY")
    ticks, tick_ok = multiple_of(px, spec["inv_tick"]); lots, lot_ok = multiple_of(q, spec["inv_step"])
    rej.mark(~tick_ok, "PRICE_NOT_TICK_MULTIPLE"); rej.mark(~lot_ok, "QTY_NOT_STEP_MULTIPLE")
    rej.mark(np.abs(px * q - qq) > 1e-8 * np.maximum(1.0, np.abs(qq)) + 1e-6, "QUOTE_NOTIONAL_MISMATCH")
    common_checks(rej, ts, d_start, d_end, spec, acq_end)
    price_plausibility(rej, px, ts, d_start, kref)
    _sequence_and_dups(rej, raw["id"], ts, [px, q, qq, ts, raw["is_buyer_maker"]])
    return rej, {"price_ticks": ticks, "quantity_lots": lots}

def validate_agg(raw: dict, d_start: int, d_end: int, spec: dict, acq_end: int, kref: dict | None = None) -> tuple[Rejects, dict]:
    n = len(raw["agg_trade_id"]); rej = Rejects(n)
    px, q, ts = raw["price"], raw["quantity"], raw["transact_time"]
    rej.mark(~np.isfinite(px) | (px <= 0), "NONPOSITIVE_PRICE")
    rej.mark(~np.isfinite(q) | (q <= 0), "NONPOSITIVE_QUANTITY")
    ticks, tick_ok = multiple_of(px, spec["inv_tick"]); lots, lot_ok = multiple_of(q, spec["inv_step"])
    rej.mark(~tick_ok, "PRICE_NOT_TICK_MULTIPLE"); rej.mark(~lot_ok, "QTY_NOT_STEP_MULTIPLE")
    rej.mark(raw["last_trade_id"] < raw["first_trade_id"], "INVALID_ID_RANGE")
    common_checks(rej, ts, d_start, d_end, spec, acq_end)
    price_plausibility(rej, px, ts, d_start, kref)
    _sequence_and_dups(rej, raw["agg_trade_id"], ts, [px, q, ts, raw["first_trade_id"], raw["last_trade_id"], raw["is_buyer_maker"]])
    return rej, {"price_ticks": ticks, "quantity_lots": lots}

def _sequence_and_dups(rej: Rejects, ids: np.ndarray, ts: np.ndarray, payload: list):
    alive = np.nonzero(rej.ok)[0]
    order = alive[np.argsort(ids[alive], kind="stable")]        # id order (files are already id-ordered)
    out, amb = sequence_outliers(ts[order])
    m = np.zeros(len(ids), bool); m[order[out]] = True; rej.mark(m, "TS_SEQUENCE_OUTLIER", "time breaks id/time monotonicity")
    m = np.zeros(len(ids), bool); m[order[amb]] = True; rej.mark(m, "TS_ORDER_AMBIGUOUS")
    resolve_duplicates(ids, payload, rej, rej.ok)

def validate_klines(raw: dict, d_start: int, d_end: int, spec: dict, acq_end: int) -> tuple[Rejects, dict]:
    n = len(raw["open_time"]); rej = Rejects(n)
    ot = raw["open_time"]
    o, h, l, c, v = (raw[k] for k in ("open", "high", "low", "close", "volume"))
    rej.mark((ot % 60_000 != 0) | (raw["close_time"] != ot + 59_999), "UNCLASSIFIED", "open/close time not a 1m boundary")
    rej.mark((h < np.maximum(o, c)) | (l > np.minimum(o, c)) | (l <= 0), "UNCLASSIFIED", "OHLC inconsistent")
    rej.mark((v < 0) | (raw["count"] < 0), "NONPOSITIVE_QUANTITY", "negative volume or count")
    common_checks(rej, ot, d_start, d_end, spec, acq_end)
    resolve_duplicates(ot, [o, h, l, c, v, raw["count"]], rej, rej.ok)
    lots, lot_ok = multiple_of(v, spec["inv_step"])
    rej.mark(~lot_ok, "QTY_NOT_STEP_MULTIPLE")
    return rej, {"volume_lots": lots}

def validate_bookdepth(raw: dict, d_start: int, d_end: int, spec: dict, acq_end: int) -> tuple[Rejects, dict]:
    n = len(raw["timestamp"]); rej = Rejects(n)
    ts = raw["timestamp"] * 1000
    rej.mark(~np.isin(raw["percentage"], list(BANDS)), "INVALID_BAND")
    rej.mark((raw["depth"] < 0) | (raw["notional"] < 0) | ~np.isfinite(raw["depth"]) | ~np.isfinite(raw["notional"]),
             "NONPOSITIVE_QUANTITY", "negative depth or notional")
    common_checks(rej, ts, d_start, d_end, spec, acq_end)
    key = ts * 1000 + np.rint(raw["percentage"] * 10).astype(np.int64) + 500   # (time, band) identity
    resolve_duplicates(key, [raw["depth"], raw["notional"]], rej, rej.ok)
    return rej, {"ts_ms": ts}

def validate_metrics(raw: dict, d_start: int, d_end: int, spec: dict, acq_end: int, symbol: str) -> tuple[Rejects, dict]:
    n = len(raw["create_time"]); rej = Rejects(n)
    ts = raw["create_time"] * 1000
    rej.mark(raw["symbol"].astype(str) != symbol, "SYMBOL_MISMATCH")
    vals = ["sum_open_interest", "sum_open_interest_value", "count_toptrader_long_short_ratio",
            "sum_toptrader_long_short_ratio", "count_long_short_ratio", "sum_taker_long_short_vol_ratio"]
    bad = np.zeros(n, bool)
    for k in vals:
        x = raw[k].astype(np.float64); raw[k] = x; bad |= ~np.isfinite(x) | (x < 0)
    rej.mark(bad, "NONPOSITIVE_QUANTITY", "negative or non-finite metric value")
    common_checks(rej, ts, d_start, d_end, spec, acq_end)
    resolve_duplicates(ts, [raw[k] for k in vals], rej, rej.ok)
    return rej, {"ts_ms": ts}

# ============================================================================ completeness evidence
def reconcile_with_agg(tid, tlots, tts, a) -> dict:
    """Assign each valid trade to the agg row whose [first,last] id range contains it; compare quantities (lots)."""
    f, l, alots = a["first_trade_id"], a["last_trade_id"], a["quantity_lots"]
    idx = np.searchsorted(f, tid, side="right") - 1
    inside = (idx >= 0) & (l[np.clip(idx, 0, None)] >= tid)
    sum_lots = np.bincount(idx[inside], weights=tlots[inside], minlength=len(f)).astype(np.int64)
    ntr = np.bincount(idx[inside], minlength=len(f))
    consistent = sum_lots == alots
    return {"inside": inside, "sum_lots": sum_lots, "ntr": ntr, "consistent": consistent,
            "no_trades": ntr == 0, "partial": ~consistent & (ntr > 0)}

def hour_index(ts: np.ndarray, d_start: int) -> np.ndarray:
    return ((ts - d_start) // HOUR_MS).astype(np.int64)

# ============================================================================ writing
def part_path(root: Path, symbol: str, dataset: str, d: str, kind: str = "normalized", provider: str = S.PROVIDER_ID) -> Path:
    return (root / kind / f"provider={provider}" / f"market={S.VENUES[provider][1]}" / f"instrument={symbol}" /
            f"data_type={dataset}" / f"date={d}" / "part-000.parquet")

def logical_digest(t: pa.Table) -> str:
    """sha256 of the table's Arrow IPC stream with schema metadata removed (content-only, deterministic)."""
    t = t.replace_schema_metadata(None).combine_chunks()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, t.schema) as w: w.write_table(t)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()

def write_atomic(t: pa.Table, path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    pq.write_table(t, tmp, compression="zstd", compression_level=3, row_group_size=262_144,
                   use_dictionary=True, write_statistics=True)
    with open(tmp, "rb") as fh: os.fsync(fh.fileno())
    os.replace(tmp, path)
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for b in iter(lambda: fh.read(1 << 20), b""): h.update(b)
    return h.hexdigest()

# ============================================================================ day worker
def process_day(job: dict) -> dict:
    """Validate + normalise every dataset for one UTC date. Runs in a worker process; returns catalog rows."""
    d = date.fromisoformat(job["date"]); d0 = day_start_ms(d); d1 = d0 + DAY_MS
    spec, sym, acq_end, root, write = job["spec"], job["symbol"], job["acq_end"], Path(job["data_root"]), job["write"]
    res = {"date": job["date"], "partitions": [], "quarantine": [], "day_status": [], "hour_status": [], "raw_stats": []}
    files = job["files"]   # dataset -> {raw_file_id, path, sha256}
    loaded = {}
    for ds in DATASETS:
        fi = files.get(ds)
        if fi is None:
            res["day_status"].append({"dataset": ds, "status": "MISSING", "evidence": {"reason": job["missing_reason"].get(ds, "no source file")}})
            continue
        h = hashlib.sha256(Path(fi["path"]).read_bytes()).hexdigest()
        if h != fi["sha256"]:
            raise RuntimeError(f"raw file changed on disk: {fi['path']} ({h} != {fi['sha256']})")
        raw, parse_bad = read_raw(Path(fi["path"]), ds)
        loaded[ds] = (raw, parse_bad, fi)

    out = {}   # dataset -> (table, quarantine rows, stats)
    kl = None; kref = None
    # ---- klines (reference)
    if "klines_1m" in loaded:
        raw, pb, fi = loaded["klines_1m"]
        rej, der = validate_klines(raw, d0, d1, spec, acq_end); ok = rej.ok
        o = np.argsort(raw["open_time"][ok], kind="stable"); sel = np.nonzero(ok)[0][o]; n = len(sel)
        cols = {**const_cols(n, sym), "open_time_utc": raw["open_time"][sel], "close_time_utc": raw["close_time"][sel],
                **{k: raw[k][sel] for k in ("open", "high", "low", "close", "volume", "quote_volume")},
                "trade_count": raw["count"][sel], "taker_buy_volume": raw["taker_buy_volume"][sel],
                "taker_buy_quote_volume": raw["taker_buy_quote_volume"][sel], "volume_lots": der["volume_lots"][sel],
                "data_quality_status": np.full(n, "VALID"), "raw_file_id": np.full(n, fi["raw_file_id"], np.int32),
                "raw_row": raw["raw_row"][sel]}
        out["klines_1m"] = (build_table(S.KLINE_SCHEMA, cols), quarantine_rows("klines_1m", rej, raw, fi["raw_file_id"], None, "open_time", pb))
        kl = {"minute": raw["open_time"][sel], "count": raw["count"][sel], "lots": der["volume_lots"][sel]}
        km_ = ((kl["minute"] - d0) // 60_000).astype(np.int64)
        kref = {"have": np.zeros(1440, bool), "low_ticks": np.zeros(1440, np.int64), "high_ticks": np.zeros(1440, np.int64),
                "inv_tick": spec["inv_tick"]}
        kref["have"][km_] = raw["count"][sel] > 0
        kref["low_ticks"][km_] = np.rint(raw["low"][sel] * spec["inv_tick"]).astype(np.int64)
        kref["high_ticks"][km_] = np.rint(raw["high"][sel] * spec["inv_tick"]).astype(np.int64)
        missing_min = 1440 - len(np.unique(kl["minute"]))
        res["day_status"].append({"dataset": "klines_1m", "status": "COMPLETE" if missing_min == 0 else "PARTIAL",
                                  "evidence": {"minutes": int(n), "missing_minutes": int(missing_min)}})
    # ---- aggTrades
    agg = None
    if "aggTrades" in loaded:
        raw, pb, fi = loaded["aggTrades"]
        rej, der = validate_agg(raw, d0, d1, spec, acq_end, kref); ok = rej.ok
        o = np.argsort(raw["agg_trade_id"][ok], kind="stable"); sel = np.nonzero(ok)[0][o]; n = len(sel)
        bm = raw["is_buyer_maker"][sel]
        agg = {"agg_trade_id": raw["agg_trade_id"][sel], "first_trade_id": raw["first_trade_id"][sel],
               "last_trade_id": raw["last_trade_id"][sel], "ts": raw["transact_time"][sel], "price": raw["price"][sel],
               "quantity": raw["quantity"][sel], "bm": bm, "price_ticks": der["price_ticks"][sel],
               "quantity_lots": der["quantity_lots"][sel], "raw_row": raw["raw_row"][sel], "raw_file_id": fi["raw_file_id"]}
        cols = {**const_cols(n, sym), "aggregate_trade_id": agg["agg_trade_id"], "first_trade_id": agg["first_trade_id"],
                "last_trade_id": agg["last_trade_id"], "exchange_event_time_utc": agg["ts"], "price": agg["price"],
                "quantity": agg["quantity"], "buyer_maker_flag": bm, "aggressor_side": np.where(bm, "SELL", "BUY"),
                "side_evidence": np.full(n, "EXCHANGE_REPORTED"), "price_ticks": agg["price_ticks"],
                "quantity_lots": agg["quantity_lots"], "data_quality_status": np.full(n, "VALID"),
                "raw_file_id": np.full(n, fi["raw_file_id"], np.int32), "raw_row": agg["raw_row"]}
        out["aggTrades"] = (build_table(S.AGG_SCHEMA, cols), quarantine_rows("aggTrades", rej, raw, fi["raw_file_id"], "agg_trade_id", "transact_time", pb))
        res["raw_stats"].append({"dataset": "aggTrades", "rows": int(len(raw["agg_trade_id"])),
                                 "id_sum": int(raw["agg_trade_id"].sum()), "lots_sum": int(der["quantity_lots"].sum())})
    # ---- trades
    if "trades" in loaded:
        raw, pb, fi = loaded["trades"]
        rej, der = validate_trades(raw, d0, d1, spec, acq_end, kref); ok = rej.ok
        res["raw_stats"].append({"dataset": "trades", "rows": int(len(raw["id"])), "id_sum": int(raw["id"].sum()),
                                 "lots_sum": int(der["quantity_lots"].sum())})
        o = np.argsort(raw["id"][ok], kind="stable"); sel = np.nonzero(ok)[0][o]
        tid, tts, tlots = raw["id"][sel], raw["time"][sel], der["quantity_lots"][sel]
        rec = None; agg_ev = {"status": "UNAVAILABLE"}; partial_hours: dict[int, list] = {}
        if agg is not None and len(agg["agg_trade_id"]):
            r = reconcile_with_agg(tid, tlots, tts, agg)
            # recover only ids absent from the raw trades file entirely (a quarantined id is never "recovered")
            single = r["no_trades"] & (agg["first_trade_id"] == agg["last_trade_id"]) & \
                     ~np.isin(agg["first_trade_id"], raw["id"])
            rec = np.nonzero(single)[0]
            # recompute after recovery: recovered rows make their agg rows consistent
            unmatched = (~r["consistent"]) & ~single
            # Rows/trades straddling midnight: an aggTrade stamped late on D can carry fills stamped on D+1 (and
            # vice versa). They are reported to the driver, which resolves them against the neighbouring day.
            lo_id, hi_id = (int(tid.min()), int(tid.max())) if len(tid) else (0, -1)
            straddle = unmatched & ((agg["last_trade_id"] > hi_id) | (agg["first_trade_id"] < lo_id))
            unmatched = unmatched & ~straddle
            res["boundary_agg"] = [{"agg_id": int(agg["agg_trade_id"][i]), "f": int(agg["first_trade_id"][i]),
                                    "l": int(agg["last_trade_id"][i]), "agg_lots": int(agg["quantity_lots"][i]),
                                    "day_lots": int(r["sum_lots"][i]), "ts": int(agg["ts"][i])} for i in np.nonzero(straddle)[0]]
            un_idx = np.nonzero(unmatched)[0]
            miss_lots = (agg["quantity_lots"][un_idx] - r["sum_lots"][un_idx])
            for i, ml in zip(un_idx, miss_lots):
                partial_hours.setdefault(int((agg["ts"][i] - d0) // HOUR_MS), []).append(("agg_unmatched", int(ml)))
            outside = ~r["inside"]
            edge = outside & ((tid < int(agg["first_trade_id"].min())) | (tid > int(agg["last_trade_id"].max())))
            res["boundary_trades"] = {"ids": tid[edge].tolist(), "lots": tlots[edge].tolist(), "ts": tts[edge].tolist()}
            outside = outside & ~edge
            agg_ev = {"status": "CONSISTENT" if not len(un_idx) else "TRADES_MISSING",
                      "agg_rows": int(len(agg["agg_trade_id"])), "agg_rows_unmatched": int(len(un_idx)),
                      "lots_in_agg_not_in_trades": int(miss_lots.sum()) if len(un_idx) else 0,
                      "recovered_single_trades": int(len(rec)), "trades_outside_agg": int(outside.sum()),
                      "boundary_agg_rows": len(res["boundary_agg"]), "boundary_trades": int(edge.sum()),
                      "lots_trades_outside_agg": int(tlots[outside].sum())}
            agg_hours = {}
            for hh in np.unique(hour_index(tts[outside], d0)): agg_hours[int(hh)] = int((hour_index(tts[outside], d0) == hh).sum())
            res["_agg_outside_hours"] = agg_hours
        # assemble trades table (archive + recovered)
        n0 = len(sel); nr = 0 if rec is None else len(rec)
        bm = np.concatenate([raw["is_buyer_maker"][sel], agg["bm"][rec] if nr else np.array([], bool)])
        cat = lambda a, b: np.concatenate([a, b]) if nr else a
        ids = cat(tid, agg["first_trade_id"][rec] if nr else None)
        ts_all = cat(tts, agg["ts"][rec] if nr else None)
        order = np.argsort(ids, kind="stable")
        qn = pa.array(np.concatenate([raw["quote_qty"][sel], np.zeros(nr)]), pa.float64(),
                      mask=np.concatenate([np.zeros(n0, bool), np.ones(nr, bool)])).take(pa.array(order))
        agg_id = pa.array(np.concatenate([np.zeros(n0, np.int64), agg["agg_trade_id"][rec] if nr else np.array([], np.int64)]),
                          pa.int64(), mask=np.concatenate([np.ones(n0, bool), np.zeros(nr, bool)])).take(pa.array(order))
        N = n0 + nr
        cols = {**const_cols(N, sym), "source_trade_id": ids[order], "aggregate_trade_id": agg_id,
                "exchange_event_time_utc": ts_all[order], "transaction_time_utc": ts_all[order], "receive_time_utc": null_ts(N),
                "price": cat(raw["price"][sel], agg["price"][rec] if nr else None)[order],
                "quantity": cat(raw["qty"][sel], agg["quantity"][rec] if nr else None)[order], "quote_notional": qn,
                "buyer_maker_flag": bm[order], "aggressor_side": np.where(bm[order], "SELL", "BUY"),
                "side_evidence": np.full(N, "EXCHANGE_REPORTED"),
                "price_ticks": cat(der["price_ticks"][sel], agg["price_ticks"][rec] if nr else None)[order],
                "quantity_lots": cat(tlots, agg["quantity_lots"][rec] if nr else None)[order],
                "record_origin": np.concatenate([np.full(n0, "ARCHIVE_TRADES"), np.full(nr, "RECOVERED_FROM_AGGTRADES")])[order],
                "data_quality_status": np.concatenate([np.full(n0, "VALID"), np.full(nr, "RECOVERED")])[order],
                "raw_file_id": np.concatenate([np.full(n0, fi["raw_file_id"], np.int32),
                                               np.full(nr, agg["raw_file_id"] if nr else 0, np.int32)])[order],
                "raw_row": cat(raw["raw_row"][sel], agg["raw_row"][rec] if nr else None)[order]}
        out["trades"] = (build_table(S.TRADE_SCHEMA, cols), quarantine_rows("trades", rej, raw, fi["raw_file_id"], "id", "time", pb))
        # klines evidence (count + volume per minute, archive + recovered)
        kl_ev = {"status": "UNAVAILABLE"}
        if kl is not None:
            tmin = (ts_all - d0) // 60_000
            c_t = np.bincount(tmin, minlength=1440)[:1440]
            v_t = np.bincount(tmin, weights=cat(tlots, agg["quantity_lots"][rec] if nr else None), minlength=1440)[:1440].astype(np.int64)
            km = ((kl["minute"] - d0) // 60_000).astype(np.int64)
            c_k = np.zeros(1440, np.int64); v_k = np.zeros(1440, np.int64); have = np.zeros(1440, bool)
            c_k[km] = kl["count"]; v_k[km] = kl["lots"]; have[km] = True
            bad_min = np.nonzero(have & ((c_k != c_t) | (v_k != v_t)))[0]
            for mm in bad_min:
                partial_hours.setdefault(int(mm // 60), []).append(("klines_mismatch", int(c_k[mm] - c_t[mm])))
            kl_ev = {"status": "MATCH" if not len(bad_min) else "MISMATCH", "minutes_compared": int(have.sum()),
                     "klines_count": int(c_k[have].sum()), "trades_count": int(c_t[have].sum()),
                     "minutes_mismatched": int(len(bad_min)),
                     "mismatched_minutes": [iso(d0 + int(m) * 60_000) for m in bad_min[:20]]}
        has_evidence = kl_ev["status"] != "UNAVAILABLE" or agg_ev["status"] != "UNAVAILABLE"
        status = "PARTIAL" if partial_hours else ("COMPLETE" if has_evidence else "UNVERIFIED")
        res["day_status"].append({"dataset": "trades", "status": status, "evidence": {
            "klines_1m": kl_ev, "aggTrades": agg_ev, "records_valid": int(n0), "records_recovered": int(nr),
            "records_quarantined": int((~ok).sum()) + len(pb),
            "id_gaps_unused_ids": int((np.diff(tid) - 1)[np.diff(tid) > 1].sum()) if len(tid) > 1 else 0}})
        for hh in range(24):
            if hh in partial_hours:
                kinds = sorted({k for k, _ in partial_hours[hh]})
                st, det = "PARTIAL", {"evidence": kinds, "agg_rows_unmatched": sum(1 for k, _ in partial_hours[hh] if k == "agg_unmatched"),
                                      "lots_missing_vs_agg": sum(v for k, v in partial_hours[hh] if k == "agg_unmatched"),
                                      "klines_minutes_mismatched": sum(1 for k, _ in partial_hours[hh] if k == "klines_mismatch")}
            else:
                st, det = ("COMPLETE" if has_evidence else "UNVERIFIED"), {}
            res["hour_status"].append({"dataset": "trades", "hour_utc": iso(d0 + hh * HOUR_MS), "status": st, "detail": det})
    # aggTrades day status (completeness of the aggTrades dataset itself, judged against trades)
    if "aggTrades" in loaded:
        oh = res.pop("_agg_outside_hours", {})
        st = "PARTIAL" if oh else ("COMPLETE" if "trades" in loaded else "UNVERIFIED")
        res["day_status"].append({"dataset": "aggTrades", "status": st,
                                  "evidence": {"trades_absent_from_aggtrades_by_hour": {str(k): v for k, v in oh.items()}}})
        for hh in range(24):
            res["hour_status"].append({"dataset": "aggTrades", "hour_utc": iso(d0 + hh * HOUR_MS),
                                       "status": "PARTIAL" if hh in oh else st if st != "PARTIAL" else "COMPLETE",
                                       "detail": {"trades_absent": oh[hh]} if hh in oh else {}})
    # ---- bookDepth
    if "bookDepth" in loaded:
        raw, pb, fi = loaded["bookDepth"]
        rej, der = validate_bookdepth(raw, d0, d1, spec, acq_end); ok = rej.ok
        key = der["ts_ms"] * 1000 + np.rint(raw["percentage"] * 10).astype(np.int64) + 500
        sel = np.nonzero(ok)[0][np.argsort(key[ok], kind="stable")]; n = len(sel)
        cols = {**const_cols(n, sym), "snapshot_time_utc": der["ts_ms"][sel], "band_pct": raw["percentage"][sel],
                "cumulative_depth_qty": raw["depth"][sel], "cumulative_notional": raw["notional"][sel],
                "book_side": np.where(raw["percentage"][sel] < 0, "BID", "ASK"), "data_quality_status": np.full(n, "VALID"),
                "raw_file_id": np.full(n, fi["raw_file_id"], np.int32), "raw_row": raw["raw_row"][sel]}
        out["bookDepth"] = (build_table(S.DEPTHBAND_SCHEMA, cols), quarantine_rows("bookDepth", rej, raw, fi["raw_file_id"], None, "timestamp", pb, 1000))
        snaps, per = np.unique(der["ts_ms"][sel], return_counts=True)
        incomplete_snaps = int((per != len(BANDS)).sum())
        edges = np.concatenate(([d0], snaps, [d1])); gaps = np.diff(edges)
        holes = [[iso(int(edges[i])), iso(int(edges[i + 1]))] for i in np.nonzero(gaps > 120_000)[0]]
        res["day_status"].append({"dataset": "bookDepth", "status": "PARTIAL" if holes or incomplete_snaps else "COMPLETE",
                                  "evidence": {"snapshots": int(len(snaps)), "snapshots_missing_bands": incomplete_snaps,
                                               "holes_over_120s": len(holes), "holes": holes[:50],
                                               "max_gap_s": float(gaps.max() / 1000)}})
    # ---- metrics
    if "metrics" in loaded:
        raw, pb, fi = loaded["metrics"]
        rej, der = validate_metrics(raw, d0, d1, spec, acq_end, sym); ok = rej.ok
        sel = np.nonzero(ok)[0][np.argsort(der["ts_ms"][ok], kind="stable")]; n = len(sel)
        vals = ["sum_open_interest", "sum_open_interest_value", "count_toptrader_long_short_ratio",
                "sum_toptrader_long_short_ratio", "count_long_short_ratio", "sum_taker_long_short_vol_ratio"]
        cols = {**const_cols(n, sym), "create_time_utc": der["ts_ms"][sel], **{k: raw[k][sel] for k in vals},
                "data_quality_status": np.full(n, "VALID"), "raw_file_id": np.full(n, fi["raw_file_id"], np.int32),
                "raw_row": raw["raw_row"][sel]}
        out["metrics"] = (build_table(S.METRICS_SCHEMA, cols), quarantine_rows("metrics", rej, raw, fi["raw_file_id"], None, "create_time", pb, 1000))
        have = set(int(x) for x in der["ts_ms"][sel])
        miss = [s for s in range(d0, d1, 300_000) if s not in have]
        res["day_status"].append({"dataset": "metrics", "status": "PARTIAL" if miss else "COMPLETE",
                                  "evidence": {"rows": int(n), "missing_5m_slots": len(miss), "missing": [iso(m) for m in miss[:30]]}})
    # ---- write / digest
    for ds, (tbl, qrows) in out.items():
        p = part_path(root, sym, ds, job["date"])
        dig = logical_digest(tbl)
        fsha = write_atomic(tbl, p) if write else None
        qcount = {}
        for q in qrows: qcount[q["reason_code"]] = qcount.get(q["reason_code"], 0) + 1
        qp = part_path(root, sym, ds, job["date"], "quarantine")
        if qrows:
            qt = pa.Table.from_pylist(qrows, schema=S.QUARANTINE_SCHEMA)
            if write: write_atomic(qt, qp)
        elif write and qp.exists():
            safe_remove(qp)
        res["partitions"].append({"dataset": ds, "path": str(p), "rows": tbl.num_rows, "logical_digest": dig, "file_sha256": fsha,
                                  "raw_file_ids": sorted(set(int(x) for x in np.unique(tbl.column("raw_file_id").to_numpy()))),
                                  "first_ts": None, "quarantined": len(qrows), "quarantine_by_reason": qcount})
        res["quarantine"].extend({"dataset": ds, "reason_code": k, "count": v} for k, v in qcount.items())
        if qrows: res.setdefault("quarantine_samples", []).extend(qrows[:5])
    return res
