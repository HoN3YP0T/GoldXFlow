"""Phase 5: exact integer price-level aggregation structures.

Two structures, both integer-only (lots of 0.001 oz, price ticks of 0.01 USDT), so every total is exact:

* ``Rows``: a columnar table of (bucket, level) groups. It is used for footprint bars, whose key is two-dimensional
  (bar start, price bin). ``Rows.reduce`` merges rows with equal keys, and is associative and order-preserving.
  So ``reduce(state + new)`` gives the same result however the input was split into chunks. That property is
  what makes snapshot/restore and chunk-size independence exact.
* ``LevelArrays``: dense per-price-level arrays for one window (a session, a day, a week, a custom range).
  They grow in either direction as new prices appear.

Column meanings (the same in both):
  buy, sell, unk       aggressive lots by exchange-reported side (UNKNOWN is never reassigned)
  cnt, bcnt, scnt      trade counts: all, buy-aggressor, sell-aggressor
  rcnt                 trades that Phase 3 recovered from aggTrades (record_origin = RECOVERED_FROM_AGGTRADES)
  notional             sum(price_ticks * quantity_lots). USDT = notional * tick_size * step_size (exactly 1e-5 here)
  ibuy, isell          lots of UNKNOWN-side trades split by the optional tick rule (INFERRED; zero when disabled)
  first_ts, last_ts    min/max exchange trade time (ms)
  first_seq, last_seq  min/max processor delivery sequence (orders open/close deterministically)
  min_id, max_id       min/max source trade id (links every aggregate back to its source trades)
"""
from __future__ import annotations
from typing import Any
import base64, hashlib, json, zlib
import numpy as np
from numpy.typing import NDArray

SUM_COLS = ("buy", "sell", "unk", "cnt", "bcnt", "scnt", "rcnt", "notional", "ibuy", "isell")
MIN_COLS = ("first_ts", "first_seq", "min_id")
MAX_COLS = ("last_ts", "last_seq", "max_id")
VAL_COLS = SUM_COLS + MIN_COLS + MAX_COLS
ROW_COLS = ("bucket", "level") + VAL_COLS
I64_MAX = np.iinfo(np.int64).max
I64_MIN = np.iinfo(np.int64).min


class Rows:
    """Columnar (bucket, level) groups. All columns are int64 arrays of equal length."""
    __slots__ = ("c",)

    def __init__(self, cols: dict[str, Any] | None = None) -> None:
        self.c = {k: np.asarray(cols[k], np.int64) for k in ROW_COLS} if cols else {k: np.empty(0, np.int64) for k in ROW_COLS}

    def __len__(self) -> int: return len(self.c["bucket"])
    def __getitem__(self, k: str) -> NDArray[Any]: return self.c[k]

    def take(self, idx: Any) -> "Rows":
        r = Rows.__new__(Rows); r.c = {k: v[idx] for k, v in self.c.items()}; return r

    def with_key(self, bucket: NDArray[Any] | None = None, level: NDArray[Any] | None = None) -> "Rows":
        r = Rows.__new__(Rows); r.c = dict(self.c)
        if bucket is not None: r.c["bucket"] = bucket
        if level is not None: r.c["level"] = level
        return r

    @staticmethod
    def concat(parts: list["Rows"]) -> "Rows":
        parts = [p for p in parts if len(p)]
        if not parts: return Rows()
        if len(parts) == 1: return parts[0]
        r = Rows.__new__(Rows); r.c = {k: np.concatenate([p.c[k] for p in parts]) for k in ROW_COLS}; return r

    def reduce(self) -> "Rows":
        """Merge rows with equal (bucket, level). Output is sorted by (bucket, level). The sort is stable, so rows
        keep their input (chronological) order inside a group; sums, mins and maxes don't depend on it anyway."""
        n = len(self)
        if n == 0: return self
        b, l = self.c["bucket"], self.c["level"]
        order = np.lexsort((l, b))
        bs, ls = b[order], l[order]
        start = np.ones(n, bool); start[1:] = (bs[1:] != bs[:-1]) | (ls[1:] != ls[:-1])
        if start.all():   # nothing to merge, just sorted
            return self.take(order)
        idx = np.flatnonzero(start)
        out = {"bucket": bs[idx], "level": ls[idx]}
        for k in SUM_COLS: out[k] = np.add.reduceat(self.c[k][order], idx)
        for k in MIN_COLS: out[k] = np.minimum.reduceat(self.c[k][order], idx)
        for k in MAX_COLS: out[k] = np.maximum.reduceat(self.c[k][order], idx)
        r = Rows.__new__(Rows); r.c = out; return r

    def merge_sorted(self, new: "Rows") -> "Rows":
        """reduce(self + new) for two reduced (sorted) inputs, sorting only the overlapping part. In-order delivery
        means only new rows in buckets <= self's last bucket can collide; later rows are appended unchanged."""
        if not len(self): return new
        if not len(new): return self
        last = self.c["bucket"][-1]
        if new.c["bucket"][0] > last: return Rows.concat([self, new])
        k = int(np.searchsorted(new.c["bucket"], last, side="right"))
        return Rows.concat([Rows.concat([self, new.take(slice(0, k))]).reduce(), new.take(slice(k, None))])

    def rebin(self, bin_ticks: int) -> "Rows":
        """Coarser price bins: level -> level // bin_ticks (floor division; prices are positive)."""
        return self if bin_ticks == 1 else self.with_key(level=self.c["level"] // bin_ticks).reduce()

    def to_state(self) -> dict[str, Any]: return encode_arrays(self.c)
    @staticmethod
    def from_state(s: dict[str, Any]) -> "Rows": return Rows(decode_arrays(s)) if s else Rows()


def bars_from_rows(rows: Rows) -> dict[str, Any]:
    """One bar per bucket from 1-tick-resolution rows sorted by (bucket, level) (the output of ``Rows.reduce``).

    high/low are the max/min level. open/close are the level of the row holding the bucket's first/last delivered
    trade (min first_seq / max last_seq). Each trade is in exactly one row, so these are unique."""
    n = len(rows)
    if n == 0: return {}
    b = rows["bucket"]
    start = np.ones(n, bool); start[1:] = b[1:] != b[:-1]
    idx = np.flatnonzero(start); counts = np.diff(np.append(idx, n))
    out = {"bucket": b[idx], "n_levels": counts.astype(np.int64)}
    for k in SUM_COLS: out[k] = np.add.reduceat(rows[k], idx)
    for k in MIN_COLS: out[k] = np.minimum.reduceat(rows[k], idx)
    for k in MAX_COLS: out[k] = np.maximum.reduceat(rows[k], idx)
    lv = rows["level"]
    out["low"] = np.minimum.reduceat(lv, idx); out["high"] = np.maximum.reduceat(lv, idx)
    out["open"] = lv[rows["first_seq"] == np.repeat(out["first_seq"], counts)]
    out["close"] = lv[rows["last_seq"] == np.repeat(out["last_seq"], counts)]
    return out


class LevelArrays:
    """Dense per-level aggregates for one window. Index i holds price level ``lo + i``."""
    __slots__ = ("lo", "a")
    GROW = 256

    def __init__(self) -> None:
        self.lo = 0; self.a = {k: np.empty(0, np.int64) for k in VAL_COLS}

    def __len__(self) -> int: return len(self.a["cnt"])

    def _ensure(self, lmin: int, lmax: int) -> None:
        n = len(self)
        if n == 0:
            self.lo = lmin - self.GROW
            size = lmax - self.lo + 1 + self.GROW
            self.a = {k: self._blank(k, size) for k in VAL_COLS}; return
        hi = self.lo + n - 1
        if lmin >= self.lo and lmax <= hi: return
        new_lo = min(self.lo, lmin - self.GROW) if lmin < self.lo else self.lo
        new_hi = max(hi, lmax + self.GROW) if lmax > hi else hi
        size = new_hi - new_lo + 1; off = self.lo - new_lo
        for k in VAL_COLS:
            arr = self._blank(k, size); arr[off:off + n] = self.a[k]; self.a[k] = arr
        self.lo = new_lo

    @staticmethod
    def _blank(k: str, size: int) -> NDArray[Any]:
        if k in MIN_COLS: return np.full(size, I64_MAX, np.int64)
        if k in MAX_COLS: return np.full(size, I64_MIN, np.int64)
        return np.zeros(size, np.int64)

    def add_rows(self, rows: Rows, mask: NDArray[Any] | None = None) -> None:
        """Add 1-tick rows (any bucket) into the window's levels."""
        if mask is not None: rows = rows.take(mask)
        if len(rows) == 0: return
        lv = rows["level"]; self._ensure(int(lv.min()), int(lv.max()))
        i = lv - self.lo
        for k in SUM_COLS: np.add.at(self.a[k], i, rows[k])
        for k in MIN_COLS: np.minimum.at(self.a[k], i, rows[k])
        for k in MAX_COLS: np.maximum.at(self.a[k], i, rows[k])

    def nonzero(self) -> dict[str, Any]:
        """Levels with at least one trade, as columns (``level`` plus every value column)."""
        if len(self) == 0: return {"level": np.empty(0, np.int64), **{k: np.empty(0, np.int64) for k in VAL_COLS}}
        m = self.a["cnt"] > 0
        return {"level": np.flatnonzero(m).astype(np.int64) + self.lo, **{k: v[m] for k, v in self.a.items()}}

    def view(self, bin_ticks: int = 1) -> dict[str, Any]:
        """Price profile at a coarser bin size (exact integer sums; level = price_ticks // bin_ticks)."""
        nz = self.nonzero()
        if bin_ticks == 1 or len(nz["level"]) == 0: return nz
        r = Rows({"bucket": np.zeros(len(nz["level"]), np.int64), **nz}).rebin(bin_ticks)
        return {k: r[k] for k in ("level",) + VAL_COLS}

    def totals(self) -> dict[str, Any]:
        nz = self.nonzero()
        t = {k: int(nz[k].sum()) for k in SUM_COLS}
        if len(nz["level"]):
            for k in MIN_COLS: t[k] = int(nz[k].min())
            for k in MAX_COLS: t[k] = int(nz[k].max())
            t["low"] = int(nz["level"][0]); t["high"] = int(nz["level"][-1])
            t["open"] = int(nz["level"][np.argmin(nz["first_seq"])]); t["close"] = int(nz["level"][np.argmax(nz["last_seq"])])
            t["n_levels"] = int(len(nz["level"]))
        else:
            for k in MIN_COLS + MAX_COLS + ("low", "high", "open", "close"): t[k] = None  # type: ignore[assignment]
            t["n_levels"] = 0
        return t

    def to_state(self) -> dict[str, Any]:   # sparse: only traded levels
        return encode_arrays(self.nonzero())

    @staticmethod
    def from_state(s: dict[str, Any]) -> "LevelArrays":
        la = LevelArrays()
        if not s: return la
        nz = decode_arrays(s)
        if len(nz["level"]):
            la._ensure(int(nz["level"].min()), int(nz["level"].max()))
            i = nz["level"] - la.lo
            for k in VAL_COLS: la.a[k][i] = nz[k]
        return la


# ---------------------------------------------------------------------------- state encoding and digests
def encode_arrays(d: dict[str, Any]) -> dict[str, Any]:
    """JSON-safe, deterministic encoding of int/bool numpy arrays (zlib-compressed raw little-endian bytes)."""
    out = {}
    for k in sorted(d):
        a = np.ascontiguousarray(d[k])
        out[k] = {"dtype": a.dtype.str, "shape": list(a.shape), "z": base64.b64encode(zlib.compress(a.tobytes(), 1)).decode()}
    return out


def decode_arrays(s: dict[str, Any]) -> dict[str, Any]:
    return {k: np.frombuffer(zlib.decompress(base64.b64decode(v["z"])), dtype=np.dtype(v["dtype"])).reshape(v["shape"]).copy()
            for k, v in s.items()}


def digest_tree(obj: Any) -> str:
    """SHA-256 over a nested structure of dicts/lists/scalars/numpy arrays. Arrays are hashed by dtype, shape and
    raw bytes (not by their compressed encoding), so the digest is machine- and zlib-independent."""
    h = hashlib.sha256()
    def walk(o: Any) -> None:
        if isinstance(o, np.ndarray):
            a = np.ascontiguousarray(o); h.update(b"A" + a.dtype.str.encode() + json.dumps(list(a.shape)).encode()); h.update(a.tobytes())
        elif isinstance(o, dict):
            h.update(b"D%d" % len(o))
            for k in sorted(o): h.update(b"K" + str(k).encode()); walk(o[k])
        elif isinstance(o, (list, tuple)):
            h.update(b"L%d" % len(o))
            for x in o: walk(x)
        elif isinstance(o, Rows): walk(o.c)
        elif isinstance(o, LevelArrays): walk(o.nonzero())
        else:
            h.update(b"S" + json.dumps(o, sort_keys=True, default=str).encode())
    walk(obj)
    return h.hexdigest()
