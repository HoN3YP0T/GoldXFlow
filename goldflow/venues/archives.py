"""Phase 15: public trade archives of other venues, and their parsers.

Raw files are kept exactly as published, in ``data/raw/<provider>/<market>/<symbol>/trades/daily/``, with a sidecar
``<file>.meta.json`` (URL, size, SHA-256, ETag, Last-Modified, download time). A file that is already present is not
downloaded again. A re-download that gives different bytes is refused, because a published day must not change.

| Venue | Archive | Day boundaries | Ids | Side | RPI flag |
|---|---|---|---|---|---|
| OKX | ``static.okx.com/cdn/okex/traderecords/trades/daily/YYYYMMDD/<inst>-trades-YYYY-MM-DD.zip`` | **UTC+8** | contiguous integers | taker side | ``source`` = 1 |
| Bybit | ``public.bybit.com/trading/<sym>/<sym>YYYY-MM-DD.csv.gz`` | UTC | UUID ``trdMatchID`` | taker side | ``RPI`` = 1 |

Parsers return the column dictionary the Phase 3 trade validator takes (``id, price, qty, quote_qty, time,
is_buyer_maker, raw_row``), plus ``rpi`` and, for Bybit, ``match_id``.
- **Bybit timestamps** are seconds with up to 4 decimals. They are parsed as text into whole milliseconds (floor), never
  through binary floats; the file's row order breaks ties.
- **Bybit ids** are UUIDs. ``source_trade_id`` is therefore derived: ``day_number * 10**7 + row``. It increases in file
  order and is labelled derived.
- **OKX quantities** are in contracts (``ctVal`` 0.001 XAU). OKX does not publish a quote notional, so ``quote_qty`` is
  the derived ``price * size`` and the validator's notional check is trivially met.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import time
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv

Fetch = Callable[[str], "tuple[bytes, dict[str, str]] | None"]
UA = {"User-Agent": "goldflow/1 (research data import)"}


def http_fetch(url: str, attempts: int = 5, timeout_s: float = 60.0) -> tuple[bytes, dict[str, str]] | None:
    """GET with bounded retries. None for 404/403-not-published; raises after the last failed attempt."""
    import requests

    last: Exception | None = None
    for i in range(attempts):
        try:
            r = requests.get(url, headers=UA, timeout=timeout_s)
            if r.status_code == 404:
                return None
            if r.status_code == 200:
                return r.content, {
                    k.lower(): v for k, v in r.headers.items() if k.lower() in ("etag", "last-modified", "content-length")
                }
            last = RuntimeError(f"HTTP {r.status_code} for {url}")
            if r.status_code in (400, 401, 403, 451):
                raise last
        except requests.RequestException as e:
            last = e
        time.sleep(min(30.0, 2.0**i))
    raise RuntimeError(f"{url}: {last}")


def archive_name(provider: str, symbol: str, day: str) -> tuple[str, str]:
    """(url, file name) of a venue's archive file for its archive day ``day`` (YYYY-MM-DD)."""
    if provider == "okx":
        f = f"{symbol}-trades-{day}.zip"
        return f"https://static.okx.com/cdn/okex/traderecords/trades/daily/{day.replace('-', '')}/{f}", f
    if provider == "bybit":
        f = f"{symbol}{day}.csv.gz"
        return f"https://public.bybit.com/trading/{symbol}/{f}", f
    raise ValueError(f"no public trade archive implemented for {provider!r}")


def raw_dir(data_root: Path, provider: str, market: str, symbol: str) -> Path:
    return Path(data_root) / "raw" / provider / market / symbol / "trades" / "daily"


def get_raw(data_root: Path, provider: str, market: str, symbol: str, day: str, fetch: Fetch = http_fetch) -> Path | None:
    url, name = archive_name(provider, symbol, day)
    d = raw_dir(data_root, provider, market, symbol)
    p, meta = d / name, d / f"{name}.meta.json"
    if p.exists() and meta.exists():
        m = json.loads(meta.read_text())
        if hashlib.sha256(p.read_bytes()).hexdigest() != m["sha256"]:
            raise RuntimeError(f"{p}: content differs from the recorded SHA-256 (raw files are immutable)")
        return p
    got = fetch(url)
    if got is None:
        return None
    body, headers = got
    sha = hashlib.sha256(body).hexdigest()
    d.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".part")
    tmp.write_bytes(body)
    tmp.replace(p)
    meta.write_text(json.dumps({"url": url, "size": len(body), "sha256": sha, "headers": headers,
                                "downloaded_utc": datetime.now(timezone.utc).isoformat()}, indent=1))  # fmt: skip
    return p


def file_interval(provider: str, day: str, offset_h: int) -> tuple[int, int]:
    """[start, end) in UTC ms covered by the archive day (OKX days are UTC+8)."""
    d0 = (
        int(datetime.combine(date.fromisoformat(day), datetime.min.time(), timezone.utc).timestamp() * 1000)
        - offset_h * 3_600_000
    )
    return d0, d0 + 86_400_000


def _ms_from_seconds_text(col: pa.Array) -> np.ndarray:
    out = np.empty(len(col), np.int64)
    for i, s in enumerate(col.to_pylist()):
        whole, _, frac = str(s).partition(".")
        out[i] = int(whole) * 1000 + int((frac + "000")[:3])
    return out


def parse_okx(path: Path, symbol: str) -> dict[str, Any]:
    with zipfile.ZipFile(path) as z:
        names = z.namelist()
        if len(names) != 1:
            raise ValueError(f"{path}: expected one CSV in the zip, found {names}")
        body = z.read(names[0])
    types = {"instrument_name": pa.string(), "trade_id": pa.int64(), "side": pa.string(), "price": pa.float64(), "size": pa.float64(),
             "created_time": pa.int64(), "source": pa.string()}  # fmt: skip
    t = pacsv.read_csv(io.BytesIO(body), convert_options=pacsv.ConvertOptions(column_types=types))
    inst = set(t.column("instrument_name").to_pylist())
    if inst != {symbol} and t.num_rows:
        raise ValueError(f"{path}: instruments {sorted(inst)} != {symbol}")
    side = t.column("side").to_numpy(zero_copy_only=False)
    if not set(np.unique(side)) <= {"buy", "sell"}:
        raise ValueError(f"{path}: unknown side values {sorted(set(np.unique(side)))}")
    px, q = t.column("price").to_numpy(), t.column("size").to_numpy()
    return {"id": t.column("trade_id").to_numpy(), "price": px, "qty": q, "quote_qty": px * q, "time": t.column("created_time").to_numpy(),
            "is_buyer_maker": side == "sell", "raw_row": np.arange(t.num_rows, dtype=np.int64),
            "rpi": t.column("source").to_numpy(zero_copy_only=False) == "1"}  # fmt: skip


def parse_bybit(path: Path, symbol: str, day: str) -> dict[str, Any]:
    types = {"timestamp": pa.string(), "symbol": pa.string(), "side": pa.string(), "size": pa.float64(), "price": pa.float64(),
             "trdMatchID": pa.string(), "foreignNotional": pa.float64(), "RPI": pa.string()}  # fmt: skip
    with gzip.open(path, "rb") as fh:
        t = pacsv.read_csv(fh, convert_options=pacsv.ConvertOptions(column_types=types, include_columns=list(types)))
    syms = set(t.column("symbol").to_pylist())
    if syms != {symbol} and t.num_rows:
        raise ValueError(f"{path}: symbols {sorted(syms)} != {symbol}")
    side = t.column("side").to_numpy(zero_copy_only=False)
    if not set(np.unique(side)) <= {"Buy", "Sell"}:
        raise ValueError(f"{path}: unknown side values {sorted(set(np.unique(side)))}")
    n = t.num_rows
    dn = (date.fromisoformat(day) - date(1970, 1, 1)).days
    return {"id": dn * 10**7 + np.arange(n, dtype=np.int64), "price": t.column("price").to_numpy(), "qty": t.column("size").to_numpy(),
            "quote_qty": t.column("foreignNotional").to_numpy(), "time": _ms_from_seconds_text(t.column("timestamp")),
            "is_buyer_maker": side == "Sell", "raw_row": np.arange(n, dtype=np.int64),
            "rpi": t.column("RPI").to_numpy(zero_copy_only=False) == "1", "match_id": t.column("trdMatchID")}  # fmt: skip


def parse(provider: str, path: Path, symbol: str, day: str) -> dict[str, Any]:
    if provider == "okx":
        return parse_okx(path, symbol)
    if provider == "bybit":
        return parse_bybit(path, symbol, day)
    raise ValueError(f"no parser for {provider!r}")
