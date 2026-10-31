"""Phase 12: broker tick history (the execution venue). Import, validation, storage and reading.

Two input formats from MetaTrader 5, both in **broker server time** (converted to UTC with ``broker.server_tz``):
* ``mt5``: the terminal's tick export (Symbols -> Ticks -> Export): tab-separated ``<DATE> <TIME> <BID> <ASK> <LAST>
  <VOLUME> <FLAGS>`` with dates like ``2026.10.23`` and times like ``12:00:00.123``. An empty BID or ASK means
  "unchanged" and is carried forward from the previous tick.
* ``csv``: a script export (``CopyTicksRange``): header with at least ``time_msc,bid,ask``; ``time_msc`` is server time
  in epoch milliseconds, as MT5 reports it.

Validation (every rejected row is quarantined with a reason, never silently dropped): price an exact multiple of the
point, bid > 0, ask >= bid, time not going backwards after conversion, ambiguous or non-existent local times during
the server's DST changes. Stored as integer points per UTC day:
  data/normalized/provider=broker_<name>/market=otc_cfd/instrument=<symbol>/data_type=broker_ticks/date=D/part-000.parquet
plus ``.../broker_ticks/_manifest.json`` (source SHA-256, counts, quarantine by reason, per-day logical digests,
broker spec, dataset version, ``synthetic`` flag). Declared capability: ``bid_ask_quotes: AVAILABLE``.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections import OrderedDict
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from numpy.typing import NDArray

from ..normalize import logical_digest, write_atomic
from ..timeutil import DAY_MS
from .settings import BrokerSpec

I64 = np.int64
SCHEMA = pa.schema([("time_ms", pa.int64()), ("bid_points", pa.int64()), ("ask_points", pa.int64()), ("flags", pa.int64())],
                   metadata={"goldflow.schema": "BrokerTick", "goldflow.schema_version": "1",
                             "goldflow.doc": "UTC epoch ms; bid/ask in integer points (10^-digits of the quote currency)"})  # fmt: skip
CAPABILITIES = {"bid_ask_quotes": "AVAILABLE", "trades": "UNAVAILABLE", "time_basis": "broker server time converted to UTC"}


def broker_root(data_root: Path, spec: BrokerSpec) -> Path:
    safe = "".join(c if c.isalnum() else "_" for c in spec.name.lower())
    return (
        Path(data_root)
        / "normalized"
        / f"provider=broker_{safe}"
        / "market=otc_cfd"
        / f"instrument={spec.symbol}"
        / "data_type=broker_ticks"
    )


def _read(path: Path, fmt: str) -> pd.DataFrame:
    if fmt == "mt5":
        df = pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
        df.columns = [c.strip("<>").lower() for c in df.columns]
        local = pd.to_datetime(df["date"] + " " + df["time"], format="%Y.%m.%d %H:%M:%S.%f", errors="coerce")
        out = pd.DataFrame({"local": local, "bid": pd.to_numeric(df["bid"].replace("", np.nan)), "ask": pd.to_numeric(df["ask"].replace("", np.nan)),
                            "flags": pd.to_numeric(df.get("flags", pd.Series(["0"] * len(df))), errors="coerce").fillna(0)})  # fmt: skip
        out[["bid", "ask"]] = out[["bid", "ask"]].ffill()
        return out
    if fmt == "csv":
        df = pd.read_csv(path)
        need = {"time_msc", "bid", "ask"}
        if not need <= set(df.columns):
            raise ValueError(f"csv tick export needs columns {sorted(need)}; got {list(df.columns)}")
        return pd.DataFrame({"local": pd.to_datetime(df["time_msc"].astype("int64"), unit="ms"), "bid": df["bid"].astype(float),
                             "ask": df["ask"].astype(float), "flags": df.get("flags", pd.Series(np.zeros(len(df)))).fillna(0)})  # fmt: skip
    raise ValueError("format must be 'mt5' or 'csv'")


def import_ticks(data_root: Path, spec: BrokerSpec, path: Path, fmt: str, synthetic: bool = False) -> dict[str, Any]:
    """Validate and store a broker tick export. Re-importing replaces the days it covers."""
    raw = Path(path).read_bytes()
    df = _read(Path(path), fmt)
    n = len(df)
    reason = np.full(n, "", dtype=object)
    reason[df["local"].isna().to_numpy()] = "UNPARSEABLE_TIME"
    reason[(reason == "") & (df["bid"].isna() | df["ask"].isna()).to_numpy()] = "MISSING_PRICE"
    scale = 10**spec.digits
    bidp, askp = (df["bid"].to_numpy(dtype=float) * scale), (df["ask"].to_numpy(dtype=float) * scale)
    bi, ai = np.rint(np.nan_to_num(bidp)).astype(I64), np.rint(np.nan_to_num(askp)).astype(I64)
    off_grid = (np.abs(np.nan_to_num(bidp) - bi) > 1e-6) | (np.abs(np.nan_to_num(askp) - ai) > 1e-6)
    reason[(reason == "") & off_grid] = "PRICE_NOT_ON_POINT"
    reason[(reason == "") & (bi <= 0)] = "NON_POSITIVE_BID"
    reason[(reason == "") & (ai < bi)] = "CROSSED_QUOTE"
    ok = reason == ""
    utc = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")
    if ok.any():
        loc = df.loc[ok, "local"]
        try:
            conv = loc.dt.tz_localize(spec.server_tz, ambiguous="infer", nonexistent="NaT")
        except Exception:  # ambiguous times that cannot be inferred from the order: flag them instead of guessing
            conv = loc.dt.tz_localize(spec.server_tz, ambiguous="NaT", nonexistent="NaT")
        utc.loc[ok] = conv.dt.tz_convert("UTC")
    bad_tz = ok & utc.isna().to_numpy()
    reason[bad_tz] = "AMBIGUOUS_OR_NONEXISTENT_LOCAL_TIME"
    ok = reason == ""
    t = np.zeros(n, I64)
    t[ok] = (utc[ok].astype("int64") // 1_000_000).to_numpy()
    # time must not go backwards (after conversion); a backwards tick is quarantined, the order is never "repaired"
    run_max = np.maximum.accumulate(np.where(ok, t, np.iinfo(I64).min))
    back = ok & (t < np.concatenate([[np.iinfo(I64).min], run_max[:-1]]))
    reason[back] = "TIME_BACKWARDS"
    ok = reason == ""
    root = broker_root(data_root, spec)
    days: dict[str, Any] = {}
    flags = df["flags"].to_numpy(dtype=float).astype(I64)
    for dn in np.unique(t[ok] // DAY_MS):
        m = ok & (t // DAY_MS == dn)
        d = str(date.fromordinal(date(1970, 1, 1).toordinal() + int(dn)))
        tbl = pa.Table.from_arrays([pa.array(t[m]), pa.array(bi[m]), pa.array(ai[m]), pa.array(flags[m])], schema=SCHEMA)
        sha = write_atomic(tbl, root / f"date={d}" / "part-000.parquet")
        days[d] = {
            "rows": int(m.sum()),
            "logical_digest": logical_digest(tbl),
            "sha256": sha,
            "first_ms": int(t[m][0]),
            "last_ms": int(t[m][-1]),
        }
    q = {r: int((reason == r).sum()) for r in sorted(set(reason.tolist()) - {""})}
    mp = root / "_manifest.json"
    old = json.loads(mp.read_text()) if mp.exists() else {}
    if old and old.get("spec") != spec.as_dict():
        old = {}  # a different broker spec: start a new manifest
    all_days = {**old.get("days", {}), **days}
    man = {"schema": "BrokerTick", "provider": f"broker:{spec.name}", "spec": spec.as_dict(), "capabilities": CAPABILITIES,
           "synthetic": bool(synthetic or old.get("synthetic", False)),
           "imports": old.get("imports", []) + [{"file": Path(path).name, "sha256": hashlib.sha256(raw).hexdigest(), "format": fmt, "rows": n,
                                                  "stored": int(ok.sum()), "quarantined": q}],
           "days": dict(sorted(all_days.items()))}  # fmt: skip
    man["dataset_version"] = hashlib.sha256(
        json.dumps({d: v["logical_digest"] for d, v in man["days"].items()}, sort_keys=True).encode()
    ).hexdigest()
    mp.parent.mkdir(parents=True, exist_ok=True)
    tmp = mp.with_name(mp.name + ".tmp")
    tmp.write_text(json.dumps(man, indent=1))
    os.replace(tmp, mp)
    if q:
        bad = pd.DataFrame({"row": np.flatnonzero(reason != ""), "reason": reason[reason != ""]})
        bad.to_csv(root / f"_quarantine_{hashlib.sha256(raw).hexdigest()[:12]}.csv", index=False)
    return {"rows": n, "stored": int(ok.sum()), "quarantined": q, "days": len(days), "dataset_version": man["dataset_version"],
            "synthetic": man["synthetic"], "root": str(root)}  # fmt: skip


class BrokerTicks:
    """Broker ticks in time order, read lazily per UTC day."""

    def __init__(self, data_root: Path, spec: BrokerSpec) -> None:
        self.root = broker_root(data_root, spec)
        mp = self.root / "_manifest.json"
        if not mp.exists():
            raise FileNotFoundError(f"no broker tick dataset at {self.root}; run `goldflow broker-import --file <export>` first")
        self.manifest = json.loads(mp.read_text())
        if self.manifest.get("spec") != spec.as_dict():
            raise ValueError("the broker tick dataset was imported with a different broker spec; re-import it")
        self.cache: OrderedDict[int, dict[str, NDArray[Any]]] = OrderedDict()

    @property
    def dataset_version(self) -> str:
        return str(self.manifest["dataset_version"])

    @property
    def synthetic(self) -> bool:
        return bool(self.manifest.get("synthetic"))

    def day(self, dn: int) -> dict[str, NDArray[Any]]:
        if dn not in self.cache:
            d = str(date.fromordinal(date(1970, 1, 1).toordinal() + dn))
            p = self.root / f"date={d}" / "part-000.parquet"
            if p.exists():
                t = pq.read_table(p)
                self.cache[dn] = {
                    k: t.column(c).to_numpy() for k, c in (("t", "time_ms"), ("bid", "bid_points"), ("ask", "ask_points"))
                }
            else:
                self.cache[dn] = {k: np.zeros(0, I64) for k in ("t", "bid", "ask")}
            while len(self.cache) > 3:
                self.cache.popitem(last=False)
        return self.cache[dn]

    def between(self, a: int, b: int) -> dict[str, NDArray[Any]]:
        """Ticks with a <= time < b."""
        if b <= a:
            return {k: np.zeros(0, I64) for k in ("t", "bid", "ask")}
        parts = []
        for dn in range(a // DAY_MS, (b - 1) // DAY_MS + 1):
            x = self.day(dn)
            i, j = int(np.searchsorted(x["t"], a, "left")), int(np.searchsorted(x["t"], b, "left"))
            parts.append({k: v[i:j] for k, v in x.items()})
        return {k: np.concatenate([p[k] for p in parts]) for k in ("t", "bid", "ask")}
