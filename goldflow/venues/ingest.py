"""Phase 15: import another venue's archived trades into its own normalised partitions and catalog.

1. The archive files that cover each requested UTC day are fetched; raw files are kept (``archives.get_raw``).
   OKX archive days are UTC+8, so UTC day D needs OKX files D and D+1.
2. Each file is validated by the **Phase 3 trade validator** over its own time interval: tick and step multiples,
   positive values, quote notional, duplicates and sequence outliers. Rejected rows are counted by reason.
3. Accepted rows become ``TradeEvent`` v1 rows with the venue's identity (``provider_id``, ``venue_id``,
   ``market_type``, ``instrument_id``). They are written per UTC day to
   ``normalized/provider=<venue>/market=<type>/instrument=<symbol>/data_type=trades/date=D/part-000.parquet``.
4. **Completeness** is recorded in the venue's own catalog (``data/state/venues/<venue>.sqlite``), in the Phase 3
   format, so the Phase 4 replay turns it into quality events:
   - **OKX:** trade ids are contiguous. A day is COMPLETE when the accepted ids are contiguous from the last trade
     before the day to the first trade after it. A missing id makes the hours it spans PARTIAL. A day whose next
     archive file is not published yet is UNVERIFIED.
   - **Bybit:** ids are UUIDs and no reference counts are reachable, so completeness cannot be proven. Every Bybit day
     is **UNVERIFIED**; signals on it are gated.
   - A day with no archive file is MISSING.
"""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from .. import schemas as S
from ..normalize import build_table, const_cols, logical_digest, part_path, validate_trades, write_atomic
from ..timeutil import DAY_MS, iso
from .archives import Fetch, file_interval, get_raw, http_fetch, parse
from .mapping import InstrumentMapping, mappings, venue_config

CATALOG = """
CREATE TABLE IF NOT EXISTS partitions(dataset TEXT, date TEXT, logical_digest TEXT, file_sha256 TEXT, rows INTEGER, PRIMARY KEY(dataset, date));
CREATE TABLE IF NOT EXISTS hour_status(dataset TEXT, hour_utc TEXT, status TEXT, detail_json TEXT, PRIMARY KEY(dataset, hour_utc));
CREATE TABLE IF NOT EXISTS day_status(dataset TEXT, date TEXT, status TEXT, evidence_json TEXT, PRIMARY KEY(dataset, date));
CREATE TABLE IF NOT EXISTS raw_files(id INTEGER PRIMARY KEY, archive_day TEXT UNIQUE, path TEXT, sha256 TEXT, size INTEGER);
CREATE TABLE IF NOT EXISTS venue_days(date TEXT PRIMARY KEY, trades INTEGER, rejected INTEGER, rejected_by_reason TEXT, lots INTEGER,
  rpi_trades INTEGER, rpi_lots INTEGER, first_ms INTEGER, last_ms INTEGER, status TEXT, imported_utc TEXT);
"""


def catalog(cfg: Any) -> sqlite3.Connection:
    p = Path(cfg.state_db)
    p.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(p))
    db.executescript(CATALOG)
    return db


def _d(day: str, k: int) -> str:
    return str(date.fromisoformat(day) + timedelta(days=k))


def _load_file(cfg: Any, m: InstrumentMapping, day: str, fetch: Fetch, db: sqlite3.Connection) -> dict[str, Any] | None:
    path = get_raw(cfg.data_root, m.provider, m.market_type, m.symbol, day, fetch)
    if path is None:
        return None
    meta = json.loads(path.with_name(path.name + ".meta.json").read_text())
    db.execute("INSERT OR IGNORE INTO raw_files(archive_day, path, sha256, size) VALUES(?,?,?,?)",
               (day, str(path.relative_to(cfg.data_root)), meta["sha256"], meta["size"]))  # fmt: skip
    fid = int(db.execute("SELECT id FROM raw_files WHERE archive_day=?", (day,)).fetchone()[0])
    raw = parse(m.provider, path, m.symbol, day)
    a, b = file_interval(m.provider, day, m.archive_day_utc_offset_h)
    spec = {"onboard_ms": int(m.onboard_ms or 0), "inv_tick": int(1 / m.tick), "inv_step": int(1 / m.step)}
    rej, der = validate_trades(raw, a, b, spec, int(time.time() * 1000), None)
    reasons: dict[str, int] = {}
    for c in rej.code[~rej.ok]:
        reasons[str(c)] = reasons.get(str(c), 0) + 1
    ok = rej.ok
    out = {k: np.asarray(raw[k])[ok] for k in ("id", "price", "qty", "quote_qty", "time", "is_buyer_maker", "raw_row", "rpi")}
    out["price_ticks"], out["quantity_lots"] = der["price_ticks"][ok], der["quantity_lots"][ok]
    out["raw_file_id"] = np.full(int(ok.sum()), fid, np.int32)
    dup_match = 0
    if "match_id" in raw:
        mid = raw["match_id"]
        dup_match = int(len(mid) - len(pc.unique(mid)))
    return {"cols": out, "rejected": reasons, "interval": (a, b), "match_id_duplicates": dup_match}


def _table(m: InstrumentMapping, c: dict[str, Any]) -> pa.Table:
    n = len(c["id"])
    bm = c["is_buyer_maker"]
    cols = {**const_cols(n, m.symbol, m.provider), "source_trade_id": c["id"], "aggregate_trade_id": pa.nulls(n, pa.int64()),
            "exchange_event_time_utc": c["time"], "transaction_time_utc": c["time"], "receive_time_utc": pa.nulls(n, S.TS),
            "price": c["price"], "quantity": c["qty"], "quote_notional": c["quote_qty"], "buyer_maker_flag": bm,
            "aggressor_side": np.where(bm, "SELL", "BUY"), "side_evidence": np.full(n, "EXCHANGE_REPORTED"),
            "price_ticks": c["price_ticks"], "quantity_lots": c["quantity_lots"], "record_origin": np.full(n, "ARCHIVE_TRADES"),
            "data_quality_status": np.full(n, "VALID"), "raw_file_id": c["raw_file_id"], "raw_row": c["raw_row"]}  # fmt: skip
    return build_table(S.TRADE_SCHEMA, cols)


def _holes(c: dict[str, Any]) -> list[tuple[int, int, int]]:
    """(time before, time after, missing ids) for every break in contiguous ids."""
    o = np.argsort(c["id"], kind="stable")
    ids, ts = c["id"][o], c["time"][o]
    br = np.flatnonzero(np.diff(ids) > 1)
    return [(int(ts[i]), int(ts[i + 1]), int(ids[i + 1] - ids[i] - 1)) for i in br]


def venue_import(cfg: Any, provider: str, days: list[str], fetch: Fetch = http_fetch) -> dict[str, Any]:
    m = mappings(cfg)[provider]
    vcfg = venue_config(cfg, provider)
    db = catalog(vcfg)
    off = m.archive_day_utc_offset_h
    need = sorted({_d(d, k) for d in days for k in ((0, 1) if off > 0 else (0,))})
    files = {a: _load_file(vcfg, m, a, fetch, db) for a in need}
    loaded = [f for f in files.values() if f is not None]
    allc = {k: np.concatenate([f["cols"][k] for f in loaded]) for k in loaded[0]["cols"]} if loaded else {}
    if allc:
        o = np.lexsort((allc["id"], allc["time"]))
        allc = {k: v[o] for k, v in allc.items()}
    holes = _holes(allc) if allc and provider == "okx" else []
    out: dict[str, Any] = {"provider": provider, "symbol": m.symbol, "days": {}}
    for d in days:
        d0 = int(np.datetime64(d, "ms").astype(np.int64))
        d1 = d0 + DAY_MS
        mine = [a for a in need if (f := files.get(a)) is not None and f["interval"][0] < d1 and f["interval"][1] > d0]
        db.execute("DELETE FROM hour_status WHERE dataset='trades' AND hour_utc LIKE ?", (f"{d}T%",))
        rejected: dict[str, int] = {}
        for a in mine:
            for k, v in (files[a] or {})["rejected"].items():
                rejected[k] = rejected.get(k, 0) + v
        sel = (allc["time"] >= d0) & (allc["time"] < d1) if allc else np.zeros(0, bool)
        c = {k: v[sel] for k, v in allc.items()} if allc else {}
        n = int(sel.sum())
        ev: dict[str, Any] = {"archive_files": mine, "rejected": rejected}
        if not mine:
            status, ev["interval"] = "MISSING", [iso(d0), iso(d1)]
        elif provider == "okx":
            covers = all(files.get(a) is not None for a in (_d(d, 0), _d(d, 1)))
            if not covers:
                status, ev["reason"] = (
                    "UNVERIFIED",
                    f"archive day {_d(d, 1)} (UTC+8) not published: the end of the day is not covered",
                )
            else:
                inside = [h for h in holes if h[1] > d0 and h[0] < d1]
                status = "PARTIAL" if inside else "COMPLETE"
                ev["id_breaks"] = [[iso(t0), iso(t1), nmiss] for t0, t1, nmiss in inside]
                for t0, t1, nmiss in inside:
                    for hs in range(max(t0, d0) // 3_600_000 * 3_600_000, min(t1, d1 - 1) + 1, 3_600_000):
                        db.execute("INSERT OR REPLACE INTO hour_status VALUES('trades', ?, 'PARTIAL', ?)",
                                   (str(iso(hs)).replace(".000Z", "Z"), json.dumps({"missing_ids": nmiss, "between": [iso(t0), iso(t1)]})))  # fmt: skip
        else:
            status = "UNVERIFIED"
            ev["reason"] = "no trade sequence (UUID ids) and no reference counts reachable: completeness cannot be proven"
            ev["match_id_duplicates"] = sum((files[a] or {})["match_id_duplicates"] for a in mine)
        digest = None
        if n:
            t = _table(m, c)
            sha = write_atomic(t, part_path(vcfg.data_root, m.symbol, "trades", d, provider=provider))
            digest = logical_digest(t)
            db.execute("INSERT OR REPLACE INTO partitions VALUES('trades', ?, ?, ?, ?)", (d, digest, sha, n))
        else:
            db.execute("DELETE FROM partitions WHERE dataset='trades' AND date=?", (d,))
        db.execute("INSERT OR REPLACE INTO day_status VALUES('trades', ?, ?, ?)", (d, status, json.dumps(ev)))
        rpi = c["rpi"] if n else np.zeros(0, bool)
        lots = c["quantity_lots"] if n else np.zeros(0, np.int64)
        row = (d, n, sum(rejected.values()), json.dumps(rejected), int(lots.sum()), int(rpi.sum()), int(lots[rpi].sum()),
               int(c["time"].min()) if n else None, int(c["time"].max()) if n else None, status, iso(int(time.time() * 1000)))  # fmt: skip
        db.execute("INSERT OR REPLACE INTO venue_days VALUES(?,?,?,?,?,?,?,?,?,?,?)", row)
        out["days"][d] = {"status": status, "trades": n, "rejected": sum(rejected.values()), "lots": int(lots.sum()),
                          "oz": float(int(lots.sum()) * m.oz_per_lot), "rpi_trades": int(rpi.sum()),
                          "rpi_share_of_lots": float(lots[rpi].sum() / lots.sum()) if n else None, "logical_digest": digest,
                          **({"id_breaks": ev["id_breaks"]} if ev.get("id_breaks") else {})}  # fmt: skip
    db.commit()
    db.close()
    return out


def venue_days(cfg: Any, provider: str) -> list[dict[str, Any]]:
    """Catalog rows of the days imported for a venue (Binance: the Phase 3 catalog)."""
    if provider == "binance":
        p = Path(cfg.state_db)
        if not p.exists():
            return []
        db = sqlite3.connect(str(p))
        db.row_factory = sqlite3.Row
        try:
            rows = [dict(r) for r in db.execute("SELECT date, status FROM day_status WHERE dataset='trades' ORDER BY date")]
            parts = {r[0]: r[1] for r in db.execute("SELECT date, logical_digest FROM partitions WHERE dataset='trades'")}
        except sqlite3.OperationalError:
            return []
        finally:
            db.close()
        return [{**r, "logical_digest": parts.get(r["date"])} for r in rows]
    vcfg = venue_config(cfg, provider)
    if not Path(vcfg.state_db).exists():
        return []
    db = catalog(vcfg)
    db.row_factory = sqlite3.Row
    rows = [dict(r) for r in db.execute("SELECT * FROM venue_days ORDER BY date")]
    db.close()
    return rows
