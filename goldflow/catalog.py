"""Phase 3 driver: raw registry, parallel normalisation/validation, catalog metadata, determinism check,
monthly-vs-daily cross-check and reports.

Catalog tables live in the same transactional SQLite DB as the acquisition checkpoints.
  raw_files       every raw file on disk with role CANONICAL | CROSS_CHECK | REJECTED | SUPERSEDED
  partitions      normalised partitions: rows, file sha256, logical (content) digest, source raw files
  day_status      per dataset/day COMPLETE | PARTIAL | UNVERIFIED | MISSING with evidence JSON
  hour_status     per dataset/hour status for trades and aggTrades
  quarantine_counts, raw_day_stats, crosscheck, validation_runs
"""
from __future__ import annotations
import glob, hashlib, json, os, stat, sqlite3, time, uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import numpy as np

from . import schemas as S
from .config import Config
from .integrity import _batches
from .normalize import DATASETS, process_day
from .timeutil import DAY_MS, day_start_ms, from_ms, iso

CATALOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_files(raw_file_id INTEGER PRIMARY KEY, unit_id TEXT, dataset TEXT, granularity TEXT,
  period TEXT, path TEXT UNIQUE, bytes INTEGER, sha256 TEXT, provider_sha256 TEXT, role TEXT, note TEXT, registered_utc TEXT);
CREATE TABLE IF NOT EXISTS partitions(dataset TEXT, date TEXT, path TEXT, schema_version INTEGER, rows INTEGER,
  quarantined INTEGER, file_sha256 TEXT, logical_digest TEXT, raw_file_ids TEXT, code_version TEXT, run_id TEXT,
  written_utc TEXT, PRIMARY KEY(dataset, date));
CREATE TABLE IF NOT EXISTS day_status(dataset TEXT, date TEXT, status TEXT, evidence_json TEXT, run_id TEXT,
  PRIMARY KEY(dataset, date));
CREATE TABLE IF NOT EXISTS hour_status(dataset TEXT, hour_utc TEXT, status TEXT, detail_json TEXT, run_id TEXT,
  PRIMARY KEY(dataset, hour_utc));
CREATE TABLE IF NOT EXISTS quarantine_counts(dataset TEXT, date TEXT, reason_code TEXT, count INTEGER, run_id TEXT,
  PRIMARY KEY(dataset, date, reason_code));
CREATE TABLE IF NOT EXISTS raw_day_stats(dataset TEXT, date TEXT, rows INTEGER, id_sum INTEGER, lots_sum INTEGER,
  PRIMARY KEY(dataset, date));
CREATE TABLE IF NOT EXISTS crosscheck(raw_file_id INTEGER, dataset TEXT, date TEXT, monthly_rows INTEGER, daily_rows INTEGER,
  monthly_id_sum INTEGER, daily_id_sum INTEGER, monthly_lots INTEGER, daily_lots INTEGER, result TEXT, detail TEXT,
  PRIMARY KEY(raw_file_id, date));
CREATE TABLE IF NOT EXISTS day_status_pre(dataset TEXT, date TEXT, status TEXT, evidence_json TEXT, PRIMARY KEY(dataset, date));
CREATE TABLE IF NOT EXISTS hour_status_pre(dataset TEXT, hour_utc TEXT, status TEXT, detail_json TEXT, PRIMARY KEY(dataset, hour_utc));
CREATE TABLE IF NOT EXISTS day_boundary(date TEXT PRIMARY KEY, boundary_agg_json TEXT, boundary_trades_json TEXT);
CREATE TABLE IF NOT EXISTS day_runs(date TEXT PRIMARY KEY, code_version TEXT, config_sha TEXT, finished_utc TEXT);
CREATE TABLE IF NOT EXISTS validation_runs(run_id TEXT PRIMARY KEY, mode TEXT, started_utc TEXT, finished_utc TEXT,
  code_version TEXT, config_sha256 TEXT, result_digest TEXT, compared_to TEXT, identical INTEGER, summary_json TEXT);
"""

def _now() -> str: return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def code_version() -> str:
    h = hashlib.sha256()
    for name in ("normalize.py", "schemas.py", "integrity.py", "catalog.py"):
        p = Path(__file__).parent / name; h.update(name.encode()); h.update(p.read_bytes())
    return h.hexdigest()[:16]

class Catalog:
    def __init__(self, cfg: Config, db_path: Path | None = None):
        self.cfg = cfg
        from .fsutil import connect
        self.db = connect(db_path or cfg.state_db, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(CATALOG_SCHEMA)

    def tx(self, statements: list[tuple[str, tuple]]):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            for sql, args in statements: self.db.execute(sql, args)
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK"); raise

    # ------------------------------------------------------------------ raw registry
    def register_raw(self, make_readonly: bool = True, full: bool = True) -> dict:
        """Register every raw zip on disk; assign roles from acquisition units; flag anything unaccounted for."""
        cfg = self.cfg
        units = {r["local_path"]: r for r in self.db.execute(
            "SELECT * FROM units WHERE method='archive' AND local_path IS NOT NULL")}
        prefer = {ds for ds, o in cfg["datasets"].items() if o.get("prefer_daily")}
        stmts, summary = [], {"files": 0, "by_role": {}, "unaccounted": [], "hash_mismatch": []}
        raw_root = cfg.data_root / "raw" / cfg["source"]   # Phase 15: other venues' raw files belong to their own catalogs
        known_files = {r["path"]: r for r in self.db.execute("SELECT path, bytes, sha256 FROM raw_files")}
        for p in sorted(raw_root.rglob("*.zip")):
            sp = str(p)
            u = units.get(sp)
            known = known_files.get(sp)
            sha = known["sha256"] if (not full and known and known["bytes"] == p.stat().st_size) else _sha(p)
            if "superseded" in p.parts:
                role, uid, ds, gran, per, prov, note = "SUPERSEDED", None, p.parts[-4], p.parts[-3], None, None, "older upstream version"
            elif u is None:
                summary["unaccounted"].append(sp); continue
            else:
                uid, ds, gran, per = u["unit_id"], u["dataset"], u["granularity"], u["period"]
                prov = u["expected_sha256"]
                if u["status"] == "rejected": role, note = "REJECTED", u["error"]
                elif u["status"] != "verified": role, note = "UNVERIFIED", u["error"]
                elif gran == "monthly" and ds in prefer: role, note = "CROSS_CHECK", "daily files are canonical"
                else: role, note = "CANONICAL", None
                if prov and sha != prov: summary["hash_mismatch"].append(sp)
            if make_readonly:
                os.chmod(p, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)   # immutable raw archive
            stmts.append(("INSERT INTO raw_files(unit_id,dataset,granularity,period,path,bytes,sha256,provider_sha256,role,note,registered_utc)"
                          " VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET sha256=excluded.sha256, role=excluded.role,"
                          " note=excluded.note, bytes=excluded.bytes",
                          (uid, ds, gran, per, sp, p.stat().st_size, sha, prov, role, note, _now())))
            summary["files"] += 1; summary["by_role"][role] = summary["by_role"].get(role, 0) + 1
        self.tx(stmts)
        return summary

    def canonical_files(self) -> dict:
        out = {}
        for r in self.db.execute("SELECT * FROM raw_files WHERE role='CANONICAL' AND granularity='daily'"):
            out.setdefault(r["period"], {})[r["dataset"]] = {"raw_file_id": r["raw_file_id"], "path": r["path"], "sha256": r["sha256"]}
        return out

def _sha(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""): h.update(b)
    return h.hexdigest()

def spec_from_config(cfg: Config) -> dict:
    ins = cfg["instrument"]
    dec = lambda s: len(s.split(".")[1]) if "." in s else 0
    return {"inv_tick": 10 ** dec(ins["tick_size"]), "inv_step": 10 ** dec(ins["step_size"]),
            "onboard_ms": int(ins["onboard_ms"]), "tick_size": ins["tick_size"], "step_size": ins["step_size"]}

# ====================================================================== run
SCRATCH_TABLES = ["partitions", "quarantine_counts", "day_status_pre", "hour_status_pre", "day_boundary", "day_runs",
                  "raw_day_stats", "day_status", "hour_status"]

def _out_catalog(cfg: Config, write: bool) -> "Catalog":
    """normalize writes the main catalog; validate writes a scratch catalog (data files are never written)."""
    return Catalog(cfg) if write else Catalog(cfg, cfg.state_db.parent / "validate_scratch.sqlite")

def _range_days(cfg: Config) -> tuple[list[str], int, dict]:
    rng = json.loads((cfg.data_root / "state" / "range.json").read_text())
    first = from_ms(rng["start_ms"]).date(); last = from_ms(rng["end_ms"]).date() - timedelta(days=1)
    return [str(first + timedelta(days=i)) for i in range((last - first).days + 1)], rng["end_ms"], rng

def process_days(cfg: Config, write: bool, workers: int = 2, dates: list[str] | None = None,
                 budget_s: float | None = None, reset: bool = False) -> dict:
    """Validate/normalise pending days and commit each day as soon as it finishes (one transaction per day).

    Resumable: a day is pending until a day_runs row exists for the current code and config versions. With a
    time budget, no new day starts once the budget is spent (in-flight days still finish and commit)."""
    main = Catalog(cfg); out = _out_catalog(cfg, write)
    cv = code_version(); cfg_sha = hashlib.sha256(json.dumps(cfg.raw, sort_keys=True).encode()).hexdigest()
    reg = main.register_raw(make_readonly=True, full=False)
    if reg["unaccounted"] or reg["hash_mismatch"]:
        raise RuntimeError(f"raw archive problem: unaccounted={reg['unaccounted'][:5]} hash_mismatch={reg['hash_mismatch'][:5]}")
    if reset:
        out.tx([(f"DELETE FROM {t}", ()) for t in (SCRATCH_TABLES if not write else ["day_runs"])])
    all_days, acq_end, _ = _range_days(cfg)
    done = {r["date"] for r in out.db.execute("SELECT date FROM day_runs WHERE code_version=? AND config_sha=?", (cv, cfg_sha))}
    pending = [d for d in all_days if d not in done and (not dates or d in dates)]
    files = main.canonical_files()
    missing_reason = {}
    for r in main.db.execute("SELECT dataset, period, error FROM units WHERE status='missing' AND granularity='daily'"):
        missing_reason.setdefault(r["period"], {})[r["dataset"]] = r["error"]
    spec = spec_from_config(cfg)
    job = lambda d: {"date": d, "files": files.get(d, {}), "spec": spec, "symbol": cfg.symbol, "acq_end": acq_end,
                     "data_root": str(cfg.data_root), "write": write, "missing_reason": missing_reason.get(d, {})}
    t0 = time.time(); n_done = 0; queue = list(pending)
    with ProcessPoolExecutor(max_workers=workers) as ex:
        inflight = {}
        while queue or inflight:
            while queue and len(inflight) < workers and (budget_s is None or time.time() - t0 < budget_s):
                d = queue.pop(0); inflight[ex.submit(process_day, job(d))] = d
            if not inflight: break
            fu = next(as_completed(list(inflight)))
            d = inflight.pop(fu); r = fu.result()
            _persist_day(out, cfg, r, cv, cfg_sha, write)
            n_done += 1
            if n_done % 10 == 0: print(f"[{'normalize' if write else 'validate'}] {n_done} days committed ({time.time() - t0:.0f}s)", flush=True)
    remaining = [d for d in all_days if d not in {r["date"] for r in out.db.execute(
        "SELECT date FROM day_runs WHERE code_version=? AND config_sha=?", (cv, cfg_sha))}]
    return {"processed": n_done, "pending_remaining": len(remaining), "elapsed_s": round(time.time() - t0), "raw_registry": reg}

def _persist_day(out: "Catalog", cfg: Config, r: dict, cv: str, cfg_sha: str, write: bool):
    d = r["date"]; now = _now()
    st = [("DELETE FROM quarantine_counts WHERE date=?", (d,)), ("DELETE FROM partitions WHERE date=?", (d,)),
          ("DELETE FROM day_status_pre WHERE date=?", (d,)), ("DELETE FROM hour_status_pre WHERE substr(hour_utc,1,10)=?", (d,))]
    for p in r["partitions"]:
        st.append(("INSERT INTO partitions VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                   (p["dataset"], d, str(Path(p["path"]).relative_to(cfg.base_dir)), S.SCHEMA_VERSION, p["rows"], p["quarantined"],
                    p["file_sha256"], p["logical_digest"], json.dumps(p["raw_file_ids"]), cv, "process_days", now)))
        for k, v in sorted(p["quarantine_by_reason"].items()):
            st.append(("INSERT INTO quarantine_counts VALUES(?,?,?,?,?)", (p["dataset"], d, k, v, "process_days")))
    for s_ in r["day_status"]:
        st.append(("INSERT INTO day_status_pre VALUES(?,?,?,?)", (s_["dataset"], d, s_["status"], json.dumps(s_["evidence"]))))
    for h in r["hour_status"]:
        st.append(("INSERT INTO hour_status_pre VALUES(?,?,?,?)", (h["dataset"], h["hour_utc"], h["status"], json.dumps(h["detail"]))))
    for s_ in r["raw_stats"]:
        st.append(("INSERT OR REPLACE INTO raw_day_stats VALUES(?,?,?,?,?)", (s_["dataset"], d, s_["rows"], s_["id_sum"], s_["lots_sum"])))
    st.append(("INSERT OR REPLACE INTO day_boundary VALUES(?,?,?)", (d, json.dumps(r.get("boundary_agg", [])),
                                                                     json.dumps(r.get("boundary_trades") or {"ids": [], "lots": [], "ts": []}))))
    st.append(("INSERT OR REPLACE INTO day_runs VALUES(?,?,?,?)", (d, cv, cfg_sha, now)))
    out.tx(st)

def finalize(cfg: Config, write: bool) -> dict:
    """Cross-day step: midnight-straddle resolution, final statuses, result digest, run record.
    Refuses to run while any day of the range is pending."""
    main = Catalog(cfg); out = _out_catalog(cfg, write)
    cv = code_version(); cfg_sha = hashlib.sha256(json.dumps(cfg.raw, sort_keys=True).encode()).hexdigest()
    all_days, acq_end, _ = _range_days(cfg)
    done = {r["date"] for r in out.db.execute("SELECT date FROM day_runs WHERE code_version=? AND config_sha=?", (cv, cfg_sha))}
    missing = [d for d in all_days if d not in done]
    if missing: raise RuntimeError(f"{len(missing)} days still pending (e.g. {missing[:3]}); run process step again")
    run_id = time.strftime(("normalize" if write else "validate") + "-%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:6]
    results = []
    qc = {}
    for r in out.db.execute("SELECT dataset, date, reason_code, count FROM quarantine_counts"):
        qc.setdefault((r["dataset"], r["date"]), {})[r["reason_code"]] = r["count"]
    parts = {}
    for r in out.db.execute("SELECT dataset, date, rows, quarantined, logical_digest FROM partitions ORDER BY dataset"):
        parts.setdefault(r["date"], []).append({"dataset": r["dataset"], "rows": r["rows"], "quarantined": r["quarantined"],
                                                 "logical_digest": r["logical_digest"], "quarantine_by_reason": qc.get((r["dataset"], r["date"]), {})})
    for d in all_days:
        b = out.db.execute("SELECT * FROM day_boundary WHERE date=?", (d,)).fetchone()
        results.append({"date": d, "partitions": parts.get(d, []),
            "day_status": [{"dataset": x["dataset"], "status": x["status"], "evidence": json.loads(x["evidence_json"])}
                           for x in out.db.execute("SELECT * FROM day_status_pre WHERE date=? ORDER BY dataset", (d,))],
            "hour_status": [{"dataset": x["dataset"], "hour_utc": x["hour_utc"], "status": x["status"], "detail": json.loads(x["detail_json"])}
                            for x in out.db.execute("SELECT * FROM hour_status_pre WHERE substr(hour_utc,1,10)=? ORDER BY dataset, hour_utc", (d,))],
            "boundary_agg": json.loads(b["boundary_agg_json"]) if b else [],
            "boundary_trades": json.loads(b["boundary_trades_json"]) if b else None})
    boundary = resolve_midnight(results)
    tail = str(from_ms(acq_end).date())
    st = [("DELETE FROM day_status", ()), ("DELETE FROM hour_status", ())]
    for r in results:
        for s_ in r["day_status"]:
            st.append(("INSERT INTO day_status VALUES(?,?,?,?,?)", (s_["dataset"], r["date"], s_["status"], json.dumps(s_["evidence"]), run_id)))
        for h in r["hour_status"]:
            st.append(("INSERT INTO hour_status VALUES(?,?,?,?,?)", (h["dataset"], h["hour_utc"], h["status"], json.dumps(h["detail"]), run_id)))
    for ds in DATASETS:
        st.append(("INSERT INTO day_status VALUES(?,?,?,?,?)", (ds, tail, "MISSING", json.dumps(
            {"interval": [iso(day_start_ms(from_ms(acq_end).date())), iso(acq_end)],
             "reason": "day not complete at acquisition time; not yet archived on data.binance.vision"}), run_id)))
    out.tx(st)
    result_digest = _result_digest(results)
    prior = {(r["dataset"], r["date"]): r["logical_digest"] for r in main.db.execute("SELECT dataset,date,logical_digest FROM partitions")}
    diffs = [(p["dataset"], r["date"]) for r in results for p in r["partitions"]
             if (p["dataset"], r["date"]) in prior and prior[(p["dataset"], r["date"])] != p["logical_digest"]]
    prev_run = main.db.execute("SELECT run_id, result_digest FROM validation_runs WHERE finished_utc IS NOT NULL "
                               "ORDER BY started_utc DESC LIMIT 1").fetchone()
    summary = {"days": len(results), "partitions": sum(len(r["partitions"]) for r in results),
               "rows_by_dataset": _sum_by(results, "rows"), "quarantined_by_dataset": _sum_by(results, "quarantined"),
               "status_counts": _status_counts(results),
               "midnight_straddles": {"rows": len(boundary), "resolved": sum(b["resolved"] for b in boundary),
                                      "unresolved": [b for b in boundary if not b["resolved"]]},
               "digest_differences_vs_catalog": diffs}
    identical = int(prev_run["result_digest"] == result_digest) if prev_run else None
    main.tx([("INSERT INTO validation_runs VALUES(?,?,?,?,?,?,?,?,?,?)",
              (run_id, "normalize" if write else "validate", _now(), _now(), cv, cfg_sha, result_digest,
               prev_run["run_id"] if prev_run else None, identical, json.dumps(summary, default=str)))])
    summary.update({"run_id": run_id, "result_digest": result_digest, "compared_to": prev_run["run_id"] if prev_run else None,
                    "identical_to_previous_run": identical})
    return summary

def run_phase3(cfg: Config, write: bool, workers: int = 2, dates: list[str] | None = None,
               budget_s: float | None = None, reset: bool | None = None) -> dict:
    """One-shot (or budgeted) run. validate resets its scratch catalog unless resuming (reset=False)."""
    if reset is None: reset = not write
    pr = process_days(cfg, write, workers, dates, budget_s, reset)
    if pr["pending_remaining"]:
        return {**pr, "finalized": False}
    fin = finalize(cfg, write)
    return {**fin, "raw_registry": pr["raw_registry"], "finalized": True, "processed": pr["processed"], "elapsed_s": pr["elapsed_s"]}

def _sum_by(results, key):
    out = {}
    for r in results:
        for p in r["partitions"]: out[p["dataset"]] = out.get(p["dataset"], 0) + p[key]
    return out

def _status_counts(results):
    out = {}
    for r in results:
        for s in r["day_status"]:
            out.setdefault(s["dataset"], {}); out[s["dataset"]][s["status"]] = out[s["dataset"]].get(s["status"], 0) + 1
    return out

def _result_digest(results) -> str:
    """Digest of everything a validation run concludes: partition contents, quarantine counts, statuses."""
    h = hashlib.sha256()
    for r in results:
        for p in sorted(r["partitions"], key=lambda x: x["dataset"]):
            h.update(f"{r['date']}|{p['dataset']}|{p['rows']}|{p['logical_digest']}|{json.dumps(p['quarantine_by_reason'], sort_keys=True)}".encode())
        for s in sorted(r["day_status"], key=lambda x: x["dataset"]):
            h.update(f"{r['date']}|{s['dataset']}|{s['status']}|{json.dumps(s['evidence'], sort_keys=True)}".encode())
        for x in r["hour_status"]:
            h.update(f"{x['dataset']}|{x['hour_utc']}|{x['status']}|{json.dumps(x['detail'], sort_keys=True)}".encode())
    return h.hexdigest()

def resolve_midnight(results) -> list:
    """Resolve aggTrades whose fills straddle midnight against the neighbouring day's edge trades.

    Each day reports aggTrade rows not matched within the day whose id range extends past the day's trades
    ("boundary_agg") and trades outside every aggTrade range of the day at its id edges ("boundary_trades").
    A boundary row is RESOLVED when day_lots + lots of neighbour-day edge trades inside [f, l] == agg_lots.
    Unresolved rows mark the trades day PARTIAL (hour of the row); unclaimed edge trades mark aggTrades PARTIAL.
    """
    by_date = {r["date"]: r for r in results}
    log = []
    claimed = {r["date"]: set() for r in results}
    for r in results:
        d = date.fromisoformat(r["date"])
        nb = [str(d - timedelta(days=1)), str(d + timedelta(days=1))]
        for row in r.get("boundary_agg", []):
            lots = row["day_lots"]; hits = []
            for nd in nb:
                bt = by_date.get(nd, {}).get("boundary_trades")
                if not bt: continue
                for i, (tid, tl) in enumerate(zip(bt["ids"], bt["lots"])):
                    if row["f"] <= tid <= row["l"]:
                        lots += tl; hits.append((nd, tid))
            ok = lots == row["agg_lots"]
            if ok:
                for nd, tid in hits: claimed[nd].add(tid)
            log.append({"date": r["date"], "agg_id": row["agg_id"], "agg_lots": row["agg_lots"], "matched_lots": lots,
                        "neighbour_trades": len(hits), "resolved": ok})
            if not ok:
                _mark_partial(r, "trades", row["ts"], {"boundary_agg_unresolved": row["agg_id"],
                                                       "lots_missing": row["agg_lots"] - lots})
    for r in results:
        bt = r.get("boundary_trades") or {"ids": []}
        un = [(i, ts) for i, ts in zip(bt["ids"], bt.get("ts", [])) if i not in claimed[r["date"]]]
        for i, ts in un:
            _mark_partial(r, "aggTrades", ts, {"trade_absent_from_aggtrades": i})
        r.pop("boundary_agg", None); r.pop("boundary_trades", None)
    return log

def _mark_partial(r: dict, ds: str, ts: int, detail: dict):
    hour = iso((ts // 3_600_000) * 3_600_000)
    for s in r["day_status"]:
        if s["dataset"] == ds:
            s["status"] = "PARTIAL"; s["evidence"].setdefault("midnight_unresolved", []).append(detail)
    for h in r["hour_status"]:
        if h["dataset"] == ds and h["hour_utc"] == hour:
            h["status"] = "PARTIAL"; h["detail"].setdefault("midnight_unresolved", []).append(detail)

def check_cross_partition(cfg: Config) -> list[dict]:
    import pyarrow.parquet as pq
    cat = Catalog(cfg); out = []
    for ds, idcol in (("trades", "source_trade_id"), ("aggTrades", "aggregate_trade_id")):
        prev = None
        for r in cat.db.execute("SELECT date, path FROM partitions WHERE dataset=? ORDER BY date", (ds,)):
            ids = pq.read_table(cfg.base_dir / r["path"], columns=[idcol]).column(0).to_numpy()
            if not len(ids): continue
            if prev and ids.min() <= prev[1]:
                out.append({"dataset": ds, "date": r["date"], "prev_date": prev[0], "prev_max_id": int(prev[1]), "min_id": int(ids.min())})
            prev = (r["date"], ids.max())
    return out

# ====================================================================== monthly cross-check
def crosscheck_monthly(cfg: Config, budget_s: float | None = None) -> list[dict]:
    """Stream each CROSS_CHECK/REJECTED monthly file and compare per-day rows / id-sum / lots-sum to the daily raw files.
    Resumable: files already cross-checked are not re-read (their stored results are returned)."""
    cat = Catalog(cfg); spec = spec_from_config(cfg); out = []; t0 = time.time()
    daily = {(r["dataset"], r["date"]): r for r in cat.db.execute("SELECT * FROM raw_day_stats")}
    done = {r[0] for r in cat.db.execute("SELECT DISTINCT raw_file_id FROM crosscheck")}
    for f in cat.db.execute("SELECT * FROM raw_files WHERE granularity='monthly' AND role IN ('CROSS_CHECK','REJECTED')").fetchall():
        if f["raw_file_id"] not in done: continue
        for r in cat.db.execute("SELECT * FROM crosscheck WHERE raw_file_id=? ORDER BY date", (f["raw_file_id"],)):
            out.append({"file": Path(f["path"]).name, "role": f["role"], "date": r["date"], "result": r["result"], "detail": r["detail"]})
    for f in cat.db.execute("SELECT * FROM raw_files WHERE granularity='monthly' AND role IN ('CROSS_CHECK','REJECTED')").fetchall():
        if f["raw_file_id"] in done: continue
        if budget_s is not None and time.time() - t0 > budget_s:
            raise TimeoutError("crosscheck budget spent; rerun to continue")
        stmts = []
        ds = f["dataset"]
        idc, tsc, qc = {"trades": ("id", "time", "qty"), "aggTrades": ("agg_trade_id", "transact_time", "quantity"),
                        "klines_1m": (None, "open_time", "volume")}[ds]
        acc = {}
        for b in _batches(Path(f["path"]), ds):
            ts = b.column(tsc).to_numpy(); day = ts // DAY_MS
            ids = b.column(idc).to_numpy() if idc else np.zeros(len(ts), np.int64)
            lots = np.rint(b.column(qc).to_numpy() * spec["inv_step"]).astype(np.int64)
            for dd in np.unique(day):
                m = day == dd; a = acc.setdefault(int(dd), [0, 0, 0])
                a[0] += int(m.sum()); a[1] += int(ids[m].sum()); a[2] += int(lots[m].sum())
        for dd, (n, isum, lsum) in sorted(acc.items()):
            dstr = str(from_ms(dd * DAY_MS).date())
            ref = daily.get((ds, dstr))
            if ds == "klines_1m":
                ref_rows = cat.db.execute("SELECT rows FROM partitions WHERE dataset='klines_1m' AND date=?", (dstr,)).fetchone()
                dr = ref_rows["rows"] if ref_rows else None
                res = "MATCH" if dr == n else "DIFF"; row = (n, dr, None, None, lsum, None)
            elif ref is None:
                res, row = "NO_DAILY_REFERENCE", (n, None, isum, None, lsum, None)
            else:
                res = "MATCH" if (n, isum, lsum) == (ref["rows"], ref["id_sum"], ref["lots_sum"]) else "DIFF"
                row = (n, ref["rows"], isum, ref["id_sum"], lsum, ref["lots_sum"])
            detail = None
            if res == "DIFF" and ref is not None:
                detail = f"monthly has {n - ref['rows']:+d} rows, {lsum - ref['lots_sum']:+d} lots vs daily"
            out.append({"file": Path(f["path"]).name, "role": f["role"], "date": dstr, "result": res, "detail": detail})
            stmts.append(("INSERT OR REPLACE INTO crosscheck VALUES(?,?,?,?,?,?,?,?,?,?,?)", (f["raw_file_id"], ds, dstr, *row, res, detail)))
        cat.tx(stmts)   # one transaction per monthly file
    return out
