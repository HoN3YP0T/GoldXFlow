"""Acquisition orchestration: plan -> download -> verify -> record, with resume.

Planning rule per dataset (archive availability is *listed*, never assumed):
  * whole calendar months inside the range with a published monthly file -> one monthly unit
  * otherwise one daily unit per day (falls back automatically if a monthly file is absent)
  * days with no published file -> a `missing` unit with an explicit reason, plus a REST
    unit when the dataset supports REST recovery (aggTrades/trades)
Raw files are kept byte-for-byte as published (zip + .CHECKSUM); no normalisation here.
"""
from __future__ import annotations
import gzip, json, logging, os, shutil, threading, time, uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import timedelta
from pathlib import Path

from .checkpoint import Checkpoint
from .fsutil import safe_remove
from .config import Config
from .integrity import verify_file, write_id_gaps
from .retry import NonRetryable
from .rest_client import BinanceFuturesREST, GeoBlocked
from .timeutil import DAY_MS, day_start_ms, from_ms, iso, month_bounds_ms
from .vision_archive import VisionArchive, archive_key, RemoteFile

log = logging.getLogger("goldflow.acquire")

@dataclass
class Unit:
    unit_id: str
    method: str          # archive | rest | missing
    dataset: str
    granularity: str     # monthly | daily | rest
    period: str
    start_ms: int
    end_ms: int
    key: str | None = None
    remote: dict | None = None
    reason: str | None = None

# ------------------------------------------------------------------ planning
def make_archive(cfg: Config) -> VisionArchive:
    a = cfg["archive"]
    return VisionArchive(a["base_url"], a["list_url"], a["timeout_s"], a["max_attempts"],
                         a["backoff_base_s"], a["backoff_cap_s"], a["chunk_bytes"])

def make_rest(cfg: Config) -> BinanceFuturesREST:
    r = cfg["rest"]
    return BinanceFuturesREST(r["base_url"], r["weight_limit_per_min"], r["weight_budget_fraction"],
                              r["max_attempts"], r["backoff_base_s"], r["backoff_cap_s"], r["timeout_s"], r["api_key_env"])

def list_dataset(arch: VisionArchive, cfg: Config, dataset: str, granularity: str) -> dict[str, RemoteFile]:
    k = archive_key(cfg.market, granularity, dataset, cfg.symbol, "X")
    return arch.list_prefix(k.rsplit("/", 1)[0] + "/")

def plan(cfg: Config, arch: VisionArchive, start_ms: int, end_ms: int) -> list[Unit]:
    units: list[Unit] = []
    d0, d1 = from_ms(start_ms).date(), from_ms(end_ms - 1).date()
    for ds, opt in cfg["datasets"].items():
        daily = list_dataset(arch, cfg, ds, "daily") if opt.get("daily") else {}
        monthly = list_dataset(arch, cfg, ds, "monthly") if opt.get("monthly") else {}
        d = d0
        while d <= d1:
            ms, me = month_bounds_ms(d.year, d.month)
            mkey = archive_key(cfg.market, "monthly", ds, cfg.symbol, f"{d.year}-{d.month:02d}")
            if d.day == 1 and ms >= day_start_ms(d0) and me <= day_start_ms(d1) and mkey in monthly:
                rf = monthly[mkey]
                units.append(Unit(f"archive:{ds}:monthly:{d:%Y-%m}", "archive", ds, "monthly", f"{d:%Y-%m}", ms, me,
                                  mkey, asdict(rf)))
                if not opt.get("prefer_daily"):
                    d = from_ms(me).date(); continue
                # prefer_daily: the monthly file is kept as an independent cross-check; days are planned below
            ds_ms = day_start_ms(d); de_ms = ds_ms + DAY_MS
            dkey = archive_key(cfg.market, "daily", ds, cfg.symbol, f"{d:%Y-%m-%d}")
            if dkey in daily:
                units.append(Unit(f"archive:{ds}:daily:{d}", "archive", ds, "daily", str(d), ds_ms, de_ms, dkey,
                                  asdict(daily[dkey])))
            else:
                partial = de_ms > end_ms
                why = ("day not complete at acquisition time; Binance publishes daily files after the UTC day closes"
                       if partial else "file not published on data.binance.vision (listing has no key; HTTP 404)")
                s, e = max(ds_ms, start_ms), min(de_ms, end_ms)
                units.append(Unit(f"missing:{ds}:daily:{d}", "missing", ds, "daily", str(d), s, e, dkey, None, why))
                if opt.get("rest_tail"):
                    units.append(Unit(f"rest:{ds}:{d}", "rest", ds, "rest", str(d), s, e, None, None,
                                      "REST recovery of interval absent from archive"))
            d += timedelta(days=1)
    return units

# ------------------------------------------------------------------ progress
class Progress:
    def __init__(self, path: Path, total_units: int, total_bytes: int):
        self.path, self.total_units, self.total_bytes = path, total_units, total_bytes
        self.done = self.skipped = self.failed = 0; self.bytes = 0; self.t0 = time.time()
        self.lock = threading.Lock(); self._stop = threading.Event()
        self.th = threading.Thread(target=self._loop, daemon=True)

    def add_bytes(self, n: int):
        with self.lock: self.bytes += n
    def unit_done(self, kind: str):
        with self.lock: setattr(self, kind, getattr(self, kind) + 1)
    def snapshot(self) -> dict:
        with self.lock:
            el = time.time() - self.t0; rate = self.bytes / el if el else 0
            rem = max(0, self.total_bytes - self.bytes)
            return {"utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "units_total": self.total_units,
                    "units_verified": self.done, "units_skipped_already_verified": self.skipped, "units_failed": self.failed,
                    "bytes_downloaded": self.bytes, "bytes_planned_to_download": self.total_bytes,
                    "rate_MBps": round(rate / 1e6, 2), "eta_s": round(rem / rate) if rate else None, "elapsed_s": round(el)}
    def _loop(self):
        while not self._stop.wait(10):
            s = self.snapshot(); self.path.write_text(json.dumps(s, indent=1))
            log.info("progress %d/%d units (skip %d, fail %d) %.0f/%.0f MB @ %.1f MB/s ETA %ss",
                     s["units_verified"] + s["units_skipped_already_verified"], s["units_total"],
                     s["units_skipped_already_verified"], s["units_failed"], s["bytes_downloaded"] / 1e6,
                     s["bytes_planned_to_download"] / 1e6, s["rate_MBps"], s["eta_s"])
    def start(self): self.th.start()
    def stop(self):
        self._stop.set(); self.path.write_text(json.dumps(self.snapshot(), indent=1))

# ------------------------------------------------------------------ archive units
def _local_path(cfg: Config, u: Unit) -> Path:
    return cfg.raw_dir(u.dataset, u.granularity) / u.key.rsplit("/", 1)[-1]

def _already_verified(ck: Checkpoint, cfg: Config, u: Unit) -> bool:
    row = ck.get_unit(u.unit_id)
    # 'rejected' = verified-bad monthly file already replaced by daily files; only redo if upstream republishes it
    if not row or row["status"] not in ("verified", "rejected"): return False
    p = _local_path(cfg, u)
    if not p.exists() or p.stat().st_size != row["bytes"]: return False
    return row["remote_etag"] == u.remote["etag"]  # republished upstream -> must re-download

def run_archive_unit(cfg: Config, ck: Checkpoint, arch: VisionArchive, u: Unit, run_id: str, prog: Progress) -> str:
    dest = _local_path(cfg, u)
    row = ck.get_unit(u.unit_id)
    if row and row["remote_etag"] and row["remote_etag"] != u.remote["etag"] and dest.exists():
        sup = dest.parent / "superseded"; sup.mkdir(exist_ok=True)
        tag = (row["remote_last_modified"] or "old").replace(":", "")
        shutil.move(str(dest), str(sup / f"{dest.stem}.{tag}{dest.suffix}"))
        ck.event(run_id, "WARN", "upstream file republished; previous version kept in superseded/", u.unit_id,
                 {"old_etag": row["remote_etag"], "new_etag": u.remote["etag"], "old_last_modified": row["remote_last_modified"],
                  "new_last_modified": u.remote["last_modified"]})
    ck.upsert_unit(u.unit_id, source=cfg["source"], method="archive", symbol=cfg.symbol, market=cfg.market,
                   dataset=u.dataset, granularity=u.granularity, period=u.period,
                   url=f"{cfg['archive']['base_url']}/{u.key}", local_path=str(dest),
                   interval_start_ms=u.start_ms, interval_end_ms=u.end_ms, status="downloading",
                   remote_etag=u.remote["etag"], remote_last_modified=u.remote["last_modified"],
                   remote_size=u.remote["size"], run_id=run_id, error=None)
    max_ck = 3
    for attempt in range(max_ck):
        ck.bump_attempts(u.unit_id)
        try:
            expected = arch.fetch_checksum(u.key)
            nbytes, sha, status = arch.download(u.key, dest, u.remote["size"], prog.add_bytes)
        except NonRetryable as e:
            ck.upsert_unit(u.unit_id, status="missing", http_status=e.status, error=str(e))
            return "failed"
        except Exception as e:
            ck.upsert_unit(u.unit_id, status="failed", error=repr(e)); return "failed"
        Path(str(dest) + ".CHECKSUM").write_text(f"{expected}  {dest.name}\n")
        if sha == expected: break
        ck.event(run_id, "WARN", f"sha256 mismatch attempt {attempt + 1}; re-downloading", u.unit_id, {"got": sha, "expected": expected})
        safe_remove(dest)
    else:
        ck.upsert_unit(u.unit_id, status="failed", error="sha256 mismatch after retries", sha256=sha, expected_sha256=expected)
        return "failed"
    ck.upsert_unit(u.unit_id, status="downloaded", http_status=status, bytes=nbytes, sha256=sha, expected_sha256=expected)
    # verification (streamed)
    side = dest.parent / "_verify"; side.mkdir(exist_ok=True)
    minute_out = side / f"{dest.stem}.minute.npz" if u.dataset == "trades" else None
    r = verify_file(dest, u.dataset, u.start_ms, u.end_ms, minute_out)
    if r.id_gaps: write_id_gaps(side / f"{dest.stem}.idgaps.jsonl.gz", r.id_gaps)
    ck.clear_gaps(u.unit_id)
    _record_file_gaps(ck, run_id, u, r)
    ck.upsert_unit(u.unit_id, status="verified" if r.ok else "failed", records=r.records, first_id=r.first_id,
                   last_id=r.last_id, first_ts=r.first_ts, last_ts=r.last_ts, id_gap_count=r.id_gap_count,
                   id_gap_ids=r.id_gap_ids, dup_ids=r.dup_ids, ts_backwards=r.ts_backwards,
                   verify_json=json.dumps(r.summary()), error=None if r.ok else "; ".join(r.problems),
                   completed_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    return "done" if r.ok else "failed"

def _record_file_gaps(ck: Checkpoint, run_id: str, u: Unit, r) -> None:
    rows = []
    for g in r.id_gaps:
        rows.append((run_id, u.dataset, "id_gap_in_file", g[2], g[3], g[0], g[1], g[1] - g[0] + 1, u.unit_id,
                     "id(s) absent from Binance's published file (never published upstream)", "no: not in any public Binance source", None))
    for a, b in r.time_gaps:
        rows.append((run_id, u.dataset, "time_gap_in_file", a, b, None, None, b - a, u.unit_id,
                     "no records in this stretch of the published file", "unknown: verify against venue status", None))
    if rows:
        with ck._lock:
            ck.db.execute("BEGIN IMMEDIATE")
            ck.db.executemany("INSERT INTO gaps(run_id,dataset,kind,start_ms,end_ms,start_id,end_id,size,unit_id,reason,recoverable,detail_json)"
                              " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", rows)
            ck.db.execute("COMMIT")

# ------------------------------------------------------------------ REST units
def run_rest_unit(cfg: Config, ck: Checkpoint, rest: BinanceFuturesREST, u: Unit, run_id: str) -> str:
    """Fetch aggTrades for [start,end) via REST, preserving raw JSON pages (gzip JSONL), resumable by agg id."""
    out_dir = cfg.raw_dir(u.dataset, "rest"); out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{cfg.symbol}-{u.dataset}-{u.period}-{u.start_ms}-{u.end_ms}.jsonl.gz"
    ck.upsert_unit(u.unit_id, source=cfg["source"], method="rest", symbol=cfg.symbol, market=cfg.market,
                   dataset=u.dataset, granularity="rest", period=u.period, url=rest.base + "/fapi/v1/aggTrades",
                   local_path=str(out), interval_start_ms=u.start_ms, interval_end_ms=u.end_ms, status="downloading",
                   run_id=run_id)
    if u.dataset == "trades" and not rest.has_key:
        ck.upsert_unit(u.unit_id, status="missing", error="individual trades via REST need an API key "
                       "(/fapi/v1/historicalTrades); without a key only REST aggTrades can cover this interval")
        return "failed"
    # Resume position comes from the file's own content (not the cursor), so an interruption between
    # "page written" and "cursor saved" can never duplicate a page. An unreadable file is set aside and refetched.
    from_id, pages, recs = None, 0, 0
    if out.exists():
        try:
            last = None
            with gzip.open(out, "rt") as f:
                for line in f: last = json.loads(line)["a"]; recs += 1
            if last is not None: from_id = last + 1
        except Exception as e:
            ck.event(run_id, "WARN", f"REST file unreadable ({e!r}); refetching", u.unit_id)
            safe_remove(out); recs = 0
    try:
        with gzip.open(out, "at") as f:
            for page in rest.iter_agg_trades(cfg.symbol, u.start_ms, u.end_ms, from_id):
                fetched = int(time.time() * 1000)
                for x in page:
                    f.write(json.dumps({"fetched_ms": fetched, **x}, separators=(",", ":")) + "\n")
                f.flush(); os.fsync(f.fileno())
                pages += 1; recs += len(page)
                ck.set_cursor(u.unit_id, page[-1]["a"], page[-1]["T"], pages, recs)
                if pages % 50 == 0: log.info("%s: %d pages, %d aggTrades, at %s", u.unit_id, pages, recs, iso(page[-1]["T"]))
    except GeoBlocked as e:
        ck.upsert_unit(u.unit_id, status="missing", http_status=451, error=str(e)); return "failed"
    except NonRetryable as e:
        # e.g. Binance -4166 "Search window is restricted to recent 2 days only": permanently unavailable via REST
        ck.upsert_unit(u.unit_id, status="missing", http_status=e.status,
                       error=f"not recoverable via public REST: {e}"); return "failed"
    except Exception as e:
        ck.upsert_unit(u.unit_id, status="failed", error=repr(e)); return "failed"
    return verify_rest_unit(ck, u, out)

def verify_rest_unit(ck: Checkpoint, u: Unit, out: Path) -> str:
    ids = []; ts = []; tids = 0; bad = 0
    with gzip.open(out, "rt") as f:
        for line in f:
            x = json.loads(line); ids.append(x["a"]); ts.append(x["T"]); tids += x["l"] - x["f"] + 1
            bad += float(x["p"]) <= 0 or float(x["q"]) <= 0
    import numpy as np
    a = np.array(ids); t = np.array(ts)
    d = np.diff(a) if len(a) else a
    probs = []
    if len(a) == 0: probs.append("no records")
    if (d < 1).any(): probs.append(f"{int((d < 1).sum())} duplicate/decreasing agg ids")
    if (np.diff(t) < 0).any(): probs.append("backwards timestamps")
    if bad: probs.append(f"{bad} invalid values")
    gaps = int((d > 1).sum()) if len(a) else 0
    ok = not probs
    ck.upsert_unit(u.unit_id, status="verified" if ok else "failed", records=len(a),
                   first_id=int(a[0]) if len(a) else None, last_id=int(a[-1]) if len(a) else None,
                   first_ts=int(t[0]) if len(t) else None, last_ts=int(t[-1]) if len(t) else None,
                   id_gap_count=gaps, id_gap_ids=int((d[d > 1] - 1).sum()) if gaps else 0, dup_ids=int((d < 1).sum()) if len(a) else 0,
                   bytes=out.stat().st_size, verify_json=json.dumps({"trade_ids_covered": tids, "problems": probs}),
                   error="; ".join(probs) or None, completed_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    return "done" if ok else "failed"

# ------------------------------------------------------------------ driver
def run(cfg: Config, start_ms: int, end_ms: int, do_archive: bool = True, do_rest: bool = True,
        datasets: list[str] | None = None, run_id: str | None = None) -> dict:
    ck = Checkpoint(cfg.state_db)
    run_id = run_id or time.strftime("run-%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:6]
    ck.start_run(run_id, start_ms, end_ms, cfg.as_dict())
    arch = make_archive(cfg)
    units = plan(cfg, arch, start_ms, end_ms)
    if datasets: units = [u for u in units if u.dataset in datasets]
    (cfg.data_root / "state").mkdir(parents=True, exist_ok=True)
    (cfg.data_root / "state" / f"plan-{run_id}.json").write_text(json.dumps([asdict(u) for u in units], indent=1))
    for u in units:
        if u.method == "missing":
            ck.upsert_unit(u.unit_id, source=cfg["source"], method="archive", symbol=cfg.symbol, market=cfg.market,
                           dataset=u.dataset, granularity=u.granularity, period=u.period,
                           url=f"{cfg['archive']['base_url']}/{u.key}", interval_start_ms=u.start_ms,
                           interval_end_ms=u.end_ms, status="missing", error=u.reason, run_id=run_id)
    todo = [u for u in units if u.method == "archive" and not _already_verified(ck, cfg, u)] if do_archive else []
    skipped = [u for u in units if u.method == "archive" and u not in todo] if do_archive else []
    prog = Progress(cfg.data_root / "state" / "progress.json", len(todo) + len(skipped),
                    sum(u.remote["size"] for u in todo))
    prog.skipped = len(skipped); prog.start()
    results = {}
    try:
        with ThreadPoolExecutor(max_workers=cfg["archive"]["max_workers"]) as ex:
            futs = {ex.submit(run_archive_unit, cfg, ck, arch, u, run_id, prog): u for u in todo}
            for fu in as_completed(futs):
                u = futs[fu]
                try: res = fu.result()
                except Exception as e:
                    ck.upsert_unit(u.unit_id, status="failed", error=repr(e)); res = "failed"
                results[u.unit_id] = res; prog.unit_done("done" if res == "done" else "failed")
                if res != "done": log.error("unit %s FAILED: %s", u.unit_id, ck.get_unit(u.unit_id)["error"])
        # Monthly files that fail verification are kept (evidence) and the month is re-acquired from daily files.
        fb_units = []
        for uid in [k for k, v in results.items() if v != "done" and ":monthly:" in k] + \
                   [r["unit_id"] for r in ck.query("SELECT unit_id FROM units WHERE status='failed' AND granularity='monthly'")]:
            mu = next((x for x in units if x.unit_id == uid), None)
            if mu is None or any(f.unit_id.startswith(f"archive:{mu.dataset}:daily:{mu.period}") for f in fb_units): continue
            daily = list_dataset(arch, cfg, mu.dataset, "daily")
            ck.event(run_id, "WARN", "monthly file failed verification; falling back to daily files", uid)
            for dms in range(mu.start_ms, mu.end_ms, DAY_MS):
                d = from_ms(dms).date(); key = archive_key(cfg.market, "daily", mu.dataset, cfg.symbol, str(d))
                if key in daily:
                    fb_units.append(Unit(f"archive:{mu.dataset}:daily:{d}", "archive", mu.dataset, "daily", str(d),
                                         dms, dms + DAY_MS, key, asdict(daily[key])))
                else:
                    ck.upsert_unit(f"missing:{mu.dataset}:daily:{d}", source=cfg["source"], method="archive", symbol=cfg.symbol,
                                   market=cfg.market, dataset=mu.dataset, granularity="daily", period=str(d), url=key,
                                   interval_start_ms=dms, interval_end_ms=dms + DAY_MS, status="missing", run_id=run_id,
                                   error="monthly file failed verification and daily file not published")
        fb_todo = [u for u in fb_units if not _already_verified(ck, cfg, u)]
        if fb_todo:
            log.info("fallback: %d daily units replacing failed monthly files", len(fb_todo))
            prog.total_units += len(fb_units); prog.total_bytes += sum(u.remote["size"] for u in fb_todo)
            prog.skipped += len(fb_units) - len(fb_todo)
            with ThreadPoolExecutor(max_workers=cfg["archive"]["max_workers"]) as ex:
                futs = {ex.submit(run_archive_unit, cfg, ck, arch, u, run_id, prog): u for u in fb_todo}
                for fu in as_completed(futs):
                    u = futs[fu]
                    try: res = fu.result()
                    except Exception as e:
                        ck.upsert_unit(u.unit_id, status="failed", error=repr(e)); res = "failed"
                    results[u.unit_id] = res; prog.unit_done("done" if res == "done" else "failed")
                    if res != "done": log.error("unit %s FAILED: %s", u.unit_id, ck.get_unit(u.unit_id)["error"])
        # A failed monthly file whose whole month is now covered by verified daily files is marked
        # 'rejected' (kept on disk + reason recorded) rather than counted as an open failure.
        for r in ck.query("SELECT * FROM units WHERE status='failed' AND granularity='monthly'"):
            days = ck.query("SELECT status FROM units WHERE dataset=? AND granularity='daily' AND interval_start_ms>=? "
                            "AND interval_end_ms<=?", (r["dataset"], r["interval_start_ms"], r["interval_end_ms"]))
            n_days = (r["interval_end_ms"] - r["interval_start_ms"]) // DAY_MS
            if len(days) == n_days and all(d["status"] == "verified" for d in days):
                ck.upsert_unit(r["unit_id"], status="rejected",
                               error=f"{r['error']} | rejected: month re-acquired from {n_days} verified daily files")
                results[r["unit_id"]] = "done"
        if do_rest:
            rest_units = [u for u in units if u.method == "rest"]
            rest = make_rest(cfg) if rest_units else None
            for u in rest_units:
                row = ck.get_unit(u.unit_id)
                if row and row["status"] == "verified": continue
                results[u.unit_id] = run_rest_unit(cfg, ck, rest, u, run_id)
    finally:
        prog.stop()
    failed = [k for k, v in results.items() if v != "done"]
    ck.finish_run(run_id, "completed_with_failures" if failed else "completed", json.dumps({"failed_units": failed}))
    return {"run_id": run_id, "planned": len(units), "attempted": len(results), "skipped_verified": len(skipped),
            "failed": failed}
