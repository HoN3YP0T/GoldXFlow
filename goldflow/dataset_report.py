"""Phase 3 reports: dataset manifest, validation report, quarantine report, daily/monthly coverage summaries."""
from __future__ import annotations
import csv, json, time
from collections import Counter, defaultdict

import pyarrow.parquet as pq

from . import schemas as S
from .catalog import Catalog, code_version
from .config import Config

def build_phase3_reports(cfg: Config, summary: dict, overlaps: list, xc: list) -> dict:
    cat = Catalog(cfg); db = cat.db
    rep_dir = cfg.data_root / "reports"; rep_dir.mkdir(parents=True, exist_ok=True)
    man_dir = cfg.data_root / "manifests"; man_dir.mkdir(parents=True, exist_ok=True)
    rng = json.loads((cfg.data_root / "state" / "range.json").read_text())
    parts = [dict(r) for r in db.execute("SELECT * FROM partitions ORDER BY dataset, date")]
    days = [dict(r) for r in db.execute("SELECT * FROM day_status ORDER BY dataset, date")]
    hours = [dict(r) for r in db.execute("SELECT * FROM hour_status WHERE status!='COMPLETE' ORDER BY dataset, hour_utc")]
    qc = [dict(r) for r in db.execute("SELECT dataset, date, reason_code, count FROM quarantine_counts ORDER BY dataset, date")]
    raws = [dict(r) for r in db.execute("SELECT raw_file_id, dataset, granularity, period, path, bytes, sha256, provider_sha256, role, note FROM raw_files ORDER BY dataset, granularity, period")]
    runs = [dict(r) for r in db.execute("SELECT run_id, mode, started_utc, finished_utc, code_version, result_digest, compared_to, identical FROM validation_runs ORDER BY started_utc")]

    # ---------------------------------------------------------------- per dataset roll-up
    ds_info = {}
    for ds in ["trades", "aggTrades", "klines_1m", "bookDepth", "metrics"]:
        dp = [p for p in parts if p["dataset"] == ds]; dd = [d for d in days if d["dataset"] == ds]
        st = Counter(d["status"] for d in dd)
        first = last = None
        if dp:
            tcol = {"trades": "exchange_event_time_utc", "aggTrades": "exchange_event_time_utc", "klines_1m": "open_time_utc",
                    "bookDepth": "snapshot_time_utc", "metrics": "create_time_utc"}[ds]
            f0 = pq.read_table(cfg.base_dir / dp[0]["path"], columns=[tcol]).column(0)
            f1 = pq.read_table(cfg.base_dir / dp[-1]["path"], columns=[tcol]).column(0)
            first = f0[0].as_py().isoformat() if len(f0) else None
            last = f1[len(f1) - 1].as_py().isoformat() if len(f1) else None
        status = "COMPLETE" if set(st) == {"COMPLETE"} else ("UNVERIFIED" if st.get("UNVERIFIED") and not st.get("PARTIAL") and not st.get("MISSING") else "PARTIAL")
        ds_info[ds] = {"schema": S.SCHEMAS[ds].metadata[b"goldflow.schema"].decode(), "schema_version": S.SCHEMA_VERSION,
                       "partitions": len(dp), "rows": sum(p["rows"] for p in dp), "quarantined": sum(p["quarantined"] for p in dp),
                       "first_event_utc": first, "last_event_utc": last, "day_status_counts": dict(st), "dataset_status": status,
                       "days_not_complete": [{"date": d["date"], "status": d["status"], "evidence": json.loads(d["evidence_json"])}
                                             for d in dd if d["status"] != "COMPLETE"]}
    # trades extras
    ev = [json.loads(d["evidence_json"]) for d in days if d["dataset"] == "trades" and d["status"] != "MISSING"]
    ds_info["trades"]["records_recovered_from_aggtrades"] = sum(e.get("records_recovered", 0) for e in ev)
    ds_info["trades"]["records_archive"] = sum(e.get("records_valid", 0) for e in ev)
    ds_info["trades"]["unused_trade_ids_total"] = sum(e.get("id_gaps_unused_ids", 0) for e in ev)
    ds_info["trades"]["klines_evidence_days"] = Counter(e["klines_1m"]["status"] for e in ev)
    ds_info["trades"]["aggtrades_evidence_days"] = Counter(e["aggTrades"]["status"] for e in ev)

    limitations = [
        "Instrument is the Binance USDⓈ-M XAUUSDT TradFi perpetual. It is not COMEX gold futures or OTC XAUUSD orderflow.",
        "No historical price-level L2 order book (DepthEvent capability UNAVAILABLE). bookDepth gives coarse cumulative bands only.",
        "No historical bid/ask quotes (bookTicker not published for XAUUSDT).",
        "Trade ids are not contiguous. Binance skips ids never used for public trades; 1m-kline trade counts confirm this is not data loss.",
        "Instrument spec (tick 0.01, step 0.001) comes from Phase 0 exchangeInfo; REST re-verification from the cloud host is blocked (HTTP 451). Every stored price and quantity is verified to be an exact multiple.",
        f"Acquisition range end {rng['end']} falls mid-day; the 2026-10-30 tail is MISSING for every dataset.",
        "Normalised coverage starts 2026-04-30T00:00Z (whole first day kept), 8h42m before the requested acquisition start.",
    ]
    tr_partial = [d for d in ds_info["trades"]["days_not_complete"]]
    for d in tr_partial:
        limitations.append(f"trades {d['date']}: {d['status']} - " + json.dumps(d["evidence"].get("klines_1m", d["evidence"]))[:300])

    manifest = {
        "manifest_version": 2, "generated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "provider": S.PROVIDER_ID, "venue": S.VENUE_ID, "market_type": S.MARKET_TYPE, "instrument": cfg.symbol,
        "instrument_id": S.instrument_id(cfg.symbol), "instrument_spec": cfg["instrument"],
        "acquisition_range": {"start": rng["start"], "end": rng["end"], "convention": "half-open [start, end), UTC"},
        "normalised_coverage": {"first_day": days[0]["date"] if days else None,
                                "last_full_day": max((d["date"] for d in days if d["status"] != "MISSING"), default=None)},
        "schema_version": S.SCHEMA_VERSION, "code_version": code_version(), "capabilities": S.CAPABILITIES,
        "dataset_status_overall": "PARTIAL" if any(v["dataset_status"] != "COMPLETE" for v in ds_info.values()) else "COMPLETE",
        "datasets": ds_info, "known_limitations": limitations,
        "raw_files": {"count": len(raws), "by_role": Counter(r["role"] for r in raws),
                      "total_bytes": sum(r["bytes"] for r in raws), "files": raws},
        "partitions": parts, "validation_runs": runs,
        "cross_partition_id_overlaps": overlaps,
    }
    (man_dir / "dataset_manifest.json").write_text(json.dumps(manifest, indent=1, default=str))

    # ---------------------------------------------------------------- coverage CSVs
    pr = {(p["dataset"], p["date"]): p for p in parts}
    with open(rep_dir / "coverage_daily.csv", "w", newline="") as f:
        w = csv.writer(f); w.writerow(["dataset", "date", "status", "rows", "quarantined", "key_evidence"])
        for d in days:
            e = json.loads(d["evidence_json"]); p = pr.get((d["dataset"], d["date"]), {})
            key = (f"klines={e['klines_1m']['status']} agg={e['aggTrades']['status']} recovered={e.get('records_recovered', 0)}"
                   if d["dataset"] == "trades" and "klines_1m" in e else
                   json.dumps({k: v for k, v in e.items() if not isinstance(v, (list, dict))})[:200])
            w.writerow([d["dataset"], d["date"], d["status"], p.get("rows", 0), p.get("quarantined", 0), key])
    mon = defaultdict(lambda: Counter())
    for d in days:
        m = mon[(d["dataset"], d["date"][:7])]; m[d["status"]] += 1; m["rows"] += pr.get((d["dataset"], d["date"]), {}).get("rows", 0)
    with open(rep_dir / "coverage_monthly.csv", "w", newline="") as f:
        w = csv.writer(f); w.writerow(["dataset", "month", "days_complete", "days_partial", "days_unverified", "days_missing", "rows"])
        for (ds, m), c in sorted(mon.items()):
            w.writerow([ds, m, c["COMPLETE"], c["PARTIAL"], c["UNVERIFIED"], c["MISSING"], c["rows"]])

    # ---------------------------------------------------------------- quarantine report
    qtot = Counter(); qby = defaultdict(Counter)
    for q in qc: qtot[(q["dataset"], q["reason_code"])] += q["count"]; qby[q["dataset"]][q["date"]] += q["count"]
    xc_sum = Counter((x["file"], x["result"]) for x in xc)
    xc_diff = [x for x in xc if x["result"] != "MATCH"]

    # ---------------------------------------------------------------- validation report (markdown + json)
    vr = {"summary": summary, "datasets": {k: {kk: vv for kk, vv in v.items() if kk != "days_not_complete"} for k, v in ds_info.items()},
          "not_complete_days": {k: v["days_not_complete"] for k, v in ds_info.items()},
          "non_complete_hours": hours, "quarantine_totals": [{"dataset": a, "reason_code": b, "count": c} for (a, b), c in qtot.items()],
          "monthly_crosscheck": {"summary": [{"file": a, "result": b, "days": c} for (a, b), c in sorted(xc_sum.items())],
                                 "differences": xc_diff}, "cross_partition_id_overlaps": overlaps, "validation_runs": runs}
    (rep_dir / "validation_report.json").write_text(json.dumps(vr, indent=1, default=str))
    L = ["# Phase 3 validation report", "", f"Generated {manifest['generated_utc']} · code {manifest['code_version']} · schema v{S.SCHEMA_VERSION}", "",
         "| Dataset | Partitions | Rows | Quarantined | Day status | Dataset status |", "|---|---|---|---|---|---|"]
    for ds, v in ds_info.items():
        L.append(f"| {ds} | {v['partitions']} | {v['rows']:,} | {v['quarantined']:,} | " +
                 ", ".join(f"{k} {n}" for k, n in sorted(v["day_status_counts"].items())) + f" | {v['dataset_status']} |")
    L += ["", "## Quarantine by reason", ""]
    L += [f"- {a} / {b}: {c:,}" for (a, b), c in sorted(qtot.items())] or ["- none"]
    L += ["", "## Days not COMPLETE", ""]
    for ds, v in ds_info.items():
        for d in v["days_not_complete"]:
            L.append(f"- **{ds} {d['date']}: {d['status']}**. " + json.dumps(d["evidence"], default=str)[:400])
    L += ["", "## Hours not COMPLETE (trades, aggTrades)", ""]
    hrs = defaultdict(list)
    for h in hours: hrs[(h["dataset"], h["hour_utc"][:10], h["status"])].append(h["hour_utc"][11:13])
    L += [f"- {a} {b} {c}: hours {', '.join(v)}" for (a, b, c), v in sorted(hrs.items())]
    L += ["", "## Monthly archive files vs daily files", ""]
    L += [f"- {a}: {b} on {c} days" for (a, b), c in sorted(xc_sum.items())]
    L += [f"  - {x['file']} {x['date']}: {x['detail']}" for x in xc_diff if x["detail"]]
    L += ["", "## Cross-partition id overlaps", "", f"- {len(overlaps)} found"]
    L += ["", "## Validation runs", ""]
    L += [f"- {r['run_id']} ({r['mode']}): digest {r['result_digest'][:16]}… compared to {r['compared_to'] or '-'} → identical={r['identical']}" for r in runs]
    (rep_dir / "validation_report.md").write_text("\n".join(L) + "\n")
    Q = ["# Quarantine report", "", "Quarantined records are stored as Parquet under data/quarantine/... (same partitioning) with",
         "reason_code, reason_detail, raw_file_id, raw_row and the original fields. Nothing is dropped silently.", ""]
    Q += [f"- {a} / {b}: {c:,} ({S.REASON_CODES.get(b, '')})" for (a, b), c in sorted(qtot.items())] or ["- No records quarantined."]
    for ds, c in qby.items():
        Q += ["", f"## {ds} by date", ""] + [f"- {d}: {n:,}" for d, n in sorted(c.items())]
    Q += ["", "## Reason code reference", ""] + [f"- `{k}`: {v}" for k, v in S.REASON_CODES.items()]
    (rep_dir / "quarantine_report.md").write_text("\n".join(Q) + "\n")
    return manifest
