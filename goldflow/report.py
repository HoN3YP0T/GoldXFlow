"""Cross-file integrity checks and the historical data manifest.

Checks across all units of a dataset:
  * time coverage of the requested range [start, end) -> uncovered intervals with reasons
  * id continuity across file boundaries (trades, aggTrades)
  * trades vs Binance 1m klines: per-minute trade count and base volume (independent aggregate)
  * trades vs aggTrades: trade ids referenced by aggTrades vs ids present in trades
"""
from __future__ import annotations
import json, time
from pathlib import Path
import numpy as np

from .checkpoint import Checkpoint
from .config import Config
from .integrity import _batches
from .timeutil import iso

def _units(ck: Checkpoint, dataset: str) -> list:
    return ck.query("SELECT * FROM units WHERE dataset=? ORDER BY interval_start_ms, method", (dataset,))

def coverage(ck: Checkpoint, dataset: str, start_ms: int, end_ms: int) -> dict:
    rows = _units(ck, dataset)
    ok = sorted((r["interval_start_ms"], r["interval_end_ms"], r["unit_id"]) for r in rows if r["status"] == "verified")
    merged = []
    for s, e, uid in ok:
        if merged and s <= merged[-1][1]: merged[-1][1] = max(merged[-1][1], e)
        else: merged.append([s, e])
    uncovered = []; cur = start_ms
    for s, e in merged:
        if e <= start_ms or s >= end_ms: continue
        if s > cur: uncovered.append([cur, min(s, end_ms)])
        cur = max(cur, e)
    if cur < end_ms: uncovered.append([cur, end_ms])
    explained = []
    for a, b in uncovered:
        why = [f"{r['unit_id']}: {r['status']} - {r['error']}" for r in rows
               if r["status"] != "verified" and r["interval_start_ms"] < b and r["interval_end_ms"] > a]
        explained.append({"start": iso(a), "end": iso(b), "duration_s": (b - a) / 1000, "start_ms": a, "end_ms": b,
                          "reasons": why or ["no unit planned for this interval"]})
    return {"verified_units": len(ok), "uncovered_intervals": explained,
            "covered_fraction": round(1 - sum(b - a for a, b in uncovered) / (end_ms - start_ms), 8)}

def boundary_continuity(ck: Checkpoint, dataset: str) -> list[dict]:
    rows = [r for r in _units(ck, dataset) if r["status"] == "verified" and r["first_id"] is not None]
    out = []
    for p, n in zip(rows, rows[1:]):
        if n["first_id"] != p["last_id"] + 1:
            out.append({"between": [p["unit_id"], n["unit_id"]], "prev_last_id": p["last_id"], "next_first_id": n["first_id"],
                        "missing_ids": n["first_id"] - p["last_id"] - 1, "prev_last_ts": iso(p["last_ts"]), "next_first_ts": iso(n["first_ts"])})
    return out

def klines_crosscheck(cfg: Config, ck: Checkpoint, start_ms: int, end_ms: int) -> dict:
    """Compare per-minute trade counts/volumes from the trades files to Binance's 1m klines."""
    tm: dict[int, tuple] = {}
    for r in _units(ck, "trades"):
        if r["status"] != "verified" or r["method"] != "archive": continue
        p = Path(r["local_path"]); npz = p.parent / "_verify" / f"{p.stem}.minute.npz"
        z = np.load(npz)
        for m, c, v, bv in zip(z["minute_ms"], z["count"], z["volume"], z["taker_buy_volume"]):
            tm[int(m)] = (int(c), float(v), float(bv))
    km: dict[int, tuple] = {}
    for r in _units(ck, "klines_1m"):
        if r["status"] != "verified": continue
        for b in _batches(Path(r["local_path"]), "klines_1m"):
            for m, c, v, bv in zip(b.column("open_time").to_numpy(), b.column("count").to_numpy(),
                                   b.column("volume").to_numpy(), b.column("taker_buy_volume").to_numpy()):
                km[int(m)] = (int(c), float(v), float(bv))
    common = sorted(m for m in km if start_ms <= m < end_ms and m + 60_000 <= end_ms)
    tc = sum(tm.get(m, (0, 0, 0))[0] for m in common); kc = sum(km[m][0] for m in common)
    tv = sum(tm.get(m, (0, 0, 0))[1] for m in common); kv = sum(km[m][1] for m in common)
    tb = sum(tm.get(m, (0, 0, 0))[2] for m in common); kb = sum(km[m][2] for m in common)
    vol_mismatch = [m for m in common if abs(tm.get(m, (0, 0, 0))[1] - km[m][1]) > 1e-6 + 1e-9 * km[m][1]]
    cnt_diff = [km[m][0] - tm.get(m, (0, 0, 0))[0] for m in common]
    return {"minutes_compared": len(common), "first_minute": iso(common[0]) if common else None,
            "last_minute": iso(common[-1]) if common else None,
            "trades_count": tc, "klines_count": kc, "count_diff_klines_minus_trades": kc - tc,
            "minutes_with_count_diff": int(sum(1 for d in cnt_diff if d)),
            "trades_volume": round(tv, 3), "klines_volume": round(kv, 3), "volume_diff": round(kv - tv, 6),
            "minutes_with_volume_mismatch": len(vol_mismatch),
            "volume_mismatch_examples": [iso(m) for m in vol_mismatch[:10]],
            "taker_buy_volume_trades": round(tb, 3), "taker_buy_volume_klines": round(kb, 3),
            "kline_minutes_missing_from_trades": int(sum(1 for m in common if m not in tm and km[m][0] > 0))}

def build_manifest(cfg: Config, run_meta: dict, start_ms: int, end_ms: int, spec: dict | None, out: Path) -> dict:
    ck = Checkpoint(cfg.state_db)
    units = ck.query("SELECT * FROM units ORDER BY dataset, interval_start_ms")
    datasets = {}
    for ds in cfg["datasets"]:
        rows = [u for u in units if u["dataset"] == ds]
        ver = [u for u in rows if u["status"] == "verified"]
        datasets[ds] = {
            "role": cfg["datasets"][ds]["role"],
            "files_verified": sum(1 for u in ver if u["method"] == "archive"),
            "rest_segments_verified": sum(1 for u in ver if u["method"] == "rest"),
            "units_missing": [{"unit": u["unit_id"], "start": iso(u["interval_start_ms"]), "end": iso(u["interval_end_ms"]),
                               "http_status": u["http_status"], "reason": u["error"]} for u in rows if u["status"] == "missing"],
            "units_failed": [{"unit": u["unit_id"], "error": u["error"]} for u in rows if u["status"] == "failed"],
            "units_rejected_and_replaced": [{"unit": u["unit_id"], "reason": u["error"]} for u in rows if u["status"] == "rejected"],
            "records": sum(u["records"] or 0 for u in ver),
            "bytes": sum(u["bytes"] or 0 for u in ver),
            "first_ts": iso(min((u["first_ts"] for u in ver if u["first_ts"]), default=None)),
            "last_ts": iso(max((u["last_ts"] for u in ver if u["last_ts"]), default=None)),
            "id_gap_count": sum(u["id_gap_count"] or 0 for u in ver),
            "id_gap_ids": sum(u["id_gap_ids"] or 0 for u in ver),
            "duplicate_ids": sum(u["dup_ids"] or 0 for u in ver),
            "backwards_timestamps": sum(u["ts_backwards"] or 0 for u in ver),
            "coverage": coverage(ck, ds, start_ms, end_ms),
        }
        if ds in ("trades", "aggTrades"):
            datasets[ds]["boundary_id_discontinuities"] = boundary_continuity(ck, ds)
    files = [{"unit": u["unit_id"], "method": u["method"], "granularity": u["granularity"], "period": u["period"],
              "url": u["url"], "local_path": str(Path(u["local_path"]).relative_to(cfg.base_dir)) if u["local_path"] else None,
              "interval": [iso(u["interval_start_ms"]), iso(u["interval_end_ms"])], "status": u["status"],
              "http_status": u["http_status"], "attempts": u["attempts"], "bytes": u["bytes"], "sha256": u["sha256"],
              "sha256_matches_binance_checksum": (u["sha256"] == u["expected_sha256"]) if u["expected_sha256"] else None,
              "remote_etag": u["remote_etag"], "remote_last_modified": u["remote_last_modified"],
              "records": u["records"], "first_id": u["first_id"], "last_id": u["last_id"],
              "first_ts": iso(u["first_ts"]), "last_ts": iso(u["last_ts"]),
              "id_gap_count": u["id_gap_count"], "id_gap_ids": u["id_gap_ids"], "error": u["error"]} for u in units]
    m = {"manifest_version": 1, "generated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
         "source": cfg["source"], "market": cfg.market, "symbol": cfg.symbol,
         "requested_range": {"start": iso(start_ms), "end": iso(end_ms), "start_ms": start_ms, "end_ms": end_ms,
                             "convention": "half-open [start, end), UTC; end = acquisition timestamp"},
         "instrument_spec": spec, "run": run_meta, "datasets": datasets,
         "crosscheck_trades_vs_klines_1m": klines_crosscheck(cfg, ck, start_ms, end_ms),
         "reconciliation_trades_vs_aggTrades": _recon_summary(cfg),
         "superseded_versions": [dict(r) for r in ck.query("SELECT * FROM events WHERE message LIKE 'upstream file republished%'")],
         "files": files}
    out.write_text(json.dumps(m, indent=1, default=str))
    return m

def trades_vs_aggtrades(cfg: Config, start_ms: int, end_ms: int, out_dir: Path) -> dict:
    """Quantity-level reconciliation of `trades` against `aggTrades`, reported per UTC day.

    Each trade is assigned to the aggTrade row whose [first_trade_id, last_trade_id] contains its id.
    For every aggTrade row, sum(trade qty) must equal the row quantity. Outcomes:
      * row qty == trades qty            -> consistent (ids inside the range with no trade are unused ids)
      * row has no trades at all         -> trade(s) missing from the trades file. If first==last the trade
        is fully known (id, price, qty, time, side) and is written to a *recovered* sidecar; raw files untouched.
      * row qty != trades qty otherwise  -> partial mismatch, recorded with the quantity difference
      * trade id outside every agg range -> trade absent from aggTrades (aggTrades incomplete)
    Only stretches covered by verified files of both datasets are compared; others are listed.
    """
    import gc, pandas as pd
    from .timeutil import from_ms, month_bounds_ms, to_ms, DAY_MS
    ck = Checkpoint(cfg.state_db)
    ck._exec("DELETE FROM gaps WHERE run_id='reconcile'")   # idempotent: recomputed from scratch each time
    out_dir.mkdir(parents=True, exist_ok=True)
    rec_rows, days = [], {}
    report = {"windows_skipped_no_aggtrades": []}
    def units(ds, a, b):
        return [r for r in ck.query("SELECT * FROM units WHERE dataset=? AND status='verified' AND method='archive' "
                                    "ORDER BY interval_start_ms", (ds,))
                if r["interval_start_ms"] < b and r["interval_end_ms"] > a]
    def day_stat(t):
        k = iso((int(t) // DAY_MS) * DAY_MS)[:10]
        return days.setdefault(k, {"trades": 0, "agg_rows": 0, "agg_rows_consistent": 0, "agg_rows_no_trades": 0,
                                   "agg_rows_partial_mismatch": 0, "qty_in_agg_not_in_trades": 0.0,
                                   "recovered_single_trades": 0, "trades_outside_agg": 0, "qty_trades_outside_agg": 0.0})
    d = from_ms(start_ms).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while to_ms(d) < end_ms:
        ms, me = month_bounds_ms(d.year, d.month); a, b = max(ms, start_ms), min(me, end_ms)
        agg_units = units("aggTrades", a, b)
        agg_cov = [(max(a, r["interval_start_ms"]), min(b, r["interval_end_ms"])) for r in agg_units]
        for x, y in coverage_gaps(agg_cov, a, b): report["windows_skipped_no_aggtrades"].append([iso(x), iso(y)])
        def in_cov(t):
            c = np.zeros(len(t), bool)
            for x, y in agg_cov: c |= (t >= x) & (t < y)
            return c
        T = {"id": [], "qty": [], "time": []}
        for r in units("trades", a, b):
            for bt in _batches(Path(r["local_path"]), "trades"):
                t = bt.column("time").to_numpy(); s = (t >= a) & (t < b) & in_cov(t)
                if s.any():
                    T["id"].append(bt.column("id").to_numpy()[s]); T["qty"].append(bt.column("qty").to_numpy()[s]); T["time"].append(t[s])
        A = {k: [] for k in ("agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id", "transact_time", "is_buyer_maker")}
        for r in agg_units:
            for bt in _batches(Path(r["local_path"]), "aggTrades"):
                t = bt.column("transact_time").to_numpy(); s = (t >= a) & (t < b)
                if s.any():
                    for k in A: A[k].append(bt.column(k).to_numpy(zero_copy_only=False)[s])
        if not A["first_trade_id"] or not T["id"]:
            d = from_ms(me); continue
        tid = np.concatenate(T["id"]); tq = np.concatenate(T["qty"]); tt = np.concatenate(T["time"]); del T
        A = {k: np.concatenate(v) for k, v in A.items()}
        f, l, q = A["first_trade_id"], A["last_trade_id"], A["quantity"]
        idx = np.searchsorted(f, tid, side="right") - 1
        inside = (idx >= 0) & (l[np.clip(idx, 0, None)] >= tid)
        sumq = np.bincount(idx[inside], weights=tq[inside], minlength=len(f))
        ntr = np.bincount(idx[inside], minlength=len(f))
        diff = q - sumq
        consistent = np.abs(diff) <= 1e-9 + 1e-9 * q
        no_tr = ntr == 0
        partial = ~consistent & ~no_tr
        for arr, key in ((tt, "trades"),):
            for k, c in zip(*np.unique((arr // DAY_MS) * DAY_MS, return_counts=True)): day_stat(k)["trades"] += int(c)
        dk = (A["transact_time"] // DAY_MS) * DAY_MS
        for name, m in (("agg_rows", np.ones(len(f), bool)), ("agg_rows_consistent", consistent),
                        ("agg_rows_no_trades", no_tr), ("agg_rows_partial_mismatch", partial)):
            for k, c in zip(*np.unique(dk[m], return_counts=True)): day_stat(k)[name] += int(c)
        miss_q = np.where(~consistent, diff, 0.0)
        for k in np.unique(dk[~consistent]):
            day_stat(k)["qty_in_agg_not_in_trades"] += float(miss_q[dk == k].sum())
        single = no_tr & (f == l)
        for i in np.nonzero(single)[0]:
            rec_rows.append({"id": int(f[i]), "price": float(A["price"][i]), "qty": float(q[i]),
                             "quote_qty": round(float(A["price"][i]) * float(q[i]), 8), "time": int(A["transact_time"][i]),
                             "is_buyer_maker": bool(A["is_buyer_maker"][i]),
                             "recovered_from": f"aggTrades agg_trade_id={int(A['agg_trade_id'][i])}"})
            day_stat(A["transact_time"][i])["recovered_single_trades"] += 1
        for i in np.nonzero((no_tr & (f != l)) | partial)[0]:
            ck.add_gap("reconcile", "trades", "trades_missing_vs_aggTrades",
                       "aggTrade row quantity not matched by trades in its id range", "partial: aggregate qty/price/side known",
                       start_ms=int(A["transact_time"][i]), start_id=int(f[i]), end_id=int(l[i]), size=int(l[i] - f[i] + 1),
                       detail={"agg_trade_id": int(A["agg_trade_id"][i]), "agg_qty": float(q[i]), "trades_qty": float(sumq[i])})
        out = ~inside
        for k in np.unique((tt[out] // DAY_MS) * DAY_MS):
            m = out & ((tt // DAY_MS) * DAY_MS == k)
            st = day_stat(k); st["trades_outside_agg"] += int(m.sum()); st["qty_trades_outside_agg"] += float(tq[m].sum())
        print("reconcile", f"{d:%Y-%m}", "rows", len(f), "consistent", int(consistent.sum()), "no-trades", int(no_tr.sum()),
              "partial", int(partial.sum()), "trades-outside-agg", int(out.sum()), flush=True)
        del tid, tq, tt, A, f, l, q, idx, inside, sumq, ntr, diff; gc.collect()
        d = from_ms(me)
    if rec_rows:
        pd.DataFrame(rec_rows).to_csv(out_dir / f"{cfg.symbol}-trades-recovered-from-aggTrades.csv", index=False)
    tot = {k: round(sum(v[k] for v in days.values()), 6) for k in next(iter(days.values()))} if days else {}
    report.update({"totals": tot, "days_with_discrepancy": {k: v for k, v in sorted(days.items())
                   if v["agg_rows_no_trades"] or v["agg_rows_partial_mismatch"] or v["trades_outside_agg"]},
                   "days": dict(sorted(days.items())), "recovered_trades": len(rec_rows)})
    (out_dir / "trades_vs_aggtrades.json").write_text(json.dumps(report, indent=1, default=str))
    return report

def pa_mask(s):
    import pyarrow as pa
    return pa.array(s)

def coverage_gaps(cov, a, b):
    out, cur = [], a
    for x, y in sorted(cov):
        if y <= a or x >= b: continue
        if x > cur: out.append((cur, min(x, b)))
        cur = max(cur, y)
    if cur < b: out.append((cur, b))
    return out


def _recon_summary(cfg: Config) -> dict | None:
    """Summarise data/recovered/trades_vs_aggtrades.json into per-day data-quality flags."""
    p = cfg.data_root / "recovered" / "trades_vs_aggtrades.json"
    if not p.exists(): return None
    r = json.loads(p.read_text())
    flags = []
    for day, v in r["days_with_discrepancy"].items():
        if v["agg_rows_no_trades"] or v["agg_rows_partial_mismatch"]:
            flags.append({"day": day, "dataset": "trades", "flag": "TRADES_INCOMPLETE",
                          "detail": f"{v['agg_rows_no_trades'] + v['agg_rows_partial_mismatch']} aggTrade rows not matched by trades; "
                                    f"{round(v['qty_in_agg_not_in_trades'], 3)} oz missing from trades file; "
                                    f"{v['recovered_single_trades']} single trades recovered from aggTrades",
                          "qty_missing": round(v["qty_in_agg_not_in_trades"], 3), "trades_in_file": v["trades"]})
        if v["trades_outside_agg"] >= 1000:
            flags.append({"day": day, "dataset": "aggTrades", "flag": "AGGTRADES_INCOMPLETE",
                          "detail": f"{v['trades_outside_agg']} trades ({round(v['qty_trades_outside_agg'], 3)} oz) absent from aggTrades"})
    minor = {d: v["trades_outside_agg"] for d, v in r["days_with_discrepancy"].items() if 0 < v["trades_outside_agg"] < 1000}
    return {"totals": r["totals"], "recovered_trades_file": f"data/recovered/{cfg.symbol}-trades-recovered-from-aggTrades.csv",
            "flags": flags, "minor_aggtrades_omissions": {"days": len(minor), "trades": sum(minor.values())},
            "windows_not_compared": r["windows_skipped_no_aggtrades"]}
