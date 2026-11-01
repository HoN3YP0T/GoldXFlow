"""Phase 5: independent reconciliation of the derived footprint dataset.

For every written date the footprint is recomputed against two independent references:
  1. the Phase 3 trades partition (total lots, trade count, buy/sell/unknown lots, first/last trade id);
  2. Binance's own 1-minute klines, on days whose Phase 3 status is COMPLETE: per-minute volume, trade count,
     taker-buy volume (an independent check of the side classification) and OHLC.
It also checks internal consistency: buy + sell + unknown = volume and delta = buy - sell for every bar, and every
bar equals the sum of its footprint levels.
Every check is exact (integer lots and ticks). The result is written to data/reports/processing_reconciliation.{json,md}.
"""
from __future__ import annotations
from typing import Any
import json, sqlite3
from pathlib import Path
import numpy as np
from numpy.typing import NDArray
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ..normalize import part_path
from .footprint_store import read_footprint, read_manifest


def _col(t: pa.Table, c: str) -> NDArray[Any]:
    a = t.column(c)
    return np.asarray(a.cast(pa.int64()).to_numpy() if pa.types.is_timestamp(a.type) else a.to_numpy())


def reconcile_date(data_root: Path, symbol: str, interval: str, d: str, day_status: str | None, tick: float, step: float) -> dict[str, Any]:
    bars, levels = read_footprint(data_root, symbol, interval, d)
    out = {"date": d, "trades_day_status": day_status, "bars": bars.num_rows, "checks": {}, "mismatches": {}}
    C, M = out["checks"], out["mismatches"]
    vol, buy, sell, unk = (_col(bars, c) for c in ("volume_lots", "buy_lots", "sell_lots", "unknown_lots"))
    C["bar_sides_sum_to_volume"] = bool((buy + sell + unk == vol).all())
    C["bar_delta_is_buy_minus_sell"] = bool((_col(bars, "delta_lots") == buy - sell).all())
    # levels -> bars
    lb = _col(levels, "bar_start"); bs = _col(bars, "bar_start")
    ub, inv = np.unique(lb, return_inverse=True)
    ok = np.array_equal(ub, bs)
    for c in ("buy_lots", "sell_lots", "unknown_lots", "trade_count", "notional_tick_lots"):
        s = np.zeros(len(ub), np.int64); np.add.at(s, inv, _col(levels, c))
        ok &= np.array_equal(s, _col(bars, c)) if len(s) == bars.num_rows else False
    C["levels_sum_to_bars"] = bool(ok)
    # partition totals
    coverage = set(bars.column("coverage").cast(pa.string()).to_pylist())
    pp = part_path(Path(data_root), symbol, "trades", d)
    if coverage != {"FULL"}:
        out["partition"] = "NOT_APPLICABLE: coverage " + ",".join(sorted(coverage))
    elif not pp.exists():
        out["partition"] = "NO_PARTITION"
    else:
        t = pq.read_table(pp, columns=["quantity_lots", "aggressor_side", "source_trade_id"])
        q = t.column("quantity_lots").to_numpy(); side = t.column("aggressor_side").cast(pa.string())
        isb = pc.equal(side, "BUY").to_numpy(zero_copy_only=False); iss = pc.equal(side, "SELL").to_numpy(zero_copy_only=False)
        ids = t.column("source_trade_id").to_numpy()
        p = {"volume_lots": int(q.sum()), "trade_count": int(len(q)), "buy_lots": int(q[isb].sum()), "sell_lots": int(q[iss].sum()),
             "unknown_lots": int(q[~isb & ~iss].sum()), "first_trade_id": int(ids.min()), "last_trade_id": int(ids.max())}
        f = {"volume_lots": int(vol.sum()), "trade_count": int(_col(bars, "trade_count").sum()), "buy_lots": int(buy.sum()),
             "sell_lots": int(sell.sum()), "unknown_lots": int(unk.sum()), "first_trade_id": int(_col(bars, "first_trade_id").min()),
             "last_trade_id": int(_col(bars, "last_trade_id").max())}
        out["partition"] = {"source": p, "footprint": f}
        C["partition_totals_equal"] = p == f
    # klines per minute (1m footprint only)
    kp = part_path(Path(data_root), symbol, "klines_1m", d)
    if interval == "1m" and kp.exists():
        k = pq.read_table(kp)
        kt = _col(k, "open_time_utc"); kn = _col(k, "trade_count"); kv = _col(k, "volume_lots")
        kb = np.rint(k.column("taker_buy_volume").to_numpy() / step).astype(np.int64)
        ko = {c: np.rint(k.column(c).to_numpy() / tick).astype(np.int64) for c in ("open", "high", "low", "close")}
        traded = kn > 0
        pos = np.searchsorted(bs, kt); pos_c = np.minimum(pos, max(len(bs) - 1, 0))
        has = (pos < len(bs)) & (bs[pos_c] == kt) if len(bs) else np.zeros(len(kt), bool)
        missing_bar = traded & ~has
        extra_bar = np.setdiff1d(bs, kt[traded]).size
        j = pos_c[has & traded]; kk = has & traded
        M["minutes_with_trades_but_no_bar"] = int(missing_bar.sum()); M["bars_without_traded_kline"] = int(extra_bar)
        M["volume"] = int((vol[j] != kv[kk]).sum()); M["trade_count"] = int((_col(bars, "trade_count")[j] != kn[kk]).sum())
        M["taker_buy_volume_vs_buy_lots"] = int((buy[j] != kb[kk]).sum())
        for c, bc in (("open", "open_ticks"), ("high", "high_ticks"), ("low", "low_ticks"), ("close", "close_ticks")):
            M[c] = int((_col(bars, bc)[j] != ko[c][kk]).sum())
        out["kline_minutes"] = int(len(kt)); out["kline_minutes_traded"] = int(traded.sum())
        if day_status == "COMPLETE":
            C["klines_per_minute_equal"] = all(v == 0 for v in M.values())
    out["status"] = "PASS" if all(C.values()) else "FAIL"
    return out


def reconcile(cfg: Any, interval: str = "1m", data_root: Path | None = None, report_dir: Path | None = None) -> dict[str, Any]:
    root = Path(data_root or cfg.data_root); sym = cfg.symbol
    man = read_manifest(root, sym, interval)
    if not man: raise FileNotFoundError(f"no footprint manifest for interval {interval} under {root}")
    status = {}
    try:
        db = sqlite3.connect(str(cfg.state_db))
        status = dict(db.execute("SELECT date, status FROM day_status WHERE dataset='trades'").fetchall())
    except sqlite3.Error:
        pass
    tick, step = float(man["tick_size"]), float(man["step_size"])
    days = [reconcile_date(root, sym, interval, d, status.get(d), tick, step) for d in sorted(man["dates"])]
    summ = {"interval": interval, "dates": len(days), "pass": sum(x["status"] == "PASS" for x in days),
            "fail": [x["date"] for x in days if x["status"] == "FAIL"],
            "complete_days_checked_against_klines": sum("klines_per_minute_equal" in x["checks"] for x in days),
            "totals": {k: sum(x["partition"]["footprint"][k] for x in days if isinstance(x.get("partition"), dict))
                       for k in ("volume_lots", "trade_count", "buy_lots", "sell_lots", "unknown_lots")},
            "processing_settings_hash": man.get("processing_settings_hash"), "source_dataset_version": man.get("source_dataset_version")}
    rep = {"summary": summ, "days": days}
    rd = Path(report_dir) if report_dir else root / "reports"
    rd.mkdir(parents=True, exist_ok=True)
    (rd / f"processing_reconciliation_{interval}.json").write_text(json.dumps(rep, indent=1))
    lines = [f"# Phase 5 footprint reconciliation ({interval})", "",
             f"Dates: {summ['dates']}; PASS: {summ['pass']}; FAIL: {', '.join(summ['fail']) or 'none'}. "
             f"Complete days checked minute-by-minute against Binance 1m klines: {summ['complete_days_checked_against_klines']}.", "",
             f"Processing settings `{str(summ['processing_settings_hash'])[:16]}`, source dataset `{str(summ['source_dataset_version'])[:16]}`.", "",
             "PASS requires: bar sides sum to volume, delta = buy − sell, levels sum to bars, footprint totals = the trades "
             "partition, and, **on COMPLETE days only**, every minute equal to Binance's kline (volume, count, taker-buy, OHLC). "
             "On PARTIAL/UNVERIFIED days kline mismatches are informational: they measure trades missing upstream, which "
             "Phase 3 already flagged and which the footprint must not invent.", "",
             "| Date | Day status | Bars | Lots (footprint = partition) | Trades | Kline mismatches (vol / count / taker-buy / OHLC) | Status |",
             "|---|---|---|---|---|---|---|"]
    for x in days:
        p = x.get("partition"); M = x["mismatches"]
        lots = f"{p['footprint']['volume_lots']:,} {'=' if p['source'] == p['footprint'] else '≠'} {p['source']['volume_lots']:,}" if isinstance(p, dict) else str(p)
        trades = f"{p['footprint']['trade_count']:,}" if isinstance(p, dict) else "–"
        km = (f"{M.get('volume', '–')} / {M.get('trade_count', '–')} / {M.get('taker_buy_volume_vs_buy_lots', '–')} / "
              f"{sum(M.get(c, 0) for c in ('open', 'high', 'low', 'close'))}") if M else "–"
        lines.append(f"| {x['date']} | {x['trades_day_status']} | {x['bars']:,} | {lots} | {trades} | {km} | {x['status']} |")
    (rd / f"processing_reconciliation_{interval}.md").write_text("\n".join(lines) + "\n")
    return rep
