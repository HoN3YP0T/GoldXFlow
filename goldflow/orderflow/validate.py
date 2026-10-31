"""Phase 7 independent validation on real days (`goldflow orderflow-validate`).

Each check recomputes a result straight from the Phase 3 partitions with simple, separate code (pandas / plain
loops), never through the Phase 5 processor or the Phase 7 engine, and compares it with the written catalogue:

  cvd_day          utc_day CVD at the day's last bar = Σ BUY lots - Σ SELL lots of the day's trades; the cumulative
                   unknown lots = Σ UNKNOWN lots
  imbalances       the set of (bar, bin, side) imbalances of the configured interval/bin/mode equals a pandas
                   recomputation from raw trades
  taker_orders     reconstructed taker orders against Binance aggTrades. Measured on this dataset: an aggTrade
                   aggregates same-price, same-side fills over up to 100 ms, across different taker orders, so it is
                   NOT an order. What aggTrades can prove: fills of one aggTrade that share one exchange millisecond
                   are never split by the reconstruction (must be 0). Multi-ms splits, same-price merges and a
                   cluster_ms sensitivity table (first day) are reported for information.
  big_trade_ids    every BIG_TRADE's trade ids exist, are consecutive in the partition, share its side, and sum to
                   its size
Written to data/reports/orderflow_validation.{json,md}.
"""

from __future__ import annotations

import json
import sqlite3
from fractions import Fraction
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .settings import OrderflowSettings
from .store import orderflow_root, read_partitions

DAY_MS = 86_400_000


def _part(cfg: Any, ds: str, d: str) -> Path:
    root: Path = cfg.data_root
    return (
        root
        / "normalized"
        / "provider=binance"
        / "market=usdm_perpetual"
        / f"instrument={cfg.symbol}"
        / f"data_type={ds}"
        / f"date={d}"
        / "part-000.parquet"
    )


def _trades(cfg: Any, d: str) -> pd.DataFrame:
    t = pq.read_table(
        _part(cfg, "trades", d),
        columns=["source_trade_id", "exchange_event_time_utc", "price_ticks", "quantity_lots", "aggressor_side"],
    )
    df = pd.DataFrame(
        {
            "id": t.column(0).to_numpy(),
            "t": t.column(1).cast("int64").to_numpy(),
            "p": t.column(2).to_numpy(),
            "q": t.column(3).to_numpy(),
            "side": t.column(4).cast("string").to_pylist(),
        }
    )
    df["s"] = df["side"].map({"BUY": 1, "SELL": -1}).fillna(0).astype(np.int64)
    return df


def _imbalances_ref(
    df: pd.DataFrame, interval_ms: int, bin_ticks: int, ratio: Fraction, min_lots: int, mode: str, zero_policy: str
) -> set[tuple[int, int, str]]:
    df = df.assign(bar=df["t"] - df["t"] % interval_ms, bin=df["p"] // bin_ticks)
    g = (
        df.assign(buy=np.where(df["s"] == 1, df["q"], 0), sell=np.where(df["s"] == -1, df["q"], 0))
        .groupby(["bar", "bin"])[["buy", "sell"]]
        .sum()
    )
    d = {k: (int(v["buy"]), int(v["sell"])) for k, v in g.iterrows()}
    out = set()
    for (bar, b), (buy, sell) in d.items():
        ob = d.get((bar, b - 1), (0, 0))[1] if mode == "diagonal" else sell
        os_ = d.get((bar, b + 1), (0, 0))[0] if mode == "diagonal" else buy
        for x, opp, side in ((buy, ob, "BUY"), (sell, os_, "SELL")):
            if x < min_lots:
                continue
            if (opp > 0 and Fraction(x) >= ratio * opp) or (opp == 0 and zero_policy == "imbalance"):
                out.add((int(bar), int(b), side))
    return out


def _orders_ref(df: pd.DataFrame, cluster_ms: int, sb: int) -> np.ndarray:
    """Plain-loop reconstruction: returns the order index of every trade (rows in id order)."""
    t, p, s = df["t"].to_numpy(), df["p"].to_numpy(), df["s"].to_numpy()
    out = np.empty(len(df), np.int64)
    k = -1
    for i in range(len(df)):
        if (
            i > 0
            and s[i] != 0
            and s[i] == s[i - 1]
            and t[i] - t[i - 1] <= cluster_ms
            and (p[i] >= p[i - 1] if s[i] > 0 else p[i] <= p[i - 1])
            and abs(p[i] // sb - p[i - 1] // sb) <= 1
        ):
            out[i] = k
        else:
            k += 1
            out[i] = k
    return out


def validate_days(
    cfg: Any, dates: list[str] | None = None, max_days: int = 7, sensitivity: tuple[int, ...] | None = (0, 10, 50, 100, 250, 1000)
) -> dict[str, Any]:
    of = OrderflowSettings.from_config(cfg)
    root = orderflow_root(cfg.data_root, cfg.symbol)
    man = json.loads((root / "_manifest.json").read_text())
    db = sqlite3.connect(str(cfg.state_db))
    status = {(r[0], r[1]): r[2] for r in db.execute("SELECT dataset, date, status FROM day_status")}
    written = sorted(man["tables"]["cvd"]["dates"])
    if not dates:
        ok = [d for d in written if status.get(("trades", d)) == "COMPLETE" and status.get(("aggTrades", d)) == "COMPLETE"]
        step = max(1, len(ok) // max_days)
        dates = ok[::step][:max_days]
    cvd_all = read_partitions(root / "cvd", "cvd")
    imb_all = read_partitions(root / "imbalances", "imbalances")
    ev_all = read_partitions(root / "events", "events")
    im, bt = of["imbalance"], of["big_trades"]
    from ..processing.settings import parse_interval

    I = parse_interval(im["interval"])
    per_day = {}
    for d in dates:
        d0 = int(np.datetime64(d, "D").astype(np.int64)) * DAY_MS
        df = _trades(cfg, d).sort_values("id", kind="stable").reset_index(drop=True)
        r: dict[str, Any] = {
            "trades": len(df),
            "trades_status": status.get(("trades", d)),
            "aggtrades_status": status.get(("aggTrades", d)),
        }
        # ---- CVD
        c = cvd_all.filter(
            (cvd_all.column("series").cast("string").to_numpy(zero_copy_only=False) == "utc_day")
            & (cvd_all.column("interval").cast("string").to_numpy(zero_copy_only=False) == of["cvd"]["intervals"][0])
            & (cvd_all.column("bar_start_utc").cast("int64").to_numpy() // DAY_MS == d0 // DAY_MS)
        )
        exp_cvd = int(df.loc[df.s == 1, "q"].sum() - df.loc[df.s == -1, "q"].sum())
        exp_unk = int(df.loc[df.s == 0, "q"].sum())
        got = c.sort_by("bar_start_utc").slice(c.num_rows - 1, 1).to_pylist()[0] if c.num_rows else {}
        r["cvd_day"] = {
            "expected": exp_cvd,
            "got": got.get("cvd_lots"),
            "unknown_expected": exp_unk,
            "unknown_got": got.get("cum_unknown_lots"),
            "ok": got.get("cvd_lots") == exp_cvd and got.get("cum_unknown_lots") == exp_unk,
        }
        # ---- imbalances
        ref = _imbalances_ref(
            df, I, int(im["bin_ticks"]), Fraction(str(im["ratio"])), int(im["min_lots"]), im["mode"], im["zero_policy"]
        )
        sel = imb_all.filter(imb_all.column("bar_start_utc").cast("int64").to_numpy() // DAY_MS == d0 // DAY_MS)
        got_set = set(
            zip(
                sel.column("bar_start_utc").cast("int64").to_pylist(),
                sel.column("bin").to_pylist(),
                sel.column("side").to_pylist(),
            )
        )
        r["imbalances"] = {
            "reference": len(ref),
            "catalogue": len(got_set),
            "missing": len(ref - got_set),
            "extra": len(got_set - ref),
            "ok": ref == got_set,
        }
        # ---- taker orders vs aggTrades
        oid = _orders_ref(df, int(bt["cluster_ms"]), int(bt["sweep_bin_ticks"]))
        r["taker_orders"] = {"orders": int(oid[-1] + 1) if len(oid) else 0}
        ap = _part(cfg, "aggTrades", d)
        if ap.exists():
            ag = pq.read_table(ap, columns=["aggregate_trade_id", "first_trade_id", "last_trade_id", "price_ticks"]).to_pandas()
            pos_f = np.searchsorted(df["id"].to_numpy(), ag["first_trade_id"].to_numpy())
            pos_l = np.searchsorted(df["id"].to_numpy(), ag["last_trade_id"].to_numpy())
            inside = (pos_f < len(df)) & (pos_l < len(df))
            inside[inside] &= (df["id"].to_numpy()[pos_f[inside]] == ag["first_trade_id"].to_numpy()[inside]) & (
                df["id"].to_numpy()[pos_l[inside]] == ag["last_trade_id"].to_numpy()[inside]
            )
            tt = df["t"].to_numpy()
            span = tt[pos_l[inside]] - tt[pos_f[inside]]  # ms between an aggTrade's first and last fill
            is_split = oid[pos_f[inside]] != oid[pos_l[inside]]
            split_same_ms = int((is_split & (span == 0)).sum())
            split_multi_ms = int((is_split & (span > 0)).sum())
            ordr = pd.DataFrame({"o": oid[pos_f[inside]], "p": ag["price_ticks"].to_numpy()[inside]})
            per_o = ordr.groupby("o").agg(n=("p", "size"), u=("p", "nunique"))
            merged = int((per_o["n"] > per_o["u"]).sum())
            r["taker_orders"].update(
                {
                    "aggtrades": int(len(ag)),
                    "aggtrades_matched": int(inside.sum()),
                    "aggtrades_multi_ms": int((span > 0).sum()),
                    "aggtrade_span_ms_max": int(span.max()) if len(span) else None,
                    "aggtrade_span_ms_p99": float(np.percentile(span, 99)) if len(span) else None,
                    # the check: fills of one aggTrade that share one exchange millisecond are never split
                    "split_same_ms": split_same_ms,
                    # information only: aggTrades spanning several ms (Binance aggregates within ~100 ms, across orders)
                    "split_multi_ms": split_multi_ms,
                    "orders_with_same_price_aggtrades": merged,
                    "ok": split_same_ms == 0,
                }
            )
            if sensitivity:
                sens = []
                for cl in sensitivity:
                    o2 = _orders_ref(df, cl, int(bt["sweep_bin_ticks"]))
                    sp = int((o2[pos_f[inside]] != o2[pos_l[inside]]).sum())
                    g2 = pd.DataFrame({"o": o2[pos_f[inside]], "p": ag["price_ticks"].to_numpy()[inside]}).groupby("o")["p"]
                    mg = int((g2.size() > g2.nunique()).sum())
                    sens.append(
                        {"cluster_ms": cl, "orders": int(o2[-1] + 1), "aggtrades_split": sp, "orders_merging_aggtrades": mg}
                    )
                r["taker_orders"]["cluster_sensitivity"] = sens
                sensitivity = None  # first validated day only
        else:
            r["taker_orders"].update({"ok": None, "note": "no aggTrades partition"})
        # ---- big trade ids
        e = ev_all.filter(
            (ev_all.column("event_type").to_numpy(zero_copy_only=False) == "BIG_TRADE")
            & (ev_all.column("evidence_start_utc").cast("int64").to_numpy() // DAY_MS == d0 // DAY_MS)
        )
        idx = pd.Series(np.arange(len(df)), index=df["id"].to_numpy())
        bad = 0
        n_checked = 0
        for row in e.to_pylist():
            ids = row["trade_ids"]
            if ids is None:
                continue
            n_checked += 1
            try:
                ix = idx.loc[ids].to_numpy()
            except KeyError:
                bad += 1
                continue
            sd = {"BUY": 1, "SELL": -1, "UNKNOWN": 0}[row["side"]]
            if not (
                np.all(np.diff(ix) == 1)
                and int(df["q"].to_numpy()[ix].sum()) == row["value_lots"]
                and np.all(df["s"].to_numpy()[ix] == sd)
            ):
                bad += 1
        r["big_trade_ids"] = {"events": e.num_rows, "checked": n_checked, "bad": bad, "ok": bad == 0}
        r["ok"] = all(v.get("ok") in (True, None) for v in r.values() if isinstance(v, dict))
        per_day[d] = r
    summary = {
        "dates": dates,
        "days_ok": sum(1 for r in per_day.values() if r["ok"]),
        "days": len(per_day),
        "fail": any(not r["ok"] for r in per_day.values()),
    }
    rep = {"summary": summary, "per_day": per_day}
    out = cfg.data_root / "reports"
    out.mkdir(parents=True, exist_ok=True)
    (out / "orderflow_validation.json").write_text(json.dumps(rep, indent=1, sort_keys=True, default=str))
    L = [
        "# Orderflow independent validation (Phase 7)",
        "",
        "Each check is recomputed from the Phase 3 partitions by separate code and compared exactly.",
        "",
        "| Date | Trades | CVD day = Σbuy−Σsell | Imbalances ref / catalogue / missing / extra | aggTrades split, same ms "
        "(must be 0) / multi-ms (info) | aggTrade span max ms | Big-trade ids ok | OK |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for d, r in per_day.items():
        to = r["taker_orders"]
        L.append(
            f"| {d} | {r['trades']:,} | {'=' if r['cvd_day']['ok'] else 'MISMATCH'} {r['cvd_day']['expected']:,} | "
            f"{r['imbalances']['reference']:,} / {r['imbalances']['catalogue']:,} / {r['imbalances']['missing']} / "
            f"{r['imbalances']['extra']} | {to.get('split_same_ms', '-')} / {to.get('split_multi_ms', '-')} | "
            f"{to.get('aggtrade_span_ms_max', '-')} | "
            f"{r['big_trade_ids']['checked'] - r['big_trade_ids']['bad']}/{r['big_trade_ids']['checked']} | {'yes' if r['ok'] else 'NO'} |"
        )
    for d, r in per_day.items():
        sens = r["taker_orders"].get("cluster_sensitivity")
        if sens:
            L += [
                "",
                f"## Taker-order clustering sensitivity ({d})",
                "",
                "aggTrades split = Binance aggTrades spread over several reconstructed orders; orders merging aggTrades = "
                "reconstructed orders holding two same-price aggTrades (two taker orders glued together).",
                "",
                "| cluster_ms | orders | aggTrades split | orders merging aggTrades |",
                "|---|---|---|---|",
            ]
            L += [
                f"| {x['cluster_ms']} | {x['orders']:,} | {x['aggtrades_split']:,} | {x['orders_merging_aggtrades']:,} |"
                for x in sens
            ]
    L += ["", f"Result: **{'FAIL' if summary['fail'] else 'PASS'}** ({summary['days_ok']}/{summary['days']} days)", ""]
    (out / "orderflow_validation.md").write_text("\n".join(L))
    return rep
