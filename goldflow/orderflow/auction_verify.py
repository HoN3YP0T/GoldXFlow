"""Phase 9: independent verification of the auction dataset (``auction-verify``).

Recomputed from the Phase 3 trades partitions with plain loops (not the engine's code paths) and compared exactly:
1. **Episodes.** Bars of ``auction.interval`` are rebuilt from trades. For every stored reference level (active from
   its ``available_at`` until the next level of the same source series) and both sides, acceptance, return,
   rejection and failed-auction events are re-derived (including the failed-auction hold) and the set of
   (type, level, side, episode start, confirmation time) plus the volume evidence must match.
2. **Profile shape.** Excess, poor high/low and single-print proxies of every fully covered window, from trades.
3. **Outcomes.** Every horizon row of every event, from the rebuilt bars.
4. **Timing invariants.** available_at = confirmed_at + shift; confirmed_at >= episode start; outcomes at
   confirmation + horizon; states at as_of + shift.
It also builds the event catalogue by session with outcome statistics per horizon.
Writes data/reports/auction_verification.{json,md}.
"""

from __future__ import annotations

import json
from fractions import Fraction
from pathlib import Path
from typing import Any

import numpy as np
from numpy.typing import NDArray

from ..processing.sessions import SessionCalendar
from ..processing.settings import ProcessingSettings
from ..timeutil import DAY_MS
from .auction_engine import AuctionStore
from .auction_settings import AuctionSettings
from .flow_verify import Trades, _ms, _rows

I64 = np.int64
SIDES = {"ABOVE": 1, "BELOW": -1}


def _store(cfg: Any) -> AuctionStore:
    return AuctionStore(cfg.data_root, cfg.symbol)


def _table(cfg: Any, table: str, d: str) -> list[dict[str, Any]]:
    import pyarrow.parquet as pq

    return _rows(pq.read_table(_store(cfg).base / f"date={d}" / f"{table}.parquet"))


def _bars(tr: dict[str, NDArray[Any]], a: int, b: int, I: int) -> list[dict[str, Any]]:
    out = []
    t = tr["t"]
    s0 = a - a % I
    for s in range(s0, b, I):
        i, j = int(np.searchsorted(t, s, "left")), int(np.searchsorted(t, s + I, "left"))
        if j <= i:
            continue
        p = tr["p"][i:j]
        out.append({"s": s, "e": s + I, "o": int(p[0]), "h": int(p.max()), "l": int(p.min()), "c": int(p[-1]), "i": i, "j": j,
                    "v": int(tr["q"][i:j].sum())})  # fmt: skip
    return out


def _beyond(x: Any, L: Fraction, side: int) -> bool:
    return bool(Fraction(x) > L) if side > 0 else bool(Fraction(x) < L)


def _episodes(
    bars: list[dict[str, Any]], tr: dict[str, NDArray[Any]], lv: dict[str, Any], st: AuctionSettings
) -> list[tuple[Any, ...]]:
    """Plain re-derivation for one level: (type, level_id, side, episode_start, confirmed_at, vol_beyond, delta_beyond, ext)."""
    L, a, u = lv["L"], lv["available_at"], lv["until"]
    out = []
    holds: list[dict[str, Any]] = []
    for side in (1, -1):
        ep: dict[str, Any] | None = None
        for k, bar in enumerate(bars):
            for h in [h for h in holds if h["side"] == side]:
                if bar["s"] < h["end"]:
                    continue
                ext = bar["h"] if side > 0 else bar["l"]
                if (ext - h["ext"]) * side > 0 or _beyond(bar["c"], L, side):
                    holds.remove(h)
                    continue
                h["held"] += 1
                if h["held"] >= st.fa_hold_bars:
                    out.append(("FAILED_AUCTION", lv["id"], side, h["start"], bar["e"], h["vb"], h["db"], h["ext"]))
                    holds.remove(h)
            if not (a <= bar["s"] and (u is None or bar["s"] < u)):
                ep = None
                continue
            ext = bar["h"] if side > 0 else bar["l"]
            if ep is None:
                if not _beyond(ext, L, side):
                    continue
                ep = {"start": bar["s"], "ext": None, "vb": 0, "db": 0, "vt": 0, "run": 0, "acc": None}
            for p, q, sd in zip(
                tr["p"][bar["i"] : bar["j"]].tolist(),
                tr["q"][bar["i"] : bar["j"]].tolist(),
                tr["side"][bar["i"] : bar["j"]].tolist(),
            ):
                if _beyond(p, L, side):
                    ep["vb"] += q
                    ep["db"] += q if sd == 1 else -q if sd == -1 else 0
            ep["vt"] += bar["v"]
            if _beyond(ext, L, side):
                ep["ext"] = ext if ep["ext"] is None else (max(ep["ext"], ext) if side > 0 else min(ep["ext"], ext))
            cb = _beyond(bar["c"], L, side)
            ep["run"] = ep["run"] + 1 if cb else 0
            if ep["acc"] is None and cb:
                if ep["run"] * st.interval_ms >= st.accept_ms or (
                    lv["ref"] is not None and Fraction(ep["vb"]) >= st.accept_vol_frac * lv["ref"]
                ):
                    ep["acc"] = bar["e"]
                    out.append(("ACCEPTANCE", lv["id"], side, ep["start"], bar["e"], ep["vb"], ep["db"], ep["ext"]))
            if not cb:
                if ep["acc"] is not None:
                    out.append(("RETURN", lv["id"], side, ep["start"], bar["e"], ep["vb"], ep["db"], ep["ext"]))
                else:
                    probe = ep["ext"] is not None and (ep["ext"] - L) * side >= st.probe_bins * st.bin_ticks
                    if probe and bar["e"] - ep["start"] <= st.reject_ms:
                        out.append(("REJECTION", lv["id"], side, ep["start"], bar["e"], ep["vb"], ep["db"], ep["ext"]))
                    weak = Fraction(ep["vb"]) <= st.fa_vol_frac * ep["vt"] or ep["db"] * side < 0
                    if probe and weak and bar["e"] - ep["start"] <= st.fa_return_ms:
                        holds.append(
                            {
                                "side": side,
                                "start": ep["start"],
                                "end": bar["e"],
                                "ext": ep["ext"],
                                "vb": ep["vb"],
                                "db": ep["db"],
                                "held": 0,
                            }
                        )
                ep = None
    return out


def _shape_ref(tr: dict[str, NDArray[Any]], start: int, end: int, st: AuctionSettings) -> set[tuple[Any, ...]]:
    i, j = int(np.searchsorted(tr["t"], start, "left")), int(np.searchsorted(tr["t"], end, "left"))
    if j <= i:
        return set()
    bins: dict[int, list[int]] = {}
    for t, p, q in zip(tr["t"][i:j].tolist(), tr["p"][i:j].tolist(), tr["q"][i:j].tolist()):
        x = bins.setdefault(p // st.bin_ticks, [0, t, t])
        x[0] += q
        x[1], x[2] = min(x[1], t), max(x[2], t)
    lo, hi = min(bins), max(bins)
    n = hi - lo + 1
    if n < 3:
        return set()
    vol = [bins.get(b, [0])[0] for b in range(lo, hi + 1)]
    total = sum(vol)
    mean = Fraction(total, n)
    out = set()
    for side, order in (("HIGH", list(range(n - 1, -1, -1))), ("LOW", list(range(n)))):
        tail = []
        for k in order:
            if vol[k] > st.tail_frac * mean:
                break
            tail.append(k)
        traded = [k for k in tail if vol[k] > 0]
        if 0 < len(tail) < n and len(tail) >= st.excess_bins and traded:
            span = max(bins[lo + k][2] for k in traded) - min(bins[lo + k][1] for k in traded)
            if span <= st.excess_ms:
                out.add((f"EXCESS_{side}", (lo + order[0]) * st.bin_ticks))
                continue
        m3 = Fraction(vol[order[0]] + vol[order[1]] + vol[order[2]], 3)
        if vol[order[0]] > 0 and vol[order[0]] >= st.poor_frac * m3:
            out.add((f"POOR_{side}", (lo + order[0]) * st.bin_ticks))
    k = 1
    while k < n - 1:
        if vol[k] <= st.sp_frac * mean:
            m = k
            while m + 1 < n - 1 and vol[m + 1] <= st.sp_frac * mean:
                m += 1
            if m - k + 1 >= st.sp_min_bins:
                out.add(("SINGLE_PRINT_PROXY", (lo + k) * st.bin_ticks))
            k = m + 1
        else:
            k += 1
    return out


def _session_of(cal: SessionCalendar, t: int) -> str:
    for i in cal.instances(t, t, []):
        if i.kind == "session" and i.start_ms <= t < i.end_ms:
            return i.name
    return "outside"


def verify(cfg: Any, dates: list[str] | None = None, report: bool = True) -> dict[str, Any]:
    st = AuctionSettings.from_config(cfg)
    pst = ProcessingSettings.from_config(cfg)
    cal = SessionCalendar.from_config(pst.sessions_cfg)
    shift = pst.trade_shift_ms
    store = _store(cfg)
    mp = store.base / "_manifest.json"
    man = json.loads(mp.read_text()) if mp.exists() else {}
    all_dates = sorted(d for d, v in man.get("dates", {}).items() if v.get("date_complete"))
    todo = [d for d in all_dates if not dates or d in dates]
    summ: dict[str, Any] = {"dates": len(todo), "pass": 0, "fail": [], "auction_settings_hash": st.hash[:16], "levels": 0,
                            "episode_events_checked": 0, "shape_events_checked": 0, "outcomes_checked": 0, "states_checked": 0,
                            "timing_violations": 0}  # fmt: skip
    if not all_dates:
        return {"summary": summ, "dates": [], "catalogue": {}}
    observe = int(man.get("observe_from") or _ms(all_dates[0]))
    run_end = _ms(all_dates[-1]) + DAY_MS
    T = Trades(cfg)
    tr = T.between(observe, run_end)
    bars = _bars(tr, observe, run_end, st.interval_ms)
    tabs = {t: [r for d in all_dates for r in _table(cfg, t, d)] for t in ("levels", "events", "outcomes", "states")}
    # ---- 1. episodes for every stored level (and user levels)
    lv_rows = sorted(tabs["levels"], key=lambda r: (r["series"], r["available_at"], r["level_id"]))
    nxt: dict[str, list[int]] = {}
    for r in lv_rows:
        nxt.setdefault(r["series"], [])
        if r["available_at"] not in nxt[r["series"]]:
            nxt[r["series"]].append(r["available_at"])
    levels = []
    for r in lv_rows:
        later = [x for x in nxt[r["series"]] if x > r["available_at"]]
        n_, d_ = (int(x) for x in r["price_exact"].split("/"))
        levels.append({"id": r["level_id"], "L": Fraction(n_, d_), "available_at": r["available_at"], "until": later[0] if later else None,
                       "ref": r["ref_lots"]})  # fmt: skip
    for name, price in st.user_levels:
        levels.append({"id": f"user:{name}", "L": price, "available_at": -(2**62), "until": None, "ref": None})
    summ["levels"] = len(levels)
    ref = set()
    for lv in levels:
        sub = [b for b in bars if b["s"] >= lv["available_at"]]
        for x in _episodes(sub, tr, lv, st):
            ref.add(x)
    types = ("ACCEPTANCE", "RETURN", "REJECTION", "FAILED_AUCTION")
    got = {(e["event_type"], e["level_id"], SIDES[e["side"]], e["episode_start"], e["confirmed_at"], e["vol_beyond"], e["delta_beyond"],
            e["probe_extreme_ticks"]) for e in tabs["events"] if e["event_type"] in types}  # fmt: skip
    per: list[dict[str, Any]] = []
    probs: dict[str, list[str]] = {}

    def flag(t_ms: int, msg: str) -> None:
        probs.setdefault(str(np.datetime64(int(t_ms) - 1, "ms").astype("datetime64[D]")), []).append(msg)

    for x in sorted(ref - got):
        flag(x[4], f"missing {x[0]} {x[1]} side {x[2]} start {x[3]}")
    for x in sorted(got - ref):
        flag(x[4], f"unexpected {x[0]} {x[1]} side {x[2]} start {x[3]}")
    summ["episode_events_checked"] = len(got)
    # ---- 2. shape
    shape_got = {(e["event_type"], json.loads(e["detail"])["window_key"], json.loads(e["detail"])["price_ticks"])
                 for e in tabs["events"] if e["event_type"] not in types and e["coverage"] == "FULL"}  # fmt: skip
    shape_time = {(e["event_type"], json.loads(e["detail"])["window_key"], json.loads(e["detail"])["price_ticks"]): e["confirmed_at"]
                  for e in tabs["events"] if e["event_type"] not in types}  # fmt: skip
    shape_ref = set()
    for day in range(observe // DAY_MS - 8, run_end // DAY_MS + 1):
        for inst in cal.instances(day * DAY_MS, day * DAY_MS + DAY_MS - 1, []):
            if (
                inst.start_ms < observe
                or inst.end_ms > run_end
                or day * DAY_MS > inst.start_ms
                or inst.start_ms >= (day + 1) * DAY_MS
            ):
                continue
            if inst.kind not in st.shape_windows and f"{inst.kind}:{inst.name}" not in st.shape_windows:
                continue
            for t, px in _shape_ref(tr, inst.start_ms, inst.end_ms, st):
                shape_ref.add((t, inst.key, px))
    for x in sorted(shape_ref - shape_got):
        flag(shape_time.get(x, run_end), f"missing shape {x}")
    for x in sorted(shape_got - shape_ref):
        flag(shape_time[x], f"unexpected shape {x}")
    summ["shape_events_checked"] = len(shape_got)
    # ---- 3. outcomes
    ev_by_id = {e["event_id"]: e for e in tabs["events"]}
    by_end = {b["e"]: b for b in bars}
    for o in tabs["outcomes"]:
        e = ev_by_id.get(o["event_id"])
        if e is None:
            flag(o["available_at"], f"outcome for unknown event {o['event_id']}")
            continue
        t0, h = e["confirmed_at"], o["horizon_minutes"] * 60_000
        refp = by_end[t0]["c"]
        side = SIDES[e["side"]]
        direction = side if e["event_type"] == "ACCEPTANCE" else -side
        inv = e["probe_extreme_ticks"] if e["event_type"] != "ACCEPTANCE" else None
        best = worst = 0
        last = refp
        hit = False
        for b in bars:
            if b["s"] < t0 or b["e"] > t0 + h:
                continue
            for x in (b["h"], b["l"]):
                best, worst = max(best, (x - refp) * direction), min(worst, (x - refp) * direction)
            last = b["c"]
            if inv is not None and ((b["h"] if direction < 0 else b["l"]) - inv) * -direction >= 0:
                hit = True
        exp = {"ref_price_ticks": refp, "close_price_ticks": last, "move_ticks": (last - refp) * direction, "mfe_ticks": best,
               "mae_ticks": worst, "invalidated": hit if inv is not None else None, "available_at": t0 + h + shift}  # fmt: skip
        bad = {k: (o[k], v) for k, v in exp.items() if o[k] != v}
        if bad:
            flag(o["available_at"], f"outcome {o['event_id']} {o['horizon_minutes']}m: {bad}")
        summ["outcomes_checked"] += 1
    # ---- 4. timing
    for e in tabs["events"]:
        if e["available_at"] != e["confirmed_at"] + shift or (
            e["episode_start"] is not None and e["confirmed_at"] < e["episode_start"]
        ):
            summ["timing_violations"] += 1
            flag(e["available_at"], f"timing {e['event_id']}")
    for s_ in tabs["states"]:
        summ["states_checked"] += 1
        if s_["available_at"] != s_["as_of"] + shift:
            summ["timing_violations"] += 1
            flag(s_["available_at"], f"state timing {s_['window_key']} {s_['as_of']}")
    for d in todo:
        ok = not probs.get(d)
        per.append({"date": d, "ok": ok, "problems": probs.get(d, [])[:8]})
        if ok:
            summ["pass"] += 1
        else:
            summ["fail"].append(d)
    cat = _catalogue(tabs, cal)
    out = {"summary": summ, "dates": per, "catalogue": cat}
    if report:
        _write(cfg, out)
    return out


def _catalogue(tabs: dict[str, list[dict[str, Any]]], cal: SessionCalendar) -> dict[str, Any]:
    ev = {e["event_id"]: e for e in tabs["events"]}
    counts: dict[str, int] = {}
    for e in tabs["events"]:
        k = f"{e['event_type']}:{e['side']}:{_session_of(cal, e['confirmed_at'] - 1)}"
        counts[k] = counts.get(k, 0) + 1
    stats: dict[str, dict[str, Any]] = {}
    for o in tabs["outcomes"]:
        src = ev.get(o["event_id"])
        if src is None:
            continue
        k = f"{src['event_type']}:{_session_of(cal, src['confirmed_at'] - 1)}:{o['horizon_minutes']}m"
        x = stats.setdefault(k, {"n": 0, "sum_move": 0, "positive": 0, "sum_mfe": 0, "sum_mae": 0, "invalidated": 0, "n_inv": 0})
        x["n"] += 1
        x["sum_move"] += o["move_ticks"]
        x["positive"] += o["move_ticks"] > 0
        x["sum_mfe"] += o["mfe_ticks"]
        x["sum_mae"] += o["mae_ticks"]
        x["invalidated"] += bool(o["invalidated"])
        x["n_inv"] += o["invalidated"] is not None
    for x in stats.values():
        n = x["n"]
        x.update(mean_move_ticks=x["sum_move"] / n, share_positive=x["positive"] / n, mean_mfe_ticks=x["sum_mfe"] / n,
                 mean_mae_ticks=x["sum_mae"] / n, share_invalidated=x["invalidated"] / x["n_inv"] if x["n_inv"] else None)  # fmt: skip
    return {"counts": dict(sorted(counts.items())), "outcomes": dict(sorted(stats.items()))}


def _write(cfg: Any, out: dict[str, Any]) -> None:
    d = Path(cfg.data_root) / "reports"
    d.mkdir(parents=True, exist_ok=True)
    (d / "auction_verification.json").write_text(json.dumps(out, indent=1, default=str))
    s = out["summary"]
    lines = [
        "# Auction verification (Phase 9)",
        "",
        f"Settings hash `{s['auction_settings_hash']}`.",
        "",
        "| Check | Result |",
        "|---|---|",
    ]
    for k in ("dates", "pass", "levels", "episode_events_checked", "shape_events_checked", "outcomes_checked", "states_checked",
              "timing_violations"):  # fmt: skip
        lines.append(f"| {k} | {s[k]} |")
    lines.append(f"| failing dates | {', '.join(s['fail']) or 'none'} |")
    lines += ["", "## Events by type, side and session", "", "| Type:side:session | Count |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in out["catalogue"]["counts"].items()]
    lines += ["", "## Outcomes (expected direction; ticks)", "", "| Type:session:horizon | n | mean move | share > 0 | mean MFE | mean MAE | invalidated |",
              "|---|---|---|---|---|---|---|"]  # fmt: skip
    for k, x in out["catalogue"]["outcomes"].items():
        lines.append(f"| {k} | {x['n']} | {x['mean_move_ticks']:.1f} | {x['share_positive']:.2f} | {x['mean_mfe_ticks']:.1f} | "
                     f"{x['mean_mae_ticks']:.1f} | {'n/a' if x['share_invalidated'] is None else format(x['share_invalidated'], '.2f')} |")  # fmt: skip
    for r in out["dates"]:
        if r["problems"]:
            lines += ["", f"## Problems {r['date']}", ""] + [f"- {p}" for p in r["problems"]]
    (d / "auction_verification.md").write_text("\n".join(lines) + "\n")
