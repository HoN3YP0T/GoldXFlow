"""Phase 7: independent verification of the persisted orderflow dataset (``orderflow-verify``).

Everything is recomputed from the Phase 3 trades partitions with separate, deliberately simple code (Python loops and
dicts where the engine is vectorised), and compared **exactly**:

1. **Flow bars**: OHLC, volume, buy/sell/unknown lots, delta, counts, first/last trade id, and the intra-bar delta
   high/low at base resolution. Every bar with trades must have a row and vice versa.
2. **CVD**: open/high/low/close of every row inside its reset instance (rows with coverage PARTIAL_START are counted
   and skipped: their offset is unknown by definition).
3. **Imbalances and stacked imbalances**: the full set per bar.
4. **Big Trades**: taker orders rebuilt fill by fill, thresholds recomputed from scratch for every order (nearest-rank
   quantile on the documented histogram), and the set of big trades with every field and every fill id.
5. **Events**: invariants (no back-dating: event_time >= evidence_end; available_at = event_time + shift; each
   resolution follows its candidate; EXPIRED exactly at the deadline), the evidence of every absorption and
   exhaustion candidate recomputed from trades, and every resolution re-derived from the base-bar price path.
6. **Phase 5 totals**: Σ flow-bar volume per date equals the trades partition.
It also counts events by session (asia / london / new_york / outside) for the event catalogue.
Writes data/reports/orderflow_verification.{json,md}.
"""

from __future__ import annotations

import json
from datetime import date
from fractions import Fraction
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from numpy.typing import NDArray

from ..normalize import part_path
from ..processing.sessions import SessionCalendar
from ..processing.settings import ProcessingSettings
from ..timeutil import DAY_MS, iso
from .big_trades import EDGES, N_BINS, size_bin
from .flow_settings import OrderflowSettings
from .flow_store import read_manifest, read_table

I64 = np.int64
RECOVERED = "RECOVERED_FROM_AGGTRADES"
SIDE = {1: "BUY", -1: "SELL", 0: "UNKNOWN"}


def _ms(d: str) -> int:
    return (date.fromisoformat(d).toordinal() - date(1970, 1, 1).toordinal()) * DAY_MS


def _rows(t: pa.Table) -> list[dict[str, Any]]:
    cols = [t.column(f.name).cast(pa.int64()) if pa.types.is_timestamp(f.type) else t.column(f.name) for f in t.schema]
    return pa.Table.from_arrays(cols, names=t.column_names).to_pylist()  # type: ignore[no-any-return]


class Trades:
    """Trades in delivery order (Phase 3 partition order) with a per-day cache."""

    def __init__(self, cfg: Any) -> None:
        self.cfg = cfg
        st = ProcessingSettings.from_config(cfg)
        self.time_col = st.trade_time_col
        self.include_recovered = st.include_recovered
        self.cache: dict[int, dict[str, NDArray[Any]]] = {}

    def day(self, dn: int) -> dict[str, NDArray[Any]]:
        if dn in self.cache:
            return self.cache[dn]
        d = date.fromordinal(date(1970, 1, 1).toordinal() + dn)
        p = part_path(self.cfg.data_root, self.cfg.symbol, "trades", str(d))
        if not p.exists():
            out = {k: np.zeros(0, I64) for k in ("t", "id", "p", "q", "side")}
        else:
            t = pq.read_table(
                p, columns=[self.time_col, "source_trade_id", "price_ticks", "quantity_lots", "buyer_maker_flag", "record_origin"]
            )
            if not self.include_recovered:
                t = t.filter(pc.invert(pc.fill_null(pc.equal(t.column("record_origin").cast(pa.string()), RECOVERED), False)))
            bm = t.column("buyer_maker_flag").to_pylist()
            out = {
                "t": t.column(self.time_col).cast(pa.int64()).to_numpy(),
                "id": t.column("source_trade_id").to_numpy().astype(I64),
                "p": t.column("price_ticks").to_numpy().astype(I64),
                "q": t.column("quantity_lots").to_numpy().astype(I64),
                "side": np.array([0 if b is None else (-1 if b else 1) for b in bm], np.int8),
            }
        self.cache[dn] = out
        for k in [k for k in self.cache if k < dn - 9]:
            del self.cache[k]
        return out

    def between(self, a: int, b: int) -> dict[str, NDArray[Any]]:
        parts = []
        for dn in range(a // DAY_MS, (b - 1) // DAY_MS + 1):
            x = self.day(dn)
            m = (x["t"] >= a) & (x["t"] < b)
            parts.append({k: v[m] for k, v in x.items()})
        return {k: np.concatenate([p[k] for p in parts]) for k in parts[0]}


# ============================================================================ 1. bars
def _slice(tr: dict[str, NDArray[Any]], a: int, b: int) -> slice:
    """Trades are in time order: [a, b) is one contiguous slice."""
    return slice(int(np.searchsorted(tr["t"], a, "left")), int(np.searchsorted(tr["t"], b, "left")))


def _bar_ref(tr: dict[str, NDArray[Any]], s: int, I: int, base: int) -> dict[str, Any] | None:
    m = _slice(tr, s, s + I)
    if m.stop <= m.start:
        return None
    t, i, p, q, sd = tr["t"][m], tr["id"][m], tr["p"][m], tr["q"][m], tr["side"][m]
    buy, sell, unk = int(q[sd == 1].sum()), int(q[sd == -1].sum()), int(q[sd == 0].sum())
    sec = (t - s) // base
    signed = np.where(sd == 1, q, np.where(sd == -1, -q, 0))
    path = np.cumsum(np.bincount(sec, weights=signed).astype(I64))  # running delta after each base bar
    hi, lo = max(0, int(path.max())), min(0, int(path.min()))
    return {"open_ticks": int(p[0]), "high_ticks": int(p.max()), "low_ticks": int(p.min()), "close_ticks": int(p[-1]),
            "volume_lots": int(q.sum()), "buy_lots": buy, "sell_lots": sell, "unknown_lots": unk, "delta_lots": buy - sell,
            "trade_count": m.stop - m.start, "buy_count": int((sd == 1).sum()), "sell_count": int((sd == -1).sum()),
            "first_trade_id": int(i.min()), "last_trade_id": int(i.max()), "delta_high": hi, "delta_low": lo}  # fmt: skip


# ============================================================================ 3. imbalances (independent, dict based)
def _imb_ref(tr: dict[str, NDArray[Any]], s: int, I: int, ip: Any, max_unknown: Fraction
             ) -> tuple[set[tuple[Any, ...]], set[tuple[Any, ...]]]:  # fmt: skip
    m = _slice(tr, s, s + I)
    vol, unk = int(tr["q"][m].sum()), int(tr["q"][m][tr["side"][m] == 0].sum())
    if vol and unk * max_unknown.denominator > max_unknown.numerator * vol:
        return set(), set()  # side quality too low: no imbalance is evaluated on this bar
    lv: dict[int, list[int]] = {}
    for p, q, sd in zip(tr["p"][m].tolist(), tr["q"][m].tolist(), tr["side"][m].tolist()):
        x = lv.setdefault(p // ip.bin_ticks, [0, 0])
        if sd == 1:
            x[0] += q
        elif sd == -1:
            x[1] += q
    r: Fraction = ip.ratio
    out = set()
    sides: dict[int, list[int]] = {1: [], -1: []}
    for b in sorted(lv):
        buy, sell = lv[b]
        ob = lv.get(b - 1, [0, 0])[1] if ip.method == "diagonal" else sell
        os_ = lv.get(b + 1, [0, 0])[0] if ip.method == "diagonal" else buy
        for side, xv, o in ((1, buy, ob), (-1, sell, os_)):
            if xv >= ip.min_lots and Fraction(xv) >= r * o and (o > 0 or ip.zero_policy == "any"):
                out.add((s, SIDE[side], b * ip.bin_ticks, xv, o))
                sides[side].append(b)
    stacked = set()
    for side, bins in sides.items():
        run: list[int] = []
        seq: list[int | None] = [*bins, None]
        for bb in seq:
            if run and (bb is None or bb != run[-1] + 1):
                if len(run) >= ip.stack_n:
                    stacked.add((s, SIDE[side], run[0] * ip.bin_ticks, run[-1] * ip.bin_ticks, len(run)))
                run = []
            if bb is not None:
                run.append(bb)
    return out, stacked


# ============================================================================ 4. big trades
class OrderRebuild:
    """Taker orders rebuilt fill by fill (plain Python), carried across day boundaries."""

    def __init__(self, cluster_ms: int, max_step: int) -> None:
        self.c, self.ms = cluster_ms, max_step
        self.cur: dict[str, Any] | None = None
        self.done: list[dict[str, Any]] = []

    def feed(self, tr: dict[str, NDArray[Any]]) -> None:
        for t, i, p, q, sd in zip(tr["t"].tolist(), tr["id"].tolist(), tr["p"].tolist(), tr["q"].tolist(), tr["side"].tolist()):
            o = self.cur
            if o is not None and sd == o["side"] and t - o["last_time"] <= self.c and abs(p - o["last_price"]) <= self.ms \
                    and (p - o["last_price"]) * sd >= 0:  # fmt: skip
                o["last_time"], o["last_price"] = t, p
                o["lots"] += q
                o["notional"] += p * q
                o["ids"].append(i)
                o["min_p"], o["max_p"] = min(o["min_p"], p), max(o["max_p"], p)
                continue
            if o is not None:
                self.done.append(o)
            self.cur = {"side": sd, "first_time": t, "last_time": t, "first_price": p, "last_price": p, "lots": q,
                        "notional": p * q, "ids": [i], "min_p": p, "max_p": p}  # fmt: skip

    def close_if(self, floor: int) -> None:
        if self.cur is not None and self.cur["last_time"] + self.c < floor:
            self.done.append(self.cur)
            self.cur = None


def _q_edge(sizes: NDArray[Any], q: Fraction, min_samples: int) -> tuple[int | None, int]:
    n = len(sizes)
    if n < min_samples or n == 0:
        return None, n
    h = np.bincount(size_bin(np.asarray(sizes, I64)).astype(I64), minlength=N_BINS)
    r = -(-q.numerator * n // q.denominator)
    c = 0
    for j in range(N_BINS):
        c += int(h[j])
        if c >= r:
            return int(EDGES[j]), n
    return int(EDGES[-1]), n


def big_trades_ref(cfg: Any, tr: Trades, start: int, end: int, cal: SessionCalendar) -> list[dict[str, Any]]:
    fs = OrderflowSettings.from_config(cfg)
    b = fs.big
    rb = OrderRebuild(b.cluster_ms, b.max_step_ticks)
    for dn in range(start // DAY_MS, (end - 1) // DAY_MS + 1):
        rb.feed(tr.between(max(start, dn * DAY_MS), min(end, (dn + 1) * DAY_MS)))
    rb.close_if(end)
    orders = rb.done
    lt = np.array([o["last_time"] for o in orders], I64)
    la = np.array([o["lots"] for o in orders], I64)
    wk_all = cal.is_weekend(lt) if len(lt) else np.zeros(0, bool)
    o_day = lt // DAY_MS
    o_bk = (lt % DAY_MS) // b.tod.window_ms
    out = []
    minute_cache: dict[int, tuple[int | None, int]] = {}
    tod_cache: dict[tuple[int, int, int], tuple[int | None, int]] = {}
    for k, o in enumerate(orders):
        L = o["lots"]
        flags: dict[str, bool | None] = {"qty": None, "notional": None, "rolling": None, "tod": None}
        if b.min_qty_lots is not None:
            flags["qty"] = L >= b.min_qty_lots
        if b.min_notional_tick_lots is not None:
            flags["notional"] = o["notional"] >= b.min_notional_tick_lots
        th_r, n_r, th_t, n_t = None, 0, None, 0
        R = o["last_time"] // 60_000 * 60_000
        if b.rolling.enabled:
            if R not in minute_cache:  # orders that ended in [R - H, R)
                i0, i1 = int(np.searchsorted(lt, R - b.rolling.window_ms, "left")), int(np.searchsorted(lt, R, "left"))
                minute_cache[R] = _q_edge(la[i0:i1], b.rolling.q, b.rolling.min_samples)
            th_r, n_r = minute_cache[R]
            flags["rolling"] = th_r is not None and L >= th_r
        wk = bool(wk_all[k])
        if b.tod.enabled:
            d, bk = int(o_day[k]), int(o_bk[k])
            ck = (d, bk, int(wk))
            if ck not in tod_cache:
                same = (o_bk == bk) & (o_day < d) & (wk_all == wk)
                days = sorted(set(o_day[same].tolist()))[-b.tod.lookback_days :]
                tod_cache[ck] = _q_edge(la[same & np.isin(o_day, days)], b.tod.q, b.tod.min_samples)
            th_t, n_t = tod_cache[ck]
            flags["tod"] = th_t is not None and L >= th_t
        en = [v for v in flags.values() if v is not None]
        if b.combine == "any":
            big = any(en)
        else:
            big = all(en) and (th_r is not None or not b.rolling.enabled) and (th_t is not None or not b.tod.enabled)
        if big:
            out.append({"first_trade_id": o["ids"][0], "last_trade_id": o["ids"][-1], "side": SIDE[o["side"]],
                        "first_time": o["first_time"], "last_time": o["last_time"], "n_fills": len(o["ids"]), "lots": L,
                        "notional_tick_lots": o["notional"], "first_price_ticks": o["first_price"], "last_price_ticks": o["last_price"],
                        "min_price_ticks": o["min_p"], "max_price_ticks": o["max_p"], "trade_ids": o["ids"],
                        "pass_qty": flags["qty"], "pass_notional": flags["notional"], "pass_rolling": flags["rolling"],
                        "pass_time_of_day": flags["tod"], "rolling_threshold_lots": th_r, "rolling_samples": n_r,
                        "tod_threshold_lots": th_t, "tod_samples": n_t, "weekend": wk,
                        "available_at": o["last_time"] + b.cluster_ms + 1})  # fmt: skip
    return out


# ============================================================================ 5. events
def _path(tr: Trades, a: int, b: int, base: int) -> tuple[list[int], list[int], list[int]]:
    """(start, high, low) per base bar with trades in [a, b)."""
    x = tr.between(a, b)
    out: dict[int, list[int]] = {}
    for t, p in zip(x["t"].tolist(), x["p"].tolist()):
        s = t - t % base
        r = out.setdefault(s, [p, p])
        r[0], r[1] = max(r[0], p), min(r[1], p)
    ks = sorted(out)
    return ks, [out[k][0] for k in ks], [out[k][1] for k in ks]


def _resolve_ref(e: dict[str, Any], tr: Trades, base: int, end_ms: int) -> tuple[str, int, int | None, str | None] | None:
    direction = -1 if e["side"] in ("BUY", "BEARISH") else 1
    lim = min(int(e["deadline"]), end_ms)
    ks, hs, ls = _path(tr, int(e["evidence_end"]), lim, base)
    c, v = e["confirm_ticks"], e["invalidate_ticks"]
    for s, h, lw in zip(ks, hs, ls):
        if direction < 0:
            ch, vh = c is not None and lw <= c, v is not None and h >= v
        else:
            ch, vh = c is not None and h >= c, v is not None and lw <= v
        if ch or vh:
            st = "CONFIRMED" if ch and not vh else "INVALIDATED"
            px = lw if (direction < 0) == (st == "CONFIRMED") else h
            return st, s + base, px, "both_in_same_base_bar" if (ch and vh) else None
    if end_ms >= int(e["deadline"]):
        return "EXPIRED", int(e["deadline"]), None, None
    return None


def _session_of(cal: SessionCalendar, t: int) -> str:
    for i in cal.instances(t, t, []):
        if i.kind == "session" and i.start_ms <= t < i.end_ms:
            return i.name
    return "outside"


def verify(cfg: Any, dates: list[str] | None = None, report: bool = True) -> dict[str, Any]:
    fs = OrderflowSettings.from_config(cfg)
    st = ProcessingSettings.from_config(cfg)
    cal = SessionCalendar.from_config(st.sessions_cfg)
    base, shift = st.base_ms, st.trade_shift_ms
    man = read_manifest(cfg.data_root, cfg.symbol)
    all_dates = sorted(d for d, v in man.get("dates", {}).items() if v.get("date_complete"))
    todo = [d for d in all_dates if not dates or d in dates]
    tr = Trades(cfg)
    observe = int(man.get("observe_from") or (_ms(all_dates[0]) if all_dates else 0))
    run_end = _ms(all_dates[-1]) + DAY_MS if all_dates else 0
    per: list[dict[str, Any]] = []
    summ: dict[str, Any] = {"dates": len(todo), "pass": 0, "fail": [], "orderflow_settings_hash": fs.hash[:16],
                            "bars_checked": 0, "cvd_rows_checked": 0, "cvd_rows_partial_start_skipped": 0,
                            "imbalances_checked": 0, "stacked_checked": 0, "big_trades_checked": 0, "events_checked": 0,
                            "resolutions_rederived": 0, "candidate_evidence_checked": 0, "no_backdating_violations": 0}  # fmt: skip
    events_all = [r for d in all_dates for r in _rows(read_table(cfg.data_root, cfg.symbol, "events", d))]
    first_status: dict[str, dict[str, Any]] = {}
    for e in sorted(events_all, key=lambda r: (r["available_at"], r["status"] != "CANDIDATE")):
        first_status.setdefault(e["event_id"], e)
    big_ref = {r["first_trade_id"]: r for r in big_trades_ref(cfg, tr, observe, run_end, cal)} if todo else {}
    catalogue: dict[str, dict[str, int]] = {}
    for d in todo:
        d0, d1 = _ms(d), _ms(d) + DAY_MS
        res: dict[str, Any] = {"date": d, "problems": []}
        P = res["problems"]
        # ---- 1. bars and 6. totals
        bars = _rows(read_table(cfg.data_root, cfg.symbol, "bars", d))
        window = tr.between(min([b["bar_start"] for b in bars] + [d0]), max([b["bar_end"] for b in bars] + [d1]))
        for iv in fs.bar_intervals:
            I = st.interval_ms[iv]
            mine = {b["bar_start"]: b for b in bars if b["interval"] == iv}
            exp_starts = set((window["t"][(window["t"] + I - window["t"] % I + shift > d0) & (window["t"] + I - window["t"] % I + shift <= d1)]
                              // I * I).tolist())  # fmt: skip
            exp_starts = {s for s in exp_starts if s >= observe - observe % I}
            if set(mine) != exp_starts:
                P.append(f"{iv}: bar set differs (missing {len(exp_starts - set(mine))}, extra {len(set(mine) - exp_starts)})")
            for s, b in mine.items():
                ref = _bar_ref(window, s, I, base)
                if ref is None or b["coverage"] == "PARTIAL_START":
                    continue
                bad = {k: (b[k], v) for k, v in ref.items() if b[k] != v}
                if bad:
                    P.append(f"{iv} bar {iso(s)}: {bad}")
                summ["bars_checked"] += 1
            if iv == fs.bar_intervals[0]:
                dm = (window["t"] >= d0 - shift) & (window["t"] < d1 - shift)
                tot = sum(b["volume_lots"] for b in mine.values() if b["available_at"] > d0 and b["available_at"] <= d1)
                if tot != int(window["q"][dm].sum()) and observe <= d0:
                    P.append(f"{iv}: flow-bar volume {tot} != trades partition {int(window['q'][dm].sum())}")
        # ---- 2. cvd: prefix sums of signed lots at base-bar boundaries (S(T) = sum of signed lots with time < T)
        cvd_rows = _rows(read_table(cfg.data_root, cfg.symbol, "cvd", d))
        full = [c for c in cvd_rows if c["coverage"] != "PARTIAL_START"]
        summ["cvd_rows_partial_start_skipped"] += len(cvd_rows) - len(full)
        if full:
            lo = min(c["series_start"] for c in full)
            hi = max(c["bar_end"] for c in full)
            x = tr.between(lo, hi)
            signed = np.where(x["side"] == 1, x["q"], np.where(x["side"] == -1, -x["q"], 0))
            nb = (hi - lo) // base + 1
            S = np.concatenate([[0], np.cumsum(np.bincount((x["t"] - lo) // base, weights=signed, minlength=nb).astype(I64))])

            def at(T: int) -> int:  # T is base-aligned
                return int(S[(T - lo) // base])

            for c in full:
                se = c["series_end"] if c["series_end"] is not None else 2**62
                a, b_ = max(c["bar_start"], c["series_start"]), min(c["bar_end"], se)
                z = at(c["series_start"])
                seg = S[(a - lo) // base + 1 : (b_ - lo) // base + 1] - z
                opn = at(a) - z
                ref = {
                    "cvd_open": opn,
                    "cvd_high": max(opn, int(seg.max())),
                    "cvd_low": min(opn, int(seg.min())),
                    "cvd_close": at(b_) - z,
                }
                bad = {k: (c[k], v) for k, v in ref.items() if c[k] != v}
                if bad:
                    P.append(f"cvd {c['series_key']} {c['interval']} {iso(c['bar_start'])}: {bad}")
                summ["cvd_rows_checked"] += 1
        # ---- 3. imbalances
        ip = fs.imbalance
        I = st.interval_ms[ip.interval]
        imb = _rows(read_table(cfg.data_root, cfg.symbol, "imbalances", d))
        stk = _rows(read_table(cfg.data_root, cfg.symbol, "stacked", d))
        got_i = {(r["bar_start"], r["side"], r["price_ticks"], r["lots"], r["opposite_lots"]) for r in imb}
        got_s = {(r["bar_start"], r["side"], r["low_price_ticks"], r["high_price_ticks"], r["n_bins"]) for r in stk}
        ref_i: set[tuple[Any, ...]] = set()
        ref_s: set[tuple[Any, ...]] = set()
        iv_bars = [b for b in bars if b["interval"] == ip.interval] if ip.interval in fs.bar_intervals else None
        starts = sorted({b["bar_start"] for b in iv_bars}) if iv_bars is not None else sorted({r[0] for r in got_i})
        for s in starts:
            a, b2 = _imb_ref(window, s, I, ip, fs.max_unknown_share)
            ref_i |= a
            ref_s |= b2
        if iv_bars is None:  # imbalance interval not persisted as bars: only check what was emitted is right
            ref_i = {x for x in ref_i if x[0] in {g[0] for g in got_i}}
            ref_s = {x for x in ref_s if x[0] in {g[0] for g in got_s}}
        if got_i != ref_i:
            P.append(f"imbalances differ: missing {len(ref_i - got_i)}, extra {len(got_i - ref_i)}")
        if got_s != ref_s:
            P.append(f"stacked differ: missing {len(ref_s - got_s)}, extra {len(got_s - ref_s)}")
        summ["imbalances_checked"] += len(got_i)
        summ["stacked_checked"] += len(got_s)
        # ---- 4. big trades
        bt = _rows(read_table(cfg.data_root, cfg.symbol, "big_trades", d))
        exp = {k: v for k, v in big_ref.items() if d0 < v["available_at"] + shift <= d1}
        got = {r["first_trade_id"]: r for r in bt}
        if set(got) != set(exp):
            P.append(f"big trades differ: missing {len(set(exp) - set(got))}, extra {len(set(got) - set(exp))}")
        for k in set(got) & set(exp):
            bad = {f: (got[k][f], v) for f, v in exp[k].items() if f != "available_at" and got[k][f] != v}
            if got[k]["available_at"] != exp[k]["available_at"] + shift:
                bad["available_at"] = (got[k]["available_at"], exp[k]["available_at"] + shift)
            if bad:
                P.append(f"big trade {k}: {bad}")
        summ["big_trades_checked"] += len(got)
        cat = catalogue.setdefault(d, {})
        for r in bt:
            key = f"BIG_TRADE:{r['side']}:{_session_of(cal, r['first_time'])}"
            cat[key] = cat.get(key, 0) + 1
        # ---- 5. events
        ev = [r for r in events_all if d0 < r["available_at"] <= d1]
        for e in ev:
            summ["events_checked"] += 1
            if e["event_time"] < e["evidence_end"]:
                summ["no_backdating_violations"] += 1
                P.append(f"event {e['event_id']} {e['status']}: event_time before evidence end")
            if e["available_at"] != e["event_time"] + shift:
                P.append(f"event {e['event_id']} {e['status']}: available_at != event_time + shift")
            first = first_status[e["event_id"]]
            if e["status"] in ("CANDIDATE",) or (e["event_type"] == "DIVERGENCE" and e is first):
                if e["detected_at"] != e["evidence_end"] or e["event_time"] != e["evidence_end"]:
                    P.append(f"event {e['event_id']}: detection not at evidence end")
                key = f"{e['event_type']}:{e['side']}:{_session_of(cal, e['detected_at'] - 1)}"
                cat[key] = cat.get(key, 0) + 1
                if e["event_type"] in ("ABSORPTION", "EXHAUSTION"):
                    P += _check_evidence(e, tr, base)
                    summ["candidate_evidence_checked"] += 1
            else:
                if first["available_at"] > e["available_at"] or first is e:
                    P.append(f"event {e['event_id']} {e['status']}: resolution without an earlier first row")
                if e["status"] == "EXPIRED" and e["event_time"] != e["deadline"]:
                    P.append(f"event {e['event_id']}: EXPIRED off the deadline")
                ref_r = _resolve_ref(e, tr, base, run_end)
                got_r = (e["status"], e["event_time"], e["resolution_price_ticks"], e["reason"])
                if ref_r != got_r:
                    P.append(f"event {e['event_id']}: resolution {got_r} != recomputed {ref_r}")
                summ["resolutions_rederived"] += 1
        res["ok"] = not P
        per.append(res)
        if P:
            summ["fail"].append(d)
        else:
            summ["pass"] += 1
    totals: dict[str, int] = {}
    for c in catalogue.values():
        for k, v in c.items():
            totals[k] = totals.get(k, 0) + v
    out = {"summary": summ, "dates": per, "catalogue_by_date_and_session": catalogue, "catalogue_totals": dict(sorted(totals.items())),
           "event_status_totals": _status_totals(events_all)}  # fmt: skip
    if report:
        _write_report(cfg, out)
    return out


def _check_evidence(e: dict[str, Any], tr: Trades, base: int) -> list[str]:
    x = tr.between(int(e["evidence_start"]), int(e["evidence_end"]))
    side = 1 if e["side"] == "BUY" else -1
    det = json.loads(e["detail"])
    if e["event_type"] == "ABSORPTION":
        m = (x["side"] == side) & (x["p"] >= e["level_low_ticks"]) & (x["p"] <= e["level_high_ticks"])
        zl = int(x["q"][m].sum())
        secs = (x["t"][m] - x["t"][m] % base).tolist()
        per: dict[int, int] = {}
        for s, q in zip(secs, x["q"][m].tolist()):
            per[s] = per.get(s, 0) + q
        ok = (
            zl == e["lots"] == det["zone_lots"]
            and len(per) == det["active_base_bars"]
            and max(per.values()) == det["max_base_bar_lots"]
        )
        ok = ok and int(x["q"][x["side"] == side].sum()) == det["side_lots"] and int(x["q"].sum()) == det["bar_volume_lots"]
        return [] if ok else [f"absorption {e['event_id']}: evidence does not recompute"]
    probs = []
    for push in det["pushes"]:
        s = push["bar_start"]
        I = int(e["evidence_end"]) - int(det["pushes"][-1]["bar_start"])
        y = tr.between(s, s + I)
        ext = int(y["p"].max() if side > 0 else y["p"].min())
        lots = int(y["q"][y["side"] == side].sum())
        if ext != push["extreme_ticks"] or lots != push["aggressor_lots"]:
            probs.append(f"exhaustion {e['event_id']}: push {iso(s)} does not recompute")
    return probs


def _status_totals(events: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for e in events:
        k = f"{e['event_type']}:{e['status']}"
        out[k] = out.get(k, 0) + 1
    return dict(sorted(out.items()))


def _write_report(cfg: Any, out: dict[str, Any]) -> None:
    d = Path(cfg.data_root) / "reports"
    d.mkdir(parents=True, exist_ok=True)
    (d / "orderflow_verification.json").write_text(json.dumps(out, indent=1, default=str))
    s = out["summary"]
    lines = ["# Orderflow verification (Phase 7)", "", f"Settings hash `{s['orderflow_settings_hash']}`.", "",
             "| Check | Result |", "|---|---|"]  # fmt: skip
    for k in ("dates", "pass", "bars_checked", "cvd_rows_checked", "cvd_rows_partial_start_skipped", "imbalances_checked",
              "stacked_checked", "big_trades_checked", "events_checked", "candidate_evidence_checked", "resolutions_rederived",
              "no_backdating_violations"):  # fmt: skip
        lines.append(f"| {k} | {s[k]} |")
    lines.append(f"| failing dates | {', '.join(s['fail']) or 'none'} |")
    lines += ["", "## Event status totals", "", "| Event:status | Count |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in out["event_status_totals"].items()]
    lines += ["", "## Event catalogue by session (first emission per event; big trades by first fill)", "",
              "| Type:side:session | Count |", "|---|---|"]  # fmt: skip
    lines += [f"| {k} | {v} |" for k, v in out["catalogue_totals"].items()]
    lines += ["", "## Per date", "", "| Date | OK | Problems |", "|---|---|---|"]
    for r in out["dates"]:
        lines.append(f"| {r['date']} | {'yes' if r['ok'] else 'NO'} | {'; '.join(r['problems'][:3])} |")
    (d / "orderflow_verification.md").write_text("\n".join(lines) + "\n")
