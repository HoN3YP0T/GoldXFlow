"""Taker-order reconstruction and Big Trades (Phase 7). Definitions: docs/METRICS_ORDERFLOW_V1.md.

Binance splits one aggressive (taker) order into many fills. A reconstructed taker order is a maximal run of
consecutive trades (delivery order = trade-id order) where each next trade
  * has the same exchange-reported aggressor side (UNKNOWN-side trades never join; each is its own order),
  * is at most `cluster_ms` later (default 0: the same exchange timestamp),
  * moves in the aggressor's direction or stays (buy: price >= previous; sell: price <= previous), and
  * steps at most one sweep bin (`sweep_bin_ticks`) from the previous fill.
Binance does not publish order ids, so two different taker orders in the same millisecond, same side and adjacent
prices are indistinguishable and merge; the validation report measures this against aggTrades.

An order is complete once a trade that cannot extend it arrives, or once every trade with time <= last fill time +
cluster_ms has been delivered. Its confirmation time is fixed: last_fill_time + cluster_ms + 1 + availability shift,
whatever chunking delivered it, so results don't depend on chunk boundaries.

Big Trade thresholds (enabled ones combined with `any` or `all`):
  abs_lots       order size >= abs_lots
  notional_usdt  Σ price*qty >= notional (exact: tick*lots * 1e-5)
  quantile       size >= nearest-rank q-quantile of all taker-order sizes confirmed in the trailing lookback,
                 refreshed every update_ms (rolling.RollingQuantile)
  time_of_day    size >= q-quantile of the same time-of-day bucket over the previous `days` days
A criterion without enough history is NOT READY: it can't pass, and the event records it.
"""

from __future__ import annotations

import json
from fractions import Fraction
from typing import Any

import numpy as np
from numpy.typing import NDArray

from .rolling import DailyBucketBaseline, RollingQuantile
from .settings import frac

ORDER_COLS = ("side", "first_t", "last_t", "size", "notional", "n", "first_id", "last_id", "first_p", "last_p", "min_p", "max_p")
SIDE_NAME = {1: "BUY", -1: "SELL", 0: "UNKNOWN"}


class OrderIds:
    """Lazy access to each completed order's trade ids: only Big Trades materialise them."""

    def __init__(self, keep: int) -> None:
        self.keep = keep
        self.parts: list[tuple[list[int] | None, NDArray[Any] | None, int, int]] = []  # (carried ids, chunk ids, a, b)

    def add(self, carry: list[int] | None, chunk: NDArray[Any] | None, a: int, b: int) -> None:
        self.parts.append((carry, chunk, a, b))

    def extend(self, other: "OrderIds") -> None:
        self.parts.extend(other.parts)

    def __len__(self) -> int:
        return len(self.parts)

    def get(self, i: int) -> list[int] | None:
        carry, chunk, a, b = self.parts[i]
        if carry is None and a < 0:
            return None  # the carried part was already too long
        out = list(carry or [])
        if chunk is not None:
            out += chunk[a:b].tolist()
        return out if len(out) <= self.keep else None


class TakerOrderBuilder:
    def __init__(self, cluster_ms: int, sweep_bin_ticks: int, keep_ids: int):
        self.cluster_ms, self.sb, self.keep_ids = cluster_ms, sweep_bin_ticks, keep_ids
        self.open: dict[str, Any] | None = None  # aggregates of the order still open, plus its trade ids (capped)

    def on_trades(
        self, t: NDArray[Any], ids: NDArray[Any], p: NDArray[Any], q: NDArray[Any], side: NDArray[Any]
    ) -> tuple[dict[str, NDArray[Any]], OrderIds]:
        """Completed orders (columns, in completion order) and lazy access to their trade ids."""
        n = len(t)
        done_ids = OrderIds(self.keep_ids)
        if n == 0:
            return _empty(), done_ids
        t = np.asarray(t, np.int64)
        ids = np.asarray(ids, np.int64)
        p = np.asarray(p, np.int64)
        q = np.asarray(q, np.int64)
        side = np.asarray(side, np.int64)
        o = self.open
        pt = np.empty(n, np.int64)
        pp = np.empty(n, np.int64)
        ps = np.empty(n, np.int64)
        pt[1:], pp[1:], ps[1:] = t[:-1], p[:-1], side[:-1]
        if o is not None:
            pt[0], pp[0], ps[0] = o["last_t"], o["last_p"], o["side"]
        else:
            pt[0], pp[0], ps[0] = -(2**62), 0, 0
        up = np.where(side > 0, p >= pp, p <= pp)
        step = np.abs(p // self.sb - pp // self.sb) <= 1
        ext = (side != 0) & (side == ps) & (t - pt <= self.cluster_ms) & up & step
        if o is None:
            ext[0] = False
        starts = np.flatnonzero(~ext)
        bounds = np.unique(np.concatenate([[0], starts])) if ext[0] else starts
        ends = np.append(bounds[1:], n)
        seg = {
            "side": side[bounds],
            "first_t": t[bounds],
            "last_t": t[ends - 1],
            "size": np.add.reduceat(q, bounds),
            "notional": np.add.reduceat(p * q, bounds),
            "n": (ends - bounds).astype(np.int64),
            "first_id": ids[bounds],
            "last_id": ids[ends - 1],
            "first_p": p[bounds],
            "last_p": p[ends - 1],
            "min_p": np.minimum.reduceat(p, bounds),
            "max_p": np.maximum.reduceat(p, bounds),
        }
        carry0: list[int] | None = []  # ids carried into segment 0 from the open order
        carry_bad = False
        done_cols: list[dict[str, NDArray[Any]]] = []
        if o is not None:
            if ext[0]:  # merge the open order into segment 0
                seg["side"][0] = o["side"]
                seg["first_t"][0] = o["first_t"]
                seg["size"][0] += o["size"]
                seg["notional"][0] += o["notional"]
                seg["n"][0] += o["n"]
                seg["first_id"][0] = o["first_id"]
                seg["first_p"][0] = o["first_p"]
                seg["min_p"][0] = min(o["min_p"], int(seg["min_p"][0]))
                seg["max_p"][0] = max(o["max_p"], int(seg["max_p"][0]))
                carry0 = o["ids"]
                carry_bad = o["ids"] is None
            else:  # the open order ended before this chunk
                done_cols.append({k: np.array([o[k]], np.int64) for k in ORDER_COLS})
                done_ids.add(o["ids"], None, 0 if o["ids"] is not None else -1, 0)
        k_last = len(bounds) - 1
        for j in range(k_last):  # completed segments
            a_, b_ = int(bounds[j]), int(ends[j])
            if j == 0 and carry_bad:
                done_ids.add(None, None, -1, 0)
            else:
                done_ids.add(carry0 if j == 0 else None, ids, a_, b_)
        if k_last > 0:
            done_cols.append({k: seg[k][:k_last] for k in ORDER_COLS})
        # the last segment stays open; keep its ids only while they fit
        a_l, b_l = int(bounds[k_last]), int(ends[k_last])
        prefix = carry0 if k_last == 0 else []
        if (k_last == 0 and carry_bad) or (len(prefix or []) + b_l - a_l > self.keep_ids):
            open_ids: list[int] | None = None
        else:
            open_ids = list(prefix or []) + ids[a_l:b_l].tolist()
        self.open = {k: int(seg[k][k_last]) for k in ORDER_COLS}
        self.open["ids"] = open_ids
        if not done_cols:
            return _empty(), done_ids
        return {k: np.concatenate([d[k] for d in done_cols]) for k in ORDER_COLS}, done_ids

    def close_upto(self, time_floor_ms: int) -> tuple[dict[str, NDArray[Any]], OrderIds]:
        """Complete the open order if no further fill can join it (all trades with time < floor were delivered)."""
        o = self.open
        ids = OrderIds(self.keep_ids)
        if o is not None and o["last_t"] + self.cluster_ms < time_floor_ms:
            self.open = None
            ids.add(o["ids"], None, 0 if o["ids"] is not None else -1, 0)
            return {k: np.array([o[k]], np.int64) for k in ORDER_COLS}, ids
        return _empty(), ids

    def reset(self) -> None:
        self.open = None

    def get_state(self) -> dict[str, Any]:
        return {"open": self.open}

    def set_state(self, s: dict[str, Any]) -> None:
        self.open = s.get("open") if s else None


def _empty() -> dict[str, NDArray[Any]]:
    return {k: np.empty(0, np.int64) for k in ORDER_COLS}


class BigTradeDetector:
    def __init__(self, cfg: dict[str, Any], shift_ms: int):
        self.cfg, self.shift = cfg, shift_ms
        self.cluster_ms = int(cfg["cluster_ms"])
        self.abs_lots = cfg["abs_lots"]
        # notional threshold in tick*lots (USDT * 1e5), exact
        self.notional_tl = None if cfg["notional_usdt"] is None else frac(cfg["notional_usdt"], "notional") * 100_000
        qc, tc = cfg["quantile"], cfg["time_of_day"]
        self.rq = (
            RollingQuantile(
                Fraction(str(qc["q"])), int(qc["lookback_hours"]) * 3_600_000, int(qc["min_samples"]), int(qc["update_ms"])
            )
            if qc["enabled"]
            else None
        )
        self.tod = (
            DailyBucketBaseline(
                Fraction(str(tc["q"])), int(tc["bucket_minutes"]), int(tc["days"]), int(tc["min_days"]), int(tc["keep_top"])
            )
            if tc["enabled"]
            else None
        )
        self.combine = cfg["combine"]
        self.orders_seen = 0
        self.not_ready = 0

    def confirm_ms(self, last_t: NDArray[Any]) -> NDArray[Any]:
        return last_t + self.cluster_ms + 1 + self.shift

    def process(self, orders: dict[str, NDArray[Any]], order_ids: "OrderIds") -> list[dict[str, Any]]:
        """Evaluate completed orders (in confirmation order); returns BIG_TRADE event rows."""
        n = len(orders["size"])
        if n == 0:
            return []
        self.orders_seen += n
        conf = self.confirm_ms(orders["last_t"])
        size, notional = orders["size"], orders["notional"]
        out: list[dict[str, Any]] = []
        upd = int(self.cfg["quantile"]["update_ms"]) if self.rq else 60_000
        bucket = int(self.cfg["time_of_day"]["bucket_minutes"]) * 60_000 if self.tod else 86_400_000
        grp = np.stack([conf // upd, conf // bucket], axis=1)
        brk = np.ones(n, bool)
        brk[1:] = (grp[1:] != grp[:-1]).any(axis=1)
        starts = np.flatnonzero(brk)
        ends = np.append(starts[1:], n)
        for a, b in zip(starts.tolist(), ends.tolist()):
            c0 = int(conf[a])
            crit: dict[str, Any] = {}
            passes: list[NDArray[Any]] = []
            if self.abs_lots is not None:
                passes.append(size[a:b] >= self.abs_lots)
                crit["abs_lots"] = {"threshold": int(self.abs_lots), "ready": True}
            if self.notional_tl is not None:
                passes.append(notional[a:b] * self.notional_tl.denominator >= self.notional_tl.numerator)
                crit["notional_usdt"] = {"threshold": str(self.cfg["notional_usdt"]), "ready": True}
            thr_q = None
            if self.rq is not None:
                thr_q, nq = self.rq.threshold(c0)
                crit["quantile"] = {
                    "q": str(self.cfg["quantile"]["q"]),
                    "threshold": thr_q,
                    "samples": nq,
                    "ready": thr_q is not None,
                }
                passes.append(size[a:b] >= thr_q if thr_q is not None else np.zeros(b - a, bool))
            if self.tod is not None:
                thr_t, nd, st = self.tod.threshold(c0)
                crit["time_of_day"] = {
                    "q": str(self.cfg["time_of_day"]["q"]),
                    "threshold": thr_t,
                    "days": nd,
                    "status": st,
                    "ready": thr_t is not None,
                }
                passes.append(size[a:b] >= thr_t if thr_t is not None else np.zeros(b - a, bool))
            if any(not v["ready"] for v in crit.values()):
                self.not_ready += b - a
            big = np.logical_or.reduce(passes) if self.combine == "any" else np.logical_and.reduce(passes)
            for ii in (np.flatnonzero(big) + a).tolist():
                out.append(self._event(orders, ii, int(conf[ii]), crit, order_ids.get(ii), thr_q))
            # samples are added after evaluation: an order never influences its own threshold
            if self.rq is not None:
                for i in range(a, b):
                    self.rq.add(int(conf[i]), int(size[i]))
            if self.tod is not None:
                for i in range(a, b):
                    self.tod.add(int(conf[i]), int(size[i]))
        return out

    def _event(
        self, o: dict[str, NDArray[Any]], i: int, conf: int, crit: dict[str, Any], ids: list[int] | None, thr_q: int | None
    ) -> dict[str, Any]:
        side = int(o["side"][i])
        size = int(o["size"][i])
        res = {}
        for k, v in crit.items():
            thr = v.get("threshold")
            if k == "notional_usdt":
                ok = int(o["notional"][i]) * self.notional_tl.denominator >= self.notional_tl.numerator  # type: ignore[union-attr]
            else:
                ok = thr is not None and size >= int(thr)
            res[k] = {**v, "pass": bool(ok)}
        details = {
            "criteria": res,
            "combine": self.combine,
            "fills": int(o["n"][i]),
            "first_price_ticks": int(o["first_p"][i]),
            "last_price_ticks": int(o["last_p"][i]),
            "min_price_ticks": int(o["min_p"][i]),
            "max_price_ticks": int(o["max_p"][i]),
            "notional_tick_lots": int(o["notional"][i]),
            "trade_ids_truncated": ids is None,
        }
        first_t, last_t = int(o["first_t"][i]), int(o["last_t"][i])
        return {
            "event_id": f"BIG_TRADE:{SIDE_NAME[side]}:{int(o['first_id'][i])}",
            "event_type": "BIG_TRADE",
            "status": "DETECTED",
            "parent_id": None,
            "side": SIDE_NAME[side],
            "interval": None,
            "bin_ticks": None,
            "evidence_start_ms": first_t,
            "evidence_end_ms": last_t + 1,
            "event_time_ms": conf,
            "ref_price_ticks": int(o["notional"][i]) // size,
            "value_lots": size,
            "threshold_lots": thr_q,
            "unknown_lots": size if side == 0 else 0,
            "first_trade_id": int(o["first_id"][i]),
            "last_trade_id": int(o["last_id"][i]),
            "n_trades": int(o["n"][i]),
            "trade_ids": ids,
            "details": json.dumps(details, sort_keys=True),
        }

    def reset(self) -> None:
        pass  # thresholds use trailing history only; a seek does not invalidate past samples

    def get_state(self) -> dict[str, Any]:
        return {
            "rq": self.rq.get_state() if self.rq else None,
            "tod": self.tod.get_state() if self.tod else None,
            "orders_seen": self.orders_seen,
            "not_ready": self.not_ready,
        }

    def set_state(self, s: dict[str, Any]) -> None:
        if not s:
            return
        if self.rq and s.get("rq"):
            self.rq.set_state(s["rq"])
        if self.tod and s.get("tod"):
            self.tod.set_state(s["tod"])
        self.orders_seen, self.not_ready = s["orders_seen"], s["not_ready"]
