"""Phase 5: the shared trade processor.

``TradeProcessor`` is a replay ``Consumer``. It takes the trades in each ``EventChunk`` (from historical replay now,
and from the Phase 14 live bus, which delivers the same ``EventChunk`` interface) and maintains:

* footprint bars for every configured interval (default 1s, 5s, 1m, 5m, 15m, 1h): OHLC from trades, volume,
  buy/sell/unknown lots, delta, counts, notional, first/last trade id, plus a per-price-level footprint;
* price-level windows: UTC day, trading day, trading week, each configured session, and custom ranges.

Rules:
* Idempotent. A per-instrument watermark (highest processed trade id) plus a bounded ring of recent ids. A trade
  whose id is <= watermark and already seen is ignored (``duplicates_ignored``). An id <= watermark that wasn't
  seen and is still inside the ring's span is a late arrival and is accepted (``late_accepted``). An id older
  than the ring's span can't be proven new, so it is dropped and counted (``unverifiable_dropped``). Nothing is
  dropped silently.
* No look-ahead. A bar or window [s, e) is *developing* until the clock reaches e + trade availability shift.
  Only then is it finalised and emitted, carrying ``available_at_ms`` = e + shift. Before then the
  same data is visible only through ``developing_*`` accessors, which label it as developing.
* Integer only. Volume is in lots (0.001 oz), price in ticks (0.01 USDT), notional in tick*lots, so totals are exact.
* Exchange-reported sides only enter buy/sell. UNKNOWN stays UNKNOWN. The optional tick rule fills separate
  INFERRED columns, only for UNKNOWN trades.
* Snapshot and restore. ``get_state``/``set_state`` capture everything, and the state is refused if the processing
  settings hash changed. Restore followed by continue equals an uninterrupted run (``state_digest``).
"""
from __future__ import annotations
from typing import Any
import numpy as np
from numpy.typing import NDArray
import pyarrow as pa
import pyarrow.compute as pc

from ..replay import Consumer, EventChunk, ReplayGuardError
from ..timeutil import iso
from .aggregation import Rows, LevelArrays, bars_from_rows, encode_arrays, decode_arrays, digest_tree
from .classify import classify_buyer_maker, side_codes_from_labels, tick_rule
from .sessions import SessionCalendar, Instance
from .settings import ProcessingSettings

DQ_RANK = {"OK": 0, "UNVERIFIED": 1, "PARTIAL": 2, "MISSING": 3}
DQ_NAMES = np.array(["OK", "UNVERIFIED", "PARTIAL", "MISSING"], dtype=object)
RECOVERED_ORIGIN = "RECOVERED_FROM_AGGTRADES"
STAT_KEYS = ("trades_received", "trades_processed", "duplicates_ignored", "late_accepted", "unverifiable_dropped",
             "recovered_included", "recovered_excluded", "side_mismatch_vs_phase3", "unknown_side_trades",
             "lots_processed", "buy_lots", "sell_lots", "unknown_lots", "windows_finalised", "seeks",
             "developing_discarded_on_seek")


class ProcessorSink:
    """Receives finalised outputs. Subclass and pass to ``TradeProcessor(sinks=[...])``.

    on_bars(interval, bars, levels, final_upto_ms)  finalised bars (columns) and their footprint levels
    on_window(summary, levels)                      a finalised window (totals dict and its 1-tick level columns)
    on_dq(start_ms, end_ms, status)                 a data-quality interval became known (Phase 3 catalog)
    on_advance(proc)                                end of every clock advance, after bars and windows (Phase 6)
    on_trades(t, ids, p, q, side)                   accepted trades (after dedup) in delivery order (Phase 7 taker orders)
    """
    name = "sink"
    def on_start(self, ctx: dict[str, Any], proc: "TradeProcessor") -> None: pass
    def on_bars(self, interval: str, bars: dict[str, Any], levels: dict[str, Any], final_upto_ms: int) -> None: pass
    def on_window(self, summary: dict[str, Any], levels: dict[str, Any]) -> None: pass
    def on_dq(self, start_ms: int, end_ms: int, status: str) -> None: pass
    def on_advance(self, proc: "TradeProcessor") -> None: pass
    def on_trades(self, t: NDArray[Any], ids: NDArray[Any], p: NDArray[Any], q: NDArray[Any], side: NDArray[Any]) -> None: pass
    def on_seek(self, t_ms: int) -> None: pass
    def on_stop(self, ctx: dict[str, Any], proc: "TradeProcessor") -> None: pass
    def get_state(self) -> dict[str, Any]: return {}
    def set_state(self, s: dict[str, Any]) -> None: pass


class TradeProcessor(Consumer):
    name = "trade_processor"

    def __init__(self, cfg: Any, sinks: list[Any] | tuple[Any, ...] = (), custom_windows: list[Any] | None = None) -> None:
        self.st = ProcessingSettings.from_config(cfg)
        self.cal = SessionCalendar.from_config(self.st.sessions_cfg)
        self.sinks = list(sinks)
        names = [s.name for s in self.sinks]
        if len(set(names)) != len(names): raise ValueError("sink names must be unique")
        self._custom0 = [dict(c) for c in (custom_windows or [])]
        for c in self._custom0:
            if c["end_ms"] <= c["start_ms"]: raise ValueError(f"custom window {c['name']}: end <= start")
            if c["start_ms"] % self.st.base_ms or c["end_ms"] % self.st.base_ms:
                raise ValueError(f"custom window {c['name']}: bounds must align to the base interval {self.st.intervals[0]}")
        self._tbl: pa.Table | None = None; self._arr: dict[str, Any] = {}
        self._reset()

    # ================================================================== state
    def _reset(self) -> None:
        st = self.st
        self.seq = 0
        self.watermark = -1
        self.ring = np.zeros(st.dedup_window_ids, np.int64); self.ring_pos = 0; self.ring_n = 0
        self.stats = {k: 0 for k in STAT_KEYS}
        self.late_after_bar_final = {n: 0 for n in st.intervals}
        self.late_after_window_final = 0
        self.bars_finalised = {n: 0 for n in st.intervals}
        self.dev = {n: Rows() for n in st.intervals}
        self.final_upto: dict[str, int | None] = {n: None for n in st.intervals}
        self.windows: dict[str, dict[str, Any]] = {}
        self.done_upto: dict[str, int] = {}
        self.dq: list[list[Any]] = []
        self.observe_from: int | None = None      # earliest trade time this processor is guaranteed to have seen
        self.filtered = False
        self.time_floor: int | None = None        # all trades with time < floor have been delivered
        self.clock: int | None = None
        self.tick_prev: int | None = None; self.tick_dir = 0
        self.window_log: list[dict[str, Any]] = []
        self.custom = [dict(c) for c in self._custom0]

    def add_custom_window(self, name: str, start_ms: int, end_ms: int, label: str = "custom") -> None:
        """Register a fixed custom range. It is aggregated like a session and finalised at end + shift."""
        if end_ms <= start_ms or start_ms % self.st.base_ms or end_ms % self.st.base_ms:
            raise ValueError("custom window bounds must be ordered and aligned to the base interval")
        self.custom.append({"name": name, "label": label, "start_ms": int(start_ms), "end_ms": int(end_ms)})

    def _ring_ordered(self) -> NDArray[Any]:
        W = len(self.ring)
        if self.ring_n < W: return self.ring[:self.ring_n].copy()
        return np.concatenate([self.ring[self.ring_pos:], self.ring[:self.ring_pos]])

    def _tree(self) -> dict[str, Any]:
        ring = self._ring_ordered()
        return {"settings_hash": self.st.hash, "seq": self.seq, "watermark": self.watermark,
                "ring_delta": np.diff(ring, prepend=np.int64(0)) if len(ring) else ring, "ring_n": self.ring_n,
                "stats": dict(self.stats), "late_after_bar_final": dict(self.late_after_bar_final),
                "late_after_window_final": self.late_after_window_final, "bars_finalised": dict(self.bars_finalised),
                "dev": {n: self.dev[n].c for n in self.st.intervals}, "final_upto": dict(self.final_upto),
                "windows": {k: {"inst": [w["inst"].kind, w["inst"].name, w["inst"].label, w["inst"].start_ms, w["inst"].end_ms],
                                "levels": w["arr"].nonzero(), "weekend_lots": w["weekend_lots"]} for k, w in sorted(self.windows.items())},
                "done_upto": dict(sorted(self.done_upto.items())), "dq": [list(x) for x in self.dq],
                "observe_from": self.observe_from, "filtered": self.filtered, "time_floor": self.time_floor, "clock": self.clock,
                "tick_prev": self.tick_prev, "tick_dir": self.tick_dir, "window_log": self.window_log, "custom": self.custom}

    def state_digest(self) -> str:
        """Digest of the complete processor state (not the sinks). Equal digests mean equal state."""
        return digest_tree(self._tree())

    def get_state(self) -> dict[str, Any]:
        t = self._tree()
        t["ring_delta"] = encode_arrays({"x": t["ring_delta"]})
        t["dev"] = {n: encode_arrays(c) for n, c in t["dev"].items()}
        t["windows"] = {k: {**w, "levels": encode_arrays(w["levels"])} for k, w in t["windows"].items()}
        t["sinks"] = {s.name: s.get_state() for s in self.sinks}
        return t

    def set_state(self, s: dict[str, Any]) -> None:
        if not s: return
        if s.get("settings_hash") != self.st.hash:
            raise ReplayGuardError(f"trade processor checkpoint was taken with processing settings {str(s.get('settings_hash'))[:12]}, "
                                   f"current settings are {self.st.hash[:12]}; start a new run name")
        self._reset()
        self.seq, self.watermark, self.ring_n = s["seq"], s["watermark"], s["ring_n"]
        ring = np.cumsum(decode_arrays(s["ring_delta"])["x"]).astype(np.int64)
        W = len(self.ring); self.ring[:len(ring)] = ring; self.ring_pos = len(ring) % W
        self.stats = {**self.stats, **s["stats"]}
        self.late_after_bar_final = dict(s["late_after_bar_final"]); self.late_after_window_final = s["late_after_window_final"]
        self.bars_finalised = dict(s["bars_finalised"])
        self.dev = {n: Rows(decode_arrays(s["dev"][n])) if s["dev"].get(n) else Rows() for n in self.st.intervals}
        self.final_upto = dict(s["final_upto"])
        for k, w in s["windows"].items():
            la = LevelArrays.from_state(w["levels"])
            self.windows[k] = {"inst": Instance(*w["inst"]), "arr": la, "weekend_lots": w["weekend_lots"]}
        self.done_upto = dict(s["done_upto"]); self.dq = [list(x) for x in s["dq"]]
        self.observe_from, self.filtered, self.time_floor, self.clock = s["observe_from"], s["filtered"], s["time_floor"], s["clock"]
        self.tick_prev, self.tick_dir = s["tick_prev"], s["tick_dir"]
        self.window_log = list(s["window_log"]); self.custom = list(s["custom"])
        for sk in self.sinks: sk.set_state((s.get("sinks") or {}).get(sk.name, {}))

    # ================================================================== consumer interface
    def on_start(self, ctx: dict[str, Any]) -> None:
        sh = self.st.trade_shift_ms
        if self.observe_from is None:
            self.observe_from = int(ctx["start_ms"]) - sh
            self.time_floor = self.observe_from
            self.filtered = bool(ctx.get("filtered"))
            for e in ctx.get("quality_before_start", []):
                if e["type"] == "DQ_BEGIN" and e["dataset"] in self.st.dq_datasets and e["end_ms"] > self.observe_from:
                    self.dq.append([e["start_ms"], e["end_ms"], e["status"]])
        for s in self.sinks: s.on_start(ctx, self)
        if not ctx.get("resumed"):
            for x in self.dq:
                for s in self.sinks: s.on_dq(x[0], x[1], x[2])

    def on_chunk(self, c: EventChunk) -> None:
        if "quality" in c.streams:
            qid = c.streams.index("quality")
            for r in c.rows[c.keys["stream"] == qid]:
                e = c.quality[r]
                if e["type"] == "DQ_BEGIN" and e["dataset"] in self.st.dq_datasets:
                    self.dq.append([e["start_ms"], e["end_ms"], e["status"]])
                    for s in self.sinks: s.on_dq(e["start_ms"], e["end_ms"], e["status"])
        if "trades" in c.streams and "trades" in c.tables:
            rows = c.stream_rows("trades")
            if len(rows):
                a = self._columns(c.tables["trades"])
                self.ingest(a["t"][rows], a["id"][rows], a["p"][rows], a["q"][rows], a["side"][rows], a["rec"][rows],
                            int(a["mismatch"][rows].sum()))
        self.advance(c.clock_ms)

    def on_seek(self, t_ms: int) -> None:
        """Skipped events are never delivered, so every developing aggregate is incomplete: discard it and restart
        coverage at t. Finalised outputs already emitted are unaffected."""
        n = sum(int(d["cnt"].sum()) for d in self.dev.values()) + sum(int(w["arr"].totals()["cnt"]) for w in self.windows.values())
        self.stats["developing_discarded_on_seek"] += n; self.stats["seeks"] += 1
        self.dev = {k: Rows() for k in self.st.intervals}; self.windows = {}
        self.observe_from = int(t_ms) - self.st.trade_shift_ms; self.time_floor = self.observe_from
        for k in self.final_upto: self.final_upto[k] = None
        self.done_upto = {}
        for s in self.sinks: s.on_seek(t_ms)

    def on_stop(self, ctx: dict[str, Any]) -> None:
        # A completed replay has delivered every event with availability < end_ms, so the clock is effectively end_ms.
        if ctx.get("status") == "completed" and ctx.get("end_ms") is not None:
            self.advance(max(self.clock or 0, int(ctx["end_ms"])))
        for s in self.sinks: s.on_stop(ctx, self)

    # ================================================================== input
    def _columns(self, tbl: pa.Table) -> dict[str, Any]:
        """numpy views of the columns the processor needs, cached per day table."""
        if self._tbl is tbl: return self._arr
        st = self.st
        inst = [x for x in pc.unique(tbl.column("instrument_id")).to_pylist()]
        if inst != [st.instrument_id] and len(tbl):
            raise ValueError(f"trades table holds instruments {inst}; this processor is for {st.instrument_id} only "
                             "(markets are never merged)")
        for col in ("price_ticks", "quantity_lots", "source_trade_id", st.trade_time_col):
            if tbl.column(col).null_count: raise ValueError(f"null {col} in trades; Phase 3 should have quarantined it")
        side = classify_buyer_maker(tbl.column("buyer_maker_flag"))
        ref = side_codes_from_labels(tbl.column("aggressor_side"))
        rec = pc.fill_null(pc.equal(tbl.column("record_origin").cast(pa.string()), RECOVERED_ORIGIN), False).to_numpy(zero_copy_only=False)
        self._arr = {"t": tbl.column(st.trade_time_col).cast(pa.int64()).to_numpy(),
                     "id": tbl.column("source_trade_id").to_numpy(), "p": tbl.column("price_ticks").to_numpy(),
                     "q": tbl.column("quantity_lots").to_numpy(), "side": side, "rec": rec.astype(bool),
                     "mismatch": side != ref}
        self._tbl = tbl
        return self._arr

    def _dedup(self, ids: NDArray[Any]) -> NDArray[Any]:
        n = len(ids)
        if n == 0: return np.zeros(0, bool)
        if ids[0] > self.watermark and (n == 1 or bool((ids[1:] > ids[:-1]).all())):
            return np.ones(n, bool)                           # fast path: strictly new and increasing
        acc = np.zeros(n, bool)
        _, first = np.unique(ids, return_index=True)
        acc[first] = True                                     # within-chunk repeats: keep the first delivery
        self.stats["duplicates_ignored"] += int(n - len(first))
        old = acc & (ids <= self.watermark)
        if old.any():
            ring = np.sort(self._ring_ordered())
            pos = np.searchsorted(ring, ids[old]); pos = np.minimum(pos, max(len(ring) - 1, 0))
            seen = (ring[pos] == ids[old]) if len(ring) else np.zeros(int(old.sum()), bool)
            full = self.ring_n >= len(self.ring)
            floor = int(ring[0]) if (full and len(ring)) else None
            beyond = (~seen) & (ids[old] < floor) if floor is not None else np.zeros(len(seen), bool)
            oi = np.flatnonzero(old)
            acc[oi[seen]] = False; acc[oi[beyond]] = False
            self.stats["duplicates_ignored"] += int(seen.sum())
            self.stats["unverifiable_dropped"] += int(beyond.sum())
            self.stats["late_accepted"] += int((~seen & ~beyond).sum())
        return acc

    def _ring_add(self, ids: NDArray[Any]) -> None:
        W = len(self.ring)
        if len(ids) >= W: ids = ids[-W:]
        k = len(ids); e = self.ring_pos + k
        if e <= W: self.ring[self.ring_pos:e] = ids
        else:
            f = W - self.ring_pos; self.ring[self.ring_pos:] = ids[:f]; self.ring[:k - f] = ids[f:]
        self.ring_pos = e % W; self.ring_n += k

    def ingest(self, t: NDArray[Any], ids: NDArray[Any], p: NDArray[Any], q: NDArray[Any], side: NDArray[Any], rec: NDArray[Any],
               mismatches: int = 0) -> None:
        """Process trades in delivery order. Arrays: time ms, trade id, price ticks, lots, side code, recovered flag.
        This is the single entry point shared by replay and (Phase 14) live."""
        st = self.st; S = self.stats
        S["trades_received"] += len(ids); S["side_mismatch_vs_phase3"] += mismatches
        acc = self._dedup(ids)
        if not st.include_recovered:
            S["recovered_excluded"] += int((acc & rec).sum()); acc &= ~rec
        if not acc.all():
            t, ids, p, q, side, rec = t[acc], ids[acc], p[acc], q[acc], side[acc], rec[acc]
        m = len(ids)
        if m == 0: return
        if self.observe_from is None:   # driven without on_start (live bus, direct use): coverage starts at the first trade
            self.observe_from = int(t.min()); self.time_floor = self.observe_from
        self._ring_add(ids); self.watermark = max(self.watermark, int(ids.max()))
        seq = np.arange(self.seq, self.seq + m, dtype=np.int64); self.seq += m
        q = q.astype(np.int64); p = p.astype(np.int64)
        isb, iss, isu = side == 1, side == -1, side == 0
        z = np.zeros(m, np.int64)
        if st.inferred_enabled:
            d, self.tick_prev, self.tick_dir = tick_rule(p, self.tick_prev, self.tick_dir)
            ibuy = np.where(isu & (d > 0), q, 0); isell = np.where(isu & (d < 0), q, 0)
        else:
            ibuy = isell = z
        S["trades_processed"] += m; S["recovered_included"] += int(rec.sum()); S["unknown_side_trades"] += int(isu.sum())
        S["lots_processed"] += int(q.sum()); S["buy_lots"] += int(q[isb].sum()); S["sell_lots"] += int(q[iss].sum())
        S["unknown_lots"] += int(q[isu].sum())
        for sk in self.sinks: sk.on_trades(t, ids, p, q, side)
        base = st.base_ms
        G = Rows({"bucket": t - t % base, "level": p, "buy": np.where(isb, q, 0), "sell": np.where(iss, q, 0),
                  "unk": np.where(isu, q, 0), "cnt": np.ones(m, np.int64), "bcnt": isb.astype(np.int64),
                  "scnt": iss.astype(np.int64), "rcnt": rec.astype(np.int64), "notional": p * q, "ibuy": ibuy, "isell": isell,
                  "first_ts": t, "last_ts": t, "first_seq": seq, "last_seq": seq, "min_id": ids, "max_id": ids}).reduce()
        # ---- bars: each interval is built from the largest finer interval that divides it (each step shrinks)
        new: dict[int, Rows] = {base: G}
        for n in st.intervals:
            I = st.interval_ms[n]
            if I not in new:
                src = max(k for k in new if I % k == 0)
                new[I] = new[src].with_key(bucket=new[src]["bucket"] - new[src]["bucket"] % I).reduce()
            r = new[I]
            fu = self.final_upto[n]
            if fu is not None:
                late = r["bucket"] < fu
                if late.any():
                    self.late_after_bar_final[n] += int(r["cnt"][late].sum()); r = r.take(~late)
            self.dev[n] = self.dev[n].merge_sorted(r)
        # ---- windows
        tmin, tmax = int(G["bucket"].min()), int(G["bucket"].max())
        self._open_windows(min(tmin, self.time_floor if self.time_floor is not None else tmin), tmax)
        late_w = np.zeros(len(G), bool)
        for end in self.done_upto.values():
            late_w |= G["bucket"] < end
        if late_w.any(): self._count_late_windows(G.take(late_w))
        wk = self.cal.is_weekend(G["bucket"]); vol = G["buy"] + G["sell"] + G["unk"]
        for w in self.windows.values():
            i = w["inst"]
            msk = (G["bucket"] >= i.start_ms) & (G["bucket"] < i.end_ms)
            if msk.any():
                w["arr"].add_rows(G, msk); w["weekend_lots"] += int(vol[msk & wk].sum())

    def _count_late_windows(self, L: Rows) -> None:
        a, b = int(L["bucket"].min()), int(L["bucket"].max())
        for inst in self.cal.instances(a, b, self.custom):
            end = self.done_upto.get(inst.series)
            if end is not None and inst.end_ms <= end:
                m = (L["bucket"] >= inst.start_ms) & (L["bucket"] < inst.end_ms)
                self.late_after_window_final += int(L["cnt"][m].sum())

    def _open_windows(self, a: int, b: int) -> None:
        for inst in self.cal.instances(a, b, self.custom):
            if inst.key in self.windows: continue
            if inst.end_ms <= self.done_upto.get(inst.series, -1): continue
            self.windows[inst.key] = {"inst": inst, "arr": LevelArrays(), "weekend_lots": 0}

    # ================================================================== clock / finalisation
    def advance(self, clock_ms: int) -> None:
        """Move the processor clock. Bars and windows ending at or before clock - shift are finalised."""
        st = self.st; sh = st.trade_shift_ms
        self.clock = clock_ms if self.clock is None else max(self.clock, clock_ms)
        X = self.clock - sh
        if self.observe_from is None: self.observe_from = X; self.time_floor = X
        if self.time_floor is not None and X > self.time_floor:
            self._open_windows(self.time_floor, X - 1)
        self.time_floor = X if self.time_floor is None else max(self.time_floor, X)
        for n in st.intervals:
            I = st.interval_ms[n]
            fu = (X // I) * I
            prev = self.final_upto[n]
            self.final_upto[n] = fu if prev is None else max(prev, fu)
            d = self.dev[n]
            if not len(d): continue
            fin = d["bucket"] + I <= X
            if fin.any():
                rows = d.take(fin); self.dev[n] = d.take(~fin)
                bars, levels = self._bar_outputs(n, rows)
                self.bars_finalised[n] += len(bars["bar_start"])
                for s in self.sinks: s.on_bars(n, bars, levels, self.final_upto[n])
        for k in sorted(self.windows, key=lambda k: (self.windows[k]["inst"].end_ms, k)):
            w = self.windows[k]; i = w["inst"]
            if i.end_ms <= X:
                summ, lv = self._window_output(w)
                self.window_log.append(summ); self.stats["windows_finalised"] += 1
                self.done_upto[i.series] = max(self.done_upto.get(i.series, i.end_ms), i.end_ms)
                del self.windows[k]
                for s in self.sinks: s.on_window(summ, lv)
        self._prune_dq()
        for s in self.sinks: s.on_advance(self)

    def _prune_dq(self) -> None:
        if not self.dq: return
        starts = [w["inst"].start_ms for w in self.windows.values()]
        starts += [int(d["bucket"].min()) for d in self.dev.values() if len(d)]
        floor = min(starts) if starts else (self.time_floor or 0)
        self.dq = [x for x in self.dq if x[1] > floor]

    def _dq_codes(self, s: NDArray[Any], e: NDArray[Any]) -> NDArray[Any]:
        code = np.zeros(len(s), np.int8)
        for a, b, status in self.dq:
            ov = (s < b) & (e > a)
            if ov.any(): code[ov] = np.maximum(code[ov], DQ_RANK.get(status, 3))
        return code

    def _coverage(self, start: NDArray[Any]) -> NDArray[Any]:
        cov = np.where(start < (self.observe_from or 0), "PARTIAL_START", "FILTERED" if self.filtered else "FULL")
        return cov.astype(object)

    def _bar_outputs(self, n: str, rows: Rows) -> tuple[dict[str, Any], dict[str, Any]]:
        st = self.st; I = st.interval_ms[n]
        b = bars_from_rows(rows)
        s = b["bucket"]; e = s + I
        vol = b["buy"] + b["sell"] + b["unk"]
        bars = {"bar_start": s, "bar_end": e, "available_at": e + st.trade_shift_ms,
                "open_ticks": b["open"], "high_ticks": b["high"], "low_ticks": b["low"], "close_ticks": b["close"],
                "volume_lots": vol, "buy_lots": b["buy"], "sell_lots": b["sell"], "unknown_lots": b["unk"],
                "delta_lots": b["buy"] - b["sell"], "trade_count": b["cnt"], "buy_count": b["bcnt"], "sell_count": b["scnt"],
                "recovered_count": b["rcnt"], "notional_tick_lots": b["notional"],
                "inferred_buy_lots": b["ibuy"], "inferred_sell_lots": b["isell"],
                "first_trade_time": b["first_ts"], "last_trade_time": b["last_ts"],
                "first_trade_id": b["min_id"], "last_trade_id": b["max_id"], "n_levels_1tick": b["n_levels"],
                "weekend": self.cal.is_weekend(s), "dq_status": DQ_NAMES[self._dq_codes(s, e)],
                "coverage": self._coverage(s), "is_final": np.ones(len(s), bool)}
        lr = rows.rebin(st.footprint_bin_ticks)
        levels = {"bar_start": lr["bucket"], "level": lr["level"], "buy_lots": lr["buy"], "sell_lots": lr["sell"],
                  "unknown_lots": lr["unk"], "trade_count": lr["cnt"], "buy_count": lr["bcnt"], "sell_count": lr["scnt"],
                  "notional_tick_lots": lr["notional"], "inferred_buy_lots": lr["ibuy"], "inferred_sell_lots": lr["isell"],
                  "first_trade_time": lr["first_ts"], "last_trade_time": lr["last_ts"],
                  "first_trade_id": lr["min_id"], "last_trade_id": lr["max_id"]}
        return bars, levels

    def _window_output(self, w: dict[str, Any], final: bool = True) -> tuple[dict[str, Any], dict[str, Any]]:
        i: Instance = w["inst"]; t = w["arr"].totals()
        dq = int(self._dq_codes(np.array([i.start_ms]), np.array([i.end_ms]))[0])
        vol = t["buy"] + t["sell"] + t["unk"]
        summ = {"key": i.key, "kind": i.kind, "name": i.name, "label": i.label, "start_ms": i.start_ms, "end_ms": i.end_ms,
                "start_utc": iso(i.start_ms), "end_utc": iso(i.end_ms), "available_at_ms": i.end_ms + self.st.trade_shift_ms,
                "is_final": final, "volume_lots": vol, "buy_lots": t["buy"], "sell_lots": t["sell"], "unknown_lots": t["unk"],
                "delta_lots": t["buy"] - t["sell"], "trade_count": t["cnt"], "buy_count": t["bcnt"], "sell_count": t["scnt"],
                "recovered_count": t["rcnt"], "notional_tick_lots": t["notional"],
                "inferred_buy_lots": t["ibuy"] if self.st.inferred_enabled else None,
                "inferred_sell_lots": t["isell"] if self.st.inferred_enabled else None,
                "open_ticks": t["open"], "high_ticks": t["high"], "low_ticks": t["low"], "close_ticks": t["close"],
                "first_trade_time": t["first_ts"], "last_trade_time": t["last_ts"], "first_trade_id": t["min_id"],
                "last_trade_id": t["max_id"], "n_levels_1tick": t["n_levels"], "weekend_lots": w["weekend_lots"],
                "dq_status": str(DQ_NAMES[dq]), "coverage": "PARTIAL_START" if i.start_ms < (self.observe_from or 0) else
                ("FILTERED" if self.filtered else "FULL"), "bin_ticks": 1}
        return summ, w["arr"].nonzero()

    # ================================================================== downstream accessors (developing state)
    def developing_bars(self, interval: str) -> tuple[dict[str, Any], dict[str, Any]]:
        """Bars of ``interval`` that are not final yet (labelled is_final=False). They must not be treated as closed."""
        d = self.dev[interval]
        if not len(d): return {}, {}
        bars, levels = self._bar_outputs(interval, d)
        bars["is_final"] = np.zeros(len(bars["bar_start"]), bool); bars["available_at"] = np.full(len(bars["bar_start"]), -1, np.int64)
        return bars, levels

    def developing_windows(self) -> list[tuple[dict[str, Any], LevelArrays]]:
        """Open windows (is_final=False) with their live LevelArrays (use ``.view(bin_ticks)`` for profiles)."""
        return [(self._window_output(w, final=False)[0], w["arr"]) for _, w in sorted(self.windows.items())]

    def window(self, kind: str, name: str | None = None) -> tuple[dict[str, Any], LevelArrays] | None:
        """The currently developing instance of a window series (e.g. ("session", "london")), or None."""
        name = name or kind
        for _, w in sorted(self.windows.items()):
            if w["inst"].kind == kind and w["inst"].name == name: return self._window_output(w, final=False)[0], w["arr"]
        return None

    def summary(self) -> dict[str, Any]:
        return {"stats": dict(self.stats), "late_after_bar_final": dict(self.late_after_bar_final),
                "late_after_window_final": self.late_after_window_final, "bars_finalised": dict(self.bars_finalised),
                "windows_finalised": self.stats["windows_finalised"], "windows_developing": sorted(self.windows),
                "watermark_trade_id": self.watermark, "clock": iso(self.clock) if self.clock is not None else None,
                "settings_hash": self.st.hash[:16], "state_digest": self.state_digest()}
