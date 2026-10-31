"""Phase 6: the profile engine. Developing and final Volume Profile, POC, Value Area, HVN/LVN and VWAP per window.

``ProfileEngine`` is a Phase 5 ``ProcessorSink``. It plugs into the shared ``TradeProcessor``, so it runs unchanged on
historical replay and on the Phase 14 live bus.

Data flow and the no-look-ahead rule:
* Input is the processor's **finalised** base-interval bars (default 1 s) with their exact 1-tick footprint. A bar
  [s, s + 1 s) is only emitted after the clock passed its end, so every trade in it has exchange time < s + 1 s.
* Rows are applied to every open window that contains them. A window is a calendar instance (UTC day, trading day,
  trading week, session, custom range) or an anchor (open-ended, from an anchor time).
* A developing snapshot "as of M" is taken at each cadence boundary M (default every 1 minute) once the processor
  has finalised the base interval up to M. It contains exactly the trades with time < M, never later ones, and is
  labelled ``available_at = M + trade shift``.
* When the processor finalises a window it hands over its own 1-tick levels. The engine's final snapshot is built from
  its incremental state and checked against them (``final_parity_mismatch``, expected 0).

State is exact integers (per-level lots, Python-int VWAP sums), so restoring a checkpoint and continuing equals an
uninterrupted run (``state_digest``).
"""

from __future__ import annotations

import logging
from typing import Any, Callable

import numpy as np
from numpy.typing import NDArray

from ..processing.aggregation import decode_arrays, digest_tree, encode_arrays
from ..processing.sessions import Instance
from ..processing.trade_processor import DQ_NAMES, DQ_RANK, ProcessorSink
from ..replay import ReplayGuardError
from ..timeutil import DAY_MS, iso
from .profile_settings import ProfileSettings
from .profile_store import ProfileStore
from .volume_profile import BinnedProfile, bin_dense, gaussian_kernel, profile_metrics
from .vwap import VwapSums, sums_from_levels, vwap_stats

log = logging.getLogger("goldflow.profile")
OPEN_END = 2**62
I64 = np.int64
STAT_KEYS = (
    "rows_applied",
    "lots_applied",
    "developing_snapshots",
    "final_snapshots",
    "final_parity_checked",
    "final_parity_mismatch",
    "dq_label_mismatch",
    "seeks",
    "anchors_added",
)
Listener = Callable[[list[dict[str, Any]], list[dict[str, Any]]], None]


class _Win:
    """Exact accumulator for one window: dense per-1-tick-level lots (vol, buy, sell) and VWAP sums."""

    __slots__ = ("inst", "lo", "vol", "buy", "sell", "lmin", "lmax", "sums", "dq", "dq_upto")
    GROW = 512

    def __init__(self, inst: Instance) -> None:
        self.inst = inst
        self.lo = 0
        self.vol = self.buy = self.sell = np.zeros(0, I64)
        self.lmin: int | None = None
        self.lmax: int | None = None
        self.sums = VwapSums()
        self.dq = 0
        self.dq_upto = inst.start_ms

    def _ensure(self, a: int, b: int) -> None:
        n = len(self.vol)
        if n == 0:
            self.lo = a - self.GROW
            size = b - self.lo + 1 + self.GROW
            self.vol, self.buy, self.sell = (np.zeros(size, I64) for _ in range(3))
            return
        hi = self.lo + n - 1
        if a >= self.lo and b <= hi:
            return
        nlo = min(self.lo, a - self.GROW)
        nhi = max(hi, b + self.GROW)
        off = self.lo - nlo
        out = []
        for x in (self.vol, self.buy, self.sell):
            y = np.zeros(nhi - nlo + 1, I64)
            y[off : off + n] = x
            out.append(y)
        self.vol, self.buy, self.sell = out
        self.lo = nlo

    def add(self, u: NDArray[Any], vol: NDArray[Any], buy: NDArray[Any], sell: NDArray[Any], sums: VwapSums) -> None:
        a, b = int(u[0]), int(u[-1])  # u is sorted and unique
        self._ensure(a, b)
        i = u - self.lo
        self.vol[i] += vol
        self.buy[i] += buy
        self.sell[i] += sell
        self.lmin = a if self.lmin is None else min(self.lmin, a)
        self.lmax = b if self.lmax is None else max(self.lmax, b)
        self.sums.add(sums)

    def dense(self) -> tuple[int, NDArray[Any], NDArray[Any], NDArray[Any]]:
        """(lowest traded level, vol, buy, sell) over the traded range only."""
        if self.lmin is None or self.lmax is None:
            z = np.zeros(0, I64)
            return 0, z, z, z
        a, b = self.lmin - self.lo, self.lmax - self.lo + 1
        return self.lmin, self.vol[a:b], self.buy[a:b], self.sell[a:b]

    def nonzero(self) -> dict[str, NDArray[Any]]:
        lmin, v, bu, se = self.dense()
        m = v > 0
        return {"level": np.flatnonzero(m).astype(I64) + lmin, "vol": v[m], "buy": bu[m], "sell": se[m]}

    def tree(self) -> dict[str, Any]:
        i = self.inst
        return {
            "inst": [i.kind, i.name, i.label, i.start_ms, i.end_ms],
            "levels": self.nonzero(),
            "sums": self.sums.as_list(),
            "dq": self.dq,
            "dq_upto": self.dq_upto,
        }

    @classmethod
    def from_tree(cls, t: dict[str, Any], levels: dict[str, NDArray[Any]]) -> "_Win":
        w = cls(Instance(*t["inst"]))
        if len(levels["level"]):
            lv = levels["level"]
            w.add(lv, levels["vol"], levels["buy"], levels["sell"], VwapSums())
        w.sums = VwapSums(*t["sums"])
        w.dq, w.dq_upto = int(t["dq"]), int(t["dq_upto"])
        return w


class ProfileEngine(ProcessorSink):
    name = "volume_profile"

    def __init__(
        self, cfg: Any, store: ProfileStore | None = None, listeners: list[Listener] | tuple[Listener, ...] = ()
    ) -> None:
        self.ps = ProfileSettings.from_config(cfg)
        n = self.ps.nodes
        self.kernel = gaussian_kernel(n.smooth_bins, n.truncate_sigmas, n.kernel_scale_bits)
        self.store = store
        self.listeners = list(listeners)
        self.proc: Any = None
        self._cache: dict[tuple[int, int], list[Instance]] = {}
        self._reset()

    def _reset(self) -> None:
        self.upto: int | None = None  # every cadence boundary <= upto has been snapshotted
        self.applied_upto: int | None = None  # every finalised row with bucket < applied_upto has been applied
        self.win: dict[str, _Win] = {}
        self.done: dict[str, int] = {}
        self.pending: list[dict[str, NDArray[Any]]] = []
        self.anchors: list[tuple[str, int]] = list(self.ps.anchors)
        self.dq: list[list[Any]] = []
        self.stats = {k: 0 for k in STAT_KEYS}
        self.latest: dict[str, dict[str, Any]] = {}

    # ================================================================== sink interface
    def on_start(self, ctx: dict[str, Any], proc: Any) -> None:
        self.proc = proc
        st = proc.st
        if st.footprint_bin_ticks != 1:
            raise ValueError("ProfileEngine needs processing.footprint_bin_ticks = 1")
        if self.ps.snapshot_ms % st.base_ms:
            raise ValueError("volume_profile.snapshot_every must be a multiple of the processor's base interval")
        self.base = st.intervals[0]
        self.shift = st.trade_shift_ms
        if self.store is not None:
            meta = {
                "profile_settings_hash": self.ps.hash,
                "processing_settings_hash": st.hash,
                "source_dataset_version": ctx.get("dataset_version"),
                "bin_ticks": list(self.ps.bin_ticks),
                "value_area": f"{self.ps.va_method}:{self.ps.va_pct}",
                "snapshot_every_ms": self.ps.snapshot_ms,
                "tick_size": st.tick_size,
                "step_size": st.step_size,
                "instrument_id": st.instrument_id,
                "trade_shift_ms": st.trade_shift_ms,
                "kernel": self.kernel.tolist(),
            }
            self.store.start(meta, self.ps.band_k, bool(ctx.get("resumed")))

    def on_dq(self, start_ms: int, end_ms: int, status: str) -> None:
        x = [int(start_ms), int(end_ms), str(status)]
        if x not in self.dq:
            self.dq.append(x)

    def on_bars(self, interval: str, bars: dict[str, Any], levels: dict[str, Any], final_upto_ms: int) -> None:
        if interval != self.base or not len(levels["bar_start"]):
            return
        self.pending.append(
            {
                "b": np.asarray(levels["bar_start"], I64),
                "l": np.asarray(levels["level"], I64),
                "v": np.asarray(levels["buy_lots"] + levels["sell_lots"] + levels["unknown_lots"], I64),
                "buy": np.asarray(levels["buy_lots"], I64),
                "sell": np.asarray(levels["sell_lots"], I64),
            }
        )

    def on_window(self, summary: dict[str, Any], levels: dict[str, Any]) -> None:
        if summary["kind"] not in self.ps.windows:
            return
        end = int(summary["end_ms"])
        self._catch_up(end)
        self._apply_until(end)
        key = summary["key"]
        w = self.win.pop(key, None)
        if w is None:
            w = _Win(Instance(summary["kind"], summary["name"], summary["label"], int(summary["start_ms"]), end))
        self._update_dq(w, end)
        # parity: the engine's incremental state against the processor's own window levels (both exact)
        mine = w.nonzero()
        theirs_v = np.asarray(levels["buy"] + levels["sell"] + levels["unk"], I64)
        m = theirs_v > 0
        same = (
            np.array_equal(mine["level"], np.asarray(levels["level"], I64)[m])
            and np.array_equal(mine["vol"], theirs_v[m])
            and np.array_equal(mine["buy"], np.asarray(levels["buy"], I64)[m])
            and np.array_equal(mine["sell"], np.asarray(levels["sell"], I64)[m])
        )
        self.stats["final_parity_checked"] += 1
        if not same:
            self.stats["final_parity_mismatch"] += 1
            log.warning("profile parity mismatch for %s", key)
        if str(DQ_NAMES[w.dq]) != summary["dq_status"]:
            self.stats["dq_label_mismatch"] += 1
        self._emit([w], end, final=True, coverage=summary["coverage"])
        self.done[w.inst.series] = max(self.done.get(w.inst.series, end), end)

    def on_after_advance(self, proc: Any) -> None:
        fu = proc.final_upto.get(self.base)
        if fu is None:
            return
        self._catch_up(int(fu))
        if self.store is not None and self.upto is not None:
            self.store.flush_complete(self.upto)
        self._prune_dq()

    def on_seek(self, t_ms: int) -> None:
        """Skipped events are never delivered: every developing window is incomplete, so discard it and restart."""
        self.win = {}
        self.pending = []
        self.upto = self.applied_upto = None
        self.latest = {}
        self.stats["seeks"] += 1
        if self.store is not None:
            self.store.discard_unwritten()

    def on_stop(self, ctx: dict[str, Any], proc: Any) -> None:
        if ctx.get("status") == "completed" and self.store is not None:
            self.store.flush_complete(self.upto or 0, force=True)

    # ================================================================== runtime anchors
    def add_anchor(self, name: str, start_ms: int) -> None:
        """Anchor a VWAP/profile at ``start_ms`` (e.g. a Phase 7 event). Rows before the engine's applied point are no
        longer held, so an anchor in the past is refused rather than silently started late."""
        if any(a[0] == name for a in self.anchors):
            raise ValueError(f"anchor {name} exists")
        base_ms = self.proc.st.base_ms if self.proc is not None else 1
        if start_ms % base_ms:
            raise ValueError("anchor start must align to the base interval")
        floor = self.applied_upto if self.applied_upto is not None else self.upto
        if floor is not None and start_ms < floor:
            raise ValueError(
                f"anchor {name} at {iso(start_ms)} is before the engine's applied point {iso(floor)}; "
                "configure it in volume_profile.anchors and rerun instead"
            )
        self.anchors.append((name, int(start_ms)))
        self._cache = {}
        self.stats["anchors_added"] += 1

    # ================================================================== windows
    def _instances(self, a: int, b: int) -> list[Instance]:
        """Configured window instances overlapping [a, b] (inclusive)."""
        kinds = self.ps.windows
        out: dict[str, Instance] = {}
        custom = list(self.proc.custom) if self.proc is not None else []
        for day in range(a // DAY_MS, b // DAY_MS + 1):
            ck = (day, len(custom))
            if ck not in self._cache:
                s = day * DAY_MS
                self._cache[ck] = [i for i in self.proc.cal.instances(s, s + DAY_MS - 1, custom) if i.kind in kinds]
                if len(self._cache) > 64:
                    self._cache.pop(next(iter(self._cache)))
            for i in self._cache[ck]:
                if i.start_ms <= b and i.end_ms > a:
                    out[i.key] = i
        if "anchor" in kinds:
            for name, start in self.anchors:
                if start <= b:
                    i = Instance("anchor", name, iso(start) or "", start, OPEN_END)
                    out[i.key] = i
        return sorted(out.values(), key=lambda i: (i.start_ms, i.key))

    def _open(self, a: int, b: int) -> None:
        for i in self._instances(a, b):
            if i.key not in self.win and i.end_ms > self.done.get(i.series, -1):
                self.win[i.key] = _Win(i)

    def _init_upto(self, t: int) -> None:
        if self.upto is None:
            of = self.proc.observe_from if self.proc is not None else None
            self.upto = int(of) if of is not None else int(t)
            self.applied_upto = self.upto

    # ================================================================== applying rows and snapshots
    def _take_pending(self, until: int) -> dict[str, NDArray[Any]] | None:
        """Remove and return the pending rows with bucket < until (pending is in bucket order)."""
        if not self.pending:
            return None
        P = (
            self.pending[0]
            if len(self.pending) == 1
            else {k: np.concatenate([p[k] for p in self.pending]) for k in self.pending[0]}
        )
        j = int(np.searchsorted(P["b"], until, side="left"))
        self.pending = [{k: v[j:] for k, v in P.items()}] if j < len(P["b"]) else []
        return {k: v[:j] for k, v in P.items()} if j else None

    def _apply_until(self, t: int) -> None:
        assert self.applied_upto is not None
        if t <= self.applied_upto:
            return
        self._open(self.applied_upto, t - 1)
        seg = self._take_pending(t)
        if seg is not None:
            self._apply(seg)
        self.applied_upto = t

    @staticmethod
    def _reduce(seg: dict[str, NDArray[Any]]) -> tuple[NDArray[Any], NDArray[Any], NDArray[Any], NDArray[Any], VwapSums]:
        u, inv = np.unique(seg["l"], return_inverse=True)
        # bincount sums in float64: exact here, a segment's lots per level are far below 2**53
        v = np.bincount(inv, weights=seg["v"], minlength=len(u)).astype(I64)
        bu = np.bincount(inv, weights=seg["buy"], minlength=len(u)).astype(I64)
        se = np.bincount(inv, weights=seg["sell"], minlength=len(u)).astype(I64)
        return u, v, bu, se, sums_from_levels(u, v)

    def _apply(self, seg: dict[str, NDArray[Any]]) -> None:
        b = seg["b"]
        if not len(b):
            return
        if int(seg["v"].sum()) >= 2**53:
            raise OverflowError("segment volume too large for exact float64 bincount")
        whole = None
        b0, b1 = int(b[0]), int(b[-1])
        for w in self.win.values():
            s, e = w.inst.start_ms, w.inst.end_ms
            if e <= b0 or s > b1:
                continue
            if s <= b0 and e > b1:
                whole = whole or self._reduce(seg)
                w.add(*whole)
            else:
                i, j = int(np.searchsorted(b, s, "left")), int(np.searchsorted(b, e, "left"))
                if j > i:
                    w.add(*self._reduce({k: v[i:j] for k, v in seg.items()}))
        self.stats["rows_applied"] += len(b)
        self.stats["lots_applied"] += int(seg["v"].sum())

    def _catch_up(self, t: int) -> None:
        """Snapshot every cadence boundary M <= t. Rows with bucket < M are applied before the snapshot at M."""
        self._init_upto(t)
        assert self.upto is not None
        cad = self.ps.snapshot_ms
        while True:
            m = (self.upto // cad + 1) * cad
            if m > t:
                break
            self._apply_until(m)
            dev = [w for w in self.win.values() if w.inst.start_ms < m < w.inst.end_ms]
            if dev:
                self._emit(dev, m, final=False)
            self.upto = m

    def _update_dq(self, w: _Win, t: int) -> None:
        if t <= w.dq_upto:
            return
        a = w.dq_upto
        for s, e, status in self.dq:
            if s < t and e > a:
                w.dq = max(w.dq, DQ_RANK.get(status, 3))
        w.dq_upto = t

    def _prune_dq(self) -> None:
        if not self.dq:
            return
        floors = [w.dq_upto for w in self.win.values()] + [self.upto or 0]
        f = min(floors)
        self.dq = [x for x in self.dq if x[1] > f]

    def _covered_from(self, w: _Win) -> int:
        of = self.proc.observe_from if self.proc is not None else None
        return max(w.inst.start_ms, int(of)) if of is not None else w.inst.start_ms

    def _coverage(self, w: _Win) -> str:
        of = self.proc.observe_from if self.proc is not None else None
        if of is not None and w.inst.start_ms < of:
            return "PARTIAL_START"
        return "FILTERED" if (self.proc is not None and self.proc.filtered) else "FULL"

    def binned(self, w: _Win, bin_ticks: int) -> BinnedProfile:
        lmin, v, bu, se = w.dense()
        return bin_dense(lmin, v, bu, se, bin_ticks, w.sums.q, w.sums.qp)

    def _emit(self, wins: list[_Win], as_of: int, final: bool, coverage: str | None = None) -> None:
        recs: list[dict[str, Any]] = []
        nodes_out: list[dict[str, Any]] = []
        levels_out: list[dict[str, Any]] = []
        ps = self.ps
        vw_cache: dict[str, dict[str, Any]] = {}
        for w in wins:
            self._update_dq(w, as_of)
            i = w.inst
            key = {
                "window_key": i.key,
                "kind": i.kind,
                "name": i.name,
                "label": i.label,
                "window_start": i.start_ms,
                "window_end": None if i.end_ms >= OPEN_END else i.end_ms,
                "covered_from": self._covered_from(w),
                "as_of": as_of,
                "available_at": as_of + self.shift,
                "is_final": final,
            }
            vw = vw_cache.setdefault(i.key, vwap_stats(w.sums, ps.band_k))
            common = {
                "coverage": coverage or self._coverage(w),
                "dq_status": str(DQ_NAMES[w.dq]),
                "low_ticks": w.lmin,
                "high_ticks": w.lmax,
                **vw,
            }
            for bt in ps.bin_ticks:
                p = self.binned(w, bt)
                m, nodes = profile_metrics(p, ps.va_pct, ps.va_method, ps.nodes, self.kernel)
                kb = {**key, "bin_ticks": bt}
                rec = {**kb, **common, **m}
                recs.append(rec)
                nodes_out += [{**kb, **n} for n in nodes]
                if final:
                    nzb = np.flatnonzero(p.vol > 0)
                    levels_out += [
                        {
                            **kb,
                            "bin": p.lo_bin + int(j),
                            "price_ticks": p.price(int(j)),
                            "lots": int(p.vol[j]),
                            "buy_lots": int(p.buy[j]),
                            "sell_lots": int(p.sell[j]),
                        }
                        for j in nzb
                    ]
            self.latest[i.key] = {"as_of": as_of, "records": [r for r in recs if r["window_key"] == i.key]}
        self.stats["final_snapshots" if final else "developing_snapshots"] += len(wins)
        if final:
            for w in wins:
                self.latest.pop(w.inst.key, None)
        if self.store is not None and self.ps.persist:
            self.store.add("snapshots", recs)
            self.store.add("nodes", nodes_out)
            if levels_out:
                self.store.add("levels", levels_out)
        for f in self.listeners:
            f(recs, nodes_out)

    # ================================================================== accessors (live use)
    def current(self, kind: str, name: str | None = None) -> dict[str, Any] | None:
        """Latest developing snapshot of a window series, e.g. ("session", "london"); records per bin size."""
        name = name or kind
        best = None
        for k, v in sorted(self.latest.items()):
            kk, nn = k.split(":")[:2]
            if kk == kind and nn == name and (best is None or v["as_of"] >= best["as_of"]):
                best = v
        return best

    # ================================================================== state
    def _tree(self) -> dict[str, Any]:
        P = (
            {k: np.concatenate([p[k] for p in self.pending]) for k in self.pending[0]}
            if self.pending
            else {k: np.zeros(0, I64) for k in ("b", "l", "v", "buy", "sell")}
        )
        return {
            "settings_hash": self.ps.hash,
            "upto": self.upto,
            "applied_upto": self.applied_upto,
            "windows": {k: w.tree() for k, w in sorted(self.win.items())},
            "done": dict(sorted(self.done.items())),
            "pending": P,
            "anchors": [list(a) for a in self.anchors],
            "dq": [list(x) for x in self.dq],
            "stats": dict(self.stats),
        }

    def state_digest(self) -> str:
        return digest_tree(self._tree())

    def get_state(self) -> dict[str, Any]:
        t = self._tree()
        t["pending"] = encode_arrays(t["pending"])
        t["windows"] = {k: {**w, "levels": encode_arrays(w["levels"])} for k, w in t["windows"].items()}
        t["latest"] = self.latest
        t["store"] = self.store.get_state() if self.store is not None else {}
        return t

    def set_state(self, s: dict[str, Any]) -> None:
        if not s:
            return
        if s.get("settings_hash") != self.ps.hash:
            raise ReplayGuardError(
                f"profile engine checkpoint was taken with volume_profile settings {str(s.get('settings_hash'))[:12]}, "
                f"current settings are {self.ps.hash[:12]}; start a new run name"
            )
        self._reset()
        self.upto, self.applied_upto = s["upto"], s["applied_upto"]
        for k, w in s["windows"].items():
            self.win[k] = _Win.from_tree(w, decode_arrays(w["levels"]))
        self.done = dict(s["done"])
        P = decode_arrays(s["pending"])
        self.pending = [P] if len(P["b"]) else []
        self.anchors = [(str(a), int(b)) for a, b in s["anchors"]]
        self.dq = [list(x) for x in s["dq"]]
        self.stats = {**self.stats, **s["stats"]}
        self.latest = dict(s.get("latest") or {})
        if self.store is not None:
            self.store.set_state(s.get("store") or {})

    def summary(self) -> dict[str, Any]:
        return {
            "stats": dict(self.stats),
            "windows_developing": sorted(self.win),
            "snapshot_upto": iso(self.upto) if self.upto else None,
            "settings_hash": self.ps.hash[:16],
            "state_digest": self.state_digest(),
        }
