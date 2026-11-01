"""Phase 14: the historical/live parity harness.

The same trades are processed twice by the full analytics and strategy stack (Phase 5 processor, profile, orderflow,
Initial Balance, auction and strategy engines), and every output is compared **exactly**:

1. **Live path:** the trades are packed into REST poll responses as the poller would receive them (one poll every
   ``poll_ms``; each response holds the trades published up to ``delay_ms`` before the poll), normalised by the live
   normaliser (the Phase 3 validator) and delivered through the ``LiveBus`` with its grace clock;
2. **Historical path:** the Phase 4 ``ReplayEngine`` over the same range of Phase 3 partitions.

Source of the trades: a live capture (``rest_trades`` records of a ``live-run``), or, where no capture exists yet, the
archived trades of a historical range (``parity --start --end``). The latter checks the live code path on real trades:
it is the same normaliser, bus and consumers a live session uses, fed with the trades a live poller would have
received.

Compared: Phase 5 finalised bars (every interval) and windows; Phase 6 final and developing profile snapshots; Phase 7
bars, CVD, imbalances, stacked imbalances, big trades and events; Phase 10 IB rows and events; Phase 9 auction levels,
events, outcomes and states; Phase 11 signals. Counts and volumes must be equal, and so must every other field.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ..normalize import part_path
from ..processing.trade_processor import ProcessorSink
from ..timeutil import DAY_MS, iso
from .bus import LiveBus
from .trades import GapTracker, normalize_rest_trades


class _Collect(ProcessorSink):
    name = "parity_collect"

    def __init__(self) -> None:
        self.rows: dict[str, list[str]] = {}

    def add(self, table: str, rows: list[dict[str, Any]]) -> None:
        self.rows.setdefault(table, []).extend(json.dumps(r, sort_keys=True, default=str) for r in rows)

    def on_bars(self, interval: str, bars: dict[str, Any], levels: dict[str, Any], final_upto_ms: int) -> None:
        keys = sorted(bars)
        cols = {k: np.asarray(bars[k]).tolist() for k in keys}
        self.add(f"phase5.bars.{interval}", [{k: cols[k][i] for k in keys} for i in range(len(cols["bar_start"]))])

    def on_window(self, summary: dict[str, Any], levels: dict[str, Any]) -> None:
        self.add("phase5.windows", [summary])


def _stack(cfg: Any) -> tuple[list[Any], _Collect]:
    from ..pipeline import build_stack

    col = _Collect()
    st = build_stack(cfg, pre_sinks=[col])
    prof, flow, ib, auc, strat = st.profile, st.orderflow, st.ib, st.auction, st.strategies

    def on_profile(recs: list[dict[str, Any]], nodes: list[dict[str, Any]]) -> None:
        col.add("phase6.snapshots", recs)
        col.add("phase6.nodes", nodes)

    def tap(prefix: str) -> Callable[[str, list[dict[str, Any]]], None]:
        def f(table: str, rows: list[dict[str, Any]]) -> None:
            col.add(f"{prefix}.{table}", rows)

        return f

    prof.listeners.append(on_profile)
    for name, eng in (("phase7", flow), ("phase10", ib), ("phase9", auc), ("phase11", strat)):
        eng.listeners.append(tap(name))
    return [st.proc], col


def archive_polls(
    cfg: Any, start_ms: int, end_ms: int, poll_ms: int, delay_ms: int, limit: int = 1000
) -> list[tuple[int, list[dict[str, Any]]]]:
    """REST-like poll responses built from archived trades: (receive time, trades published by recv - delay)."""
    polls = []
    for dn in range(start_ms // DAY_MS, (end_ms - 1) // DAY_MS + 1):
        p = part_path(cfg.data_root, cfg.symbol, "trades", str(np.datetime64(dn, "D")))
        if not p.exists():
            continue
        t = pq.read_table(
            p,
            columns=[
                "source_trade_id",
                "exchange_event_time_utc",
                "price",
                "quantity",
                "quote_notional",
                "buyer_maker_flag",
                "record_origin",
            ],
        )
        ts = t.column("exchange_event_time_utc").cast(pa.int64()).to_numpy()
        m = (ts >= start_ms) & (ts < end_ms)
        t = t.filter(pa.array(m))
        ts = ts[m]
        origin = set(t.column("record_origin").cast(pa.string()).to_pylist())
        if t.column("buyer_maker_flag").null_count or origin - {"ARCHIVE_TRADES", "LIVE_REST_TRADES"}:
            raise ValueError(
                f"{p}: trades with an unknown side or recovered from aggTrades ({sorted(origin)}) cannot be"
                " represented as REST trades; parity needs a range of exchange-reported individual trades"
            )
        ids, px, q, qq, bm = (
            t.column(c).to_pylist() for c in ("source_trade_id", "price", "quantity", "quote_notional", "buyer_maker_flag")
        )
        recv = (ts + delay_ms) // poll_ms * poll_ms + poll_ms  # the first poll at least delay_ms after the trade
        for r in np.unique(recv):
            idx = np.flatnonzero(recv == r)
            rows = [{"id": ids[i], "price": repr(px[i]), "qty": repr(q[i]), "quoteQty": repr(qq[i]) if qq[i] is not None else repr(px[i] * q[i]),
                     "time": int(ts[i]), "isBuyerMaker": bool(bm[i]), "isRPITrade": False} for i in idx.tolist()]  # fmt: skip
            for k in range(0, len(rows), limit):
                polls.append((int(r), rows[k : k + limit]))
    return polls


def capture_polls(cfg: Any, dates: list[str] | None = None) -> list[tuple[int, list[dict[str, Any]]]]:
    """Poll responses recorded by ``live-run`` (``rest_trades`` capture records), in capture order."""
    from ..liquidity.capture import capture_files, iter_capture

    root = Path(cfg.data_root) / "raw" / "binance" / cfg.market / cfg.symbol / "live"
    out = []
    for rec in iter_capture(capture_files(root, dates)):
        if rec.kind == "rest_trades" and not rec.synthetic_marker:
            out.append((rec.recv_ms, json.loads(rec.raw)))
    return out


def run_parity(cfg: Any, start_ms: int, end_ms: int, poll_ms: int = 750, delay_ms: int = 200, grace_ms: int = 3000,
               polls: list[tuple[int, list[dict[str, Any]]]] | None = None, report: bool = True) -> dict[str, Any]:  # fmt: skip
    from ..replay import ReplayEngine, load_quality_events

    if polls is None:
        polls = archive_polls(cfg, start_ms, end_ms, poll_ms, delay_ms)
    # The Phase 3 quality annotations of the trades dataset are given to both paths (live, a gap would be detected by
    # the poller instead; the point here is that the same inputs give the same outputs).
    quality = [e for e in load_quality_events(cfg, ["trades"]) if e["avail_ms"] < end_ms]
    before = [e for e in quality if e["avail_ms"] < start_ms]
    pending_q = [e for e in quality if e["avail_ms"] >= start_ms and e["type"] == "DQ_BEGIN"]
    # ---- live path
    cons, live = _stack(cfg)
    bus = LiveBus(cons, grace_ms)
    bus.start(start_ms, quality_before_start=before)
    rejected = 0

    def feed_quality(clock: int) -> None:
        while pending_q and pending_q[0]["avail_ms"] <= clock:
            e = pending_q.pop(0)
            bus.push_quality(e["dataset"], e["status"], e["start_ms"], e["end_ms"])

    tracker = GapTracker(limit=2**31)  # overlapping poll pages (captures): only trades not seen before go on the bus
    for recv, rows in polls:
        fresh, _ = tracker.new_rows([r for r in rows if start_ms <= int(r["time"]) < end_ms])
        tracker.advance(fresh)
        tbl, rej, _ = normalize_rest_trades(fresh, recv, cfg)
        rejected += sum(rej.values())
        bus.push_trades(tbl)
        feed_quality(recv - grace_ms)
        bus.advance(recv)
    feed_quality(end_ms)
    bus.advance(end_ms + grace_ms)
    for c in cons:
        c.on_stop({"status": "completed", "end_ms": end_ms})
    late = cons[0].late_after_bar_final
    # ---- historical path: the Phase 4 replay of the trades partitions (other streams are not used by the stack)
    cons2, hist = _stack(cfg)
    streams = {"trades": cfg["replay"]["streams"]["trades"]}
    ReplayEngine(
        cfg, cons2, start=start_ms, end=end_ms, streams=streams, name=f"parity-{start_ms}-{end_ms}", checkpoint=False
    ).run()
    # ---- compare
    tables = sorted(set(live.rows) | set(hist.rows))
    res = {}
    for t in tables:
        a, b = sorted(live.rows.get(t, [])), sorted(hist.rows.get(t, []))
        sa, sb = set(a), set(b)
        diff = [x[:300] for x in a if x not in sb][:3] + [x[:300] for x in b if x not in sa][:3]
        res[t] = {"live": len(a), "historical": len(b), "equal": a == b, "examples": diff}
    out = {"range": [iso(start_ms), iso(end_ms)], "polls": len(polls), "trades": sum(len(r) for _, r in polls), "rejected": rejected,
           "late_after_bar_final": late, "tables": res, "all_equal": all(v["equal"] for v in res.values()) and bool(res)}  # fmt: skip
    if report:
        d = Path(cfg.data_root) / "reports"
        d.mkdir(parents=True, exist_ok=True)
        (d / "parity_report.json").write_text(json.dumps(out, indent=1, default=str))
        lines = ["# Historical / live parity (Phase 14)", "", f"Range {out['range'][0]} → {out['range'][1]}; {out['trades']} trades in {out['polls']} polls; "
                 f"rejected {rejected}; late after bar final {late}.", "", f"**All outputs equal: {out['all_equal']}**", "",
                 "| Output | Live path | Historical path | Equal |", "|---|---|---|---|"]  # fmt: skip
        lines += [f"| {t} | {v['live']} | {v['historical']} | {'yes' if v['equal'] else 'NO'} |" for t, v in res.items()]
        (d / "parity_report.md").write_text("\n".join(lines) + "\n")
    return out
