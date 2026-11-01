"""Phase 15: run the **unchanged** analytics and strategy stack on one venue's data.

The venue's configuration (``mapping.venue_config``) is a normal config whose provider, instrument and catalog are the
venue's. The Phase 4 replay reads ``provider=<venue>`` partitions and the venue catalog's quality events. The Phase 5
processor, Phase 6 profile, Phase 7 orderflow, Phase 10 IB, Phase 9 auction and Phase 11 strategy engines run as they
are; the processor refuses any other instrument's trades. Outputs go to
``derived/provider=<venue>/market=<type>/instrument=<symbol>/venue_run=<name>/<table>.parquet``, and are reconciled:
Σ 1 m bar volume = Σ partition lots, and Σ 1 m bar trade count = partition rows.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ..normalize import part_path
from ..processing.trade_processor import ProcessorSink
from ..schemas import VENUES
from .mapping import mappings, venue_config


class _Rows(ProcessorSink):
    name = "venue_rows"

    def __init__(self) -> None:
        self.rows: dict[str, list[dict[str, Any]]] = {}

    def add(self, table: str, rows: list[dict[str, Any]]) -> None:
        self.rows.setdefault(table, []).extend(rows)

    def on_bars(self, interval: str, bars: dict[str, Any], levels: dict[str, Any], final_upto_ms: int) -> None:
        if interval != "1m":
            return
        keys = sorted(bars)
        cols = {k: np.asarray(bars[k]).tolist() for k in keys}
        self.add("bars_1m", [{k: cols[k][i] for k in keys} for i in range(len(cols["bar_start"]))])

    def on_window(self, summary: dict[str, Any], levels: dict[str, Any]) -> None:
        self.add("windows", [summary])


def _flat(rows: list[dict[str, Any]]) -> pa.Table:
    keys = sorted({k for r in rows for k in r})
    out = {}
    for k in keys:
        vals = [r.get(k) for r in rows]
        if any(isinstance(v, (dict, list, tuple)) for v in vals):
            vals = [json.dumps(v, default=str) if v is not None else None for v in vals]
        try:
            out[k] = pa.array(vals)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            out[k] = pa.array([None if v is None else str(v) for v in vals], pa.string())
    return pa.table(out)


def venue_run(
    cfg: Any, provider: str, name: str = "venue", start: str | int | None = None, end: str | int | None = None
) -> dict[str, Any]:
    from ..pipeline import build_stack
    from ..replay import ChainHasher, ReplayEngine

    vcfg = venue_config(cfg, provider)
    col = _Rows()
    st = build_stack(vcfg, pre_sinks=[col])
    prof, flow, ib, auc, strat = st.profile, st.orderflow, st.ib, st.auction, st.strategies

    def on_profile(recs: list[dict[str, Any]], nodes: list[dict[str, Any]]) -> None:
        col.add("profile_final", [r for r in recs if r.get("is_final")])

    def tap(prefix: str) -> Any:
        def f(table: str, rows: list[dict[str, Any]]) -> None:
            col.add(f"{prefix}_{table}", rows)

        return f

    prof.listeners.append(on_profile)
    for nm, eng in (("orderflow", flow), ("ib", ib), ("auction", auc), ("strategies", strat)):
        eng.listeners.append(tap(nm))
    proc = st.proc
    hasher = ChainHasher()  # type: ignore[no-untyped-call]
    res = ReplayEngine(vcfg, [hasher, proc], start=start, end=end, name=f"venue-{provider}-{name}", checkpoint=False).run()
    m = mappings(cfg)[provider]
    root = (
        Path(vcfg.data_root)
        / "derived"
        / f"provider={provider}"
        / f"market={VENUES[provider][1]}"
        / f"instrument={m.symbol}"
        / f"venue_run={name}"
    )
    root.mkdir(parents=True, exist_ok=True)
    counts = {}
    for t, rows in sorted(col.rows.items()):
        if rows:
            pq.write_table(_flat(rows), root / f"{t}.parquet")
        counts[t] = len(rows)
    # reconciliation against the partitions
    lots = trades = 0
    for d in sorted({str(np.datetime64(int(r["bar_start"]), "ms").astype("datetime64[D]")) for r in col.rows.get("bars_1m", [])}):
        p = part_path(vcfg.data_root, m.symbol, "trades", d, provider=provider)
        if p.exists():
            t = pq.read_table(p, columns=["quantity_lots"])
            lots += int(np.asarray(t.column("quantity_lots")).sum())
            trades += t.num_rows
    bars = col.rows.get("bars_1m", [])
    bl = sum(int(r["volume_lots"]) for r in bars)
    bc = sum(int(r["trade_count"]) for r in bars)
    out = {"provider": provider, "instrument_id": m.instrument_id, "status": res["status"], "events": res["events"],
           "sequence_digest": hasher.digest, "processor_state_digest": proc.state_digest(), "tables": counts,
           "reconciliation": {"partition_trades": trades, "bar_trades": bc, "partition_lots": lots, "bar_lots": bl,
                              "equal": trades == bc and lots == bl},
           "oz": float(bl * m.oz_per_lot), "strategies": strat.summary(), "output": str(root)}  # fmt: skip
    (root / "run.json").write_text(json.dumps(out, indent=1, default=str))
    return out
