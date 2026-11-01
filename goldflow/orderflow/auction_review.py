"""Phase 9: reviewer-friendly output (``auction-review``).

* ``data/reports/auction_review.csv``: every auction event with its level, session, timing, evidence and outcomes,
  plus the path of its chart snapshot when one was drawn.
* ``data/derived/.../auction/charts/<event>.svg``: a self-contained chart per sampled event, drawn from the raw
  trades: 1-minute candles from 30 minutes before the episode to 60 minutes after the confirmation, the reference
  level, the probe extreme, and markers at the episode start (E) and at the confirmation time (C). Nothing in the
  chart comes from the engine except the event row itself, so a reviewer can check the event against the market.
The sample takes events evenly across types (``limit`` charts in total), in time order within each type.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import numpy as np

from ..processing.sessions import SessionCalendar
from ..processing.settings import ProcessingSettings
from ..timeutil import iso
from .auction_verify import _bars, _session_of, _store, _table
from .flow_verify import Trades


def _svg(bars: list[dict[str, Any]], e: dict[str, Any], tick: float) -> str:
    W, H, pad, right = 980, 360, 64, 130
    if not bars:
        return f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="60"><text x="10" y="30">no trades</text></svg>'
    lv = e["level_price_ticks"]
    ys = [b["h"] for b in bars] + [b["l"] for b in bars] + ([lv] if lv is not None else [])
    lo, hi = min(ys), max(ys)
    span = max(hi - lo, 1)
    t0, t1 = bars[0]["s"], bars[-1]["e"]

    def X(t: float) -> float:
        return float(pad + (t - t0) / max(t1 - t0, 1) * (W - pad - right))

    def Y(p: float) -> float:
        return float(pad + (hi - p) / span * (H - 2 * pad))

    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" font-family="sans-serif" font-size="11">',
           '<rect width="100%" height="100%" fill="#ffffff"/>']  # fmt: skip
    cw = max((W - pad - right) / max(len(bars), 1) * 0.6, 1)
    for p_ in (hi, (hi + lo) / 2, lo):
        out.append(f'<text x="{pad - 6}" y="{Y(p_) + 4:.1f}" text-anchor="end" fill="#57606a">{p_ * tick:.2f}</text>')
        out.append(f'<line x1="{pad}" x2="{W - right}" y1="{Y(p_):.1f}" y2="{Y(p_):.1f}" stroke="#eaeef2"/>')
    for b in bars:
        x = X(b["s"] + (b["e"] - b["s"]) / 2)
        col = "#1a7f37" if b["c"] >= b["o"] else "#cf222e"
        out.append(f'<line x1="{x:.1f}" x2="{x:.1f}" y1="{Y(b["h"]):.1f}" y2="{Y(b["l"]):.1f}" stroke="{col}"/>')
        y0, y1 = sorted((Y(b["o"]), Y(b["c"])))
        out.append(f'<rect x="{x - cw / 2:.1f}" y="{y0:.1f}" width="{cw:.1f}" height="{max(y1 - y0, 1):.1f}" fill="{col}"/>')
    if lv is not None:
        out.append(
            f'<line x1="{pad}" x2="{W - right}" y1="{Y(lv):.1f}" y2="{Y(lv):.1f}" stroke="#0969da" stroke-dasharray="6 3"/>'
        )
        out.append(f'<text x="{W - right + 4}" y="{Y(lv) + 4:.1f}" fill="#0969da">{e["level_type"] or ""} {lv * tick:.2f}</text>')
    if e.get("probe_extreme_ticks") is not None:
        y = Y(e["probe_extreme_ticks"])
        out.append(f'<line x1="{pad}" x2="{W - right}" y1="{y:.1f}" y2="{y:.1f}" stroke="#9a6700" stroke-dasharray="2 3"/>')
        out.append(f'<text x="{W - right + 4}" y="{y + 4:.1f}" fill="#9a6700">probe {e["probe_extreme_ticks"] * tick:.2f}</text>')
    for t, lab in ((e.get("episode_start"), "E"), (e["confirmed_at"], "C")):
        if t is not None and t0 <= t <= t1:
            out.append(
                f'<line x1="{X(t):.1f}" x2="{X(t):.1f}" y1="{pad}" y2="{H - pad}" stroke="#57606a" stroke-dasharray="3 3"/>'
            )
            out.append(f'<text x="{X(t) + 3:.1f}" y="{pad - 6}" fill="#57606a">{lab}</text>')
    title = f"{e['event_type']} {e['side']} | {e['level_id'] or ''} | confirmed {iso(e['confirmed_at'])}"
    out.append(f'<text x="{pad}" y="16" font-size="12">{title}</text>')
    out.append(
        f'<text x="{pad}" y="{H - 10}" fill="#57606a">{iso(t0)} to {iso(t1)}; price {lo * tick:.2f} - {hi * tick:.2f}</text>'
    )
    out.append("</svg>")
    return "\n".join(out)


def review(cfg: Any, limit: int = 120) -> dict[str, Any]:
    pst = ProcessingSettings.from_config(cfg)
    cal = SessionCalendar.from_config(pst.sessions_cfg)
    tick = float(pst.tick_size)
    store = _store(cfg)
    mp = store.base / "_manifest.json"
    man = json.loads(mp.read_text()) if mp.exists() else {}
    dates = sorted(man.get("dates", {}))
    events = sorted((e for d in dates for e in _table(cfg, "events", d)), key=lambda e: (e["confirmed_at"], e["event_id"]))
    outs: dict[str, dict[int, dict[str, Any]]] = {}
    for d in dates:
        for o in _table(cfg, "outcomes", d):
            outs.setdefault(o["event_id"], {})[o["horizon_minutes"]] = o
    by_type: dict[str, list[dict[str, Any]]] = {}
    for e in events:
        by_type.setdefault(e["event_type"], []).append(e)
    per = max(1, limit // max(len(by_type), 1))
    sample = set()
    for xs in by_type.values():
        idx: list[int] = np.linspace(0, len(xs) - 1, min(per, len(xs))).round().astype(int).tolist() if xs else []
        sample |= {xs[i]["event_id"] for i in idx}
    charts = store.base / "charts"
    charts.mkdir(parents=True, exist_ok=True)
    T = Trades(cfg)
    horizons = sorted({h for o in outs.values() for h in o})
    rows = []
    for e in events:
        path = ""
        if e["event_id"] in sample:
            a = (e.get("episode_start") or e["event_time"]) - 30 * 60_000
            b = e["confirmed_at"] + 60 * 60_000
            bars = _bars(T.between(a, b), a, b, 60_000)
            safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in e["event_id"])[:150] + ".svg"
            (charts / safe).write_text(_svg(bars, e, tick))
            path = str((charts / safe).relative_to(Path(cfg.data_root)))
        oc = outs.get(e["event_id"], {})
        rows.append({"event_id": e["event_id"], "type": e["event_type"], "side": e["side"], "level_id": e["level_id"] or "",
                     "level_type": e["level_type"] or "", "level_price": "" if e["level_price_ticks"] is None else f"{e['level_price_ticks'] * tick:.4f}",
                     "session": _session_of(cal, e["confirmed_at"] - 1), "episode_start_utc": iso(e["episode_start"]) if e["episode_start"] else "",
                     "confirmed_utc": iso(e["confirmed_at"]), "probe_extreme": "" if e["probe_extreme_ticks"] is None else f"{e['probe_extreme_ticks'] * tick:.2f}",
                     "vol_beyond_oz": "" if e["vol_beyond"] is None else e["vol_beyond"] / 1000, "delta_beyond_oz": "" if e["delta_beyond"] is None else e["delta_beyond"] / 1000,
                     **{f"move_{h}m": oc[h]["move_ticks"] * tick if h in oc else "" for h in horizons},
                     **{f"invalidated_{h}m": oc[h]["invalidated"] if h in oc else "" for h in horizons},
                     "dq_status": e["dq_status"], "coverage": e["coverage"], "chart": path, "detail": e["detail"]})  # fmt: skip
    rep = Path(cfg.data_root) / "reports" / "auction_review.csv"
    rep.parent.mkdir(parents=True, exist_ok=True)
    with rep.open("w", newline="") as f:
        if rows:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
    return {"events": len(rows), "charts": len(sample), "csv": str(rep), "charts_dir": str(charts)}
