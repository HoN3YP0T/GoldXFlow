"""Phase 15: coverage and capability report per provider (``providers-report``).

For every registered real provider:
- the declared capabilities, which are honest (``Availability`` with a note);
- the mapping record;
- a reachability probe from this host (optional);
- the coverage of the data imported so far: days by status, trades, ounces, the RPI share where the venue flags it,
  and the first and last day.

Written to ``data/reports/providers.{json,md}``.
"""

from __future__ import annotations

import json
from fractions import Fraction
from pathlib import Path
from typing import Any

from ..providers import available_providers
from .ingest import venue_days
from .mapping import mappings


def check_spec(cfg: Any, pid: str) -> dict[str, Any]:
    """Live discovery compared with the mapping record (tick, step, ounces per quantity unit). Mismatches are listed."""
    from ..providers import create_provider

    m = mappings(cfg)[pid]
    try:
        s = create_provider(cfg, provider_id=pid).discover_instrument(m.symbol)
    except Exception as e:  # noqa: BLE001 - reported, never fatal
        return {"checked": False, "detail": repr(e)}
    pairs = {"tick_size": (s.tick_size, m.tick_size), "step_size": (s.step_size, m.step_size),
             "oz_per_qty_unit": (s.contract_multiplier, m.oz_per_qty_unit)}  # fmt: skip
    bad = {k: {"discovered": a, "mapping": b} for k, (a, b) in pairs.items() if Fraction(str(a)) != Fraction(str(b))}
    return {"checked": True, "status": s.status, "mismatches": bad}


def _spec_text(c: dict[str, Any] | None) -> str:
    if c is None:
        return "not checked (no reachable discovery endpoint, or not probed)"
    if not c["checked"]:
        return f"failed: {c['detail']}"
    return "matches" if not c["mismatches"] else f"**MISMATCH** {c['mismatches']}"


def providers_report(cfg: Any, probe: bool = True) -> dict[str, Any]:
    mp = mappings(cfg)
    out: dict[str, Any] = {"providers": {}}
    for pid, cls in sorted(available_providers().items()):
        if cls.synthetic or pid not in mp:
            continue
        m = mp[pid]
        caps = cls.capabilities(cls.__new__(cls)).as_dict()  # capabilities() reads no instance state
        health = None
        if probe:
            try:
                prov = cls.from_config(cfg)  # type: ignore[attr-defined]
                h = prov.health()
                health = {"state": h.state, "detail": h.detail}
            except Exception as e:  # noqa: BLE001 - the report must be produced even if a provider cannot be built
                health = {"state": "DOWN", "detail": repr(e)}
        spec_check = None
        if probe and health is not None and health["state"] == "OK" and pid != "binance":
            spec_check = check_spec(cfg, pid)
        days = venue_days(cfg, pid)
        by: dict[str, int] = {}
        for d in days:
            by[d["status"]] = by.get(d["status"], 0) + 1
        cov: dict[str, Any] = {
            "days": len(days),
            "by_status": by,
            "first": days[0]["date"] if days else None,
            "last": days[-1]["date"] if days else None,
        }
        if days and "trades" in days[0]:
            lots = sum(int(d["lots"] or 0) for d in days)
            rpi = sum(int(d["rpi_lots"] or 0) for d in days)
            cov |= {"trades": sum(int(d["trades"] or 0) for d in days), "oz": float(lots * m.oz_per_lot), "rejected": sum(int(d["rejected"] or 0) for d in days),
                    "rpi_share_of_lots": rpi / lots if lots else None}  # fmt: skip
        out["providers"][pid] = {
            "mapping": m.row(),
            "capabilities": caps,
            "health": health,
            "spec_check": spec_check,
            "coverage": cov,
        }
    rd = Path(cfg.data_root) / "reports"
    rd.mkdir(parents=True, exist_ok=True)
    (rd / "providers.json").write_text(json.dumps(out, indent=1, default=str))
    lines = ["# Providers: capabilities and coverage (Phase 15)", "", "Venues are separate markets: nothing below is merged.", ""]
    for pid, v in out["providers"].items():
        m, c = v["mapping"], v["coverage"]
        lines += [f"## {pid}: `{m['instrument_id']}`", "",
                  f"- **Contract:** tick {m['tick_size']} {m['quote']}, quantity step {m['step_size']} {m['qty_unit']}, {m['oz_per_qty_unit']} oz per {m['qty_unit']} "
                  f"(1 lot = {m['oz_per_lot']} oz); {m['price_basis']}; session {m['session']}.",
                  f"- **Specification source:** {m['spec_source']}",
                  f"- **Execution instrument:** {m['execution_instrument'] or 'not configured (broker block, D2)'}",
                  f"- **Specification check (live discovery vs mapping):** {_spec_text(v['spec_check'])}",
                  f"- **Reachability from this host:** {v['health']['state'] + ': ' + v['health']['detail'] if v['health'] else 'not probed'}",
                  f"- **Coverage:** {c['days']} days {c['by_status']} {c.get('first') or ''}..{c.get('last') or ''}"
                  + (f"; {c['trades']:,} trades, {c['oz']:,.3f} oz, {c['rejected']} rejected, RPI share of volume "
                     f"{'n/a' if c['rpi_share_of_lots'] is None else format(c['rpi_share_of_lots'], '.2%')}" if "trades" in c else ""),
                  "", "| Capability | Availability | Note |", "|---|---|---|"]  # fmt: skip
        lines += [f"| {k} | {x['availability']} | {x['note']} |" for k, x in v["capabilities"].items()]
        lines.append("")
    (rd / "providers.md").write_text("\n".join(lines) + "\n")
    return out
