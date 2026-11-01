"""Phase 17: the operational status snapshot. **Real engine state only.**

Every value is read from the files and databases the engine writes, and carries its ``source`` (the file it came
from) and, where it has one, its ``as_of`` time. Nothing is estimated, interpolated or defaulted:
- a value with no source on disk is ``{"value": null, "reason": "..."}``;
- the dashboard shows that as "not available".

Sections:
- **market:** last trade price, feed freshness and live health from the live checkpoint; current sessions from the
  calendar;
- **orderflow:** the latest Phase 6 snapshot (POC, VAH/VAL, VWAP and bands, HVN/LVN counts), the latest CVD row, recent
  Phase 7 events and Phase 9 auction events;
- **trading:** the latest signals and suppressed signals; per execution run, orders, positions, risk state, emergency
  latch, blocks, last heartbeat, and audit-chain status;
- **research:** Phase 3 coverage by status, venue coverage, backtests, studies, research-ledger chain, test lock,
  reports;
- **system:** disk space, log files and recent errors, catalog database integrity, version.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import time
from collections import Counter
from pathlib import Path
from typing import Any, Callable

import pyarrow.parquet as pq

from ..schemas import MARKET_TYPE, PROVIDER_ID


def item(value: Any, source: Path | str | None, as_of: int | None = None, **extra: Any) -> dict[str, Any]:
    return {"value": value, "source": str(source) if source is not None else None, "as_of": as_of, **extra}


def missing(source: Path | str | None, reason: str) -> dict[str, Any]:
    return {"value": None, "source": str(source) if source is not None else None, "reason": reason}


def _derived(cfg: Any, dataset: str) -> Path:
    return (
        Path(cfg.data_root)
        / "derived"
        / f"provider={PROVIDER_ID}"
        / f"market={MARKET_TYPE}"
        / f"instrument={cfg.symbol}"
        / dataset
    )


def _latest_file(root: Path, name: str) -> Path | None:
    days = sorted(root.glob(f"date=*/{name}"))
    return days[-1] if days else None


def _tail_rows(p: Path, n: int, sort_col: str | None = None, cols: list[str] | None = None) -> list[dict[str, Any]]:
    t = pq.read_table(p, columns=cols)
    rows = t.to_pylist()
    if sort_col:
        rows.sort(key=lambda r: (r.get(sort_col) is None, r.get(sort_col)))
    return [{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in r.items()} for r in rows[-n:]]


def market(cfg: Any, now_ms: int) -> dict[str, Any]:
    out: dict[str, Any] = {}
    cps = sorted((Path(cfg.data_root) / "live").glob("*.checkpoint.json"), key=lambda p: p.stat().st_mtime)
    if not cps:
        out["live"] = missing(Path(cfg.data_root) / "live", "no live checkpoint: live-run has not run on this data root")
    else:
        p = cps[-1]
        s = json.loads(p.read_text())
        h = s.get("health", {})
        last = h.get("last", {})
        out["live"] = item({"name": s["name"], "run_status": s.get("status", "running"), "checkpoint_age_s": round((now_ms - int(s["saved_ms"])) / 1000, 1), "counters": s["counters"],
                            "feeds": {k: {"last_ms": v, "age_s": None if v is None else round((now_ms - v) / 1000, 1),
                                          "stale": k in (h.get("open") or {})} for k, v in last.items()},
                            "last_trade_id": s["tracker"].get("last_id"), "last_trade_ms": s["tracker"].get("last_time"),
                            "suspected_gaps": len(s["tracker"].get("suspected_gaps", []))}, p, int(s["saved_ms"]))  # fmt: skip
    try:
        from ..processing.sessions import SessionCalendar

        cal = SessionCalendar.from_config(cfg.raw.get("sessions"))
        now_sessions = [i.name for i in cal.instances(now_ms, now_ms) if i.kind == "session" and i.start_ms <= now_ms < i.end_ms]
        out["sessions_now"] = item(
            now_sessions, "config: sessions", now_ms, weekend=bool(cal.is_weekend(__import__("numpy").array([now_ms]))[0])
        )
    except Exception as e:  # noqa: BLE001 - a status page must render even with a broken calendar
        out["sessions_now"] = missing("config: sessions", repr(e))
    return out


def orderflow(cfg: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    p = _latest_file(_derived(cfg, "profile"), "snapshots.parquet")
    if p is None:
        out["profile"] = missing(_derived(cfg, "profile"), "no Phase 6 profile dataset (run vprofile)")
    else:
        cols = ["window_key", "as_of", "available_at", "is_final", "dq_status", "poc_ticks", "val_ticks", "vah_ticks", "vwap_ticks", "sigma_ticks",
                "band_up_1_ticks", "band_dn_1_ticks", "hvn_count", "lvn_count", "total_lots"]  # fmt: skip
        rows = _tail_rows(p, 400, "available_at", cols)
        latest: dict[str, dict[str, Any]] = {}
        for r in rows:
            latest[str(r["window_key"]).split(":")[0] + ":" + str(r["window_key"]).split(":")[1]] = r
        out["profile"] = item(latest, p)
    for name, file, sort in (("cvd", "cvd.parquet", "bar_end"), ("events", "events.parquet", "available_at")):
        f = _latest_file(_derived(cfg, "orderflow"), file)
        out[name] = (
            missing(_derived(cfg, "orderflow"), "no Phase 7 dataset (run orderflow)")
            if f is None
            else item(_tail_rows(f, 10, sort), f)
        )
    f = _latest_file(_derived(cfg, "auction"), "events.parquet")
    out["auction_events"] = (
        missing(_derived(cfg, "auction"), "no Phase 9 dataset (run auction)")
        if f is None
        else item(_tail_rows(f, 10, "available_at"), f)
    )
    out["depth"] = missing(None, "price-level depth exists only from the Phase 8 live recorder; none recorded on this data root") if not list(
        (Path(cfg.data_root) / "normalized").rglob("data_type=depth_l2")) else item("recorded", Path(cfg.data_root) / "normalized")  # fmt: skip
    return out


def trading(cfg: Any, now_ms: int) -> dict[str, Any]:
    out: dict[str, Any] = {}
    roots = [_derived(cfg, "signals")] + sorted((Path(cfg.data_root) / "live").glob("*/derived/*/*/*/signals"))
    sig = []
    for r in roots:
        f = _latest_file(r, "signals.parquet")
        if f is not None:
            sig.append(item(_tail_rows(f, 10, "available_at", ["signal_id", "strategy_id", "direction", "event_time", "available_at", "entry_price_ticks",
                                                               "stop_loss_ticks", "take_profit_ticks", "setup_score", "evidence"]), f))  # fmt: skip
    out["signals"] = sig or [missing(_derived(cfg, "signals"), "no signals dataset (run signals or live-run)")]
    runs = {}
    from ..execution.audit import AuditBroken, AuditLog

    for d in sorted((Path(cfg.data_root) / "execution").glob("*/state.json")):
        s = json.loads(d.read_text())
        orders = s["orders"]["orders"]
        audit = d.parent / "audit.jsonl"
        chain: dict[str, Any]
        last_in = None
        last_kind = None
        try:
            lg = AuditLog(audit)
            chain = {"ok": True, "records": lg.verify()}
            for rec in lg.records():
                last_kind = rec["kind"]
                if rec["kind"] == "IN":
                    last_in = int(rec["at"])
            lg.close()
        except AuditBroken as e:
            chain = {"ok": False, "error": str(e)}
        runs[d.parent.name] = item({"run_status": "stopped" if last_kind == "STOP" else "running", "orders_by_state": dict(Counter(o["state"] for o in orders.values())),
                                    "open_positions": [{k: o[k] for k in ("key", "side", "open_volume", "avg_price", "sl", "tp", "strategy_id")}
                                                       for o in orders.values() if o["state"] in ("FILLED", "PARTIALLY_FILLED")],
                                    "realized": round(sum(float(o["realized"]) for o in orders.values()), 2),
                                    "risk": {k: s["risk"][k] for k in ("emergency", "emergency_reason", "blocked", "consecutive_losses", "day",
                                                                        "day_start_equity", "peak_equity")},
                                    "last_message_from_ea_age_s": None if last_in is None else round((now_ms - last_in) / 1000, 1),
                                    "audit_chain": chain}, d, int(s.get("saved_ms") or 0))  # fmt: skip
    out["execution"] = runs or missing(Path(cfg.data_root) / "execution", "no execution run (bridge-run) on this data root")
    return out


def research(cfg: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    db = Path(cfg.state_db)
    if db.exists():
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            by = dict(con.execute("SELECT status, COUNT(*) FROM day_status WHERE dataset='trades' GROUP BY status").fetchall())
            rng = con.execute("SELECT MIN(date), MAX(date) FROM day_status WHERE dataset='trades'").fetchone()
            out["coverage"] = item({"trades_days_by_status": by, "first": rng[0], "last": rng[1]}, db)
        except sqlite3.OperationalError as e:
            out["coverage"] = missing(db, f"catalog not initialised: {e}")
        finally:
            con.close()
    else:
        out["coverage"] = missing(db, "no Phase 3 catalog")
    try:
        from ..venues.ingest import venue_days

        out["venues"] = {
            v: item(dict(Counter(d["status"] for d in venue_days(cfg, v))), f"venue catalog {v}") for v in ("okx", "bybit")
        }
    except Exception as e:  # noqa: BLE001
        out["venues"] = missing(None, repr(e))
    bts = []
    for p in sorted((Path(cfg.data_root) / "backtests").glob("*/run.json")):
        r = json.loads(p.read_text())
        st = r.get("statistics", {})
        bts.append(
            item(
                {
                    "name": p.parent.name,
                    "SYNTHETIC_BROKER_DATA": bool(r.get("broker_data_synthetic")),
                    "broker": (r.get("broker") or {}).get("name"),
                    **{k: st.get(k) for k in ("trades", "net_cents", "profit_factor", "max_drawdown_pct")},
                },
                p,
            )
        )
    out["backtests"] = bts or missing(Path(cfg.data_root) / "backtests", "no backtests (they need the broker ticks, D2)")
    rr = Path(cfg.data_root) / "research"
    try:
        from ..research import ResearchLedger

        out["ledger"] = item({"entries": len(ResearchLedger(rr).verify()), "chain": "OK"}, rr / "ledger.jsonl") if (rr / "ledger.jsonl").exists() else missing(
            rr / "ledger.jsonl", "no research ledger yet")  # fmt: skip
    except Exception as e:  # noqa: BLE001 - a broken chain is a status, not a crash
        out["ledger"] = missing(rr / "ledger.jsonl", f"ledger verification FAILED: {e}")
    tl = rr / "test_lock.json"
    out["test_lock"] = item(json.loads(tl.read_text()), tl) if tl.exists() else missing(tl, "test set not opened")
    out["studies"] = [p.parent.name for p in sorted((rr / "studies").glob("*/report.md"))]
    out["reports"] = sorted(p.name for p in (Path(cfg.data_root) / "reports").glob("*.md"))
    return out


def system(cfg: Any) -> dict[str, Any]:
    root = Path(cfg.data_root)
    du = shutil.disk_usage(root if root.exists() else Path("."))
    logs = sorted((root / "state").glob("goldflow.jsonl*"))
    errors: list[dict[str, Any]] = []
    if logs:
        with open(logs[0], encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()[-2000:]
        for ln in lines:
            try:
                r = json.loads(ln)
            except ValueError:
                continue
            if r.get("severity") in ("ERROR", "CRITICAL"):
                errors.append({k: r.get(k) for k in ("ts", "component", "message")})
    db = Path(cfg.state_db)
    integ = None
    if db.exists():
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            integ = con.execute("PRAGMA quick_check").fetchone()[0]
        except sqlite3.DatabaseError as e:
            integ = f"ERROR {e}"
        finally:
            con.close()
    from .. import __version__ as v

    return {"disk": item({"free_gb": round(du.free / 1e9, 2), "total_gb": round(du.total / 1e9, 2), "free_pct": round(100 * du.free / du.total, 1)}, root),
            "logs": item([{"file": p.name, "mb": round(p.stat().st_size / 1e6, 2)} for p in logs], root / "state"),
            "recent_errors": item(errors[-20:], logs[0]) if logs else missing(root / "state", "no log file yet"), "catalog_integrity": item(integ, db) if integ else missing(db, "no catalog"),
            "version": v}  # fmt: skip


def collect(cfg: Any, now_ms: int | None = None) -> dict[str, Any]:
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    out: dict[str, Any] = {"generated_ms": now, "symbol": cfg.symbol}
    sections: dict[str, Callable[[], Any]] = {
        "market": lambda: market(cfg, now),
        "orderflow": lambda: orderflow(cfg),
        "trading": lambda: trading(cfg, now),
        "research": lambda: research(cfg),
        "system": lambda: system(cfg),
    }
    for name, fn in sections.items():
        try:
            out[name] = fn()
        except Exception as e:  # noqa: BLE001 - one broken section must not hide the others; the error is shown
            out[name] = missing(None, f"section failed: {e!r}")
    return out
