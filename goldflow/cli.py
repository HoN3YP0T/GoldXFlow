"""Command line entry point.

  python -m goldflow.cli discover  --config config/binance_xauusdt.json
  python -m goldflow.cli acquire   --config ... [--months 6 | --start ISO --end ISO] [--new-range] [--no-rest]
  python -m goldflow.cli rest      --config ...          # REST recovery units only (run where fapi is reachable)
  python -m goldflow.cli manifest  --config ...
  python -m goldflow.cli pipeline  --name run1 [--resume] [--backtest] [--if-changed]  # Phases 5-11 (12) in ONE replay + every verifier
  python -m goldflow.cli data-status                                            # offline: is the download complete? (exit 0 = yes)
  python -m goldflow.cli process   --name p1 [--resume] [--dates ..] [--budget S]   # Phase 5 trade processor
  python -m goldflow.cli reconcile [--interval 1m]                              # Phase 5 footprint checks
  python -m goldflow.cli vprofile  --name v1 [--resume] [--footprint]          # Phase 6 volume profile / VWAP snapshots
  python -m goldflow.cli vprofile-verify [--sample-hours 6] | vprofile-export --window KEY --bin 10 [--as-of ISO]
  python -m goldflow.cli orderflow --name o1 [--resume]                       # Phase 7 delta/CVD/imbalance/big trades/events
  python -m goldflow.cli orderflow-verify [--dates ..]                        # Phase 7 independent recomputation
  python -m goldflow.cli auction --name a1 [--resume] | auction-verify | auction-review [--limit N]   # Phase 9 AMT
  python -m goldflow.cli ib --name i1 [--resume] | ib-verify                                        # Phase 10 Initial Balance
  python -m goldflow.cli signals --name s1 [--resume] | signals-verify                              # Phase 11 strategy signals
  python -m goldflow.cli broker-import --file F --format mt5|csv | backtest --name b1 [--resume]    # Phase 12 backtesting
  python -m goldflow.cli perf-report --name b1 | study --plan P.json --name s1 | ledger-verify     # Phase 13 research
  python -m goldflow.cli live-run --name L [--duration S] | parity --start ISO --end ISO [--from-capture]  # Phase 14 live
  python -m goldflow.cli venue-import --provider okx|bybit --dates D.. | venue-run --provider P --name N    # Phase 15 venues
  python -m goldflow.cli basis --pair binance:okx --dates D.. | volume-oz --venues binance okx bybit --dates D.. | providers-report
  python -m goldflow.cli bridge-run --profile demo --name E1 | bridge-status | emergency-stop | emergency-reset --operator N  # Phase 16
  python -m goldflow.cli exec-gate --name E1 --operator N | bridge-secret --out F | audit-verify --name E1 | ea-vector
  python -m goldflow.cli dashboard [--port P] | ops-status | ops-check | backup --dest D | backup-verify --src S | restore --src S --target T
  python -m goldflow.cli depth-record [--duration S]   # Phase 8 live L2 recorder (non-US host)
  python -m goldflow.cli depth-replay | depth-normalize
The acquisition range is frozen in data/state/range.json on first run so that resumed runs
cover exactly the same [start, end); pass --new-range to start a fresh range.
Exit code 1 if any unit failed or any interval of a primary dataset is uncovered.
"""
from __future__ import annotations
import argparse, json, logging, os, sys
from pathlib import Path

from .acquire import list_dataset, make_archive, run
from .checkpoint import Checkpoint
from .config import Config
from .report import build_manifest
from .rest_client import GeoBlocked
from .timeutil import iso, minus_months, parse_iso, to_ms, utcnow

def _range(cfg: Config, a) -> tuple[int, int]:
    rp = cfg.data_root / "state" / "range.json"
    if rp.exists() and not a.new_range:
        r = json.loads(rp.read_text())
        asked = (to_ms(parse_iso(a.start)) if a.start else None, to_ms(parse_iso(a.end)) if a.end else None)
        if a.months or (asked[0] is not None and asked[0] != r["start_ms"]) or (asked[1] is not None and asked[1] != r["end_ms"]):
            # an explicit range that differs from the frozen one used to be ignored silently
            print(f"goldflow: the acquisition range is frozen at {r['start']} -> {r['end']} ({rp}); "
                  "add --new-range to use the range you gave, or drop --start/--end/--months to continue the frozen one", file=sys.stderr)
            raise SystemExit(2)
        return r["start_ms"], r["end_ms"]
    end = parse_iso(a.end) if a.end else utcnow()
    start = parse_iso(a.start) if a.start else minus_months(end, a.months or cfg["range"]["months"])
    s, e = to_ms(start), to_ms(end)
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps({"start_ms": s, "end_ms": e, "start": iso(s), "end": iso(e),
                              "frozen_at": iso(to_ms(utcnow()))}, indent=1))
    return s, e

def discover(cfg: Config) -> dict:
    out = {"symbol": cfg.symbol, "market": cfg.market, "checked_utc": iso(to_ms(utcnow()))}
    from .providers import ProviderUnavailable, create_provider
    prov = create_provider(cfg)
    rest = prov.rest
    try:
        spec = prov.discover_instrument(cfg.symbol).raw
        problems = []
        if spec["status"] != "TRADING": problems.append(f"status {spec['status']}")
        if spec["contract_type"] != cfg["expected_contract_type"]: problems.append(f"contract type {spec['contract_type']}")
        if spec["underlying_type"] != cfg["expected_underlying_type"]: problems.append(f"underlying {spec['underlying_type']}")
        out["rest"] = {"base_url": rest.base, "ok": not problems, "problems": problems, "spec": spec}
    except (GeoBlocked, ProviderUnavailable) as e:
        out["rest"] = {"base_url": rest.base, "ok": False, "geo_blocked": True, "error": str(e)}
    arch = make_archive(cfg); avail = {}
    for ds, opt in cfg["datasets"].items():
        for g in ("daily", "monthly"):
            if not opt.get(g): continue
            keys = sorted(k for k in list_dataset(arch, cfg, ds, g) if k.endswith(".zip"))
            avail[f"{ds}/{g}"] = {"files": len(keys), "first": keys[0].rsplit("/", 1)[-1] if keys else None,
                                  "last": keys[-1].rsplit("/", 1)[-1] if keys else None}
    out["archive_availability"] = avail
    p = cfg.data_root / "state" / "instrument_discovery.json"; p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(out, indent=1))
    return out

def _phase3_report(cfg, summary, budget=None):
    from .catalog import check_cross_partition, crosscheck_monthly
    from .dataset_report import build_phase3_reports
    xc = crosscheck_monthly(cfg, budget)
    build_phase3_reports(cfg, summary, check_cross_partition(cfg), xc)

def _replay_cmd(cfg, a, log) -> int:
    """Phase 4 replay. Ctrl-C / SIGTERM / --budget stop gracefully at a checkpoint; --resume continues exactly."""
    import signal, threading
    from .replay import ReplayEngine, ChainHasher, IntegrityChecker
    streams = None
    if a.streams:
        streams = {k: {**v, "enabled": k in a.streams} for k, v in cfg["replay"]["streams"].items()}
    sessions = [tuple(x.split("-")) for x in (a.session or [])]
    hasher, checker = ChainHasher(), IntegrityChecker()
    prog = lambda m: log.info("replay %s day=%s events=%d clock=%s %.0f ev/s rss=%dMB", a.name or "", m.get("day"),
                              m["events"], m["sim_clock"], m["events_per_s"] or 0, m["peak_rss_mb"])
    eng = ReplayEngine(cfg, [hasher, checker], start=a.start, end=a.end, dates=a.dates, sessions=sessions, streams=streams,
                       name=a.name, speed=0.0 if a.speed == "max" else float(a.speed), progress=prog)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: (log.info("stop requested; checkpointing"), eng.stop()))
    if a.budget:
        threading.Timer(a.budget, eng.stop).start()
    res = eng.run(resume=a.resume, until=a.until)
    out = {"name": eng.name, "status": res["status"], "events": res["events"], "events_this_run": res["events_this_run"], "per_stream": res["per_stream"],
           "sim_clock": res["sim_clock"], "wall_s": res["wall_s"], "events_per_s": res["events_per_s"],
           "peak_rss_mb": res["peak_rss_mb"], "sequence_digest": hasher.digest, "integrity_violations": checker.violations,
           "dataset_version": eng.dataset_version[:16], "config_hash": eng.config_hash[:16]}
    print(json.dumps(out, indent=1))
    if checker.violations: return 1
    return 0 if res["status"] == "completed" else 3

def _status(cfg, profile) -> int:
    """Validate configuration and report the non-trading state (Phase 1 acceptance: starts in non-trading mode)."""
    from .fsutil import connect
    from .migrations import current_version
    from .profiles import live_gate_status, trading_mode
    from .providers import create_provider
    st = {"config": "VALID", "config_hash": cfg.config_hash[:16], "profile": profile.value,
          "trading_mode": trading_mode(profile), "live_trading_enabled": cfg.settings.execution.live_trading_enabled,
          "live_gate": live_gate_status(cfg.data_root), "instrument": cfg.symbol, "provider": cfg["source"],
          "state_db": str(cfg.state_db), "schema_migration_version": current_version(connect(cfg.state_db))}
    prov = create_provider(cfg, profile)
    st["provider_health"] = prov.health().__dict__
    st["capabilities"] = {k: v["availability"] for k, v in prov.capabilities().as_dict().items()}
    print(json.dumps(st, indent=1, default=str))
    return 0

def _providers(cfg, profile) -> int:
    from .providers import available_providers
    out = {}
    for pid, cls in sorted(available_providers().items()):
        caps = cls.capabilities(cls.__new__(cls))  # capability sets are static declarations
        out[pid] = {"synthetic": cls.synthetic, "allowed_in_profile": (not cls.synthetic) or profile.value == "test",
                    "capabilities": caps.as_dict()}
    print(json.dumps(out, indent=1))
    return 0

def _process_cmd(cfg, a, log) -> int:
    """Phase 5: replay -> TradeProcessor -> footprint Parquet, then reconciliation. Resumable like replay."""
    import signal, threading
    from .replay import ReplayEngine, ChainHasher, IntegrityChecker
    from .processing.trade_processor import TradeProcessor
    from .processing.footprint_store import FootprintWriter
    from .processing.reconcile import reconcile
    name = a.name or "process"
    writer = FootprintWriter(cfg.data_root, cfg.symbol, replace=not a.resume)
    proc = TradeProcessor(cfg, sinks=[writer]); hasher, checker = ChainHasher(), IntegrityChecker()
    prog = lambda m: log.info("process %s day=%s events=%d clock=%s %.0f ev/s rss=%dMB", name, m.get("day"), m["events"],
                              m["sim_clock"], m["events_per_s"] or 0, m["peak_rss_mb"])
    eng = ReplayEngine(cfg, [hasher, checker, proc], start=a.start, end=a.end, dates=a.dates, name=name, progress=prog)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: (log.info("stop requested; checkpointing"), eng.stop()))
    if a.budget: threading.Timer(a.budget, eng.stop).start()
    res = eng.run(resume=a.resume, until=a.until)
    out = {"name": name, "status": res["status"], "events": res["events"], "events_this_run": res["events_this_run"],
           "per_stream": res["per_stream"], "sim_clock": res["sim_clock"], "wall_s": res["wall_s"], "events_per_s": res["events_per_s"],
           "peak_rss_mb": res["peak_rss_mb"], "sequence_digest": hasher.digest, "integrity_violations": checker.violations,
           "processor": proc.summary()}
    if res["status"] == "completed" and not a.no_reconcile:
        rep = reconcile(cfg, interval=proc.st.persist_intervals[0]) if proc.st.persist_intervals else None
        if rep: out["reconciliation"] = rep["summary"]
    print(json.dumps(out, indent=1, default=str))
    bad = checker.violations or (out.get("reconciliation") or {}).get("fail")
    return 1 if bad else (0 if res["status"] == "completed" else 3)

def _vprofile_cmd(cfg, a, log) -> int:
    """Phase 6: replay -> TradeProcessor -> ProfileEngine (+ optional footprint) -> profile Parquet, then verification."""
    import signal, threading
    from .replay import ReplayEngine, ChainHasher, IntegrityChecker
    from .processing.trade_processor import TradeProcessor
    from .processing.footprint_store import FootprintWriter
    from .orderflow.engine import ProfileEngine
    from .orderflow.store import ProfileStore
    from .orderflow.verify import verify
    name = a.name or "vprofile"
    engine = ProfileEngine(cfg, store=ProfileStore(cfg.data_root, cfg.symbol, replace=not a.resume))
    sinks = [engine] + ([FootprintWriter(cfg.data_root, cfg.symbol, replace=not a.resume)] if a.footprint else [])
    proc = TradeProcessor(cfg, sinks=sinks); hasher, checker = ChainHasher(), IntegrityChecker()
    prog = lambda m: log.info("vprofile %s day=%s events=%d clock=%s %.0f ev/s rss=%dMB", name, m.get("day"), m["events"],
                              m["sim_clock"], m["events_per_s"] or 0, m["peak_rss_mb"])
    eng = ReplayEngine(cfg, [hasher, checker, proc], start=a.start, end=a.end, dates=a.dates, name=name, progress=prog)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: (log.info("stop requested; checkpointing"), eng.stop()))
    if a.budget: threading.Timer(a.budget, eng.stop).start()
    res = eng.run(resume=a.resume, until=a.until)
    out = {"name": name, "status": res["status"], "events": res["events"], "events_this_run": res["events_this_run"],
           "sim_clock": res["sim_clock"], "wall_s": res["wall_s"], "events_per_s": res["events_per_s"],
           "peak_rss_mb": res["peak_rss_mb"], "sequence_digest": hasher.digest, "integrity_violations": checker.violations,
           "processor_state_digest": proc.state_digest(), "profile": engine.summary()}
    if res["status"] == "completed" and not a.no_reconcile:
        out["verification"] = verify(cfg, sample_hours=a.sample_hours)["summary"]
    print(json.dumps(out, indent=1, default=str))
    bad = checker.violations or engine.stats["final_parity_mismatch"] or (out.get("verification") or {}).get("fail")
    return 1 if bad else (0 if res["status"] == "completed" else 3)

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="goldflow")
    ap.add_argument("cmd", choices=["status", "providers", "discover", "acquire", "rest", "manifest", "normalize",
                                    "validate", "report", "replay", "process", "reconcile",
                                    "vprofile", "vprofile-verify", "vprofile-export", "orderflow", "orderflow-verify",
                                    "auction", "auction-verify", "auction-review", "ib", "ib-verify",
                                    "signals", "signals-verify", "broker-import", "backtest",
                                    "perf-report", "study", "ledger-verify", "live-run", "parity",
                                    "venue-import", "venue-run", "basis", "volume-oz", "providers-report",
                                    "bridge-run", "bridge-status", "emergency-stop", "emergency-reset", "exec-gate", "bridge-secret",
                                    "audit-verify", "ea-vector", "dashboard", "ops-status", "ops-check", "backup", "backup-verify",
                                    "restore", "pipeline", "data-status",
                                    "depth-record", "depth-replay", "depth-normalize"])
    ap.add_argument("--profile", help="research (default) | test | demo (MT5 demo routing) | live; or env GOLDFLOW_PROFILE")
    ap.add_argument("--config", default="config/binance_xauusdt.json")
    ap.add_argument("--months", type=int); ap.add_argument("--start"); ap.add_argument("--end")
    ap.add_argument("--new-range", action="store_true"); ap.add_argument("--no-rest", action="store_true")
    ap.add_argument("--skip-reconcile", action="store_true")
    ap.add_argument("--datasets", nargs="*")
    ap.add_argument("--dates", nargs="*", help="normalize/validate only these UTC dates (YYYY-MM-DD)")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--budget", type=float, help="normalize/validate: stop starting new days after this many seconds (resumable)")
    ap.add_argument("--resume", action="store_true", help="validate: continue a budgeted pass instead of starting fresh")
    ap.add_argument("--reset", action="store_true", help="normalize: reprocess every day")
    ap.add_argument("--no-report", action="store_true")
    ap.add_argument("--name", help="replay: checkpoint name")
    ap.add_argument("--speed", default="max", help="replay: max | <factor> (1 = real time, 60 = 1 min per second)")
    ap.add_argument("--session", action="append", help="replay: UTC time-of-day window HH:MM-HH:MM (repeatable)")
    ap.add_argument("--until", help="replay: stop before this UTC time (resumable)")
    ap.add_argument("--streams", nargs="*", help="replay: enable only these streams (default: config)")
    ap.add_argument("--no-reconcile", action="store_true", help="process: skip the reconciliation after completion")
    ap.add_argument("--interval", default="1m", help="reconcile: footprint interval to check")
    ap.add_argument("--footprint", action="store_true", help="vprofile: also write the Phase 5 footprint dataset")
    ap.add_argument("--sample-hours", type=int, default=6, help="vprofile-verify: rebuild developing snapshots every N hours")
    ap.add_argument("--window", help="vprofile-export: window key, e.g. session:london:2026-10-26")
    ap.add_argument("--bin", type=int, default=10, help="vprofile-export: profile bin size in ticks")
    ap.add_argument("--as-of", help="vprofile-export: developing snapshot time (UTC ISO); default = latest/final")
    ap.add_argument("--file", help="broker-import: MT5 tick export to import")
    ap.add_argument("--plan", help="study: research plan (JSON)")
    ap.add_argument("--format", default="mt5", help="broker-import: mt5 (terminal tick export) | csv (time_msc,bid,ask)")
    ap.add_argument("--limit", type=int, default=120, help="auction-review: number of event charts")
    ap.add_argument("--duration", type=float, help="depth-record/live-run: stop after this many seconds (default: run until Ctrl-C)")
    ap.add_argument("--from-capture", action="store_true", help="parity: use the trades recorded by live-run instead of the archive")
    ap.add_argument("--provider", help="venue-import/venue-run: okx | bybit (Phase 15)")
    ap.add_argument("--pair", help="basis: venue pair A:B, e.g. binance:okx")
    ap.add_argument("--venues", nargs="*", help="volume-oz: venues to aggregate (labelled cross-venue aggregate)")
    ap.add_argument("--no-probe", action="store_true", help="providers-report: skip the reachability probes")
    ap.add_argument("--operator", help="emergency-reset / exec-gate: operator name recorded with the action")
    ap.add_argument("--reason", default="operator emergency stop", help="emergency-stop: reason")
    ap.add_argument("--out", help="bridge-secret: file to write the new shared secret to")
    ap.add_argument("--dest", help="backup: backup root (snapshots go to <dest>/<UTC timestamp>/)")
    ap.add_argument("--src", help="backup-verify/restore: snapshot directory")
    ap.add_argument("--target", help="restore: new, empty data root to restore into")
    ap.add_argument("--port", type=int, help="dashboard: port (default ops.dashboard_port)")
    ap.add_argument("--backtest", action="store_true", help="pipeline: also run the Phase 12 backtester (needs the broker block, D2)")
    ap.add_argument("--no-verify", action="store_true", help="pipeline: skip the independent verifiers")
    ap.add_argument("--if-changed", action="store_true", help="pipeline/validate: do nothing if the last run is up to date (same data, config, code)")
    a = ap.parse_args(argv)
    from .migrations import apply_migrations
    from .observability import new_run_id, setup_logging
    from .profiles import LiveTradingLocked, assert_profile_allowed, resolve_profile
    from .settings import ConfigError
    try:
        profile = resolve_profile(a.profile)
        cfg = Config.load(a.config)              # validates; invalid or conflicting settings stop here
        assert_profile_allowed(profile, cfg.settings, cfg.data_root)
    except (ConfigError, ValueError, LiveTradingLocked) as e:
        print(f"goldflow: {e}", file=sys.stderr)
        return 2
    (cfg.data_root / "state").mkdir(parents=True, exist_ok=True)
    run_id = new_run_id(a.cmd)
    _ops = (cfg.raw.get("ops") or {})
    setup_logging(cfg.data_root / "state", run_id, profile.value, cfg["source"], cfg.symbol,
                  max_mb=int(_ops.get("log_max_mb", 50)), backups=int(_ops.get("log_backups", 10)))
    for h in logging.getLogger().handlers:      # console to stdout, as before (wrappers capture stdout)
        if type(h) is logging.StreamHandler: h.setStream(sys.stdout)
    from .fsutil import connect
    _db = connect(cfg.state_db); apply_migrations(_db); _db.close()
    log = logging.getLogger("goldflow")
    log.info("start cmd=%s profile=%s config_hash=%s", a.cmd, profile.value, cfg.config_hash[:16])
    if a.cmd == "status":
        return _status(cfg, profile)
    if a.cmd == "providers":
        return _providers(cfg, profile)
    if a.cmd == "discover":
        print(json.dumps(discover(cfg), indent=1, default=str)[:4000]); return 0
    if a.cmd in ("normalize", "validate"):
        # Phase 3. normalize: validate + write partitions/catalog. validate: recompute everything into a scratch
        # catalog, write no data files, and compare the result digest with the previous run.
        from .catalog import run_phase3
        if a.cmd == "validate" and a.if_changed and not a.dates:
            why = _validate_current(cfg)
            if why is None:
                print(json.dumps({"validate": "up_to_date", "reason": "the last validation ran after the last normalisation, "
                                  "with the same code and config, and found no differences"})); return 0
            log.info("validate runs: %s", why)
        reset = a.reset if a.cmd == "normalize" else not a.resume
        summary = run_phase3(cfg, write=a.cmd == "normalize", workers=a.workers, dates=a.dates, budget_s=a.budget, reset=reset)
        log.info("phase3 %s", json.dumps({k: v for k, v in summary.items() if k != "raw_registry"}, default=str))
        if not summary["finalized"]:
            log.info("PENDING %d days remain; rerun with %s", summary["pending_remaining"],
                     "the same command" if a.cmd == "normalize" else "--resume")
            return 3
        if not a.no_report and not a.dates:
            _phase3_report(cfg, summary)
        return 0 if not summary["digest_differences_vs_catalog"] else 1
    if a.cmd == "replay":
        return _replay_cmd(cfg, a, log)
    if a.cmd == "process":
        return _process_cmd(cfg, a, log)
    if a.cmd == "reconcile":
        from .processing.reconcile import reconcile
        rep = reconcile(cfg, interval=a.interval)
        print(json.dumps(rep["summary"], indent=1)); return 1 if rep["summary"]["fail"] else 0
    if a.cmd == "vprofile":
        return _vprofile_cmd(cfg, a, log)
    if a.cmd == "vprofile-verify":
        from .orderflow.verify import verify
        rep = verify(cfg, dates=a.dates, sample_hours=a.sample_hours)
        print(json.dumps(rep["summary"], indent=1)); return 1 if rep["summary"]["fail"] else 0
    if a.cmd == "orderflow":
        return _orderflow_cmd(cfg, a, log)
    if a.cmd == "orderflow-verify":
        from .orderflow.flow_verify import verify as of_verify
        rep = of_verify(cfg, dates=a.dates)
        print(json.dumps(rep["summary"], indent=1)); return 1 if rep["summary"]["fail"] else 0
    if a.cmd == "auction":
        return _auction_cmd(cfg, a, log)
    if a.cmd == "auction-verify":
        from .orderflow.auction_verify import verify as au_verify
        rep = au_verify(cfg, dates=a.dates)
        print(json.dumps(rep["summary"], indent=1)); return 1 if rep["summary"]["fail"] else 0
    if a.cmd == "ib":
        return _ib_cmd(cfg, a, log)
    if a.cmd == "ib-verify":
        from .orderflow.ib_verify import verify as ib_verify
        rep = ib_verify(cfg, dates=a.dates)
        print(json.dumps(rep["summary"], indent=1)); return 1 if rep["summary"]["fail"] else 0
    if a.cmd == "broker-import":
        from .backtesting import BrokerSpec, import_ticks
        spec = BrokerSpec.from_raw(cfg.raw.get("broker"))
        if spec is None or not a.file:
            print("goldflow: broker-import needs the broker block filled in (decision D2) and --file", file=sys.stderr); return 2
        print(json.dumps(import_ticks(cfg.data_root, spec, Path(a.file), a.format), indent=1)); return 0
    if a.cmd == "backtest":
        return _backtest_cmd(cfg, a, log)
    if a.cmd == "perf-report":
        import pyarrow.parquet as pq
        from .research import ResearchSettings, performance
        rs = ResearchSettings.from_config(cfg)
        d = cfg.data_root / "backtests" / (a.name or "backtest")
        if not (d / "run.json").exists():
            print(f"goldflow: no backtest run at {d} (run `backtest --name {a.name or 'backtest'}` first; it needs the broker block, D2)",
                  file=sys.stderr); return 2
        rd = lambda n: pq.read_table(d / f"{n}.parquet").to_pylist() if (d / f"{n}.parquet").exists() else []
        rec = json.loads((d / "run.json").read_text())
        sig = {r["signal_id"]: r for r in rd("signals")}
        p = performance(rd("trades"), rd("cash"), int(rec["statistics"]["starting_cents"]), rd("fills"), sig, rs.min_trades_conclusion,
                        (rs.boot_samples, rs.boot_seed))
        (d / "performance.json").write_text(json.dumps(p, indent=1, default=str))
        print(json.dumps({k: p[k] for k in ("trades", "sample", "net_cents", "profit_factor", "profit_factor_ci95", "expectancy_r",
                                            "max_drawdown_pct")}, indent=1, default=str)); return 0
    if a.cmd == "study":
        from .research import run_study
        out = run_study(cfg, json.loads(Path(a.plan).read_text()), a.name or "study")
        print(json.dumps({"approved": out["approval"]["approved"], "final_candidate": out.get("final_candidate"),
                          "report": str(cfg.data_root / "research" / "studies" / (a.name or "study") / "report.md")}, indent=1)); return 0
    if a.cmd == "ledger-verify":
        from .research import ResearchLedger
        es = ResearchLedger(cfg.data_root / "research").verify()
        print(json.dumps({"entries": len(es), "chain": "OK"}, indent=1)); return 0
    if a.cmd == "signals":
        return _signals_cmd(cfg, a, log)
    if a.cmd == "pipeline":
        return _pipeline_cmd(cfg, a, log)
    if a.cmd == "data-status":
        from .datastatus import data_status
        st = data_status(cfg); print(json.dumps(st, indent=1)); return 0 if st["complete"] else 1
    if a.cmd == "live-run":
        return _live_cmd(cfg, a, log)
    if a.cmd in ("venue-import", "venue-run", "basis", "volume-oz", "providers-report"):
        return _venue_cmd(cfg, a, log)
    if a.cmd in ("dashboard", "ops-status", "ops-check", "backup", "backup-verify", "restore"):
        return _ops_cmd(cfg, a, log)
    if a.cmd in ("bridge-run", "bridge-status", "emergency-stop", "emergency-reset", "exec-gate", "bridge-secret", "audit-verify", "ea-vector"):
        return _exec_cmd(cfg, a, log, profile)
    if a.cmd == "parity":
        from .live import capture_polls, run_parity
        if not (a.start and a.end):
            print("goldflow: parity needs --start and --end", file=sys.stderr); return 2
        s0, e0 = to_ms(parse_iso(a.start)), to_ms(parse_iso(a.end))
        polls = capture_polls(cfg, a.dates) if a.from_capture else None
        out = run_parity(cfg, s0, e0, polls=polls)
        print(json.dumps({k: v for k, v in out.items() if k != "tables"} | {"unequal": [t for t, v in out["tables"].items() if not v["equal"]]},
                         indent=1, default=str))
        return 0 if out["all_equal"] else 1
    if a.cmd == "signals-verify":
        from .strategies.verify import verify as sig_verify
        rep = sig_verify(cfg)
        print(json.dumps(rep["summary"], indent=1)); return 1 if rep["summary"]["fail"] else 0
    if a.cmd == "auction-review":
        from .orderflow.auction_review import review
        print(json.dumps(review(cfg, limit=a.limit), indent=1)); return 0
    if a.cmd == "vprofile-export":
        from .orderflow.export import export_window
        if not a.window: print("goldflow: vprofile-export needs --window", file=sys.stderr); return 2
        p = export_window(cfg, a.window, a.bin, to_ms(parse_iso(a.as_of)) if a.as_of else None)
        print(str(p)); return 0
    if a.cmd.startswith("depth-"):   # Phase 8: live L2 recorder, capture replay, DepthEvent normalisation
        from .liquidity.commands import run_depth_command
        return run_depth_command(cfg, a)
    if a.cmd == "report":
        from .fsutil import connect
        db = connect(cfg.state_db)
        row = db.execute("SELECT summary_json FROM validation_runs WHERE mode='normalize' ORDER BY started_utc DESC LIMIT 1").fetchone()
        try:
            _phase3_report(cfg, json.loads(row[0]), a.budget); return 0
        except TimeoutError as e:
            log.info("PENDING %s", e); return 3
    s, e = _range(cfg, a)
    log.info("range [%s, %s)", iso(s), iso(e))
    if a.cmd in ("acquire", "rest"):
        res = run(cfg, s, e, do_archive=a.cmd == "acquire", do_rest=not a.no_rest, datasets=a.datasets)
        log.info("run result %s", json.dumps(res))
        if a.cmd == "acquire":
            from .datastatus import record_complete
            mk = record_complete(cfg, res, a.datasets, with_rest=not a.no_rest)
            log.info("download %s", f"complete: recorded in {mk}" if mk else "not complete yet: re-run acquire to resume")
    if a.cmd in ("acquire", "manifest") and not a.skip_reconcile:
        from .report import trades_vs_aggtrades
        log.info("reconciling trades vs aggTrades (quantity level)...")
        trades_vs_aggtrades(cfg, s, e, cfg.data_root / "recovered")
    disc = cfg.data_root / "state" / "instrument_discovery.json"
    spec = json.loads(disc.read_text()) if disc.exists() else None
    runs = [dict(r) for r in Checkpoint(cfg.state_db).query("SELECT run_id,started_utc,finished_utc,status,notes FROM runs ORDER BY started_utc")]
    m = build_manifest(cfg, {"runs": runs}, s, e, spec, cfg.data_root / "manifest.json")
    bad = any(d["units_failed"] for d in m["datasets"].values()) or \
          m["datasets"]["trades"]["coverage"]["uncovered_intervals"]
    return 1 if bad else 0

def _orderflow_cmd(cfg, a, log) -> int:
    """Phase 7: replay -> TradeProcessor -> OrderflowEngine -> orderflow Parquet, then independent verification."""
    import signal, threading
    from .replay import ReplayEngine, ChainHasher, IntegrityChecker
    from .processing.trade_processor import TradeProcessor
    from .orderflow.flow_engine import OrderflowEngine
    from .orderflow.flow_store import OrderflowStore
    from .orderflow.flow_verify import verify as of_verify
    name = a.name or "orderflow"
    engine = OrderflowEngine(cfg, store=OrderflowStore(cfg.data_root, cfg.symbol, replace=not a.resume))
    proc = TradeProcessor(cfg, sinks=[engine]); hasher, checker = ChainHasher(), IntegrityChecker()
    prog = lambda m: log.info("orderflow %s day=%s events=%d clock=%s %.0f ev/s rss=%dMB", name, m.get("day"), m["events"],
                              m["sim_clock"], m["events_per_s"] or 0, m["peak_rss_mb"])
    eng = ReplayEngine(cfg, [hasher, checker, proc], start=a.start, end=a.end, dates=a.dates, name=name, progress=prog)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: (log.info("stop requested; checkpointing"), eng.stop()))
    if a.budget: threading.Timer(a.budget, eng.stop).start()
    res = eng.run(resume=a.resume, until=a.until)
    out = {"name": name, "status": res["status"], "events": res["events"], "events_this_run": res["events_this_run"],
           "sim_clock": res["sim_clock"], "wall_s": res["wall_s"], "events_per_s": res["events_per_s"],
           "peak_rss_mb": res["peak_rss_mb"], "sequence_digest": hasher.digest, "integrity_violations": checker.violations,
           "processor_state_digest": proc.state_digest(), "orderflow": engine.summary()}
    if res["status"] == "completed" and not a.no_reconcile:
        out["verification"] = of_verify(cfg)["summary"]
    print(json.dumps(out, indent=1, default=str))
    bad = checker.violations or (out.get("verification") or {}).get("fail")
    return 1 if bad else (0 if res["status"] == "completed" else 3)

def _auction_cmd(cfg, a, log) -> int:
    """Phase 9: replay -> TradeProcessor -> ProfileEngine -> AuctionEngine -> auction Parquet, then verification."""
    import signal, threading
    from .replay import ReplayEngine, ChainHasher, IntegrityChecker
    from .processing.trade_processor import TradeProcessor
    from .orderflow.engine import ProfileEngine
    from .orderflow.auction_engine import AuctionEngine, AuctionStore
    from .orderflow.auction_verify import verify as au_verify
    name = a.name or "auction"
    from .orderflow.auction_settings import AuctionSettings
    from .orderflow.initial_balance import IBEngine
    prof = ProfileEngine(cfg)            # in memory only: the profile dataset is written by vprofile
    ib = IBEngine(cfg) if any(x.startswith("ib") for x in AuctionSettings.from_config(cfg).sources) else None   # IB levels (Phase 10)
    engine = AuctionEngine(cfg, prof, store=AuctionStore(cfg.data_root, cfg.symbol, replace=not a.resume), ib_engine=ib)
    proc = TradeProcessor(cfg, sinks=[prof] + ([ib] if ib else []) + [engine]); hasher, checker = ChainHasher(), IntegrityChecker()
    prog = lambda m: log.info("auction %s day=%s events=%d clock=%s %.0f ev/s rss=%dMB", name, m.get("day"), m["events"],
                              m["sim_clock"], m["events_per_s"] or 0, m["peak_rss_mb"])
    eng = ReplayEngine(cfg, [hasher, checker, proc], start=a.start, end=a.end, dates=a.dates, name=name, progress=prog)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: (log.info("stop requested; checkpointing"), eng.stop()))
    if a.budget: threading.Timer(a.budget, eng.stop).start()
    res = eng.run(resume=a.resume, until=a.until)
    out = {"name": name, "status": res["status"], "events": res["events"], "events_this_run": res["events_this_run"],
           "sim_clock": res["sim_clock"], "wall_s": res["wall_s"], "events_per_s": res["events_per_s"],
           "peak_rss_mb": res["peak_rss_mb"], "sequence_digest": hasher.digest, "integrity_violations": checker.violations,
           "processor_state_digest": proc.state_digest(), "profile_state_digest": prof.state_digest(), "auction": engine.summary()}
    if res["status"] == "completed" and not a.no_reconcile:
        out["verification"] = au_verify(cfg)["summary"]
    print(json.dumps(out, indent=1, default=str))
    bad = checker.violations or (out.get("verification") or {}).get("fail")
    return 1 if bad else (0 if res["status"] == "completed" else 3)

def _ib_cmd(cfg, a, log) -> int:
    """Phase 10: replay -> TradeProcessor -> IBEngine -> initial_balance Parquet, then verification."""
    import signal, threading
    from .replay import ReplayEngine, ChainHasher, IntegrityChecker
    from .processing.trade_processor import TradeProcessor
    from .orderflow.initial_balance import IBEngine, IBStore
    from .orderflow.ib_verify import verify as ib_verify
    name = a.name or "ib"
    engine = IBEngine(cfg, store=IBStore(cfg.data_root, cfg.symbol, replace=not a.resume))
    proc = TradeProcessor(cfg, sinks=[engine]); hasher, checker = ChainHasher(), IntegrityChecker()
    eng = ReplayEngine(cfg, [hasher, checker, proc], start=a.start, end=a.end, dates=a.dates, name=name,
                       progress=lambda m: log.info("ib %s day=%s events=%d clock=%s", name, m.get("day"), m["events"], m["sim_clock"]))
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: (log.info("stop requested; checkpointing"), eng.stop()))
    if a.budget: threading.Timer(a.budget, eng.stop).start()
    res = eng.run(resume=a.resume, until=a.until)
    out = {"name": name, "status": res["status"], "events": res["events"], "wall_s": res["wall_s"], "events_per_s": res["events_per_s"],
           "sequence_digest": hasher.digest, "integrity_violations": checker.violations, "processor_state_digest": proc.state_digest(),
           "ib": engine.summary()}
    if res["status"] == "completed" and not a.no_reconcile:
        out["verification"] = ib_verify(cfg)["summary"]
    print(json.dumps(out, indent=1, default=str))
    bad = checker.violations or (out.get("verification") or {}).get("fail")
    return 1 if bad else (0 if res["status"] == "completed" else 3)

def _validate_current(cfg) -> str | None:
    """None if a clean validation already covers the current catalog; otherwise why validation must run."""
    import hashlib, sqlite3
    from .catalog import code_version
    if not cfg.state_db.exists(): return "no catalog"
    db = sqlite3.connect(f"file:{cfg.state_db}?mode=ro", uri=True)
    try:
        # day_runs is written only when a day is (re)normalised: a re-run that found every day done does not count
        last_norm = db.execute("SELECT MAX(finished_utc) FROM day_runs").fetchone()[0]
        v = db.execute("SELECT finished_utc, code_version, config_sha256, summary_json FROM validation_runs WHERE mode='validate' "
                       "AND finished_utc IS NOT NULL ORDER BY started_utc DESC LIMIT 1").fetchone()
    except sqlite3.OperationalError:
        return "no validation yet"
    finally:
        db.close()
    if v is None: return "no validation yet"
    if last_norm is not None and v[0] < last_norm: return "data was normalised after the last validation"
    if v[1] != code_version(): return "code changed since the last validation"
    if v[2] != hashlib.sha256(json.dumps(cfg.raw, sort_keys=True).encode()).hexdigest(): return "config changed since the last validation"
    s = json.loads(v[3] or "{}")
    if s.get("digest_differences_vs_catalog"): return "the last validation found differences"   # (only finished runs are selected)
    return None

def _pipeline_cmd(cfg, a, log) -> int:
    """Phases 5-11 (and 12 with --backtest) in one replay: every dataset written, then every independent verifier."""
    import signal, threading
    from .pipeline import run_pipeline
    from .backtesting import BrokerNotConfigured
    name = a.name or "pipeline"
    stops: list = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: (log.info("stop requested; checkpointing"), [f() for f in stops]))
    if a.budget: threading.Timer(a.budget, lambda: [f() for f in stops]).start()
    if a.if_changed and not a.resume:
        from .pipeline import up_to_date
        why = up_to_date(cfg, name, start=a.start, end=a.end, dates=a.dates, backtest=a.backtest)
        if why is None:
            print(json.dumps({"name": name, "status": "up_to_date", "report": str(cfg.data_root / "reports" / f"pipeline_{name}.json")}))
            return 0
        log.info("pipeline %s runs: %s", name, why)
    try:
        out = run_pipeline(cfg, name, start=a.start, end=a.end, dates=a.dates, resume=a.resume, until=a.until, backtest=a.backtest,
                           verify=not a.no_verify, stop_event=stops,
                           progress=lambda m: log.info("pipeline %s day=%s events=%d clock=%s %.0f ev/s", name, m.get("day"), m["events"],
                                                       m["sim_clock"], m["events_per_s"] or 0))
    except (BrokerNotConfigured, FileNotFoundError, ValueError) as e:
        print(f"goldflow: {e}", file=sys.stderr); return 2
    print(json.dumps(out, indent=1, default=str))
    if out["status"] != "completed": return 3
    return 0 if out["ok"] else 1

def _signals_cmd(cfg, a, log) -> int:
    """Phase 11: replay -> TradeProcessor -> Profile/Orderflow/IB/Auction engines -> StrategyEngine -> signals Parquet."""
    import signal, threading
    from .replay import ReplayEngine, ChainHasher, IntegrityChecker
    from .pipeline import build_stack
    from .strategies.verify import verify as sig_verify
    name = a.name or "signals"
    st = build_stack(cfg, stores=("signals",), resume=a.resume)   # analytics in memory; their datasets come from their own commands
    proc, strat = st.proc, st.strategies; hasher, checker = ChainHasher(), IntegrityChecker()
    eng = ReplayEngine(cfg, [hasher, checker, proc], start=a.start, end=a.end, dates=a.dates, name=name,
                       progress=lambda m: log.info("signals %s day=%s events=%d clock=%s", name, m.get("day"), m["events"], m["sim_clock"]))
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: (log.info("stop requested; checkpointing"), eng.stop()))
    if a.budget: threading.Timer(a.budget, eng.stop).start()
    res = eng.run(resume=a.resume, until=a.until)
    out = {"name": name, "status": res["status"], "events": res["events"], "wall_s": res["wall_s"], "events_per_s": res["events_per_s"],
           "peak_rss_mb": res["peak_rss_mb"], "sequence_digest": hasher.digest, "integrity_violations": checker.violations,
           "strategies": strat.summary()}
    if res["status"] == "completed" and not a.no_reconcile:
        out["verification"] = sig_verify(cfg)["summary"]
    print(json.dumps(out, indent=1, default=str))
    bad = checker.violations or (out.get("verification") or {}).get("fail")
    return 1 if bad else (0 if res["status"] == "completed" else 3)

def _days(a) -> list[str]:
    if a.dates: return list(a.dates)
    if a.start and a.end:
        from datetime import date, timedelta
        d, e = date.fromisoformat(a.start[:10]), date.fromisoformat(a.end[:10]); out = []
        while d < e: out.append(str(d)); d += timedelta(days=1)
        return out
    raise SystemExit("goldflow: give --dates D.. or --start/--end (UTC days)")

def _venue_cmd(cfg, a, log) -> int:
    """Phase 15: other venues (separate markets; never merged)."""
    from . import venues as V
    if a.cmd == "providers-report":
        rep = V.providers_report(cfg, probe=not a.no_probe)
        print(json.dumps({p: {"health": v["health"], "coverage": v["coverage"]} for p, v in rep["providers"].items()}, indent=1, default=str)); return 0
    if a.cmd == "venue-import":
        if a.provider not in ("okx", "bybit"):
            print("goldflow: venue-import --provider okx|bybit (Bitget: archive unreachable, see providers-report)", file=sys.stderr); return 2
        out = V.venue_import(cfg, a.provider, _days(a)); print(json.dumps(out, indent=1, default=str))
        return 0 if all(d["status"] != "MISSING" for d in out["days"].values()) else 1
    if a.cmd == "venue-run":
        out = V.venue_run(cfg, a.provider, a.name or "venue", a.start, a.end); print(json.dumps(out, indent=1, default=str))
        return 0 if out["reconciliation"]["equal"] and out["status"] == "completed" else 1
    if a.cmd == "basis":
        x, y = (a.pair or "binance:okx").split(":")
        out = V.basis(cfg, x, y, _days(a)); print(json.dumps({k: v for k, v in out.items() if k != "hourly"}, indent=1)); return 0
    out = V.volume_oz(cfg, a.venues or ["binance", "okx", "bybit"], _days(a)); print(json.dumps(out, indent=1)); return 0

def _ops_cmd(cfg, a, log) -> int:
    """Phase 17: dashboard, status, alert check, backups."""
    import time as _t
    from .ops import AlertManager, Dashboard, backup, collect, evaluate, notifiers_for, ops_settings, restore, verify_backup
    if a.cmd == "dashboard":
        d = Dashboard(cfg, a.port)
        log.info("dashboard on http://%s:%d/ (read-only%s)", d.ops["dashboard_host"], d.port, "; token required" if d.token else "")
        print(f"http://{d.ops['dashboard_host']}:{d.port}/")
        try:
            d.serve()
        except KeyboardInterrupt:
            d.close()
        return 0
    if a.cmd in ("ops-status", "ops-check"):
        st = collect(cfg)
        if a.cmd == "ops-status":
            print(json.dumps(st, indent=1, default=str)); return 0
        ops = ops_settings(cfg)
        am = AlertManager(cfg.data_root / "ops", notifiers_for(ops))
        ev = am.update(evaluate(st, ops), int(_t.time() * 1000))
        crit = [x for x in am.open.values() if x["severity"] == "critical"]
        print(json.dumps({"open": list(am.open.values()), "transitions": ev}, indent=1, default=str))
        return 2 if crit else 0
    if a.cmd == "backup":
        if not a.dest: print("goldflow: backup --dest DIR (outside the data root)", file=sys.stderr); return 2
        out = backup(cfg, Path(a.dest), Path(a.config))
        print(json.dumps(out, indent=1)); return 0 if out["ok"] else 1
    if a.cmd == "backup-verify":
        out = verify_backup(Path(a.src)); print(json.dumps(out, indent=1)); return 0 if out["ok"] else 1
    if not (a.src and a.target): print("goldflow: restore --src SNAPSHOT --target NEW_EMPTY_DIR", file=sys.stderr); return 2
    try:
        out = restore(Path(a.src), Path(a.target))
    except (ValueError, FileExistsError) as e:
        print(f"goldflow: {e}", file=sys.stderr); return 2
    print(json.dumps(out, indent=1)); return 0

def _exec_cmd(cfg, a, log, profile) -> int:
    """Phase 16: MT5 execution bridge. Order routing only in the demo profile (demo accounts) or the gated live profile."""
    import secrets as _secrets
    from .execution import AuditLog, hmac_test_vector
    from .execution.gate import evidence, record_gate
    from .settings import Profile
    name = a.name or "exec"
    root = cfg.data_root / "execution" / name
    if a.cmd == "ea-vector":
        print(json.dumps(hmac_test_vector(), indent=1)); return 0
    if a.cmd == "bridge-secret":
        if not a.out: print("goldflow: bridge-secret --out FILE (e.g. MT5 Common Files\\goldflow\\bridge.secret)", file=sys.stderr); return 2
        p = Path(a.out); p.parent.mkdir(parents=True, exist_ok=True)
        if p.exists(): print(f"goldflow: {p} exists; not overwritten", file=sys.stderr); return 2
        p.write_text(_secrets.token_hex(32)); os.chmod(p, 0o600) if hasattr(os, "chmod") else None
        print(str(p)); return 0
    if a.cmd == "emergency-stop":
        root.mkdir(parents=True, exist_ok=True); (root / "EMERGENCY_STOP").write_text(a.reason)
        print(f"emergency stop requested for {name}: the running bridge flattens and blocks on its next tick"); return 0
    if a.cmd == "emergency-reset":
        if not a.operator: print("goldflow: emergency-reset needs --operator NAME", file=sys.stderr); return 2
        root.mkdir(parents=True, exist_ok=True); (root / "EMERGENCY_RESET").write_text(a.operator); print("reset requested"); return 0
    if a.cmd == "audit-verify":
        lg = AuditLog(root / "audit.jsonl"); n = lg.verify(); lg.close(); print(json.dumps({"records": n, "chain": "OK"})); return 0
    if a.cmd == "bridge-status":
        p = root / "state.json"
        if not p.exists(): print(f"goldflow: no state for {name}", file=sys.stderr); return 2
        s = json.loads(p.read_text())
        print(json.dumps({"risk": s["risk"], "orders": {k: v["state"] for k, v in s["orders"]["orders"].items()}}, indent=1, default=str)); return 0
    if a.cmd == "exec-gate":
        model = (cfg.raw.get("backtest") or {}).get("slippage_points")
        try:
            ev = evidence(root / "audit.jsonl", float(model) if model is not None else None)
        except FileNotFoundError as e:
            print(f"goldflow: {e}", file=sys.stderr); return 2
        print(json.dumps(ev, indent=1))
        if not ev["passed"]: return 1
        if not a.operator: print("goldflow: evidence passes; approve with --operator NAME", file=sys.stderr); return 2
        print(str(record_gate(cfg.data_root, ev, a.operator))); return 0
    # bridge-run
    if profile not in (Profile.DEMO, Profile.LIVE):
        print("goldflow: bridge-run routes orders: use --profile demo (MT5 demo account) or the gated live profile", file=sys.stderr); return 2
    import asyncio
    from .execution import BridgeSettings, ExecutionBridge, FileQueue, TcpServer
    from .live import LiveRunner
    from .pipeline import build_stack
    from .processing.sessions import SessionCalendar
    bs = BridgeSettings.from_config(cfg)
    try:
        secret = bs.secret(cfg.base_dir)
    except ValueError as e:
        print(f"goldflow: {e}", file=sys.stderr); return 2
    st = build_stack(cfg, stores=("signals",), resume=True, signals_root=cfg.data_root / "live" / name)
    proc, strat = st.proc, st.strategies
    def dq_open(now: int) -> bool:
        tt = proc.clock if proc.clock is not None else now
        return any(x[0] <= tt < x[1] for x in strat.dq) or any(x[0] <= tt < x[1] for x in proc.dq)
    t = TcpServer(bs.host, bs.port) if bs.transport == "tcp" else FileQueue(Path(bs.file_queue_dir))
    try:
        br = ExecutionBridge(cfg, name, t, secret, mode="live" if profile is Profile.LIVE else "demo", dq_open=dq_open,
                             calendar=SessionCalendar.from_config(cfg.raw.get("sessions")))
    except ValueError as e:
        t.close(); print(f"goldflow: {e}", file=sys.stderr); return 2
    st.signal_listeners.append(br.on_signal)
    runner = LiveRunner(cfg, [proc], name)
    runner.hooks.append(lambda now: br.tick())
    br.start()
    from .replay import ReplayGuardError
    try:
        out = asyncio.run(runner.run(a.duration))
    except KeyboardInterrupt:
        runner.stop(); out = {"status": "stopped"}
    except ReplayGuardError as e:
        print(f"goldflow: {e}. Start a new run with another --name, or delete {runner.state_path}", file=sys.stderr)
        return 2   # the finally block closes the bridge and the socket
    finally:
        br.close(); t.close()
    out["bridge"] = br.status()
    print(json.dumps(out, indent=1, default=str))
    return 0

def _live_cmd(cfg, a, log) -> int:
    """Phase 14 paper mode: REST trade poller + WebSocket monitor -> LiveBus -> the replay's analytics and strategy stack."""
    import asyncio
    from .live import LiveRunner
    from .pipeline import build_stack
    name = a.name or "live"
    st = build_stack(cfg, stores=("signals",), resume=True, signals_root=cfg.data_root / "live" / name)
    proc, strat = st.proc, st.strategies
    from .replay import ReplayGuardError
    runner = LiveRunner(cfg, [proc], name)
    try:
        out = asyncio.run(runner.run(a.duration))
    except KeyboardInterrupt:
        runner.stop(); out = {"status": "stopped", "counters": runner.counters}
    except ReplayGuardError as e:   # checkpoint from different settings: never mix states
        print(f"goldflow: {e}. Start a new live run with another --name, or delete {runner.state_path}", file=sys.stderr); return 2
    out["strategies"] = strat.summary()
    print(json.dumps(out, indent=1, default=str))
    return 2 if out["status"] == "geo_blocked" else 0

def _backtest_cmd(cfg, a, log) -> int:
    """Phase 12: replay -> analytics -> StrategyEngine -> Backtester (fills on broker ticks) -> data/backtests/<name>/."""
    import signal, threading
    from .replay import ReplayEngine, ChainHasher, IntegrityChecker
    from .backtesting import BrokerNotConfigured
    from .pipeline import build_stack
    name = a.name or "backtest"
    try:
        st = build_stack(cfg, backtest={"run_name": name})
    except (BrokerNotConfigured, FileNotFoundError, ValueError) as e:
        print(f"goldflow: {e}", file=sys.stderr); return 2
    proc, bt = st.proc, st.backtester; hasher, checker = ChainHasher(), IntegrityChecker()
    eng = ReplayEngine(cfg, [hasher, checker, proc], start=a.start, end=a.end, dates=a.dates, name=name,
                       progress=lambda m: log.info("backtest %s day=%s events=%d clock=%s", name, m.get("day"), m["events"], m["sim_clock"]))
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: (log.info("stop requested; checkpointing"), eng.stop()))
    if a.budget: threading.Timer(a.budget, eng.stop).start()
    res = eng.run(resume=a.resume, until=a.until)
    out = {"name": name, "status": res["status"], "events": res["events"], "wall_s": res["wall_s"], "sequence_digest": hasher.digest,
           "integrity_violations": checker.violations, "output": str(bt.out_root / name) if res["status"] == "completed" else None}
    print(json.dumps(out, indent=1, default=str))
    return 1 if checker.violations else (0 if res["status"] == "completed" else 3)

if __name__ == "__main__":
    sys.exit(main())
