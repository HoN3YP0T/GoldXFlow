"""Command line entry point.

  python -m goldflow.cli discover  --config config/binance_xauusdt.json
  python -m goldflow.cli acquire   --config ... [--months 6 | --start ISO --end ISO] [--new-range] [--no-rest]
  python -m goldflow.cli rest      --config ...          # REST recovery units only (run where fapi is reachable)
  python -m goldflow.cli manifest  --config ...
  python -m goldflow.cli depth-record [--duration S]   # Phase 8 live L2 recorder (non-US host)
  python -m goldflow.cli depth-replay | depth-normalize
The acquisition range is frozen in data/state/range.json on first run so that resumed runs
cover exactly the same [start, end); pass --new-range to start a fresh range.
Exit code 1 if any unit failed or any interval of a primary dataset is uncovered.
"""
from __future__ import annotations
import argparse, json, logging, sys

from .acquire import list_dataset, make_archive, run
from .checkpoint import Checkpoint
from .config import Config
from .report import build_manifest
from .rest_client import GeoBlocked
from .timeutil import iso, minus_months, parse_iso, to_ms, utcnow

def _range(cfg: Config, a) -> tuple[int, int]:
    rp = cfg.data_root / "state" / "range.json"
    if rp.exists() and not a.new_range:
        r = json.loads(rp.read_text()); return r["start_ms"], r["end_ms"]
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

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="goldflow")
    ap.add_argument("cmd", choices=["status", "providers", "discover", "acquire", "rest", "manifest", "normalize",
                                    "validate", "report", "replay", "depth-record", "depth-replay", "depth-normalize"])
    ap.add_argument("--profile", help="research (default) | test | live; or env GOLDFLOW_PROFILE")
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
    ap.add_argument("--duration", type=float, help="depth-record: stop after this many seconds (default: run until Ctrl-C)")
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
    setup_logging(cfg.data_root / "state", run_id, profile.value, cfg["source"], cfg.symbol)
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

if __name__ == "__main__":
    sys.exit(main())
