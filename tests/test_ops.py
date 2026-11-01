"""Phase 17 tests.

- **Status honesty:** an empty data root gives unavailable values with reasons, never invented ones.
- **Alert rules:** their open/close lifecycle, and webhook opt-in.
- **Dashboard:** read-only HTTP API with an optional token.
- **Backup:** snapshot, incremental hard links, tamper detection, a restore whose rebuilt replay digest equals the
  original, and refusal of unsafe targets.
- **Hardening and stress:**
  - a live checkpoint failure does not stop ingestion;
  - REST rate limits and errors hold the clock and raise STALE;
  - the execution bridge blocks trading when it cannot persist;
  - an engine crash mid-order does not duplicate the order;
  - a broker outage rejection does not cause a retry storm.

Execution scenarios use the SIMULATED EA."""
import copy, json, os, sys, urllib.error, urllib.request
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_execution import ready  # noqa: E402
from test_processing import REAL_CFG, make_dataset  # noqa: E402

from goldflow.config import Config  # noqa: E402
from goldflow.execution import AuditLog  # noqa: E402
from goldflow.live import LiveRunner  # noqa: E402
from goldflow.ops import AlertManager, Dashboard, backup, collect, evaluate, notifiers_for, ops_settings, restore, verify_backup  # noqa: E402
from goldflow.processing import TradeProcessor  # noqa: E402
from goldflow.replay import ReplayEngine  # noqa: E402
from goldflow.settings import ConfigError, validate_config  # noqa: E402

T0 = 1_793_000_000_000


def cfg_at(tmp):
    raw = copy.deepcopy(REAL_CFG)
    raw["paths"] = {"data_root": "data", "state_db": "data/state/acquisition.sqlite"}
    raw["ops"].update({"disk_warn_pct": 2, "disk_crit_pct": 1})       # this host's free space must not drive the assertions
    (tmp / "data" / "state").mkdir(parents=True, exist_ok=True)
    return Config(raw, tmp)


def _values(o, path=""):
    if isinstance(o, dict):
        if "value" in o and ("source" in o or "reason" in o):
            yield path, o
        for k, v in o.items():
            yield from _values(v, f"{path}.{k}")
    elif isinstance(o, list):
        for i, v in enumerate(o):
            yield from _values(v, f"{path}[{i}]")


# ============================================================================ status and alerts
def test_status_on_an_empty_root_invents_nothing(tmp_path):
    cfg = cfg_at(tmp_path)
    st = collect(cfg, T0)
    assert set(st) >= {"market", "orderflow", "trading", "research", "system"}
    vals = dict(_values(st))
    for key in (".market.live", ".orderflow.profile", ".orderflow.cvd", ".orderflow.depth", ".trading.execution", ".research.coverage", ".research.ledger"):
        assert vals[key]["value"] is None and vals[key]["reason"], key          # unavailable, and says why
    for p, v in vals.items():
        assert v["value"] is None or v.get("source"), f"{p} has a value without a source"
    assert evaluate(st, ops_settings(cfg)) == {}


def _exec_state(root, risk, audit_ok=True):
    root.mkdir(parents=True, exist_ok=True)
    rs = {"day": "2026-10-31", "day_start_equity": 10000.0, "peak_equity": 10000.0, "consecutive_losses": 0, "slippages": [], "emergency": False,
          "emergency_reason": None, "blocked": {}, **risk}
    (root / "state.json").write_text(json.dumps({"orders": {"orders": {}, "by_position": {}, "anomalies": []}, "risk": rs, "saved_ms": T0}))
    lg = AuditLog(root / "audit.jsonl")
    lg.write(T0, "START", {"mode": "demo"})
    lg.write(T0 + 1, "IN", {"line": "x"})
    lg.close()
    if not audit_ok:
        p = root / "audit.jsonl"
        p.write_text(p.read_text().replace('"START"', '"STOP"'))


def test_alert_rules_open_and_close(tmp_path, monkeypatch):
    cfg = cfg_at(tmp_path)
    live = cfg.data_root / "live"
    live.mkdir()
    cp = {"name": "L1", "start_ms": T0, "saved_ms": T0 - 600_000, "counters": {"polls": 1, "poll_errors": 9, "trades": 5, "rpi_trades": 0, "late_trades": 0},
          "tracker": {"last_id": 7, "last_time": T0 - 700_000, "suspected_gaps": [[1, 2]]},
          "health": {"last": {"trades": T0 - 700_000, "markPrice": T0 - 1000}, "open": {"trades": T0 - 700_000}, "events": []}}
    (live / "L1.checkpoint.json").write_text(json.dumps(cp))
    _exec_state(cfg.data_root / "execution" / "d1", {"emergency": True, "emergency_reason": "DAILY_LOSS", "blocked": {"RECONCILE_MISMATCH": "x", "NO_HEARTBEAT": "y"}},
                audit_ok=False)
    ops = ops_settings(cfg)
    cur = evaluate(collect(cfg, T0), ops)
    kinds = {v["kind"] for v in cur.values()}
    assert {"STALE_FEED", "DQ_OPEN", "RECONCILE_MISMATCH", "RISK_LIMIT", "BRIDGE_DOWN", "AUDIT_BROKEN"} <= kinds
    assert cur["STALE_FEED:live:L1"]["severity"] == "critical"
    got = []
    am = AlertManager(cfg.data_root / "ops", [got.append])
    ev = am.update(cur, T0)
    assert {e["event"] for e in ev} == {"OPEN"} and len(ev) == len(cur) == len(got)
    assert am.update(cur, T0 + 1000) == []                                   # still open: no new transition
    (live / "L1.checkpoint.json").unlink()
    _exec_state(cfg.data_root / "execution" / "d1", {})
    (cfg.data_root / "execution" / "d1" / "audit.jsonl").unlink()
    _exec_state(cfg.data_root / "execution" / "d1", {})
    ev2 = AlertManager(cfg.data_root / "ops", [got.append]).update(evaluate(collect(cfg, T0 + 2000), ops), T0 + 2000)
    assert {e["event"] for e in ev2} == {"CLOSE"} and not json.loads((cfg.data_root / "ops" / "alerts_open.json").read_text())
    assert len((cfg.data_root / "ops" / "alerts.jsonl").read_text().splitlines()) == len(ev) + len(ev2)
    assert len(notifiers_for(ops)) == 1                                      # webhook off by default
    monkeypatch.setenv("HOOK", "http://127.0.0.1:9/x")
    assert len(notifiers_for({**ops, "webhook_env": "HOOK"})) == 2
    with pytest.raises(ConfigError, match="loopback"):
        r = copy.deepcopy(REAL_CFG); r["ops"]["dashboard_host"] = "0.0.0.0"
        validate_config(r, None, check_discovery=False)


def test_dashboard_is_read_only_and_token_protected(tmp_path, monkeypatch):
    cfg = cfg_at(tmp_path)
    d = Dashboard(cfg, 0)
    d.start_background()
    base = f"http://127.0.0.1:{d.port}"
    try:
        st = json.loads(urllib.request.urlopen(base + "/api/status", timeout=10).read())
        assert st["symbol"] == "XAUUSDT" and st["alerts_open"] == [] and st["market"]["live"]["value"] is None
        assert json.loads(urllib.request.urlopen(base + "/api/health", timeout=10).read()) == {"ok": True, "critical": 0}
        assert b"GoldXFlow operations" in urllib.request.urlopen(base + "/", timeout=10).read()
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(urllib.request.Request(base + "/api/status", data=b"{}", method="POST"), timeout=10)
        assert e.value.code == 501                                            # no write endpoints at all
    finally:
        d.close()
    monkeypatch.setenv("GOLDFLOW_DASHBOARD_TOKEN", "t0ken")
    d2 = Dashboard(cfg, 0)
    d2.start_background()
    try:
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(f"http://127.0.0.1:{d2.port}/api/status", timeout=10)
        assert e.value.code == 401
        req = urllib.request.Request(f"http://127.0.0.1:{d2.port}/api/health", headers={"Authorization": "Bearer t0ken"})
        assert json.loads(urllib.request.urlopen(req, timeout=10).read())["ok"] is True
    finally:
        d2.close()


# ============================================================================ backup and restore
def _digest(cfg):
    p = TradeProcessor(cfg, sinks=[])
    ReplayEngine(cfg, [p], name="digest", checkpoint=False).run()
    return p.state_digest()


def test_backup_incremental_tamper_detection_and_restore_rebuilds_identically(tmp_path):
    src = tmp_path / "src"
    cfg = make_dataset(src, n_per_day=1500)
    _exec_state(cfg.data_root / "execution" / "d1", {})
    d0 = _digest(cfg)
    with pytest.raises(ValueError, match="outside the data root"):
        backup(cfg, cfg.data_root / "bk")
    b1 = backup(cfg, tmp_path / "bk")
    assert b1["ok"] and b1["hard_linked"] == 0 and b1["sqlite"] and all(v == "ok" for v in b1["sqlite"].values())
    import time as _t
    _t.sleep(1.1)                                                             # a new snapshot name (second resolution)
    b2 = backup(cfg, tmp_path / "bk")
    assert b2["hard_linked"] > 0 and b2["copied"] == len(b1["sqlite"])        # only the databases are copied again
    v = verify_backup(Path(b1["snapshot"]))
    assert v["ok"] and v["chains"]["d1"] == 2
    target = tmp_path / "restored" / "data"
    out = restore(Path(b2["snapshot"]), target)
    assert out["verified"] and out["restored_files"] == b2["files"]
    raw = copy.deepcopy(cfg.raw)
    rcfg = Config(raw, tmp_path / "restored")
    assert _digest(rcfg) == d0                                                # rebuilt from the restored copy: identical state
    with pytest.raises(FileExistsError, match="not empty"):
        restore(Path(b2["snapshot"]), target)
    victim = next(p for p in (Path(b1["snapshot"]) / "data").rglob("*.parquet"))
    victim.write_bytes(victim.read_bytes() + b"x")                            # tamper (breaks the hard link's twin too: same inode)
    bad = verify_backup(Path(b1["snapshot"]))
    assert not bad["ok"] and any("content differs" in p for p in bad["problems"])
    with pytest.raises(ValueError, match="does not verify"):
        restore(Path(b1["snapshot"]), tmp_path / "r2")


# ============================================================================ hardening and stress
def test_live_runner_survives_checkpoint_failure_and_rate_limits(tmp_path):
    cfg = cfg_at(tmp_path)
    cfg.raw["live"].update({"poll_ms": 1000, "grace_ms": 2000})
    clock = [T0]
    calls = {"n": 0}

    def rest(path, params, raw=False):
        calls["n"] += 1
        if 5 <= calls["n"] < 30:
            raise RuntimeError("HTTP 429 Too Many Requests")                # rate limited for 25 polls
        return "[]"

    r = LiveRunner(cfg, [TradeProcessor(cfg, sinks=[])], "L", rest_get=rest, now=lambda: clock[0], capture=False)
    (tmp_path / "blocker").write_text("")
    r.state_path = tmp_path / "blocker" / "cp.json"                           # the checkpoint cannot be written
    r.start()
    for _ in range(60):
        clock[0] += 1000
        r.step(clock[0])
    assert r.counters["poll_errors"] == 25 and r.counters["checkpoint_errors"] >= 1 and r.counters["polls"] == 35
    assert any(e["event"] == "STALE" for e in r.health.events) and any(e["event"] == "RECOVERED" for e in r.health.events)
    assert r.bus.clock is not None and r.bus.clock <= clock[0] - 2000


def test_bridge_blocks_when_it_cannot_persist(tmp_path):
    r = ready(tmp_path)
    (r.br.root / "state.json.tmp").mkdir()                                    # every save now fails
    r.run(300)
    assert "PERSISTENCE_FAILED" in r.br.risk.st.blocked
    r.br.on_signal("signals", [r.signal()])
    r.run(500)
    assert r.ea.orders_sent == 0 and not r.br.book.orders                     # nothing sent that could not be recorded
    (r.br.root / "state.json.tmp").rmdir()
    r.run(300)
    assert "PERSISTENCE_FAILED" not in r.br.risk.st.blocked
    r.br.on_signal("signals", [r.signal("sig-2")])
    r.run(500)
    assert r.ea.orders_sent == 1


def test_engine_crash_mid_order_and_broker_outage(tmp_path):
    r = ready(tmp_path / "a")
    o = r.br.submit(r.signal())
    r.ea.tick()                                                               # the EA executes; the engine dies before reading replies
    assert len(r.broker.positions) == 1 and o.state == "TRANSMITTED"
    r.br.audit.close()
    r.br = r.new_bridge()                                                     # restart from disk (no close() of the dead process)
    r.run(5000)
    assert r.br.book.orders[o.key].state == "FILLED" and r.ea.orders_sent == 1 and len(r.broker.positions) == 1
    r2 = ready(tmp_path / "b")
    r2.ea.reject_next = (10031, "no connection to the trade server")
    o2 = r2.br.submit(r2.signal())
    r2.run(8000)
    assert o2.state == "REJECTED" and o2.attempts == 1 and r2.ea.orders_sent == 0  # no retry storm on a broker outage
    o3 = r2.br.submit(r2.signal("later"))
    r2.run(500)
    assert o3.state == "FILLED"
    assert os.path.exists(r2.br.root / "audit.jsonl") and AuditLog(r2.br.root / "audit.jsonl").verify() > 0

