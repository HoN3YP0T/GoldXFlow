"""Phase 16 tests. The broker and EA here are the SIMULATED ones (goldflow/execution/sim.py); no real account is used.

They cover:
- **Protocol:** MAC, replay, skew, session, schema and flatness; the HELLO reset; the HMAC vector shared with the MQL5
  EA.
- **Lifecycle:** state changes only on broker confirmation; no fill is assumed when a message is lost.
- **Duplicate prevention under fault injection:** lost ACKs and fills, retries, EA restart, engine restart, network
  drop.
- **Risk limits:** DQ, spread, positions, invalid stops, daily loss leading to an emergency flatten; limits looser than
  the backtest are refused.
- **Reconciliation:** manual close, a silent close while disconnected, an unknown broker position.
- **Start-up refusals:** account mode and symbol checks.
- **Operations:** the emergency stop and reset files; the audit chain and its tamper detection.
- **Deployment gate:** refusal, live still locked; and the TCP and file transports."""
import copy, json, socket, sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_processing import REAL_CFG  # noqa: E402

from goldflow.config import Config  # noqa: E402
from goldflow.execution import (AuditBroken, AuditLog, Endpoint, ExecutionBridge, FileQueue, Pipe, ProtocolError, TcpServer,  # noqa: E402
                                evidence, hmac_test_vector, idempotency_key, record_gate)
from goldflow.execution.protocol import encode  # noqa: E402
from goldflow.execution.sim import SimBroker, SimEA  # noqa: E402
from goldflow.profiles import LiveTradingLocked, assert_profile_allowed  # noqa: E402
from goldflow.settings import ConfigError, Profile, validate_config  # noqa: E402

SECRET = b"s" * 40
T0 = 1_793_000_000_000
BROKER = {"name": "SimBroker (SYNTHETIC)", "symbol": "XAUUSD", "digits": 2, "contract_size": "100", "volume_min": "0.01", "volume_step": "0.01",
          "volume_max": "50", "commission_per_lot_side": "0", "swap_long_per_lot": "0", "swap_short_per_lot": "0", "swap_time": "00:00",
          "triple_swap_day": "Wed", "stops_level_points": 0, "server_tz": "UTC", "account_currency": "USD"}


def make_cfg(tmp, **risk):
    raw = copy.deepcopy(REAL_CFG)
    raw["paths"] = {"data_root": "data", "state_db": "data/state/acquisition.sqlite"}
    raw["broker"] = dict(BROKER)
    raw["risk"].update(risk)
    return Config(raw, tmp)


class Rig:
    def __init__(self, tmp, name="t", mode="demo", trade_mode="DEMO", broker=None, dq=None, cfg=None, symbol=None):
        self.clock = [T0]
        self.cfg = cfg or make_cfg(tmp)
        self.tmp, self.name, self.mode = tmp, name, mode
        self.pipe = Pipe()
        self.broker = broker or SimBroker(trade_mode)
        self.dq = dq or [False]
        self.ea = SimEA(self.pipe.b, SECRET, self.broker, lambda: self.clock[0], symbol=symbol)
        self.br = self.new_bridge()

    def new_bridge(self):
        b = ExecutionBridge(self.cfg, self.name, self.pipe.a, SECRET, mode=self.mode, now=lambda: self.clock[0], dq_open=lambda t: self.dq[0])
        b.start()
        return b

    def run(self, ms=1000, step=100):
        for _ in range(ms // step):
            self.clock[0] += step
            self.br.tick()
            self.ea.tick()
            self.br.tick()

    def signal(self, sid="sig-1", direction="LONG", stop=239500, target=241000, entry=240000):
        return {"signal_id": sid, "strategy_id": "test", "direction": direction, "entry_price_ticks": entry, "stop_loss_ticks": stop,
                "take_profit_ticks": target, "expiry_at": self.clock[0] + 60_000, "available_at": self.clock[0]}

    def audit(self, kind):
        return [r["data"] for r in AuditLog(self.br.root / "audit.jsonl").records() if r["kind"] == kind]


def ready(tmp, **kw):
    r = Rig(tmp, **kw)
    r.run(1500)
    assert r.br.risk.st.blocked == {}, r.br.risk.st.blocked
    return r


# ============================================================================ protocol
def test_protocol_authentication_replay_skew_schema():
    py, ea = Endpoint(SECRET, "py", "S1"), Endpoint(SECRET, "ea", "")
    _, hello = py.make("HELLO", T0)
    assert ea.parse(hello, T0)["type"] == "HELLO" and ea.session == "S1"
    _, line = py.make("ORDER_NEW", T0, key="gfabc", symbol="XAUUSD", side="BUY", volume="0.10", sl="2395.00", tp="2410.00", deviation=30)
    assert ea.parse(line, T0)["key"] == "gfabc"
    with pytest.raises(ProtocolError, match="replayed"):
        ea.parse(line, T0)                                              # the same line again
    _, l2 = py.make("HEARTBEAT", T0)
    with pytest.raises(ProtocolError, match="bad MAC"):
        ea.parse(("f" * 64) + l2[64:], T0)
    with pytest.raises(ProtocolError, match="skew"):
        ea.parse(l2, T0 + 60_000)
    with pytest.raises(ProtocolError, match="bad MAC"):
        Endpoint(b"x" * 40, "ea", "S1").parse(l2, T0)                    # another secret
    other = Endpoint(SECRET, "py", "S2")
    _, l3 = other.make("HEARTBEAT", T0)
    other.seq_out = 99
    with pytest.raises(ProtocolError, match="session"):
        ea.parse(other.make("HEARTBEAT", T0)[1], T0)
    with pytest.raises(ProtocolError, match="flat"):
        py.make("FLATTEN_ALL", T0, reason="x", extra={"a": 1})
    with pytest.raises(ProtocolError, match="volume"):
        py.make("ORDER_NEW", T0, key="k", symbol="XAUUSD", side="BUY", volume=0.1, sl="1", tp="2", deviation=30)   # volume must be text
    with pytest.raises(ProtocolError, match="may not send"):
        py.make("TXN", T0)
    raw = encode(SECRET, {"v": 1, "type": "NOPE", "seq": 50, "ts": T0, "session": "S1", "sender": "py"})
    with pytest.raises(ProtocolError, match="unknown message type"):
        ea.parse(raw, T0)
    # HELLO starts a new incarnation: its seq may restart
    py2 = Endpoint(SECRET, "py", "S3")
    assert ea.parse(py2.make("HELLO", T0)[1], T0)["seq"] == 1 and ea.session == "S3"
    assert idempotency_key("sig-1") == idempotency_key("sig-1") and len("gf:" + idempotency_key("sig-1")) <= 31   # MT5 comment limit


def test_hmac_vector_is_the_one_in_the_mql5_ea():
    v = hmac_test_vector()
    ea = (Path(__file__).resolve().parents[1] / "mt5" / "GoldflowExecutor.mq5").read_text()
    assert f'#define SELFTEST_MAC  "{v["mac"]}"' in ea and f'#define SELFTEST_KEY  "{v["key"]}"' in ea
    assert v["text"].replace('"', '\\"') in ea
    for token in ('"gf:"', "ORDER_NEW", "POSITIONS_REQ", "FLATTEN_ALL", "OnTradeTransaction", "KeyExists", "InpAllowReal", "SocketConnect"):
        assert token in ea


# ============================================================================ lifecycle
def test_handshake_then_lifecycle_only_on_broker_confirmation(tmp_path):
    r = Rig(tmp_path)
    assert {"NO_EA", "NO_SYMBOL_CHECK", "NOT_RECONCILED"} <= set(r.br.risk.st.blocked)
    r.run(1500)
    assert r.br.risk.st.blocked == {} and r.br.hello["trade_mode"] == "DEMO" and r.br.sym["symbol"] == "XAUUSD"
    o = r.br.submit(r.signal())
    assert o.state == "TRANSMITTED" and o.volume == "0.10"          # 0.5 % of 10,000 / (500 ticks x 1.0 per lot) = 0.10 lots
    assert o.sl == "2395.30" and o.tp == "2410.30"                  # distances kept from the broker ask 2400.30
    r.run(500)
    assert o.state == "FILLED" and [h[2] for h in o.history] == ["TRANSMITTED", "ACCEPTED", "FILLED"]
    assert len(r.broker.positions) == 1
    r.ea.set_price(2395.00)                                        # through the stop
    r.run(500)
    assert o.state == "CLOSED" and o.close_reason == "SL" and r.br.risk.st.consecutive_losses == 1
    assert AuditLog(r.br.root / "audit.jsonl").verify() > 10
    assert r.br.submit(r.signal()) is None and r.audit("RISK")[-1]["reason"] == "DUPLICATE_SIGNAL"   # same signal: never twice


def test_no_fill_assumed_and_retry_reuses_the_key(tmp_path):
    r = ready(tmp_path)
    r.ea.drop_in = 1                                                # the ORDER_NEW is lost on the way
    o = r.br.submit(r.signal())
    r.run(2000)
    assert o.state == "TRANSMITTED" and not r.broker.positions      # nothing assumed
    r.run(2000)                                                     # ack timeout 3 s: re-sent with the same key
    assert o.state == "FILLED" and o.attempts == 2 and len(r.broker.positions) == 1 and r.ea.orders_sent == 1


def test_lost_ack_and_fill_reports_do_not_duplicate(tmp_path):
    r = ready(tmp_path)
    r.ea.drop_out = 3                                               # ACK, REQUEST and DEAL reports all lost: but the broker filled
    o = r.br.submit(r.signal())
    r.run(2500)
    assert o.state == "TRANSMITTED" and len(r.broker.positions) == 1
    r.run(3000)                                                     # retry -> ACK DUPLICATE -> positions -> fill adopted
    assert o.state == "FILLED" and r.ea.orders_sent == 1 and len(r.broker.positions) == 1
    assert any(a["kind"] == "FILL_ADOPTED" for rep in r.audit("RECONCILE") for a in rep["adopted"])


def test_ea_restart_engine_restart_and_network_drop(tmp_path):
    r = ready(tmp_path)
    o = r.br.submit(r.signal())
    r.ea.restart()                                                  # EA restarts before handling the order
    r.run(6000)
    assert o.state == "FILLED" and r.ea.orders_sent == 1 and len(r.broker.positions) == 1
    assert any(a.get("kind") == "EA_CONNECTED" for a in r.audit("ALERT"))
    # engine restart: state restored, reconciled before trading, the old signal is still a duplicate
    r.br.close()
    r.br = r.new_bridge()
    assert r.br.book.orders[o.key].state == "FILLED" and "NOT_RECONCILED" in r.br.risk.st.blocked
    r.run(1500)
    assert r.br.risk.st.blocked == {} and r.br.last_reconcile["clean"]
    assert r.br.submit(r.signal()) is None and len(r.broker.positions) == 1
    # network drop: no heartbeat -> trading blocked; back -> handshake + reconcile -> unblocked
    r.pipe.up = False
    r.run(12_000)
    assert "NO_HEARTBEAT" in r.br.risk.st.blocked
    assert r.br.submit(r.signal("sig-2")) is None and r.audit("RISK")[-1]["failures"][0] in ("TRADING_BLOCKED", "MAX_POSITIONS")
    r.pipe.up = True
    r.run(4000)
    assert r.br.risk.st.blocked == {}


# ============================================================================ risk
def test_risk_limits_block_and_daily_loss_flattens(tmp_path):
    r = ready(tmp_path)
    r.dq[0] = True
    assert r.br.submit(r.signal("a")) is None and "DQ_OPEN" in r.audit("RISK")[-1]["failures"]
    r.dq[0] = False
    r.ea.set_price(2400.00, spread=1.00)                            # 100 points > 60
    r.run(1100)
    assert r.br.submit(r.signal("b")) is None and "SPREAD" in r.audit("RISK")[-1]["failures"]
    r.ea.set_price(2400.00)
    r.run(1100)
    assert r.br.submit(r.signal("c", stop=240500)) is None and "STOP_INVALID" in r.audit("RISK")[-1]["failures"]   # stop above a long entry
    assert r.br.submit(r.signal("d", stop=239999)) is None and "EXPOSURE" in r.audit("RISK")[-1]["failures"]  # 1-tick stop: 50 lots > 5
    o = r.br.submit(r.signal("e"))
    r.run(500)
    assert o.state == "FILLED"
    assert r.br.submit(r.signal("f")) is None and "MAX_POSITIONS" in r.audit("RISK")[-1]["failures"]
    assert r.ea.orders_sent == 1                                    # nothing refused ever reached the broker
    r.broker.balance -= 300                                         # -3 % today (> 2 % daily loss limit)
    r.run(1500)
    assert r.br.risk.st.emergency and not r.broker.positions        # latched and flattened
    assert any(e.get("flattened") for e in r.audit("EMERGENCY"))
    assert r.br.submit(r.signal("g")) is None and r.audit("RISK")[-1]["failures"][0] == "EMERGENCY_STOP"
    with pytest.raises(ConfigError, match="looser"):
        raw = copy.deepcopy(REAL_CFG); raw["risk"]["risk_per_trade_pct"] = "1.0"
        validate_config(raw, None, check_discovery=False)
    with pytest.raises(ConfigError, match="block_on_dq"):
        raw = copy.deepcopy(REAL_CFG); raw["risk"]["block_on_dq"] = False
        validate_config(raw, None, check_discovery=False)


def test_emergency_stop_and_reset_by_file(tmp_path):
    r = ready(tmp_path)
    o = r.br.submit(r.signal())
    r.run(500)
    assert o.state == "FILLED"
    (r.br.root / "EMERGENCY_STOP").write_text("operator test")
    r.run(1000)
    assert r.br.risk.st.emergency and r.br.risk.st.emergency_reason == "operator test" and not r.broker.positions and o.state == "CLOSED"
    (r.br.root / "EMERGENCY_RESET").write_text("alice")
    r.run(300)
    assert not r.br.risk.st.emergency and r.br.risk.st.emergency_reason == "reset by alice"


# ============================================================================ reconciliation
def test_reconciliation_adopts_broker_changes_and_blocks_on_mismatch(tmp_path):
    r = ready(tmp_path)
    o = r.br.submit(r.signal())
    r.run(500)
    tk = next(iter(r.broker.positions))
    r.ea.manual_close(tk)                                           # manual close reported by the EA
    r.run(300)
    assert o.state == "CLOSED" and o.close_reason == "CLIENT"
    o2 = r.br.submit(r.signal("s2"))
    r.run(500)
    r.pipe.up = False
    r.ea.manual_close_silent(next(iter(r.broker.positions)))        # closed while the link was down: never reported
    r.run(12_000)
    r.pipe.up = True
    r.run(4000)
    assert o2.state == "CLOSED" and o2.close_reason == "RECONCILED_MISSING"
    r.broker.positions[9999] = {"key": "gfdeadbeefdeadbeef", "side": "BUY", "volume": 0.5, "price_open": 2400.0, "sl": 0, "tp": 0}
    r.run(31_000)                                                   # periodic reconciliation (30 s)
    assert "RECONCILE_MISMATCH" in r.br.risk.st.blocked
    assert r.br.submit(r.signal("s3")) is None and "TRADING_BLOCKED" in r.audit("RISK")[-1]["failures"]


def test_account_mode_symbol_mismatch_and_tampering_refused(tmp_path):
    r = Rig(tmp_path / "a", trade_mode="REAL")                      # demo run against a real account
    r.run(1500)
    assert "ACCOUNT_MODE" in r.br.risk.st.blocked
    r2 = Rig(tmp_path / "b", symbol={**SimEA.SYMBOL, "digits": 3, "point": 0.001, "tick_size": 0.001})
    r2.run(1500)
    assert "SYMBOL_MISMATCH" in r2.br.risk.st.blocked
    r3 = ready(tmp_path / "c")
    r3.ea.tamper_next = True
    r3.run(1100)
    assert r3.audit("REFUSED") and "bad MAC" in r3.audit("REFUSED")[0]["reason"]
    with pytest.raises(ValueError, match="broker block"):
        cfg = make_cfg(tmp_path / "d"); cfg.raw["broker"]["name"] = None
        ExecutionBridge(cfg, "x", Pipe().a, SECRET)


# ============================================================================ audit, gate, profiles
def test_audit_chain_gate_refusal_and_live_lock(tmp_path):
    r = ready(tmp_path)
    o = r.br.submit(r.signal())
    r.run(500)
    r.br.close()
    p = r.br.root / "audit.jsonl"
    ev = evidence(p, 2.0)
    assert not ev["passed"] and ev["fills"] == 1 and ev["account_modes"] == ["DEMO"] and any("days" in f for f in ev["failures"])
    with pytest.raises(PermissionError, match="not passed"):
        record_gate(r.cfg.data_root, ev, "alice")
    assert o.state == "FILLED"
    lines = p.read_text().splitlines()
    lines[3] = lines[3].replace('"kind":"OUT"', '"kind":"IN"') if '"kind":"OUT"' in lines[3] else lines[3][:-2] + ' }'
    p.write_text("\n".join(lines) + "\n")
    with pytest.raises((AuditBroken, json.JSONDecodeError)):
        AuditLog(p).verify()
    s = r.cfg.settings if False else validate_config(copy.deepcopy(REAL_CFG), None, check_discovery=False)
    with pytest.raises(LiveTradingLocked):
        assert_profile_allowed(Profile.LIVE, s, r.cfg.data_root)
    assert_profile_allowed(Profile.DEMO, s, r.cfg.data_root)        # demo routing needs no gate (demo accounts only)
    passed = {**ev, "passed": True, "failures": []}
    gp = record_gate(tmp_path / "g", passed, "alice")
    assert json.loads(gp.read_text())["M7_demo_passed"] is True


# ============================================================================ transports
def test_tcp_transport_and_file_queue(tmp_path):
    srv = TcpServer("127.0.0.1", 0)
    c = socket.create_connection(("127.0.0.1", srv.port))
    c.sendall(b"line-1\nline-2\npart")
    got = []
    for _ in range(50):
        got += srv.receive()
        if len(got) >= 2:
            break
    assert got == ["line-1\n", "line-2\n"] and srv.connected and srv.connections == 1
    assert srv.send("to-ea\n") and c.recv(100) == b"to-ea\n"
    c2 = socket.create_connection(("127.0.0.1", srv.port))          # EA restart: the new connection replaces the old one
    for _ in range(50):
        srv.receive()
        if srv.connections == 2:
            break
    assert srv.connections == 2
    c.close(); c2.close(); srv.close()
    py, ea = FileQueue(tmp_path / "q", "py"), FileQueue(tmp_path / "q", "ea")
    py.send("a\n"); py.send("b\n"); ea.send("c\n")
    assert ea.receive() == ["a\n", "b\n"] and py.receive() == ["c\n"] and ea.receive() == []
    assert not list((tmp_path / "q").rglob("*.tmp"))


def test_unknown_order_is_resent_or_expired_and_partial_fills(tmp_path):
    r = ready(tmp_path / "a")
    r.pipe.up = False                                               # the order and all three retries go nowhere
    o = r.br.submit({**r.signal(), "expiry_at": r.clock[0] + 600_000})
    r.run(14_000)
    assert o.state == "UNKNOWN" and {"UNKNOWN_ORDER", "NO_HEARTBEAT"} <= set(r.br.risk.st.blocked) and not r.broker.positions
    r.pipe.up = True
    r.run(5000)                                                     # handshake -> absent at the broker -> re-sent (same key) -> filled
    assert o.state == "FILLED" and len(r.broker.positions) == 1 and r.ea.orders_sent == 1 and r.br.risk.st.blocked == {}
    r2 = ready(tmp_path / "b")
    r2.pipe.up = False
    o2 = r2.br.submit({**r2.signal(), "expiry_at": r2.clock[0] + 5_000})
    r2.run(14_000)
    r2.pipe.up = True
    r2.run(5000)
    assert o2.state == "REJECTED" and not r2.broker.positions and "RECONCILED_ABSENT_EXPIRED" in o2.history[-1][3]
    r3 = ready(tmp_path / "c")
    r3.ea.partial_fills = True
    o3 = r3.br.submit(r3.signal())
    r3.run(500)
    assert [h[2] for h in o3.history] == ["TRANSMITTED", "ACCEPTED", "PARTIALLY_FILLED", "FILLED"] and o3.filled == "0.10"


class _TcpEnd:
    """The simulated EA's side of a REAL loopback TCP connection (the MQL5 EA is a TCP client too)."""

    def __init__(self, port):
        from types import SimpleNamespace
        self.s = socket.create_connection(("127.0.0.1", port))
        self.s.setblocking(False)
        self.buf = b""
        self.pipe = SimpleNamespace(connections=0)

    def send(self, line):
        self.s.sendall(line.encode("ascii"))
        return True

    def receive(self):
        try:
            while True:
                b = self.s.recv(65536)
                if not b:
                    break
                self.buf += b
        except BlockingIOError:
            pass
        out = []
        while b"\n" in self.buf:
            ln, self.buf = self.buf.split(b"\n", 1)
            out.append(ln.decode() + "\n")
        return out


def test_bridge_over_a_real_tcp_socket(tmp_path):
    import time as _time
    clock = [T0]
    srv = TcpServer("127.0.0.1", 0)
    br = ExecutionBridge(make_cfg(tmp_path), "tcp", srv, SECRET, now=lambda: clock[0])
    br.start()
    broker = SimBroker()
    ea = SimEA(_TcpEnd(srv.port), SECRET, broker, lambda: clock[0])

    def run(n):
        for _ in range(n):
            clock[0] += 100
            br.tick()
            _time.sleep(0.002)
            ea.tick()
            _time.sleep(0.002)
            br.tick()

    run(40)
    assert br.risk.st.blocked == {} and br.hello["trade_mode"] == "DEMO" and srv.connections == 1
    o = br.submit({"signal_id": "tcp-1", "strategy_id": "t", "direction": "SHORT", "entry_price_ticks": 240000, "stop_loss_ticks": 240500,
                   "take_profit_ticks": 239000, "expiry_at": clock[0] + 60_000, "available_at": clock[0]})
    run(20)
    assert o.state == "FILLED" and len(broker.positions) == 1 and ea.orders_sent == 1 and o.side == "SELL"
    br.close()
    srv.close()
