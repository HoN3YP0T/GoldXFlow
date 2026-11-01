"""Phase wiring tests: the shared stack builder, and the one-pass ``pipeline`` command, whose datasets must be identical,
file by file, to those written by the single-phase commands (process, vprofile, orderflow, ib, auction, signals).
CLI refusals are clean: no traceback. The data is SYNTHETIC (test_processing.make_dataset)."""
import json, shutil, sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_processing import make_dataset  # noqa: E402

from goldflow import cli  # noqa: E402
from goldflow.pipeline import STORES, build_stack  # noqa: E402


def _cfg_file(cfg, tmp):
    p = tmp / "config" / "cfg.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    raw = json.loads(json.dumps(cfg.raw))
    raw["paths"] = {"data_root": "data", "state_db": "data/state/acquisition.sqlite"}   # relative to the project root, as a user's
    p.write_text(json.dumps(raw))
    return p


def _tables(root):
    out = {}
    for p in sorted(Path(root).rglob("*.parquet")):
        if "_staging" in p.parts:
            continue
        out[str(p.relative_to(root))] = pq.read_table(p).replace_schema_metadata(None)
    return out


def test_build_stack_order_and_refusals(tmp_path):
    cfg = make_dataset(tmp_path, n_per_day=500)
    st = build_stack(cfg)
    names = [s.name for s in st.proc.sinks]
    assert names.index(st.profile.name) < names.index(st.auction.name) < names.index(st.strategies.name)   # sources before consumers
    assert names.index(st.ib.name) < names.index(st.auction.name) and st.backtester is None
    with pytest.raises(ValueError, match="unknown stores"):
        build_stack(cfg, stores=("nope",))
    assert set(STORES) == {"footprint", "profile", "orderflow", "ib", "auction", "signals"}
    got = []
    st.signal_listeners.append(lambda t, rows: got.append(t))
    st.strategies.listeners[0]("signals", [])                               # the fan-out reaches every registered listener
    assert got == ["signals"]


def test_pipeline_equals_single_phase_commands_and_cli_refusals(tmp_path, capsys):
    cfg = make_dataset(tmp_path / "a", n_per_day=2500, partial_hour=None)
    cp = _cfg_file(cfg, tmp_path / "a")
    base = ["--config", str(cp)]
    for cmd in (["process", "--name", "p"], ["vprofile", "--name", "v"], ["orderflow", "--name", "o"], ["ib", "--name", "i"],
                ["auction", "--name", "au"], ["signals", "--name", "s"]):
        assert cli.main(cmd + base) == 0, cmd
    derived = Path(cfg.data_root) / "derived"
    ref = _tables(derived)
    shutil.rmtree(derived)
    capsys.readouterr()
    assert cli.main(["pipeline", "--name", "all"] + base) == 0
    rep = json.loads((Path(cfg.data_root) / "reports" / "pipeline_all.json").read_text())
    assert rep["ok"] and rep["status"] == "completed" and set(rep["verification"]) == {"phase5_reconcile", "phase6_profile", "phase7_orderflow",
                                                                                     "phase10_ib", "phase9_auction", "phase11_signals"}
    got = _tables(derived)
    assert set(got) == set(ref) and len(ref) > 10
    assert [k for k in ref if not ref[k].equals(got[k])] == []                # identical, file by file
    # clean refusals (exit 2, a message, no traceback)
    assert cli.main(["pipeline", "--name", "bt", "--backtest"] + base) == 2   # no broker block (D2)
    assert cli.main(["perf-report", "--name", "missing"] + base) == 2
    err = capsys.readouterr().err
    assert "broker" in err and "no backtest run" in err and "Traceback" not in err


def test_acquire_refuses_an_explicit_range_that_differs_from_the_frozen_one(tmp_path, capsys):
    """Regression: with data/state/range.json present, --start/--end/--months were silently ignored."""
    cfg = make_dataset(tmp_path, n_per_day=10)
    cp = _cfg_file(cfg, tmp_path)
    rp = Path(cfg.data_root) / "state" / "range.json"
    rp.write_text(json.dumps({"start_ms": 1792713600000, "end_ms": 1793059200000, "start": "2026-10-23T00:00:00.000Z",
                              "end": "2026-10-27T00:00:00.000Z", "frozen_at": "2026-10-31T00:00:00.000Z"}))
    for args in (["--start", "2026-10-01T00:00:00Z"], ["--months", "6"]):
        with pytest.raises(SystemExit) as e:
            cli.main(["manifest", "--config", str(cp)] + args)
        assert e.value.code == 2
    assert "--new-range" in capsys.readouterr().err


def test_data_status_only_complete_after_a_clean_full_download(tmp_path):
    """download_data.bat / run_windows.bat rely on this: COMPLETE only when acquire finished without failures for the
    frozen range and every verified file is still on disk; never a network call."""
    from goldflow.checkpoint import Checkpoint
    from goldflow.config import Config
    from goldflow.datastatus import data_status, record_complete
    from test_processing import REAL_CFG
    raw = json.loads(json.dumps(REAL_CFG)); raw["paths"] = {"data_root": "data", "state_db": "data/state/acquisition.sqlite"}
    cfg = Config(raw, tmp_path)
    (tmp_path / "data" / "state").mkdir(parents=True)
    assert data_status(cfg)["state"] == "NOT_STARTED"
    rng = {"start_ms": 1792713600000, "end_ms": 1793059200000, "start": "2026-10-23T00:00:00.000Z", "end": "2026-10-27T00:00:00.000Z"}
    (tmp_path / "data" / "state" / "range.json").write_text(json.dumps(rng))
    ck = Checkpoint(cfg.state_db)
    f = tmp_path / "data" / "raw" / "binance" / "futures_um" / "XAUUSDT" / "trades" / "daily" / "XAUUSDT-trades-2026-10-23.zip"
    f.parent.mkdir(parents=True)
    f.write_bytes(b"x" * 100)
    ck.upsert_unit("u1", source="binance", method="archive", dataset="trades", status="verified", local_path=str(f), bytes=100)
    assert data_status(cfg)["state"] == "INCOMPLETE"                                  # no completion record yet
    assert record_complete(cfg, {"failed": ["u2"]}, None, True) is None               # a run with failures never certifies
    assert record_complete(cfg, {"failed": []}, ["trades"], True) is None             # nor does a partial --datasets run
    assert record_complete(cfg, {"failed": [], "run_id": "r1"}, None, False) is not None
    st = data_status(cfg)
    assert st["complete"] and st["state"] == "COMPLETE" and st["verified_files"] == 1
    f.write_bytes(b"x" * 99)                                                          # a verified file changed on disk
    assert data_status(cfg)["state"] == "FILES_MISSING"
    f.write_bytes(b"x" * 100)
    rng2 = {**rng, "end_ms": rng["end_ms"] + 86_400_000, "end": "2026-10-28T00:00:00.000Z"}
    (tmp_path / "data" / "state" / "range.json").write_text(json.dumps(rng2))          # a new range is not covered
    assert data_status(cfg)["state"] == "INCOMPLETE"


def test_pipeline_if_changed_skips_only_when_up_to_date(tmp_path, capsys):
    cfg = make_dataset(tmp_path, n_per_day=800, partial_hour=None)
    cp = _cfg_file(cfg, tmp_path)
    base = ["--config", str(cp)]
    rep = Path(cfg.data_root) / "reports" / "pipeline_m.json"
    assert cli.main(["pipeline", "--name", "m", "--if-changed", "--no-verify"] + base) == 0
    first = rep.read_text()
    capsys.readouterr()
    assert cli.main(["pipeline", "--name", "m", "--if-changed", "--no-verify"] + base) == 0
    assert '"up_to_date"' in capsys.readouterr().out and rep.read_text() == first     # nothing recomputed
    raw = json.loads(cp.read_text()); raw["strategies"]["warmup"] = "3h"                 # a config change must re-run it
    cp.write_text(json.dumps(raw))
    assert cli.main(["pipeline", "--name", "m", "--if-changed", "--no-verify"] + base) == 0
    assert '"up_to_date"' not in capsys.readouterr().out and rep.read_text() != first
