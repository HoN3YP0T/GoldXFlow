"""Phase 10 tests: IB windows across the US and UK DST changes, settings validation, holiday calendar, IVB refusal,
and the IB engine on replay (clock check, verification, restore and chunk independence, live-style chunks, closed
holidays, IB levels feeding the auction engine)."""
import copy, json, sys
from datetime import date
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_processing import DAYS, MS, REAL_CFG, make_dataset, trades_of  # noqa: E402

from goldflow.config import Config  # noqa: E402
from goldflow.orderflow import ProfileEngine  # noqa: E402
from goldflow.orderflow.auction_engine import AuctionEngine  # noqa: E402
from goldflow.orderflow.ib_verify import verify  # noqa: E402
from goldflow.orderflow.initial_balance import IBEngine, IBSettings, IBStore, ib_bounds  # noqa: E402
from goldflow.orderflow.ivb import IVBUnavailable, ivb  # noqa: E402
from goldflow.processing import TradeProcessor  # noqa: E402
from goldflow.processing.holidays import HolidayCalendar  # noqa: E402
from goldflow.processing.sessions import SessionCalendar  # noqa: E402
from goldflow.replay import KEY_DTYPE, ChainHasher, EventChunk, ReplayEngine, ReplayGuardError  # noqa: E402
from goldflow.settings import ConfigError, validate_config  # noqa: E402
from goldflow.timeutil import DAY_MS, day_start_ms  # noqa: E402


def ibset(raw=None):
    return IBSettings.from_config(Config(raw or copy.deepcopy(REAL_CFG), Path(".")))


def test_ib_windows_across_us_and_uk_dst_changes():
    st = ibset(); cal = SessionCalendar.from_config(REAL_CFG["sessions"])
    ny, ld = (next(d for d in st.defs if d.name == n) for n in ("new_york", "london"))
    utc = lambda d, x: (ib_bounds(x, d, cal)[0] - day_start_ms(d)) // 60_000  # noqa: E731  minutes after 00:00 UTC
    assert utc(date(2026, 3, 6), ny) == 13 * 60 + 20 and utc(date(2026, 3, 9), ny) == 12 * 60 + 20    # US DST on 8 Mar
    assert utc(date(2026, 3, 27), ld) == 8 * 60 and utc(date(2026, 3, 30), ld) == 7 * 60              # UK BST on 29 Mar
    assert utc(date(2026, 3, 16), ny) == 12 * 60 + 20 and utc(date(2026, 3, 16), ld) == 8 * 60        # the gap week
    assert utc(date(2026, 10, 23), ld) == 7 * 60 and utc(date(2026, 10, 26), ld) == 8 * 60            # UK back on 25 Oct
    assert utc(date(2026, 10, 26), ny) == 12 * 60 + 20 and utc(date(2026, 11, 2), ny) == 13 * 60 + 20 # US back on 1 Nov
    assert all(ib_bounds(x, date(2026, 7, 1), cal)[1] - ib_bounds(x, date(2026, 7, 1), cal)[0] == 3_600_000 for x in (ny, ld))


@pytest.mark.parametrize("patch,msg", [
    ({"definitions": [{"name": "x", "session": "london", "start": "06:00", "length": "60m"}]}, "not inside session"),
    ({"definitions": [{"name": "x", "session": "london", "start": "08:00", "offset": "0m", "length": "60m"}]}, "exactly one"),
    ({"definitions": [{"name": "x", "session": "sydney", "offset": "0m", "length": "60m"}]}, "not in the session calendar"),
    ({"definitions": [{"name": "x", "session": "london", "offset": "0m", "length": "8m"}]}, "multiple of break_interval"),
    ({"extensions": ["1", "1"]}, "distinct"), ({"break_interval": "2m"}, "processing.intervals")])
def test_invalid_ib_settings_fail_at_startup(patch, msg):
    raw = copy.deepcopy(REAL_CFG); raw["initial_balance"].update(patch)
    with pytest.raises(ConfigError, match=msg):
        validate_config(raw, None, check_discovery=False)


def test_holiday_calendar_and_ivb_refusal():
    h = HolidayCalendar(REAL_CFG["holidays"])
    assert h.closed(date(2026, 12, 25)) and not h.closed(date(2026, 5, 25)) and h.label(date(2026, 5, 25)) == "MODIFIED_HOURS:Memorial Day"
    with pytest.raises(ValueError, match="type"):
        HolidayCalendar({"days": [{"date": "2026-01-01", "name": "x", "type": "HALF"}]})
    for ivbcfg in ({"enabled": True}, {"enabled": True, "definition": {k: "x" for k in
                   ("meaning", "formula", "inputs", "window", "interpretation", "worked_example", "signed_off_by")}}):
        raw = copy.deepcopy(REAL_CFG); raw["ivb"] = ivbcfg
        with pytest.raises(ConfigError, match="ivb"):
            validate_config(raw, None, check_discovery=False)
    with pytest.raises(IVBUnavailable, match="never replaced"):
        ivb()


# ============================================================================ engine on replay
class Listen:
    def __init__(self):
        self.rows = {}; self.violations = 0; self.proc = None
    def __call__(self, table, rows):
        if table != "ib_level" and self.proc is not None and any(r["available_at"] > self.proc.clock for r in rows):
            self.violations += 1
        self.rows.setdefault(table, []).extend(rows)


def irun(cfg, name="i", resume=False, until=None, store=True, listen=None, **kw):
    eng = IBEngine(cfg, store=IBStore(cfg.data_root, cfg.symbol, replace=not resume) if store else None, listeners=[listen] if listen else [])
    p = TradeProcessor(cfg, sinks=[eng])
    if listen: listen.proc = p
    r = ReplayEngine(cfg, [ChainHasher(), p], name=name, **kw).run(resume=resume, until=until)
    return eng, p, r


def short_ib(cfg):  # shorter windows and more extensions so a small synthetic day exercises every event type
    cfg.raw["initial_balance"]["definitions"] = [{"name": "new_york", "session": "new_york", "offset": "0m", "length": "30m"},
                                                 {"name": "london", "session": "london", "start": "08:00", "length": "30m"}]
    cfg.raw["initial_balance"]["extensions"] = ["0.25", "0.5", "1"]
    return cfg


@pytest.fixture(scope="module")
def base(tmp_path_factory):
    cfg = short_ib(make_dataset(tmp_path_factory.mktemp("ib"), n_per_day=4000))
    lis = Listen()
    eng, p, r = irun(cfg, "base", listen=lis)
    return cfg, eng, p, r, lis


def test_ib_engine_outputs_and_verification(base):
    cfg, eng, p, r, lis = base
    assert r["status"] == "completed" and lis.violations == 0
    fin = [x for x in lis.rows["ib"] if x["is_final"]]
    assert sorted(x["ib_key"] for x in fin) == ["ib:london:2026-10-23", "ib:london:2026-10-26", "ib:new_york:2026-10-23", "ib:new_york:2026-10-26"]
    ny = next(x for x in fin if x["ib_key"] == "ib:new_york:2026-10-26")
    assert ny["ib_start"] == MS("2026-10-26T12:20:00Z") and ny["as_of"] == ny["ib_end"] == MS("2026-10-26T12:50:00Z")
    dev = [x for x in lis.rows["ib"] if x["ib_key"] == ny["ib_key"] and not x["is_final"]]
    assert len(dev) == 29 and all(x["as_of"] < ny["ib_end"] for x in dev)
    assert dev[-1]["high_ticks"] <= ny["high_ticks"] and dev[-1]["low_ticks"] >= ny["low_ticks"]
    types = {e["event_type"] for e in lis.rows["ib_events"]}
    assert {"SESSION_SUMMARY"} <= types and any(t.startswith("BREAKOUT") for t in types) and any(t.startswith("EXTENSION") for t in types), types
    assert all(e["event_time"] >= next(x for x in fin if x["ib_key"] == e["ib_key"])["ib_end"] for e in lis.rows["ib_events"])
    assert len(lis.rows["ib_level"]) == 4 and {x["levels"][0][0] for x in lis.rows["ib_level"]} == {"IB_HIGH"}
    rep = verify(cfg, report=False)["summary"]
    assert rep["fail"] == [] and rep["final_ibs"] == 4 and rep["ib_rows_checked"] > 100 and rep["events_checked"] > 0, rep


def test_restore_chunking_and_live_style_chunks(tmp_path):
    cfg = short_ib(make_dataset(tmp_path, n_per_day=1500, partial_hour=None))
    e0, _, _ = irun(cfg, "full")
    for cut in ("2026-10-23T12:35:42.123Z", "2026-10-26T08:10:00.000Z"):
        irun(cfg, "cut", until=MS(cut))
        e2, _, r2 = irun(cfg, "cut", resume=True)
        assert r2["status"] == "completed" and e2.state_digest() == e0.state_digest(), cut
    cfg.raw["replay"]["max_events_per_chunk"] = 61
    assert irun(cfg, "small", store=False)[0].state_digest() == e0.state_digest()
    l1, l3 = Listen(), Listen()
    irun(cfg, "a", store=False, listen=l1)
    e3 = IBEngine(cfg, listeners=[l3]); p3 = TradeProcessor(cfg, sinks=[e3])
    p3.on_start({"start_ms": day_start_ms(DAYS[0])})
    for d in DAYS:
        t = trades_of(cfg, d); ts = t.column("exchange_event_time_utc").cast(pa.int64()).to_numpy(); rows = np.arange(len(ts))
        for i in range(0, len(ts), 251):
            k = np.empty(len(ts[i:i + 251]), KEY_DTYPE); k["avail"] = ts[i:i + 251]; k["stream"] = 0; k["rank"] = 0; k["seq"] = 0
            p3.on_chunk(EventChunk(k, rows[i:i + 251], {"trades": t}, ["trades"], int(k["avail"][-1])))
    p3.on_stop({"status": "completed", "end_ms": day_start_ms(DAYS[-1]) + DAY_MS})
    key = lambda L, t: sorted(json.dumps(r, sort_keys=True, default=str) for r in L.rows.get(t, []))  # noqa: E731
    for tb in ("ib", "ib_events", "ib_level"):
        assert key(l3, tb) == key(l1, tb), tb
    cfg.raw["initial_balance"]["failed_break_window"] = "15m"
    irun(cfg, "chg", until=MS("2026-10-24T00:00:00Z"), store=False)
    cfg.raw["initial_balance"]["failed_break_window"] = "30m"
    with pytest.raises(ReplayGuardError, match="initial_balance settings"):
        irun(cfg, "chg", resume=True, store=False)


def test_closed_holiday_skips_ib_and_modified_hours_is_labelled(tmp_path):
    cfg = short_ib(make_dataset(tmp_path, n_per_day=800, partial_hour=None))
    cfg.raw["holidays"] = {"version": "t", "tz": "America/New_York", "days": [{"date": "2026-10-26", "name": "Test closure", "type": "CLOSED"},
                                                                             {"date": "2026-10-23", "name": "Test early", "type": "MODIFIED_HOURS"}]}
    lis = Listen()
    eng, _, _ = irun(cfg, "h", store=False, listen=lis)
    keys = {x["ib_key"] for x in lis.rows["ib"] if x["is_final"]}
    assert keys == {"ib:london:2026-10-23", "ib:new_york:2026-10-23"} and eng.stats["holiday_closed_skipped"] == 2
    assert all(x["holiday"] == "MODIFIED_HOURS:Test early" for x in lis.rows["ib"])


def test_ib_levels_reach_the_auction_engine(tmp_path):
    cfg = short_ib(make_dataset(tmp_path, n_per_day=1500, partial_hour=None))
    got = []
    prof = ProfileEngine(cfg); ib = IBEngine(cfg); auc = AuctionEngine(cfg, prof, ib_engine=ib, listeners=[lambda t, r: got.extend(r) if t == "levels" else None])
    p = TradeProcessor(cfg, sinks=[prof, ib, auc])
    ReplayEngine(cfg, [ChainHasher(), p], name="feed").run()
    ibl = [x for x in got if x["series"].startswith("ib:")]
    assert {x["level_type"] for x in ibl} == {"IB_HIGH", "IB_LOW", "IB_MID"} and len(ibl) == 12
    lon = next(x for x in ibl if x["level_id"] == "ib:london:2026-10-26:IB_HIGH")
    assert lon["available_at"] == MS("2026-10-26T08:30:00Z")          # 08:00 London (GMT) + 30 min
