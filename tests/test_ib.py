"""Phase 10 tests: holiday rules, IB session boundaries across the US/UK DST weeks, hand-built IB behaviour (breakout,
failed break, held, return, acceptance by time and by volume, range extensions, filters), IVB disabled, and the IB
engine on replay (verification against raw trades, no look-ahead, snapshot/restore, chunking, live-style chunks)."""
import copy, json, sys
from datetime import date
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_processing import DAYS, MS, REAL_CFG, make_dataset, trades_of  # noqa: E402

from goldflow.config import Config  # noqa: E402
from goldflow.ib import HolidayCalendar, IBEngine, IBSettings, IBStore, IBTracker, easter  # noqa: E402
from goldflow.ib.engine import drive  # noqa: E402
from goldflow.ib.holidays import rule_date  # noqa: E402
from goldflow.ib.store import ib_root, read_manifest  # noqa: E402
from goldflow.ib.verify import verify  # noqa: E402
from goldflow.normalize import part_path  # noqa: E402
from goldflow.processing import TradeProcessor  # noqa: E402
from goldflow.replay import KEY_DTYPE, ChainHasher, EventChunk, ReplayEngine, ReplayGuardError  # noqa: E402
from goldflow.timeutil import DAY_MS, day_start_ms, iso  # noqa: E402

ST = IBSettings.from_config(Config(copy.deepcopy(REAL_CFG), Path(".")))
M = 60_000


# ============================================================================ holidays
def test_easter_and_rule_dates():
    assert [easter(y) for y in (2000, 2019, 2024, 2025, 2026)] == [date(2000, 4, 23), date(2019, 4, 21), date(2024, 3, 31),
                                                                   date(2025, 4, 20), date(2026, 4, 5)]
    got = {r: rule_date(r, 2026) for r in ("good_friday", "mlk_day", "presidents_day", "memorial_day", "juneteenth",
                                           "independence_day", "labor_day", "thanksgiving", "christmas")}
    assert got == {"good_friday": date(2026, 4, 3), "mlk_day": date(2026, 1, 19), "presidents_day": date(2026, 2, 16),
                   "memorial_day": date(2026, 5, 25), "juneteenth": date(2026, 6, 19), "independence_day": date(2026, 7, 3),
                   "labor_day": date(2026, 9, 7), "thanksgiving": date(2026, 11, 26), "christmas": date(2026, 12, 25)}
    assert rule_date("independence_day", 2027) == date(2027, 7, 5)      # Sunday -> Monday
    assert rule_date("christmas", 2021) == date(2021, 12, 24)           # Saturday -> Friday


def test_holiday_calendar_labels_and_overrides():
    cal = HolidayCalendar({"years": [2026]})
    assert cal.label(date(2026, 5, 25)) == {"holiday": "memorial_day", "holiday_status": "UNVERIFIED", "gold_closed": False}
    assert cal.label(date(2026, 5, 26))["holiday"] is None
    cal = HolidayCalendar({"years": [2026], "overrides": [{"date": "2026-04-03", "status": "CLOSED", "source": "CME notice"}]})
    assert cal.label(date(2026, 4, 3)) == {"holiday": "good_friday", "holiday_status": "CLOSED", "gold_closed": True}
    with pytest.raises(ValueError, match="halt_local"):
        HolidayCalendar({"overrides": [{"date": "2026-05-25", "status": "EARLY_HALT", "source": "x"}]})
    with pytest.raises(ValueError, match="source"):
        HolidayCalendar({"overrides": [{"date": "2026-05-25", "status": "CLOSED"}]})


# ============================================================================ session boundaries across DST
def test_ib_bounds_across_us_and_uk_dst_weeks():
    ny, lon = (d for d in ST.defs)
    u = lambda df, d: tuple(iso(x) for x in df.bounds(d)[:2])
    # US DST starts Sun 8 Mar 2026, UK DST starts Sun 29 Mar: between them New York and London are one hour closer
    assert u(ny, date(2026, 3, 6)) == ("2026-03-06T13:20:00.000Z", "2026-03-06T14:20:00.000Z")
    assert u(ny, date(2026, 3, 9)) == ("2026-03-09T12:20:00.000Z", "2026-03-09T13:20:00.000Z")
    assert u(lon, date(2026, 3, 9)) == ("2026-03-09T08:00:00.000Z", "2026-03-09T09:00:00.000Z")
    assert u(lon, date(2026, 3, 30)) == ("2026-03-30T07:00:00.000Z", "2026-03-30T08:00:00.000Z")
    # UK DST ends Sun 25 Oct, US DST ends Sun 1 Nov
    assert u(lon, date(2026, 10, 23)) == ("2026-10-23T07:00:00.000Z", "2026-10-23T08:00:00.000Z")
    assert u(lon, date(2026, 10, 26)) == ("2026-10-26T08:00:00.000Z", "2026-10-26T09:00:00.000Z")
    assert u(ny, date(2026, 10, 30)) == ("2026-10-30T12:20:00.000Z", "2026-10-30T13:20:00.000Z")
    assert u(ny, date(2026, 11, 2)) == ("2026-11-02T13:20:00.000Z", "2026-11-02T14:20:00.000Z")
    assert iso(ny.bounds(date(2026, 11, 2))[2]) == "2026-11-02T22:00:00.000Z"


# ============================================================================ settings
@pytest.mark.parametrize("patch,msg", [({"ivb": {"enabled": True}}, "D1"), ({"extensions": [1.5]}, "decimal string"),
                                       ({"behaviour_interval": "2m"}, "not in processing.intervals"),
                                       ({"failed_break_window": "7m"}, "multiple of behaviour_interval"),
                                       ({"definitions": [{"name": "x", "tz": "America/New_York", "start": "08:23", "minutes": 60,
                                                          "session_end": "17:00"}]}, "align"),
                                       ({"definitions": [{"name": "x", "tz": "America/New_York", "start": "16:30", "minutes": 60,
                                                          "session_end": "17:00"}]}, "session_end")])
def test_invalid_ib_settings_fail(patch, msg):
    raw = copy.deepcopy(REAL_CFG); raw["initial_balance"].update(patch)
    with pytest.raises(ValueError, match=msg):
        IBSettings.from_config(Config(raw, Path(".")))


def test_ivb_enable_refused_at_config_load(tmp_path):
    from goldflow.settings import ConfigError
    raw = copy.deepcopy(REAL_CFG); raw["initial_balance"]["ivb"]["enabled"] = True
    (tmp_path / "config").mkdir(); p = tmp_path / "config" / "c.json"; p.write_text(json.dumps(raw))
    with pytest.raises(ConfigError, match="IVB"):
        Config.load(p)


# ============================================================================ hand-built behaviour
S0 = MS("2026-10-26T12:20:00Z"); E0 = S0 + 60 * M; SE = MS("2026-10-26T21:00:00Z")


def bar(s, iv, o, h, lo, c, v=10, buy=5, lv=None):
    b = {"bar_start": s, "bar_end": s + iv, "open": o, "high": h, "low": lo, "close": c, "volume": v, "buy": buy, "sell": v - buy}
    if lv is not None: b["lv"], b["lots"] = [x for x, _ in lv], [q for _, q in lv]
    return b


def run_bars(form, beh, labels=None):
    tr = IBTracker("new_york", "2026-10-26", S0, E0, SE, ST, 5 * M, labels or {})
    for b in form: b.setdefault("lv", [b["close"]]); b.setdefault("lots", [b["volume"]])
    return tr, drive([tr], form, None, beh, SE, SE, True)


def ib_bars(hi=1100, lo=1000):
    """60 one-minute IB bars: low 1000 in minute 3, high 1100 in minute 40, volume 10 each (IB volume 600)."""
    out = []
    for i in range(60):
        h = hi if i == 40 else 1060; l = lo if i == 3 else 1040
        out.append(bar(S0 + i * M, M, 1050, h, l, 1050))
    return out


def test_ib_formed_levels_and_extensions():
    tr, rows = run_bars(ib_bars(), [])
    dev = [r for r in rows if r["event"] == "IB_DEVELOPING"]
    assert len(dev) == 60 and dev[2]["ib_low_ticks"] == 1040 and dev[3]["ib_low_ticks"] == 1000 and dev[39]["ib_high_ticks"] == 1060
    f = [r for r in rows if r["event"] == "IB_FORMED"][0]
    assert (f["ib_high_ticks"], f["ib_low_ticks"], f["ib_range_ticks"], f["ib_mid_ticks"]) == (1100, 1000, 100, 1050.0)
    assert f["event_time"] == E0 and f["volume_lots"] == 600 and f["open_ticks"] == 1050
    assert f["ext_up_0.5_ticks"] == 1150 and f["ext_dn_1.5_ticks"] == 850 and f["ext_up_3_ticks"] == 1400
    s = [r for r in rows if r["event"] == "SESSION_SUMMARY"][0]
    assert s["event_time"] == SE and s["close_location"] == "inside" and s["max_ext_up"] is None and s["range_ratio"] == 1.0


def test_breakout_failed_break_held_and_return():
    form = ib_bars()
    # 5m bars after the IB: break up at 13:25 (close 1110 > 1100), back inside at 13:40 (bar_end 13:45 <= 13:25 + 30m) -> failed
    beh = [bar(E0, 5 * M, 1090, 1112, 1090, 1110, v=200, buy=150),        # BREAKOUT_UP, end 13:25
           bar(E0 + 5 * M, 5 * M, 1110, 1115, 1101, 1105),                 # still above
           bar(E0 + 20 * M, 5 * M, 1100, 1102, 1095, 1098),                # end 13:45 <= 13:55 -> FAILED_BREAK_UP
           bar(E0 + 30 * M, 5 * M, 1098, 1120, 1098, 1118, v=20, buy=5),   # second BREAKOUT_UP at 13:55 (weak: delta < 0)
           bar(E0 + 95 * M, 5 * M, 1118, 1119, 1090, 1095)]                # end 15:00 > 13:55 + 30m: held at 14:25, then RETURN_INSIDE
    tr, rows = run_bars(form, beh)
    ev = [(r["event"], iso(r["event_time"])) for r in rows if r["event"] not in ("IB_DEVELOPING", "SESSION_SUMMARY", "IB_FORMED")]
    assert ev == [("BREAKOUT_UP", "2026-10-26T13:25:00.000Z"), ("FAILED_BREAK_UP", "2026-10-26T13:45:00.000Z"),
                  ("BREAKOUT_UP", "2026-10-26T13:55:00.000Z"), ("BREAKOUT_HELD_UP", "2026-10-26T14:25:00.000Z"),
                  ("RETURN_INSIDE_UP", "2026-10-26T15:00:00.000Z")]
    b1, b2 = [r for r in rows if r["event"] == "BREAKOUT_UP"]
    # mean IB 5m-bar volume = 600 / 12 = 50; 200 >= 1.5 * 50 -> volume confirms; 20 does not
    assert (b1["delta_confirms"], b1["volume_confirms"]) == (True, True) and (b2["delta_confirms"], b2["volume_confirms"]) == (False, False)
    s = [r for r in rows if r["event"] == "SESSION_SUMMARY"][0]
    assert (s["breakout_UP"], s["failed_UP"], s["held_UP"], s["returned_UP"]) == (2, 1, 1, 1)


def test_acceptance_by_time_and_by_volume_and_range_extensions():
    form = ib_bars()
    # 15 consecutive 1m closes above 1100 starting 13:20 -> ACCEPTED_ABOVE by time at 13:35; little volume beyond
    form += [bar(E0 + i * M, M, 1101, 1104, 1099, 1102, v=10, lv=[(1099, 9), (1102, 1)]) for i in range(15)]
    # back inside, then one heavy minute far below: 400 lots below 1000 vs window volume 600+150+450 -> 400/1200 >= 0.30
    form += [bar(E0 + 15 * M, M, 1050, 1060, 1040, 1050)]
    form += [bar(E0 + 16 * M, M, 1000, 1000, 940, 950, v=450, lv=[(1000, 50), (950, 400)])]
    tr, rows = run_bars(form, [])
    acc = [(r["event"], r["criterion"], iso(r["event_time"])) for r in rows if r["event"].startswith("ACCEPTED")]
    assert acc == [("ACCEPTED_ABOVE", "time", "2026-10-26T13:35:00.000Z"), ("ACCEPTED_BELOW", "volume", "2026-10-26T13:37:00.000Z")]
    a = [r for r in rows if r["event"] == "ACCEPTED_BELOW"][0]
    assert a["beyond_lots"] == 400 and a["window_lots"] == 600 + 150 + 10 + 450
    ext = [(r["event"], r["k"]) for r in rows if r["event"].startswith("RANGE_EXT")]
    # low 940 = 1000 - 0.6 * 100: only k = 0.5 is reached; high 1104 never reaches 1150
    assert ext == [("RANGE_EXT_DOWN", "0.5")]
    s = [r for r in rows if r["event"] == "SESSION_SUMMARY"][0]
    assert s["max_ext_down"] == "0.5" and s["close_location"] == "below" and s["accepted_UP"] == 1 and s["accepted_DOWN"] == 1


def test_events_never_before_their_evidence_and_empty_ib():
    tr, rows = run_bars([], [])
    assert [r["event"] for r in rows] == ["IB_EMPTY", "SESSION_SUMMARY"]
    _, rows = run_bars(ib_bars(), [bar(E0, 5 * M, 1090, 1112, 1090, 1110)])
    assert all(r["event_time"] >= r["evidence_end"] >= r["evidence_start"] for r in rows)


# ============================================================================ engine on replay
class Listen:
    def __init__(self): self.rows = []; self.violations = 0; self.proc = None
    def __call__(self, rows):
        if self.proc is not None and any(r["available_at"] > self.proc.clock for r in rows): self.violations += 1
        self.rows += rows


def irun(cfg, name="i", resume=False, until=None, store=True, listen=None):
    eng = IBEngine(cfg, store=IBStore(cfg.data_root, cfg.symbol, replace=not resume) if store else None,
                   listeners=[listen] if listen else [])
    p = TradeProcessor(cfg, sinks=[eng])
    if listen: listen.proc = p
    r = ReplayEngine(cfg, [ChainHasher(), p], name=name).run(resume=resume, until=until)
    return eng, p, r


@pytest.fixture(scope="module")
def ibase(tmp_path_factory):
    cfg = make_dataset(tmp_path_factory.mktemp("ib"), n_per_day=6000)
    lis = Listen(); eng, p, r = irun(cfg, "base", listen=lis)
    return cfg, eng, p, r, lis


def test_engine_verifies_against_raw_trades(ibase):
    cfg, eng, p, r, lis = ibase
    assert r["status"] == "completed" and lis.violations == 0
    rep = verify(cfg, report=False)
    assert rep["summary"]["pass"] and rep["summary"]["checked_full_coverage"] == 4    # Fri 23 and Mon 26, two definitions
    man = read_manifest(cfg.data_root, cfg.symbol)
    assert all(v["date_complete"] for v in man["dates"].values())
    assert not list(ib_root(cfg.data_root, cfg.symbol).rglob("_staging/*.parquet"))


def test_engine_session_boundaries_follow_dst(ibase):
    cfg, eng, p, r, lis = ibase
    f = {(x["ib"], x["label"]): x for x in lis.rows if x["event"] in ("IB_FORMED", "IB_EMPTY")}
    assert iso(f[("london", "2026-10-23")]["ib_start"]) == "2026-10-23T07:00:00.000Z"
    assert iso(f[("london", "2026-10-26")]["ib_start"]) == "2026-10-26T08:00:00.000Z"
    assert iso(f[("new_york", "2026-10-26")]["ib_start"]) == "2026-10-26T12:20:00.000Z"
    assert not [k for k in f if k[1] in ("2026-10-24", "2026-10-25")]                  # no IB on the weekend
    # the IB high/low equal the raw trade extremes in [ib_start, ib_end)
    t = trades_of(cfg, DAYS[3]); ts = t.column("exchange_event_time_utc").cast(pa.int64()).to_numpy(); px = t.column("price_ticks").to_numpy()
    x = f[("new_york", "2026-10-26")]; m = (ts >= x["ib_start"]) & (ts < x["ib_end"])
    assert (x["ib_high_ticks"], x["ib_low_ticks"]) == (int(px[m].max()), int(px[m].min()))
    assert all(y["available_at"] == y["event_time"] for y in lis.rows)


def test_snapshot_restore_equals_uninterrupted(tmp_path):
    cfg = make_dataset(tmp_path, n_per_day=3000)
    e0, _, _ = irun(cfg, "full"); d0 = e0.state_digest()
    m0 = {d: [v[t]["logical_digest"] for t in ("events", "developing", "sessions")] for d, v in read_manifest(cfg.data_root, cfg.symbol)["dates"].items()}
    for cut in ("2026-10-23T07:31:17.250Z", "2026-10-26T13:42:00.000Z"):
        _, _, r1 = irun(cfg, "cut", until=MS(cut)); assert r1["status"] == "stopped"
        e2, _, r2 = irun(cfg, "cut", resume=True)
        assert r2["status"] == "completed" and e2.state_digest() == d0, cut
        assert {d: [v[t]["logical_digest"] for t in ("events", "developing", "sessions")]
                for d, v in read_manifest(cfg.data_root, cfg.symbol)["dates"].items()} == m0, cut
    cfg.raw["initial_balance"]["failed_break_window"] = "15m"
    irun(cfg, "chg", until=MS("2026-10-24T00:00:00Z"))
    cfg.raw["initial_balance"]["failed_break_window"] = "30m"
    with pytest.raises(ReplayGuardError, match="initial_balance settings"):
        irun(cfg, "chg", resume=True)


def test_chunking_and_live_style_chunks_identical(tmp_path):
    cfg = make_dataset(tmp_path, n_per_day=3000, partial_hour=None)
    key = lambda L: sorted(json.dumps({k: v for k, v in r.items()}, sort_keys=True, default=str) for r in L.rows)
    l1 = Listen(); irun(cfg, "a", store=False, listen=l1)
    cfg.raw["replay"]["max_events_per_chunk"] = 83
    l2 = Listen(); irun(cfg, "b", store=False, listen=l2)
    assert key(l1) == key(l2)
    l3 = Listen(); e3 = IBEngine(cfg, listeners=[l3]); p3 = TradeProcessor(cfg, sinks=[e3])
    p3.on_start({"start_ms": day_start_ms(DAYS[0])})
    for d in DAYS:
        t = trades_of(cfg, d); ts = t.column("exchange_event_time_utc").cast(pa.int64()).to_numpy()
        kl = pq.read_table(part_path(cfg.data_root, "XAUUSDT", "klines_1m", str(d)))
        kav = kl.column("close_time_utc").cast(pa.int64()).to_numpy() + 1
        av = np.concatenate([ts, kav]); st = np.concatenate([np.ones(len(ts), np.int8), np.zeros(len(kav), np.int8)])
        rows = np.concatenate([np.arange(len(ts)), np.arange(len(kav))]); o = np.lexsort((st, av))
        av, st, rows = av[o], st[o], rows[o]; keep = av < day_start_ms(DAYS[-1]) + DAY_MS
        av, st, rows = av[keep], st[keep], rows[keep]
        for i in range(0, len(av), 911):
            k = np.empty(len(av[i:i + 911]), KEY_DTYPE); k["avail"] = av[i:i + 911]; k["stream"] = st[i:i + 911]; k["rank"] = 0; k["seq"] = 0
            p3.on_chunk(EventChunk(k, rows[i:i + 911], {"klines_1m": kl, "trades": t}, ["klines_1m", "trades"], int(k["avail"][-1])))
    p3.on_stop({"status": "completed", "end_ms": day_start_ms(DAYS[-1]) + DAY_MS})
    assert key(l3) == key(l1)
