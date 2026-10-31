"""Phase 9 tests: reference levels from profiles, acceptance / rejection / return episodes, failed auctions with their
hold, outcomes without hindsight, activity, profile shape (excess, poor extremes, single-print proxy), and the auction
engine on replay (clock check, verification and tamper, restore and chunk independence, wiring rules, review)."""
import copy, json, sys
from fractions import Fraction
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_processing import DAYS, MS, REAL_CFG, make_dataset  # noqa: E402

from goldflow.config import Config  # noqa: E402
from goldflow.normalize import write_atomic  # noqa: E402
from goldflow.orderflow import ProfileEngine  # noqa: E402
from goldflow.orderflow.auction import activity, beyond, levels_from_snapshot, profile_shape, step_episode  # noqa: E402
from goldflow.orderflow.auction_engine import AuctionEngine, AuctionStore  # noqa: E402
from goldflow.orderflow.auction_review import review  # noqa: E402
from goldflow.orderflow.auction_settings import AuctionSettings  # noqa: E402
from goldflow.orderflow.auction_verify import verify  # noqa: E402
from goldflow.orderflow.failed_auction import FailedAuctionHold, Outcome, failed_auction_evidence  # noqa: E402
from goldflow.processing import TradeProcessor  # noqa: E402
from goldflow.replay import ChainHasher, ReplayEngine, ReplayGuardError  # noqa: E402
from goldflow.settings import ConfigError, validate_config  # noqa: E402

I64 = np.int64


def settings(**patch):
    raw = copy.deepcopy(REAL_CFG)
    for blk, v in patch.items():
        if isinstance(v, dict):
            raw["auction"][blk].update(v)
        else:
            raw["auction"][blk] = v
    return AuctionSettings.from_config(Config(raw, Path(".")))


def bar(i, o, h, l, c, vol=100, I=60_000):
    return {"bar_start": i * I, "bar_end": (i + 1) * I, "available_at": (i + 1) * I, "open_ticks": o, "high_ticks": h, "low_ticks": l,
            "close_ticks": c, "volume_lots": vol, "delta_lots": 0, "first_trade_id": i, "last_trade_id": i, "dq_status": "OK",
            "coverage": "FULL"}  # fmt: skip


def fp(levels):
    """1-tick footprint: [(price, buy, sell)]"""
    p, b, s = (np.array(x, I64) for x in zip(*levels)) if levels else (np.zeros(0, I64),) * 3
    return {"level": p, "buy": b, "sell": s, "unk": np.zeros(len(p), I64)}


# ============================================================================ levels
def test_levels_from_snapshot_exact_prices():
    rec = {"bin_ticks": 10, "total_lots": 1000, "high_ticks": 4127, "low_ticks": 4001, "poc_ticks": 4050, "vah_ticks": 4090,
           "val_ticks": 4020, "sum_q": 3, "sum_qp": 12100, "band_up_1_ticks": 4040.5, "band_dn_1_ticks": 4026.0}
    nodes = [{"node_type": "HVN", "price_ticks": 4060}, {"node_type": "LVN", "price_ticks": 4100}]
    got = dict(levels_from_snapshot(rec, nodes, ("HIGH", "LOW", "POC", "VAH", "VAL", "VWAP", "VWAP_BAND", "HVN", "LVN"), (1,)))
    assert got["HIGH"] == 4127 and got["LOW"] == 4001
    assert got["POC"] == Fraction(8109, 2) and got["VAH"] == 4099 and got["VAL"] == 4020   # bin centre; VA top/bottom ticks
    assert got["VWAP"] == Fraction(12100, 3) and got["VWAP_+1SD"] == Fraction(4040.5) and got["HVN1"] == Fraction(8129, 2)
    assert levels_from_snapshot({**rec, "total_lots": 0}, [], ("HIGH",), ()) == []
    assert beyond(4100, Fraction(8199, 2), 1) and not beyond(4099, Fraction(8199, 2), 1) and beyond(4099, Fraction(8199, 2), -1)


# ============================================================================ episodes
def run_episode(bars, foot, L, side=1, ref=None, st=None):
    st = st or settings()
    ep, out = None, []
    for b, f in zip(bars, foot):
        ep, ev = step_episode(ep, b, f, Fraction(L), side, ref, st)
        out += [(t, x["end"]) for t, x in ev if t != "CLOSED"]
    return out, ep


def test_acceptance_by_time_then_return():
    # 15 consecutive closes above 1000 (accept_time 15m on 1m bars), then a close back inside
    bars = [bar(i, 1001, 1005, 999, 1002) for i in range(15)] + [bar(15, 1001, 1002, 990, 995)]
    out, ep = run_episode(bars, [fp([(1002, 10, 0)])] * 16, 1000)
    assert out == [("ACCEPTANCE", 15 * 60_000), ("RETURN", 16 * 60_000)] and ep is None
    out, _ = run_episode(bars[:14], [fp([(1002, 10, 0)])] * 14, 1000)       # 14 closes: not yet
    assert out == []


def test_acceptance_by_volume_and_rejection_rules():
    st = settings()
    # volume beyond 10 % of the source window (ref 1000 lots) with a close beyond: accepted on the second bar
    bars = [bar(0, 999, 1003, 998, 1001), bar(1, 1001, 1004, 1000, 1002)]
    out, _ = run_episode(bars, [fp([(1003, 60, 0)]), fp([(1004, 50, 0)])], 1000, ref=1000, st=st)
    assert out == [("ACCEPTANCE", 120_000)]
    # rejection: probe 3 bins (30 ticks) beyond and back inside within 30 min, never accepted
    bars = [bar(0, 995, 1031, 994, 1005), bar(1, 1005, 1010, 990, 996)]
    out, _ = run_episode(bars, [fp([(1031, 5, 0)]), fp([(1010, 1, 0)])], 1000, st=st)
    assert out == [("REJECTION", 120_000)]
    out, _ = run_episode([bar(0, 995, 1029, 994, 996)], [fp([(1029, 5, 0)])], 1000, st=st)   # 29 ticks: probe too small
    assert out == []
    slow = [bar(0, 995, 1031, 994, 1005)] + [bar(i, 1005, 1006, 1001, 1003) for i in range(1, 31)] + [bar(31, 1003, 1003, 990, 995)]
    out, _ = run_episode(slow, [fp([(1031, 1, 0)])] * 32, 1000, st=settings(acceptance={"accept_time": "60m"}))
    assert out == []                                                                         # returned after 32 min: not a rejection
    out, _ = run_episode([bar(0, 1005, 1010, 960, 965)], [fp([(960, 0, 5)])], 970, side=-1, st=st)   # BELOW mirror: still beyond
    assert out == []


def test_failed_auction_needs_weak_participation_and_a_held_return():
    st = settings()
    snap = {"start": 0, "end": 120_000, "ext": 1040, "vol_beyond": 20, "vol_total": 200, "delta_beyond": 5, "accepted_at": None}
    assert failed_auction_evidence(snap, Fraction(1000), 1, st) == {"weak_volume": True, "responsive_delta": False}
    strong = {**snap, "vol_beyond": 100}
    assert failed_auction_evidence(strong, Fraction(1000), 1, st) is None                  # strong participation, buyers in control
    assert failed_auction_evidence({**strong, "delta_beyond": -3}, Fraction(1000), 1, st)["responsive_delta"]
    assert failed_auction_evidence({**snap, "end": 31 * 60_000 + 1}, Fraction(1000), 1, st) is None      # slow return
    h = FailedAuctionHold("L", Fraction(1000), 1, snap, {}, 3)
    assert h.step(bar(1, 0, 0, 0, 0)) is None                                               # before the return: ignored
    assert [h.step(bar(i, 995, 999, 990, 996)) for i in (2, 3, 4)] == [None, None, "CONFIRMED"]
    h2 = FailedAuctionHold("L", Fraction(1000), 1, snap, {}, 3)
    assert h2.step(bar(2, 995, 1041, 990, 996)) == "DROPPED"                                # traded beyond the probe extreme
    h3 = FailedAuctionHold("L", Fraction(1000), 1, snap, {}, 3)
    assert h3.step(bar(2, 995, 1010, 990, 1001)) == "DROPPED"                               # closed beyond the level again


def test_outcome_horizons_never_see_later_bars():
    o = Outcome("e", 60_000, 1000, -1, 1040, [60_000, 120_000])
    rows = []
    for b in (bar(1, 1000, 1005, 990, 992), bar(2, 992, 1041, 985, 1030)):
        rows += o.step(b, 0)
    rows += o.due(10**9, 0)
    assert rows[0] == {"event_id": "e", "horizon_minutes": 1, "ref_price_ticks": 1000, "close_price_ticks": 992, "move_ticks": 8,
                       "mfe_ticks": 10, "mae_ticks": -5, "invalidated": False, "available_at": 120_000}
    assert rows[1]["mfe_ticks"] == 15 and rows[1]["mae_ticks"] == -41 and rows[1]["invalidated"] and rows[1]["move_ticks"] == -30


def test_activity_classification():
    val, vah, vw = Fraction(1000), Fraction(1100), Fraction(1050)
    assert activity(1150, 10, val, vah, vw)[2] == "INITIATIVE_BUY" and activity(1150, -10, val, vah, vw)[2] == "RESPONSIVE_SELL"
    assert activity(950, -10, val, vah, vw)[2] == "INITIATIVE_SELL" and activity(950, 10, val, vah, vw)[2] == "RESPONSIVE_BUY"
    assert activity(1060, 10, val, vah, vw) == ("INSIDE_VALUE", "ABOVE", "INITIATIVE_BUY")
    assert activity(1040, 10, val, vah, vw) == ("INSIDE_VALUE", "BELOW", "RESPONSIVE_BUY")
    assert activity(1040, 0, val, vah, vw)[2] == "NEUTRAL" and activity(1040, 5, None, None, None)[2] == "UNDETERMINED"


# ============================================================================ profile shape
def shape(vol, first=None, last=None, bt=1, **patch):
    st = settings(profile_shape={**{"excess_bins": 2, "tail_frac": "0.25", "excess_time": "5m", "poor_frac": "0.9",
                                    "single_print_frac": "0.1", "single_print_min_bins": 2}, **patch})  # fmt: skip
    v = np.array(vol, I64)
    f = np.array(first if first is not None else [0] * len(v), I64)
    la = np.array(last if last is not None else [1000] * len(v), I64)
    return {x["type"]: x for x in profile_shape({"lo": np.array([100], I64), "vol": v, "first": f, "last": la}, bt, st)}


def test_excess_poor_and_single_print_proxy():
    # mean 50: a 2-bin thin top tail (<= 12.5) traded within 1 s -> EXCESS_HIGH; the low is flat (60 >= 0.9 * mean(60, 60, 60))
    r = shape([60, 60, 60, 80, 80, 80, 80, 3, 5])
    assert set(r) == {"EXCESS_HIGH", "POOR_LOW"} and r["EXCESS_HIGH"]["tail_bins"] == 2 and r["EXCESS_HIGH"]["price_ticks"] == 108
    slow = shape([60, 60, 60, 80, 80, 80, 80, 3, 5], last=[1000] * 7 + [600_000, 600_000])
    assert "EXCESS_HIGH" not in slow                                   # the tail took 10 minutes: not a fast rejection
    sp = shape([60, 60, 2, 1, 70, 70, 70])
    assert sp["SINGLE_PRINT_PROXY"]["bins"] == 2 and sp["SINGLE_PRINT_PROXY"]["label"].startswith("LOW_VOLUME_PROXY")
    assert shape([5, 5]) == {}                                         # fewer than 3 bins: no shape claims


def test_invalid_auction_settings_fail_at_startup():
    for patch, msg in [({"interval": "2m"}, "processing.intervals"), ({"bin_ticks": 7}, "volume_profile.bin_ticks"),
                       ({"acceptance": {"accept_vol_frac": 0.1}}, "decimal string"), ({"acceptance": {"accept_time": "90s"}}, "multiple"),
                       ({"levels": {"types": ["HIGH", "TOP"]}}, "distinct values"), ({"levels": {"sources": ["anchor"]}}, "not anchor")]:  # fmt: skip
        raw = copy.deepcopy(REAL_CFG)
        for k, v in patch.items():
            raw["auction"][k] = {**raw["auction"][k], **v} if isinstance(v, dict) else v
        with pytest.raises(ConfigError, match=msg):
            validate_config(raw, None, check_discovery=False)


# ============================================================================ engine on replay
LOOSE = {"rejection": {"probe_bins": 1, "reject_time": "60m"}, "acceptance": {"accept_time": "10m", "accept_vol_frac": "0.02"},
         "failed_auction": {"fa_vol_frac": "0.6", "fa_return_time": "60m", "fa_hold_bars": 2, "outcome_minutes": [5, 15]},
         "state": {"every": "5m", "rot_n": 1, "ov_frac": "0.2", "poc_shift_bins": 1},
         "levels": {"user": [{"name": "u", "price": "4100.00"}]},
         "profile_shape": {"tail_frac": "0.6", "excess_bins": 1, "excess_time": "6h", "poor_frac": "0.5", "single_print_frac": "0.5"}}  # fmt: skip


def loosen(cfg):
    for k, v in LOOSE.items():
        cfg.raw["auction"][k].update(copy.deepcopy(v))
    return cfg


class Listen:
    def __init__(self):
        self.rows = {}; self.violations = 0; self.proc = None
    def __call__(self, table, rows):
        if self.proc is not None and self.proc.clock is not None and any(r["available_at"] > self.proc.clock for r in rows):
            self.violations += 1
        self.rows.setdefault(table, []).extend(rows)


def arun(cfg, name="a", resume=False, until=None, store=True, listen=None, **kw):
    prof = ProfileEngine(cfg)
    eng = AuctionEngine(cfg, prof, store=AuctionStore(cfg.data_root, cfg.symbol, replace=not resume) if store else None,
                        listeners=[listen] if listen else [])  # fmt: skip
    p = TradeProcessor(cfg, sinks=[prof, eng])
    if listen: listen.proc = p
    r = ReplayEngine(cfg, [ChainHasher(), p], name=name, **kw).run(resume=resume, until=until)
    return eng, prof, p, r


@pytest.fixture(scope="module")
def base(tmp_path_factory):
    cfg = loosen(make_dataset(tmp_path_factory.mktemp("auc"), n_per_day=3000))
    lis = Listen()
    eng, prof, p, r = arun(cfg, "base", listen=lis)
    return cfg, eng, prof, p, r, lis


def test_engine_emits_nothing_before_its_time_and_uses_only_full_windows(base):
    cfg, eng, prof, p, r, lis = base
    assert r["status"] == "completed" and lis.violations == 0
    s = eng.stats
    assert s["acceptances"] and s["rejections"] and s["failed_auctions"] and s["states"] and s["shape_events"], s
    assert s["partial_windows_not_used"] > 0
    lv = lis.rows["levels"]
    assert not [x for x in lv if x["window_key"].startswith("session:asia:2026-10-23")]     # began before processing
    for e in lis.rows["events"]:
        assert e["available_at"] == e["confirmed_at"] and (e["episode_start"] is None or e["confirmed_at"] > e["episode_start"])
        if e["event_type"] in ("ACCEPTANCE", "REJECTION", "FAILED_AUCTION", "RETURN") and e["level_id"] != "user:u":
            assert next(x for x in lv if x["level_id"] == e["level_id"])["available_at"] <= e["episode_start"]
    assert {x["state"] for x in lis.rows["states"]} - {"UNDETERMINED"}
    # the profile engine's own state is unchanged by having the auction engine listen to it
    prof2 = ProfileEngine(cfg); p2 = TradeProcessor(cfg, sinks=[prof2])
    ReplayEngine(cfg, [ChainHasher(), p2], name="bare").run()
    assert prof2.state_digest() == prof.state_digest() and p2.state_digest() == p.state_digest()


def test_verification_passes_and_detects_tampering(base):
    cfg, eng, prof, p, r, lis = base
    rep = verify(cfg, report=False)
    s = rep["summary"]
    assert s["fail"] == [] and s["pass"] == len(DAYS) and s["episode_events_checked"] > 0 and s["outcomes_checked"] > 0, rep["dates"]
    assert rep["catalogue"]["counts"] and rep["catalogue"]["outcomes"]
    root = AuctionStore(cfg.data_root, cfg.symbol).base
    d = next(x for x in sorted(json.loads((root / "_manifest.json").read_text())["dates"])
             if pq.read_table(root / f"date={x}" / "events.parquet").filter(pa.compute.equal(pa.compute.field("event_type"), "REJECTION")).num_rows)  # noqa: E501
    f = root / f"date={d}" / "events.parquet"
    t = pq.read_table(f)
    i = t.column("event_type").to_pylist().index("REJECTION")
    write_atomic(pa.concat_tables([t.slice(0, i), t.slice(i + 1)]), f)
    try:
        assert d in verify(cfg, report=False)["summary"]["fail"]
    finally:
        write_atomic(t, f)
    out = review(cfg, limit=10)
    assert out["events"] == len(lis.rows["events"]) and out["charts"] > 0
    svg = next((root / "charts").glob("*.svg")).read_text()
    assert svg.startswith("<svg") and "confirmed" in svg


def test_restore_and_chunking_do_not_change_results(tmp_path):
    cfg = loosen(make_dataset(tmp_path, n_per_day=1500))
    e0, _, _, _ = arun(cfg, "full")
    dig = e0.state_digest()
    man = lambda: {d: {t: v[t]["logical_digest"] for t in ("levels", "events", "outcomes", "states", "activity")}  # noqa: E731
                   for d, v in json.loads((AuctionStore(cfg.data_root, cfg.symbol).base / "_manifest.json").read_text())["dates"].items()}
    m0 = man()
    for cut in ("2026-10-23T15:07:42.123Z", "2026-10-25T00:00:00.500Z"):
        arun(cfg, "cut", until=MS(cut))
        e2, _, _, r2 = arun(cfg, "cut", resume=True)
        assert r2["status"] == "completed" and e2.state_digest() == dig and man() == m0, cut
    cfg.raw["replay"]["max_events_per_chunk"] = 61
    e3, _, _, _ = arun(cfg, "small", store=False)
    assert e3.state_digest() == dig
    cfg.raw["replay"]["max_events_per_chunk"] = 997
    cfg.raw["auction"]["rejection"]["probe_bins"] = 2
    arun(cfg, "chg", until=MS("2026-10-24T00:00:00Z"), store=False)
    cfg.raw["auction"]["rejection"]["probe_bins"] = 1
    with pytest.raises(ReplayGuardError, match="auction settings"):
        arun(cfg, "chg", resume=True, store=False)


def test_auction_engine_must_follow_its_profile_engine(tmp_path):
    cfg = make_dataset(tmp_path, n_per_day=200)
    prof = ProfileEngine(cfg); eng = AuctionEngine(cfg, prof)
    with pytest.raises(ValueError, match="after its ProfileEngine"):
        ReplayEngine(cfg, [TradeProcessor(cfg, sinks=[eng, prof])], name="x").run()
