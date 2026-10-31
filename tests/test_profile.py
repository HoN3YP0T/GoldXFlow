"""Phase 6 tests: hand-computed POC / Value Area / node prominence, exact VWAP and bands, and the profile engine on
replay (no look-ahead, session resets across DST, anchors, snapshot/restore, chunk independence, live-style chunks,
parity with a from-scratch rebuild, persisted dataset, verification and export)."""
import copy, json, sys
from fractions import Fraction
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_processing import DAYS, MS, REAL_CFG, make_dataset, trades_of  # noqa: E402

from goldflow.config import Config  # noqa: E402
from goldflow.normalize import part_path, write_atomic  # noqa: E402
from goldflow.orderflow import ProfileEngine, ProfileSettings, ProfileStore, read_manifest, read_table  # noqa: E402
from goldflow.orderflow.export import export_window  # noqa: E402
from goldflow.orderflow.rebuild import TradeSource, float_vwap, snapshot_from_trades, state_from_trades  # noqa: E402
from goldflow.orderflow.settings import NodeParams  # noqa: E402
from goldflow.orderflow.store import profile_root, row_date  # noqa: E402
from goldflow.orderflow.verify import verify  # noqa: E402
from goldflow.orderflow.volume_profile import (BinnedProfile, _prominences, bin_dense, bin_sparse, find_nodes,  # noqa: E402
                                               gaussian_kernel, poc_index, profile_metrics, value_area)
from goldflow.orderflow.vwap import VwapSums, sums_from_levels, vwap_stats  # noqa: E402
from goldflow.processing import TradeProcessor  # noqa: E402
from goldflow.replay import KEY_DTYPE, ChainHasher, EventChunk, ReplayEngine, ReplayGuardError  # noqa: E402
from goldflow.timeutil import DAY_MS, day_start_ms  # noqa: E402

PRM = NodeParams(5, 4, 20, Fraction(1, 10), Fraction(1), Fraction(1, 2), 10, Fraction(1, 2))


def prof(v, lo=100, b=1):
    v = np.asarray(v, np.int64); lv = np.arange(len(v)) + lo
    return BinnedProfile(b, lo, v, v, v * 0, int(v.sum()), int(np.dot(lv * b, v)))


# ============================================================================ POC
def test_poc_unique_and_tie_breaks():
    assert poc_index(prof([1, 2, 9, 3])) == 2
    # tie between bins 100 and 102; mean = 1111/11 = 101 exactly -> equidistant -> lower price
    assert poc_index(prof([5, 1, 5])) == 0
    # tie between 100 and 102; extra volume at 103 moves the mean (101.18) closer to 102
    assert poc_index(prof([5, 0, 5, 1])) == 2
    assert poc_index(prof([0, 0])) is None and poc_index(prof([])) is None


def test_poc_tie_uses_bin_centre_for_wide_bins():
    # bins of 10 ticks: bin 10 = ticks 100..109 (centre 104.5), bin 11 = 110..119 (centre 114.5)
    lv = np.array([100, 119]); q = np.array([5, 5])
    p = bin_sparse(lv, q, q, q * 0, 10)                        # mean 109.5: equidistant from both centres -> lower
    assert p.lo_bin == 10 and p.vol.tolist() == [5, 5] and poc_index(p) == 0
    p = bin_sparse(np.array([109, 119]), q, q, q * 0, 10)      # mean 114 -> closer to centre 114.5 -> upper
    assert poc_index(p) == 1


# ============================================================================ Value Area
def test_value_area_pair_rule_hand_computed():
    # total 30, target 0.7*30 = 21. POC=9 (idx 3). step 1: up (4+4)=8 > down (5+2)=7 -> up, acc 17.
    # step 2: up (1+1)=2 < down 7 -> down, acc 24 >= 21 -> stop. VA = idx 1..5
    assert value_area(prof([1, 2, 5, 9, 4, 4, 1, 1, 3]), 3, Fraction(7, 10), "pair") == (1, 5, 24, 2)


def test_value_area_single_bin_variant():
    # down 5 > up 4 (acc 14); up 4 > down 2 (18); up 4 > down 2 (22 >= 21)
    assert value_area(prof([1, 2, 5, 9, 4, 4, 1, 1, 3]), 3, Fraction(7, 10), "single") == (2, 5, 22, 3)


def test_value_area_equal_pairs_mean_side_then_both():
    # symmetric: equal pairs (6 = 6) and the mean is exactly at the POC centre -> add both in one step
    assert value_area(prof([1, 3, 3, 9, 3, 3, 1]), 3, Fraction(7, 10)) == (1, 5, 21, 1)
    # mean (87/26 = 3.35) above the POC centre (3): the equal up pair goes first; then 4 (one bin left) < 6 -> down
    assert value_area(prof([1, 3, 3, 9, 3, 3, 4], lo=0), 3, Fraction(7, 10)) == (1, 5, 21, 2)


def test_value_area_at_profile_edge_and_full():
    # POC at the lowest bin: only the upper side exists; 9 < 9.1 -> add (1+1)
    assert value_area(prof([9, 1, 1, 1, 1]), 0, Fraction(7, 10)) == (0, 2, 11, 1)
    # fewer than two bins left above: use what remains. Total 16, target 11.2: single upper bin 3 > lower pair 2
    assert value_area(prof([1, 1, 1, 1, 9, 3]), 4, Fraction(7, 10)) == (4, 5, 12, 1)
    # ... and when the remaining bin is smaller, the lower pair wins: total 14, target 9.8
    assert value_area(prof([1, 1, 1, 1, 9, 1]), 4, Fraction(7, 10)) == (2, 4, 11, 1)
    v = [1, 0, 3, 7, 2, 0, 0, 1]
    lo, hi, acc, _ = value_area(prof(v), 3, Fraction(1))
    assert (lo, hi, acc) == (0, 7, sum(v))


def test_profile_metrics_reports_prices_and_bin_size():
    m, _ = profile_metrics(prof([1, 2, 5, 9, 4, 4, 1, 1, 3], lo=40000, b=10), Fraction(7, 10), "pair", PRM, gaussian_kernel(5, 4, 20))
    assert m["bin_ticks"] == 10 and m["poc_ticks"] == 400030 and m["val_ticks"] == 400010 and m["vah_ticks"] == 400050
    assert m["va_lots"] == 24 and m["total_lots"] == 30 and m["va_target"] == "7/10"


# ============================================================================ nodes
def test_prominence_definition_hand_computed():
    s = np.array([0, 5, 1, 3, 2, 8, 0], np.int64)
    pk = {p: pr for p, pr, _, _ in _prominences(s, 1)}
    assert pk == {1: 4, 3: 1, 5: 8}            # 5 - max(0, 1); 3 - max(1, 2); 8 - 0
    vl = {p: pr for p, pr, _, _ in _prominences(s, -1)}
    assert vl == {2: 4, 4: 1}                  # min(5, 8) - 1; min(3, 8) - 2
    assert [(p, pr) for p, pr, _, _ in _prominences(np.array([0, 3, 3, 3, 0]), 1)] == [(2, 3)]   # plateau: middle
    assert [(p, pr) for p, pr, _, _ in _prominences(np.array([0, 3, 3, 0]), 1)] == [(1, 3)]      # even plateau: lower middle
    assert _prominences(np.array([5, 4, 3]), 1) == []                                            # edge plateaus are not peaks


def test_nodes_on_bimodal_profile():
    x = np.arange(400)
    v = (1000 * np.exp(-0.5 * ((x - 100) / 15) ** 2) + 600 * np.exp(-0.5 * ((x - 280) / 20) ** 2) + 5).astype(np.int64)
    v[200] += 40          # a tiny bump: below the prominence threshold
    p = bin_sparse(x + 400000, v, v, v * 0, 1)
    nodes = find_nodes(p, PRM, gaussian_kernel(5, 4, 20))
    hv = [n for n in nodes if n["node_type"] == "HVN"]; lv = [n for n in nodes if n["node_type"] == "LVN"]
    assert [n["price_ticks"] for n in hv] == [400100, 400280]
    assert len(lv) == 1 and 400150 < lv[0]["price_ticks"] < 400250
    # half-prominence width of a Gaussian (sigma 15 smoothed by sigma 5 -> sqrt(250)) is 2.355 * 15.81 = 37.2 ticks
    assert abs(hv[0]["width_ticks"] - 2.3548 * np.sqrt(250)) < 0.5
    assert all(n["prominence_lots"] > 0 for n in nodes) and nodes == sorted(nodes, key=lambda n: n["price_ticks"])


def test_nodes_min_separation_keeps_the_more_prominent():
    x = np.arange(300)
    v = (900 * np.exp(-0.5 * ((x - 100) / 3) ** 2) + 800 * np.exp(-0.5 * ((x - 118) / 3) ** 2) + 10).astype(np.int64)
    p = bin_sparse(x + 1000, v, v, v * 0, 1)
    k = gaussian_kernel(2, 4, 20)
    near = NodeParams(2, 4, 20, Fraction(1, 10), Fraction(1), Fraction(1, 2), 10, Fraction(1, 2))
    far = NodeParams(2, 4, 20, Fraction(1, 10), Fraction(1), Fraction(1, 2), 30, Fraction(1, 2))
    assert [n["price_ticks"] for n in find_nodes(p, near, k) if n["node_type"] == "HVN"] == [1100, 1118]
    assert [n["price_ticks"] for n in find_nodes(p, far, k) if n["node_type"] == "HVN"] == [1100]


def test_kernel_is_integer_and_smoothing_exact():
    k = gaussian_kernel(5, 4, 20)
    assert k.dtype == np.int64 and len(k) == 41 and k[20] == 1 << 20 and (k == k[::-1]).all()


def test_bin_dense_equals_bin_sparse():
    rng = np.random.default_rng(1); lv = np.sort(rng.choice(np.arange(400000, 401000), 300, replace=False))
    q = rng.integers(1, 1000, 300); b = rng.integers(0, 1000, 300) % (q + 1); s = q - b
    d = {k: np.zeros(lv[-1] - lv[0] + 1, np.int64) for k in "vbs"}
    d["v"][lv - lv[0]], d["b"][lv - lv[0]], d["s"][lv - lv[0]] = q, b, s
    for bt in (1, 7, 10, 50):
        x = bin_sparse(lv, q, b, s, bt); y = bin_dense(int(lv[0]), d["v"], d["b"], d["s"], bt, x.sum_q, x.sum_qp)
        assert x.lo_bin == y.lo_bin and all(np.array_equal(getattr(x, a), getattr(y, a)) for a in ("vol", "buy", "sell"))
        assert int(x.vol.sum()) == int(q.sum())


# ============================================================================ VWAP
def test_vwap_exact_bands_and_overflow_fallback():
    lv = np.array([400000, 400010, 400025]); q = np.array([3, 5, 2])
    s = sums_from_levels(lv, q)
    assert (s.q, s.qp, s.qp2) == (10, int((lv * q).sum()), sum(int(a) ** 2 * int(b) for a, b in zip(lv, q)))
    st = vwap_stats(s, (1, 2))
    vw = (lv * q).sum() / q.sum(); sd = np.sqrt(((lv - vw) ** 2 * q).sum() / q.sum())
    assert abs(st["vwap_ticks"] - vw) < 1e-9 and abs(st["sigma_ticks"] - sd) < 1e-9
    assert st["band_up_2_ticks"] == st["vwap_ticks"] + 2 * st["sigma_ticks"]
    # huge range and volume: numpy would overflow int64; the Python-int fallback stays exact
    lv2 = np.array([1, 3_000_000_000]); q2 = np.array([10**9, 10**9])
    s2 = sums_from_levels(lv2, q2)
    assert s2.qp2 == 10**9 * (1 + 3_000_000_000 ** 2) and s2.vwap == Fraction(3_000_000_001, 2)
    e = vwap_stats(VwapSums(), (1,))
    assert e["vwap_ticks"] is None and e["band_up_1_ticks"] is None


# ============================================================================ settings
@pytest.mark.parametrize("patch,msg", [({"value_area": {"pct": 0.7}}, "decimal string"), ({"value_area": {"pct": "1.5"}}, r"\(0, 1\]"),
                                       ({"value_area": {"method": "triple"}}, "pair"), ({"bin_ticks": [10, 10]}, "distinct"),
                                       ({"snapshot_every": "1500ms"}, "interval"), ({"windows": ["moon"]}, "unknown kinds"),
                                       ({"vwap": {"variance": "sample"}}, "population"),
                                       ({"anchors": [{"name": "a", "start": "2026-10-26T10:00:00.500Z"}]}, "align")])
def test_invalid_profile_settings_fail(patch, msg):
    raw = copy.deepcopy(REAL_CFG); raw.setdefault("volume_profile", {}).update(patch)
    with pytest.raises(ValueError, match=msg):
        ProfileSettings.from_config(Config(raw, Path(".")))


def test_profile_needs_one_tick_footprint_and_config_load_validates(tmp_path):
    from goldflow.settings import ConfigError
    raw = copy.deepcopy(REAL_CFG); raw["processing"]["footprint_bin_ticks"] = 10
    with pytest.raises(ValueError, match="footprint_bin_ticks"):
        ProfileSettings.from_config(Config(raw, Path(".")))
    raw = copy.deepcopy(REAL_CFG); raw["volume_profile"]["value_area"]["pct"] = "0"
    (tmp_path / "config").mkdir(); p = tmp_path / "config" / "c.json"; p.write_text(json.dumps(raw))
    with pytest.raises(ConfigError, match="volume_profile"):
        Config.load(p)


# ============================================================================ engine on replay
class Listen:
    def __init__(self): self.recs = []; self.nodes = []; self.violations = 0; self.proc = None
    def __call__(self, recs, nodes):
        if self.proc is not None and any(r["available_at"] > self.proc.clock for r in recs): self.violations += 1
        self.recs += recs; self.nodes += nodes


def prun(cfg, name="v", resume=False, until=None, store=True, listen=None, **kw):
    lis = [listen] if listen else []
    eng = ProfileEngine(cfg, store=ProfileStore(cfg.data_root, cfg.symbol, replace=not resume) if store else None, listeners=lis)
    p = TradeProcessor(cfg, sinks=[eng])
    if listen: listen.proc = p
    r = ReplayEngine(cfg, [ChainHasher(), p], name=name, **kw).run(resume=resume, until=until)
    return eng, p, r


def snap_rows(cfg):
    out = []
    for d in sorted(read_manifest(cfg.data_root, cfg.symbol)["dates"]):
        out += read_table(cfg.data_root, cfg.symbol, "snapshots", d).to_pylist()
    return out


@pytest.fixture(scope="module")
def base(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("prof")
    cfg = make_dataset(tmp, n_per_day=3000, extra_times=[MS("2026-10-26T10:00:00.000Z"), MS("2026-10-23T06:30:00.000Z"),
                                                          MS("2026-10-26T06:30:00.000Z")])
    cfg.raw["volume_profile"]["anchors"] = [{"name": "sat-noon", "start": "2026-10-24T12:00:00Z"}]
    lis = Listen()
    eng, p, r = prun(cfg, "base", listen=lis)
    return cfg, eng, p, r, lis


def test_engine_parity_with_processor_and_rebuild(base):
    cfg, eng, p, r, lis = base
    assert r["status"] == "completed"
    assert eng.stats["final_parity_mismatch"] == 0 and eng.stats["final_parity_checked"] == eng.stats["final_snapshots"] > 0
    assert eng.stats["dq_label_mismatch"] == 0 and lis.violations == 0
    assert eng.stats["lots_applied"] == p.stats["lots_processed"]
    src = TradeSource(cfg); ps = eng.ps
    rows = [x for x in lis.recs if x["is_final"] or x["as_of"] % (3 * 3_600_000) == 0]
    assert len(rows) > 100
    nodes = {}
    for n in lis.nodes: nodes.setdefault((n["window_key"], n["as_of"], n["bin_ticks"]), []).append(n)
    for x in rows:
        ref = snapshot_from_trades(cfg, src, x["covered_from"], x["as_of"], ps)[x["bin_ticks"]]
        for k, v in ref["metrics"].items():
            assert x[k] == v, (x["window_key"], x["as_of"], k)
        assert nodes.get((x["window_key"], x["as_of"], x["bin_ticks"]), []) == [{**{k: x[k] for k in ("window_key", "kind", "name", "label",
               "window_start", "window_end", "covered_from", "as_of", "available_at", "is_final", "bin_ticks")}, **n} for n in ref["nodes"]]


def test_developing_snapshot_excludes_trades_at_or_after_as_of(base):
    cfg, eng, p, r, lis = base
    # a trade exactly at 10:00:00.000 on 26 Oct belongs to the snapshot as of 10:01, not to the one as of 10:00
    t = trades_of(cfg, DAYS[3]); ts = t.column("exchange_event_time_utc").cast(pa.int64()).to_numpy()
    q_at = int(t.column("quantity_lots").to_numpy()[ts == MS("2026-10-26T10:00:00Z")].sum()); assert q_at > 0
    d = {x["as_of"]: x for x in lis.recs if x["window_key"] == "utc_day:utc_day:2026-10-26"}
    before = int(t.column("quantity_lots").to_numpy()[ts < MS("2026-10-26T10:00:00Z")].sum())
    assert d[MS("2026-10-26T10:00:00Z")]["total_lots"] == before
    m = (ts >= MS("2026-10-26T10:00:00Z")) & (ts < MS("2026-10-26T10:01:00Z"))
    assert d[MS("2026-10-26T10:01:00Z")]["total_lots"] == before + int(t.column("quantity_lots").to_numpy()[m].sum())
    assert all(x["available_at"] == x["as_of"] + p.st.trade_shift_ms for x in lis.recs)
    assert all(not x["is_final"] for x in lis.recs if x["window_end"] is not None and x["as_of"] < x["window_end"])


def test_session_resets_across_dst(base):
    cfg, eng, p, r, lis = base
    lon = {}
    for x in lis.recs:
        if x["kind"] == "session" and x["name"] == "london": lon.setdefault(x["label"], []).append(x)
    # BST on Fri 23 Oct: the session starts 06:00 UTC; GMT on Mon 26 Oct: 07:00 UTC
    assert lon["2026-10-23"][0]["window_start"] == MS("2026-10-23T06:00:00Z") and lon["2026-10-23"][0]["as_of"] == MS("2026-10-23T06:01:00Z")
    assert lon["2026-10-26"][0]["window_start"] == MS("2026-10-26T07:00:00Z") and lon["2026-10-26"][0]["as_of"] == MS("2026-10-26T07:01:00Z")
    fin = {k: [x for x in v if x["is_final"]][0] for k, v in lon.items()}
    assert fin["2026-10-23"]["as_of"] == MS("2026-10-23T15:00:00Z") and fin["2026-10-26"]["as_of"] == MS("2026-10-26T16:00:00Z")
    # the first snapshot after the reset holds only the session's own trades (no carry-over from the previous session)
    src = TradeSource(cfg)
    for lab, a in (("2026-10-23", MS("2026-10-23T06:00:00Z")), ("2026-10-26", MS("2026-10-26T07:00:00Z"))):
        x = lon[lab][0]
        assert x["total_lots"] == int(state_from_trades(src, a, a + 60_000).vol.sum())


def test_anchor_vwap_resets_exactly_and_matches_float(base):
    cfg, eng, p, r, lis = base
    an = [x for x in lis.recs if x["kind"] == "anchor"]
    assert an and an[0]["as_of"] == MS("2026-10-24T12:01:00Z") and an[0]["window_end"] is None
    src = TradeSource(cfg); tick = 0.01
    for x in an:
        if x["vwap_ticks"] is None: assert float_vwap(src, MS("2026-10-24T12:00:00Z"), x["as_of"]) == (None, None)
    traded = [x for x in an if x["vwap_ticks"] is not None]
    for x in (traded[0], traded[len(traded) // 2], traded[-1]):   # the last one is after the UK DST change on 25 Oct
        fv, fs = float_vwap(src, MS("2026-10-24T12:00:00Z"), x["as_of"])
        assert abs(x["vwap_ticks"] * tick - fv) / fv < 1e-12 and abs(x["sigma_ticks"] * tick - fs) / fs < 1e-9
    with pytest.raises(ValueError, match="before the engine"):
        eng.add_anchor("late", MS("2026-10-24T00:00:00Z"))


def test_store_verify_and_tamper(base, tmp_path):
    cfg, eng, p, r, lis = base
    man = read_manifest(cfg.data_root, cfg.symbol)
    assert sorted(man["dates"]) == [str(d) for d in DAYS] and all(v["date_complete"] for v in man["dates"].values())
    assert not list(profile_root(cfg.data_root, cfg.symbol).rglob("_staging/*.parquet"))
    assert sum(v["snapshots"]["rows"] for v in man["dates"].values()) == len(lis.recs)
    assert all(row_date(x["as_of"]) in man["dates"] for x in lis.recs)
    rep = verify(cfg, sample_hours=4, report=False)
    s = rep["summary"]
    assert s["fail"] == [] and s["parity_mismatches"] == 0 and s["vwap_max_rel_err"] < 1e-12 and s["checked_final"] > 0
    assert s["phase5_days_equal_partition"] == len(DAYS)
    # tamper with one stored POC -> verification fails on that date
    d = "2026-10-26"; f = profile_root(cfg.data_root, cfg.symbol) / f"date={d}" / "snapshots.parquet"
    t = pq.read_table(f); orig = t
    fin = np.asarray(t.column("is_final").to_pylist()); i = int(np.flatnonzero(fin)[0])
    v = t.column("poc_ticks").to_pylist(); v[i] += 10
    j = t.schema.get_field_index("poc_ticks")
    write_atomic(t.set_column(j, t.schema.field(j), pa.array(v, pa.int64())), f)
    try:
        assert verify(cfg, sample_hours=24, report=False)["summary"]["fail"] == [d]
    finally:
        write_atomic(orig, f)


def test_export_chart_json(base):
    cfg, eng, p, r, lis = base
    f = export_window(cfg, "session:london:2026-10-26", 10)
    doc = json.loads(f.read_text())
    assert doc["is_final"] and doc["window"]["window_start"] == MS("2026-10-26T07:00:00Z")
    assert sum(b["lots"] for b in doc["profile"]) == [x for x in lis.recs if x["window_key"] == "session:london:2026-10-26"
                                                       and x["is_final"] and x["bin_ticks"] == 10][0]["total_lots"]
    assert doc["value_area"]["val"] <= doc["poc"]["price"] <= doc["value_area"]["vah"] and len(doc["developing"]) == 9 * 60
    g = export_window(cfg, "session:london:2026-10-26", 10, as_of=MS("2026-10-26T09:00:00Z"))
    dev = json.loads(g.read_text())
    assert not dev["is_final"] and dev["as_of"] == MS("2026-10-26T09:00:00Z") and len(dev["developing"]) == 120
    with pytest.raises(ValueError, match="not configured"):
        export_window(cfg, "session:london:2026-10-26", 7)


def test_snapshot_restore_equals_uninterrupted(tmp_path):
    cfg = make_dataset(tmp_path, n_per_day=1500)
    e0, _, _ = prun(cfg, "full")
    dig0 = e0.state_digest()
    m0 = {d: (v["snapshots"]["logical_digest"], v["nodes"]["logical_digest"], v["levels"]["logical_digest"])
          for d, v in read_manifest(cfg.data_root, cfg.symbol)["dates"].items()}
    for cut in ("2026-10-23T09:17:42.123Z", "2026-10-25T13:00:30.000Z"):
        _, _, r1 = prun(cfg, "cut", until=MS(cut))
        assert r1["status"] == "stopped"
        e2, _, r2 = prun(cfg, "cut", resume=True)
        assert r2["status"] == "completed" and e2.state_digest() == dig0, cut
        m2 = {d: (v["snapshots"]["logical_digest"], v["nodes"]["logical_digest"], v["levels"]["logical_digest"])
              for d, v in read_manifest(cfg.data_root, cfg.symbol)["dates"].items()}
        assert m2 == m0, cut
    cfg.raw["volume_profile"]["value_area"]["pct"] = "0.68"
    prun(cfg, "chg", until=MS("2026-10-24T00:00:00Z"))
    cfg.raw["volume_profile"]["value_area"]["pct"] = "0.70"
    with pytest.raises(ReplayGuardError, match="volume_profile settings"):
        prun(cfg, "chg", resume=True)


def test_chunk_size_and_live_style_chunks_do_not_change_results(tmp_path):
    cfg = make_dataset(tmp_path, n_per_day=1500, partial_hour=None)
    l1 = Listen(); e1, _, _ = prun(cfg, "a", store=False, listen=l1)
    cfg.raw["replay"]["max_events_per_chunk"] = 61
    l2 = Listen(); e2, _, _ = prun(cfg, "b", store=False, listen=l2)
    assert e1.state_digest() == e2.state_digest()
    key = lambda L: sorted((x["window_key"], x["as_of"], x["bin_ticks"], x["poc_ticks"], x["sum_qp"]) for x in L.recs)
    assert key(l1) == key(l2)
    # live-style: hand-built EventChunks with no replay engine (the Phase 14 bus delivers the same interface)
    l3 = Listen(); e3 = ProfileEngine(cfg, listeners=[l3]); p3 = TradeProcessor(cfg, sinks=[e3])
    p3.on_start({"start_ms": day_start_ms(DAYS[0])})
    for d in DAYS:
        t = trades_of(cfg, d); ts = t.column("exchange_event_time_utc").cast(pa.int64()).to_numpy()
        kl = pq.read_table(part_path(cfg.data_root, "XAUUSDT", "klines_1m", str(d)))
        kav = kl.column("close_time_utc").cast(pa.int64()).to_numpy() + 1
        av = np.concatenate([ts, kav]); st = np.concatenate([np.ones(len(ts), np.int8), np.zeros(len(kav), np.int8)])
        rows = np.concatenate([np.arange(len(ts)), np.arange(len(kav))]); o = np.lexsort((st, av))
        av, st, rows = av[o], st[o], rows[o]; keep = av < day_start_ms(DAYS[-1]) + DAY_MS
        av, st, rows = av[keep], st[keep], rows[keep]
        for i in range(0, len(av), 777):
            k = np.empty(len(av[i:i + 777]), KEY_DTYPE); k["avail"] = av[i:i + 777]; k["stream"] = st[i:i + 777]; k["rank"] = 0; k["seq"] = 0
            p3.on_chunk(EventChunk(k, rows[i:i + 777], {"klines_1m": kl, "trades": t}, ["klines_1m", "trades"], int(k["avail"][-1])))
    p3.on_stop({"status": "completed", "end_ms": day_start_ms(DAYS[-1]) + DAY_MS})
    assert key(l3) == key(l1) and e3.stats["final_parity_mismatch"] == 0


def test_seek_discards_developing_and_marks_partial_start(tmp_path):
    cfg = make_dataset(tmp_path, n_per_day=1500)
    lis = Listen(); eng = ProfileEngine(cfg, listeners=[lis]); p = TradeProcessor(cfg, sinks=[eng])
    e = ReplayEngine(cfg, [ChainHasher(), p], name="sk"); e.run(until=MS("2026-10-24T10:00:30Z"))
    e.seek(MS("2026-10-26T10:00:00Z")); e._stop.clear(); e.run(resume=True)
    assert eng.stats["seeks"] == 1 and eng.stats["final_parity_mismatch"] == 0
    after = [x for x in lis.recs if x["as_of"] > MS("2026-10-26T10:00:00Z")]
    day = [x for x in after if x["window_key"] == "utc_day:utc_day:2026-10-26"]
    assert day and all(x["coverage"] == "PARTIAL_START" and x["covered_from"] == MS("2026-10-26T10:00:00Z") for x in day)
    assert not [x for x in lis.recs if MS("2026-10-24T10:01:00Z") < x["as_of"] <= MS("2026-10-26T10:00:00Z")]
    src = TradeSource(cfg)
    assert day[-1]["total_lots"] == int(state_from_trades(src, MS("2026-10-26T10:00:00Z"), day[-1]["as_of"]).vol.sum())
