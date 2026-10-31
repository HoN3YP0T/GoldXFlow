"""Phase 3 tests: record validation, quarantine reasons, duplicates, ordering, recovery, idempotency."""
import sys, zipfile, json
from datetime import datetime, timezone, date
from pathlib import Path
import numpy as np
import pyarrow.parquet as pq
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from goldflow import normalize as N
from goldflow import schemas as S
from goldflow.timeutil import day_start_ms, DAY_MS

SPEC = {"inv_tick": 100, "inv_step": 1000, "onboard_ms": 0, "tick_size": "0.01", "step_size": "0.001"}
D = date(2026, 6, 3); D0 = day_start_ms(D); D1 = D0 + DAY_MS; ACQ = D1 + DAY_MS
TH = "id,price,qty,quote_qty,time,is_buyer_maker"
AH = "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker"

def zcsv(tmp, name, header, rows):
    p = tmp / f"{name}.zip"
    with zipfile.ZipFile(p, "w") as z: z.writestr(f"{name}.csv", header + "\n" + "\n".join(rows) + "\n")
    return p

def trow(i, t, px="4000.00", q="0.100", bm="true"):
    return f"{i},{px},{q},{float(px) * float(q):.5f},{D0 + t},{bm}"

def run_trades(tmp, rows):
    raw, pb = N.read_raw(zcsv(tmp, "T", TH, rows), "trades")
    rej, der = N.validate_trades(raw, D0, D1, SPEC, ACQ)
    return raw, rej, pb

# ---------------------------------------------------------------- record rules
def test_value_rules_each_get_reason(tmp_path):
    rows = [trow(1, 0), f"2,0,0.1,0,{D0 + 1},true", f"3,4000.00,-0.1,-400,{D0 + 2},true",
            trow(4, 3, px="4000.005"), trow(5, 4, q="0.0005"), f"6,4000.00,0.100,999,{D0 + 5},true",
            trow(7, -10), trow(8, DAY_MS + 5)]
    raw, rej, _ = run_trades(tmp_path, rows)
    got = dict(zip(raw["id"].tolist(), rej.code.tolist()))
    assert got == {1: "", 2: "NONPOSITIVE_PRICE", 3: "NONPOSITIVE_QUANTITY", 4: "PRICE_NOT_TICK_MULTIPLE",
                   5: "QTY_NOT_STEP_MULTIPLE", 6: "QUOTE_NOTIONAL_MISMATCH", 7: "TS_OUTSIDE_SOURCE_INTERVAL",
                   8: "TS_OUTSIDE_SOURCE_INTERVAL"}

def test_tick_multiple_float_safety():
    px = np.array([4441.56, 4070.32, 0.01, 123456.78, 4441.565])
    ticks, ok = N.multiple_of(px, 100)
    assert ok.tolist() == [True, True, True, True, False] and ticks[0] == 444156

def test_parse_error_row_is_isolated(tmp_path):
    rows = [trow(1, 0), "2,abc,0.1,1,%d,true" % (D0 + 1), "3,4000.00,0.100", trow(4, 3)]
    raw, rej, pb = run_trades(tmp_path, rows)
    assert raw["id"].tolist() == [1, 4] and rej.ok.all()
    assert sorted(b["raw_row"] for b in pb) == [1, 2] and {b["reason_code"] for b in pb} == {"PARSE_ERROR"}

# ---------------------------------------------------------------- duplicates and ordering
def test_exact_duplicate_keeps_first(tmp_path):
    raw, rej, _ = run_trades(tmp_path, [trow(1, 0), trow(2, 5), trow(2, 5), trow(3, 9)])
    assert rej.code.tolist() == ["", "", "DUPLICATE_EXACT", ""]

def test_conflicting_duplicate_quarantines_all_copies(tmp_path):
    raw, rej, _ = run_trades(tmp_path, [trow(1, 0), trow(2, 5, q="0.200"), trow(2, 5), trow(3, 9)])
    assert rej.code.tolist() == ["", "DUPLICATE_CONFLICT", "DUPLICATE_CONFLICT", ""]

def test_june_monthly_pattern_bogus_timestamp_copy(tmp_path):
    # the defect found in XAUUSDT-trades-2026-06.zip: id 908 written twice, first copy days later in time
    rows = [trow(906, 100), trow(907, 108), trow(908, 50_000_000), trow(908, 141), trow(909, 400)]
    raw, rej, _ = run_trades(tmp_path, rows)
    assert rej.code.tolist() == ["", "", "TS_SEQUENCE_OUTLIER", "", ""]

def test_sequence_outliers_unit():
    out, amb = N.sequence_outliers(np.array([1, 2, 99, 3, 4]))       # late spike
    assert out.tolist() == [False, False, True, False, False] and not amb.any()
    out, amb = N.sequence_outliers(np.array([10, 11, 1, 12, 13]))    # early spike
    assert out.tolist() == [False, False, True, False, False]
    out, amb = N.sequence_outliers(np.array([1, 5, 3, 7]))           # cannot tell which is wrong
    assert amb.tolist() == [False, True, True, False] and not out.any()
    out, amb = N.sequence_outliers(np.array([1, 1, 2, 2, 3]))        # equal timestamps are fine
    assert not out.any() and not amb.any()

# ---------------------------------------------------------------- day pipeline, recovery, idempotency
def _day_files(tmp):
    trades = [trow(10, 1000, bm="false"), trow(11, 1000), trow(13, 2000), trow(14, 2000), trow(14, 2000)]
    aggs = [f"500,4000.00,0.200,10,11,{D0 + 1000},false",      # 2 trades, qty matches
            f"501,4001.00,0.300,12,12,{D0 + 1500},false",      # single trade id 12 absent from trades -> recovered
            f"502,4000.00,0.200,13,14,{D0 + 2000},true"]
    return {"trades": zcsv(tmp, "XAUUSDT-trades-2026-06-03", TH, trades),
            "aggTrades": zcsv(tmp, "XAUUSDT-aggTrades-2026-06-03", AH, aggs)}

def _job(tmp, files, write=True):
    import hashlib
    f = {ds: {"raw_file_id": i + 1, "path": str(p), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
         for i, (ds, p) in enumerate(files.items())}
    return {"date": str(D), "files": f, "spec": SPEC, "symbol": "XAUUSDT", "acq_end": ACQ,
            "data_root": str(tmp / "data"), "write": write, "missing_reason": {}}

def test_day_pipeline_recovers_and_flags(tmp_path):
    res = N.process_day(_job(tmp_path, _day_files(tmp_path)))
    p = {x["dataset"]: x for x in res["partitions"]}
    t = pq.read_table(p["trades"]["path"]).to_pydict()
    assert t["source_trade_id"] == [10, 11, 12, 13, 14]
    assert t["record_origin"][2] == "RECOVERED_FROM_AGGTRADES" and t["quote_notional"][2] is None
    assert t["aggregate_trade_id"][2] == 501 and t["aggressor_side"][:2] == ["BUY", "SELL"]
    assert p["trades"]["quarantine_by_reason"] == {"DUPLICATE_EXACT": 1}
    st = {s["dataset"]: s for s in res["day_status"]}
    assert st["trades"]["status"] == "COMPLETE" and st["trades"]["evidence"]["aggTrades"]["recovered_single_trades"] == 1
    assert st["klines_1m"]["status"] == "MISSING"
    q = pq.read_table(Path(p["trades"]["path"].replace("/normalized/", "/quarantine/"))).to_pydict()
    assert q["reason_code"] == ["DUPLICATE_EXACT"] and q["raw_row"] == [4]
    meta = pq.read_schema(p["trades"]["path"]).metadata
    prov = json.loads(meta[b"goldflow.field_provenance"])
    assert prov["price"] == "reported" and prov["aggressor_side"] == "derived" and prov["raw_row"] == "reference"

def test_day_pipeline_flags_missing_trades_partial(tmp_path):
    files = _day_files(tmp_path)
    files["aggTrades"] = zcsv(tmp_path, "XAUUSDT-aggTrades-2026-06-03", AH,
                              [f"500,4000.00,0.500,10,11,{D0 + 1000},false"])   # agg qty > trades qty
    res = N.process_day(_job(tmp_path, files))
    st = {s["dataset"]: s for s in res["day_status"]}
    assert st["trades"]["status"] == "PARTIAL"
    hrs = [h for h in res["hour_status"] if h["dataset"] == "trades" and h["status"] == "PARTIAL"]
    assert len(hrs) == 1 and hrs[0]["hour_utc"].endswith("T00:00:00.000Z")

def test_idempotent_rewrite_same_bytes_and_digest(tmp_path):
    files = _day_files(tmp_path)
    a = N.process_day(_job(tmp_path, files)); b = N.process_day(_job(tmp_path, files))
    c = N.process_day(_job(tmp_path, files, write=False))
    key = lambda r: [(p["dataset"], p["logical_digest"], p["file_sha256"]) for p in r["partitions"]]
    assert key(a) == key(b)
    assert [p["logical_digest"] for p in a["partitions"]] == [p["logical_digest"] for p in c["partitions"]]

def test_raw_change_is_detected(tmp_path):
    files = _day_files(tmp_path); job = _job(tmp_path, files)
    files["trades"].write_bytes(files["trades"].read_bytes() + b"\x00")
    try:
        N.process_day(job); assert False, "should have raised"
    except RuntimeError as e:
        assert "raw file changed" in str(e)

def test_schemas_label_every_field():
    for name, sch in S.SCHEMAS.items():
        prov = json.loads(sch.metadata[b"goldflow.field_provenance"])
        assert set(prov) == set(sch.names) and set(prov.values()) <= {"reported", "derived", "constant", "reference"}

def test_midnight_straddling_aggtrade_resolves_across_days(tmp_path):
    from goldflow.catalog import resolve_midnight
    import hashlib
    d2 = date(2026, 6, 4); E0 = day_start_ms(d2)
    day1 = {"trades": zcsv(tmp_path, "t1", TH, [trow(10, DAY_MS - 100), trow(11, DAY_MS - 60)]),
            "aggTrades": zcsv(tmp_path, "a1", AH, [f"500,4000.00,0.100,10,10,{D1 - 100},false",
                                                   f"501,4000.00,0.300,11,13,{D1 - 60},false"])}   # 11 today, 12-13 tomorrow
    day2_tr = [f"{i},4000.00,0.100,400.00000,{E0 + 18},true" for i in (12, 13)] + [f"14,4000.00,0.100,400.00000,{E0 + 500},true"]
    day2 = {"trades": zcsv(tmp_path, "t2", TH, day2_tr),
            "aggTrades": zcsv(tmp_path, "a2", AH, [f"502,4000.00,0.100,14,14,{E0 + 500},true"])}
    j1 = _job(tmp_path, day1); j2 = _job(tmp_path, day2); j2["date"] = str(d2)
    r1, r2 = N.process_day(j1), N.process_day(j2)
    assert len(r1["boundary_agg"]) == 1 and r2["boundary_trades"]["ids"] == [12, 13]
    log = resolve_midnight([r1, r2])
    assert log == [{"date": str(D), "agg_id": 501, "agg_lots": 300, "matched_lots": 300, "neighbour_trades": 2, "resolved": True}]
    assert {s["dataset"]: s["status"] for s in r1["day_status"]}["trades"] == "COMPLETE"
    assert {s["dataset"]: s["status"] for s in r2["day_status"]}["aggTrades"] == "COMPLETE"
    # without the neighbouring day the straddle cannot be proven -> PARTIAL, never silently accepted
    r1b = N.process_day(j1); resolve_midnight([r1b])
    assert {s["dataset"]: s["status"] for s in r1b["day_status"]}["trades"] == "PARTIAL"

KH = "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore"

def test_foreign_instrument_rows_rejected_by_kline_range(tmp_path):
    # pattern found in the June monthly file: rows from another instrument (price ~95 / ~1140) with tiny ids
    k = [f"{D0},4000.00,4001.00,3999.00,4000.50,0.300,{D0 + 59999},1200.0,3,0.1,400.0,0"]
    trades = [trow(10, 100), trow(11, 200), "152997,95.62,0.070,6.69340,%d,true" % (D0 + 300),
              trow(12, 400, px="4001.00")]
    files = {"klines_1m": zcsv(tmp_path, "k", KH, k), "trades": zcsv(tmp_path, "t", TH, trades)}
    res = N.process_day(_job(tmp_path, files))
    p = {x["dataset"]: x for x in res["partitions"]}
    assert p["trades"]["quarantine_by_reason"] == {"PRICE_OUTSIDE_KLINE_RANGE": 1}
    st = {s["dataset"]: s for s in res["day_status"]}["trades"]
    assert st["status"] == "COMPLETE" and st["evidence"]["klines_1m"]["status"] == "MATCH"

def test_price_implausible_without_klines(tmp_path):
    raw, pb = N.read_raw(zcsv(tmp_path, "T", TH, [trow(1, 0), trow(2, 1), "3,1140.08,0.002,2.28016,%d,true" % (D0 + 2)]), "trades")
    rej, _ = N.validate_trades(raw, D0, D1, SPEC, ACQ, None)
    assert rej.code.tolist() == ["", "", "PRICE_IMPLAUSIBLE"]
