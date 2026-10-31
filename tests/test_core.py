"""Unit tests: range arithmetic, planner fallbacks, and verifier failure detection."""
import io, json, sys, zipfile
from datetime import datetime, timezone
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from goldflow.timeutil import minus_months, to_ms, DAY_MS
from goldflow.integrity import verify_file
from goldflow.vision_archive import RemoteFile, archive_key
from goldflow import acquire
from goldflow.config import Config

def test_minus_months_clamps():
    assert minus_months(datetime(2026, 8, 31, tzinfo=timezone.utc), 6) == datetime(2026, 2, 28, tzinfo=timezone.utc)
    assert minus_months(datetime(2026, 10, 30, 8, tzinfo=timezone.utc), 6) == datetime(2026, 4, 30, 8, tzinfo=timezone.utc)

def _zip(tmp, name, rows, header):
    p = tmp / f"{name}.zip"
    with zipfile.ZipFile(p, "w") as z:
        z.writestr(f"{name}.csv", header + "\n" + "\n".join(rows) + "\n")
    return p

def test_verifier_flags_gap_dup_and_backwards(tmp_path):
    s = to_ms(datetime(2026, 1, 1, tzinfo=timezone.utc))
    rows = [f"{i},4000.0,0.1,400.0,{s + t},true" for i, t in [(1, 0), (2, 5), (5, 10), (5, 11), (6, 3)]]
    p = _zip(tmp_path, "T-trades-2026-01-01", rows, "id,price,qty,quote_qty,time,is_buyer_maker")
    r = verify_file(p, "trades", s, s + DAY_MS)
    assert r.id_gap_count == 1 and r.id_gap_ids == 2      # ids 3,4 missing -> recorded, not hidden
    assert r.dup_ids == 1 and r.ts_backwards == 1 and not r.ok

def test_verifier_rejects_corrupt_zip(tmp_path):
    p = tmp_path / "bad.zip"; p.write_bytes(b"not a zip")
    r = verify_file(p, "trades", 0, DAY_MS)
    assert not r.ok and "parse error" in r.problems[0]

class FakeArchive:
    def __init__(self, keys): self.keys = keys
    def list_prefix(self, prefix):
        return {k: RemoteFile(k, 10, "e", "t") for k in self.keys if k.startswith(prefix)}

def test_planner_monthly_daily_missing_and_rest(tmp_path):
    raw = json.loads(Path(__file__).resolve().parents[1].joinpath("config/binance_xauusdt.json").read_text())
    raw["datasets"] = {"aggTrades": {"monthly": True, "daily": True, "rest_tail": True, "role": "x"}}
    cfg = Config(raw, tmp_path)
    k = lambda g, p: archive_key("futures_um", g, "aggTrades", "XAUUSDT", p)
    keys = [k("daily", "2026-04-30"), k("monthly", "2026-05"), k("daily", "2026-06-01")] + \
           [k("daily", f"2026-06-{d:02d}") for d in range(2, 31) if d != 15]    # June monthly absent, 06-15 absent
    start = to_ms(datetime(2026, 4, 30, 8, tzinfo=timezone.utc)); end = to_ms(datetime(2026, 7, 1, 8, tzinfo=timezone.utc))
    units = acquire.plan(cfg, FakeArchive(keys), start, end)
    ids = [u.unit_id for u in units]
    assert "archive:aggTrades:monthly:2026-05" in ids                  # whole month -> monthly
    assert "archive:aggTrades:daily:2026-06-01" in ids                  # monthly missing -> daily fallback
    assert "missing:aggTrades:daily:2026-06-15" in ids and "rest:aggTrades:2026-06-15" in ids
    tail = [u for u in units if u.unit_id == "rest:aggTrades:2026-07-01"][0]
    assert tail.start_ms == end - 8 * 3_600_000 and tail.end_ms == end  # partial final day bounded by end
    # every millisecond of [start,end) is planned exactly once by archive or missing units
    cov = sorted((u.start_ms, u.end_ms) for u in units if u.method in ("archive", "missing"))
    assert cov[0][0] <= start and cov[-1][1] >= end
    assert all(a[1] == b[0] for a, b in zip(cov, cov[1:]))
