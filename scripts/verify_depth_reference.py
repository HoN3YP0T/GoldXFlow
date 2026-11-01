"""Phase 8 real-data reference check of the local book, usable even where the REST snapshot is geo-blocked.

Records the live diff stream `<sym>@depth@100ms` together with Binance's partial-book stream `<sym>@depth20@100ms`
(top 20 levels per side, which carries U/u/pu too) into a raw capture, then verifies offline:

  1. The diff stream's pu chain is continuous (pu == previous u) within each connection.
  2. Seeding a LocalOrderBook from one depth20 message (as a snapshot with limit=20, so depth beyond its 20th level is
     marked unknown) and applying the diff stream with the production sync code gives, for every later depth20
     message with the same update id u, exactly the same levels inside the price range both views cover.

This is an independent reference (Binance's own top-20 book against our reconstruction from diffs). It is a verification
tool, not a data path: production syncing uses the REST snapshot (`goldflow depth-record`).

  python scripts/verify_depth_reference.py --seconds 120 [--out DIR] [--symbol XAUUSDT]
  python scripts/verify_depth_reference.py --verify-only --out DIR
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from goldflow.liquidity import (  # noqa: E402
    BookKey,
    CaptureWriter,
    LocalOrderBook,
    UnitError,
    UnitScale,
    capture_files,
    iter_capture,
    parse_depth_update,
    parse_snapshot,
)


async def record(url: str, out: Path, seconds: float) -> int:
    import websockets

    w = CaptureWriter(out, flush_every_s=1.0)
    n = 0
    t_end = time.monotonic() + seconds
    while time.monotonic() < t_end:
        try:
            async with websockets.connect(url, ping_interval=20, max_size=2**24) as ws:
                w.write("conn", int(time.time() * 1000), "", {"event": "OPEN", "url": url})
                while time.monotonic() < t_end:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), 1.0)
                    except asyncio.TimeoutError:
                        continue
                    w.write("ws", int(time.time() * 1000), msg if isinstance(msg, str) else msg.decode())
                    n += 1
        except Exception as e:  # recorded; the verifier treats it as a break in continuity
            w.write("conn", int(time.time() * 1000), "", {"event": "CLOSE", "reason": repr(e)})
            await asyncio.sleep(1)
            continue
        w.write("conn", int(time.time() * 1000), "", {"event": "CLOSE", "reason": "done"})
    w.close()
    return n


def _levels_in(levels: dict[int, int], lo: int, hi: int) -> dict[int, int]:
    return {p: q for p, q in levels.items() if lo <= p <= hi}


def verify(out: Path, symbol: str) -> dict:
    P, Q = UnitScale("0.01"), UnitScale("0.001")
    diff_stream, top_stream = f"{symbol.lower()}@depth@100ms", f"{symbol.lower()}@depth20@100ms"
    key = BookKey("binance", "binance-futures-usdm", "usdm_perpetual", symbol)
    book = LocalOrderBook(key)
    res = {
        "diff_events": 0,
        "top20_messages": 0,
        "pu_chain_breaks": 0,
        "connections": 0,
        "seeded_at_u": [],
        "compared": 0,
        "matched": 0,
        "mismatches": [],
        "levels_compared": 0,
        "top20_skipped_no_match": 0,
        "payload_errors": 0,
    }
    prev_u = None
    pending_top: list[dict] = []  # depth20 messages waiting for the book to reach their u

    def compare(top: dict) -> None:
        bids = dict(parse_snapshot({"lastUpdateId": top["u"], "bids": top["b"], "asks": top["a"]}, P, Q).bids)
        asks = dict(parse_snapshot({"lastUpdateId": top["u"], "bids": top["b"], "asks": top["a"]}, P, Q).asks)
        bids = {p: q for p, q in bids.items() if q > 0}
        asks = {p: q for p, q in asks.items() if q > 0}
        # common range: inside the depth20 view AND inside what our book knows
        b_lo = max(min(bids) if bids else 0, book.known_bid_floor or 0)
        a_hi = min(max(asks) if asks else 10**12, book.known_ask_cap or 10**12)
        mine_b, mine_a = _levels_in(book.bids, b_lo, 10**12), _levels_in(book.asks, 0, a_hi)
        ref_b, ref_a = _levels_in(bids, b_lo, 10**12), _levels_in(asks, 0, a_hi)
        res["compared"] += 1
        res["levels_compared"] += len(ref_b) + len(ref_a)
        if mine_b == ref_b and mine_a == ref_a:
            res["matched"] += 1
        elif len(res["mismatches"]) < 5:
            res["mismatches"].append(
                {
                    "u": top["u"],
                    "bid_diff": sorted(set(mine_b.items()) ^ set(ref_b.items()))[:6],
                    "ask_diff": sorted(set(mine_a.items()) ^ set(ref_a.items()))[:6],
                }
            )

    for rec in iter_capture(capture_files(out)):
        if rec.kind == "conn":
            if rec.meta.get("event") == "OPEN":
                res["connections"] += 1
            book.on_disconnect(str(rec.meta.get("event")))
            prev_u, pending_top = None, []
            continue
        msg = json.loads(rec.raw)
        stream, d = msg.get("stream"), msg.get("data", {})
        if stream == top_stream:
            res["top20_messages"] += 1
            if book.needs_snapshot and book.buffer:  # seed the book from Binance's own top-20 view
                book.on_snapshot(
                    parse_snapshot(
                        {"lastUpdateId": d["u"], "E": d["E"], "T": d.get("T"), "bids": d["b"], "asks": d["a"]}, P, Q, limit=20
                    )
                )
                if book.is_valid:
                    res["seeded_at_u"].append(d["u"])
            elif book.is_valid:
                if d["u"] == book.last_update_id:
                    compare(d)
                elif d["u"] > book.last_update_id:
                    pending_top.append(d)
                else:
                    res["top20_skipped_no_match"] += 1
            continue
        if stream != diff_stream:
            continue
        res["diff_events"] += 1
        if prev_u is not None and d.get("pu") != prev_u:
            res["pu_chain_breaks"] += 1
        prev_u = d.get("u")
        try:
            book.on_update(parse_depth_update(msg, P, Q))
        except UnitError:
            res["payload_errors"] += 1
            book.on_invalid_payload("parse")
        if book.is_valid and pending_top:
            keep = []
            for t in pending_top:
                if t["u"] == book.last_update_id:
                    compare(t)
                elif t["u"] > book.last_update_id:
                    keep.append(t)
                else:
                    res["top20_skipped_no_match"] += 1
            pending_top = keep
    res["book_counters"] = dict(book.counters.__dict__)
    res["valid_intervals"] = len(book.intervals)
    res["seeded_at_u"] = res["seeded_at_u"][:5]
    res["result"] = "PASS" if res["compared"] > 0 and res["matched"] == res["compared"] and not res["pu_chain_breaks"] else "FAIL"
    return res


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=120)
    ap.add_argument("--symbol", default="XAUUSDT")
    ap.add_argument("--out", type=Path, default=Path("data/scratch/depth_reference"))
    ap.add_argument("--verify-only", action="store_true")
    a = ap.parse_args()
    s = a.symbol.lower()
    if not a.verify_only:
        url = f"wss://fstream.binance.com/stream?streams={s}@depth@100ms/{s}@depth20@100ms"
        n = asyncio.run(record(url, a.out, a.seconds))
        print(f"recorded {n} messages into {a.out}", file=sys.stderr)
    r = verify(a.out, a.symbol)
    print(json.dumps(r, indent=1))
    return 0 if r["result"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
