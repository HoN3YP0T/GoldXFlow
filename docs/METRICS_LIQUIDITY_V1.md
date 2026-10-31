# Order book and liquidity metrics, v1 (Phase 8)

Package: `goldflow/liquidity/`. These metrics are **live-recorded only**. There is no historical price-level L2 for the Phase 2 dataset (2026-04-30 → 2026-10-30), so depth capability is **ABSENT** for that whole period and no depth-based metric or signal can be backtested on it.

## 1. Sources

| Source | Use |
|---|---|
| WebSocket `<symbol>@depth@100ms` (combined stream `wss://fstream.binance.com/stream?streams=…`) | Diff events: `E` (event time), `T` (transaction time), `U` (first update id), `u` (final update id), `pu` (previous event's `u`), `b`/`a` (absolute quantities per price; `0` removes the level) |
| REST `GET /fapi/v1/depth?symbol=…&limit=1000` (weight 20) | Snapshot: `lastUpdateId`, `E`, `T`, up to 1,000 levels per side |
| `<symbol>@rpiDepth@500ms` and `/fapi/v1/rpiDepth` | RPI liquidity. **Unverified names and format; disabled by default** (`depth.rpi_enabled`). If enabled, it builds a separate book (`book_kind = "rpi"`). If its messages don't follow the diff format, the book never becomes valid; nothing is guessed. |

All endpoints are settings (`depth.*` in `goldflow/settings.py`). Their defaults live in code, so the config file and its hash (`f0b71abc…`) are unchanged.

**Verified on real data (2026-10-31, from the cloud host).** The WebSocket stream is reachable, but REST answers HTTP 451.
- Live XAUUSDT diff messages have exactly the fields above.
- Prices and quantities are exact multiples of tick 0.01 and step 0.001.
- `pu` equals the previous `u` on every consecutive event (653 events, 0 breaks).
- `U` is **not** `previous u + 1` (ids jump by thousands), so continuity is checked with `pu` only.
- Receive latency is about 57 ms.

## 2. Book synchronisation (`book.py`)

This is Binance's documented procedure, implemented by `LocalOrderBook`:

1. Buffer diff events. A snapshot is requested only after at least one event is buffered.
2. Apply the snapshot (`lastUpdateId` = L). Drop buffered events with `u < L` (outcome `DROPPED_BEFORE_SNAPSHOT`).
3. The first applied event must satisfy `U ≤ L ≤ u`. If the first remaining event has `U > L`, updates are missing between the snapshot and the stream: the snapshot is rejected (`SNAPSHOT_REJECTED`) and a new one is requested.
4. Each later event must have `pu ==` the previous applied `u`. Otherwise the book is **invalidated**: it is cleared, its validity interval is closed, and the event that broke the chain is kept in the buffer, because it may bridge the next snapshot.
5. Quantity 0 removes a level. An event with `u ≤` the last applied `u` is a `DUPLICATE` and is ignored.

The book is also invalidated by:
- a malformed price or quantity (`INVALID_PAYLOAD`), which is never rounded;
- a crossed book, `best_bid ≥ best_ask` (`depth.invalidate_on_crossed_book`, default on);
- a lost connection, a new connection, a capture truncation or sequence gap, a recorder session boundary, or a replay seek (`CONNECTION_LOST`).

States: `NO_BOOK` (a snapshot is needed), `SYNCING` (snapshot applied, waiting for the bridging event) and `VALID`. Every input record gets exactly one final outcome, which is stored as `DepthEvent.sequence_validity`. A snapshot's outcome is decided when it bridges or fails to, so the label doesn't depend on whether it arrived before or after the stream events it covers.

**Units.** Prices are integer ticks (price / 0.01) and quantities are integer lots (qty / 0.001). They are parsed from the decimal strings exactly (`units.py`), and the state digest is exact.

**Known depth range.** A snapshot with `limit` levels per side shows only part of the book. When a side returns `limit` levels, nothing beyond its deepest level is known: `known_bid_floor` and `known_ask_cap` record those deepest prices. Levels outside that range may appear later through diffs, but the totals there are incomplete. At about $3,900 and 1,000 levels, the known range is only about ±0.1–0.3 %, depending on how sparse the book is.

## 3. Validity intervals and depth capability (`coverage.py`)

These use the exchange clock (`E`).
- A **VALID interval** starts at the `E` of the event that bridged the snapshot.
- It ends at (`E` of the last event applied before the break) + 1 ms, because a missed update could have happened any time after that.
- While a book is VALID, its open interval extends to `last E + 1`.

`depth_capability(start, end)` for the half-open window `[start, end)`:

| Value | Rule |
|---|---|
| VALID | One continuous VALID interval covers the whole window |
| PARTIAL | A VALID interval overlaps the window but doesn't cover it (a gap or resync inside it) |
| ABSENT | No VALID interval overlaps the window (not recorded, or the book was invalid throughout) |

Every depth-based calculation must call `require_valid_depth(coverage, start, end)` (or `DepthCoverage.require_valid`), which raises `DepthUnavailable` (a `CapabilityUnavailable`) unless the capability is VALID. Phase 11 strategies that declare `l2_depth_valid` use this gate. Gating may only suppress, never trigger.

## 4. Snapshot metrics (`metrics.book_metrics`)

These are computed only from a VALID book with both sides non-empty; otherwise `DepthUnavailable` is raised. The time stamp is `book_time_ms` = `E` of the last applied event.

| Metric | Definition (integer ticks `p`, lots `q`) |
|---|---|
| `best_bid_ticks/lots`, `best_ask_ticks/lots` | Highest bid and lowest ask with their quantities |
| `spread_ticks` | `best_ask − best_bid` |
| `mid_x2_ticks` | `best_bid + best_ask` (2 × mid, kept exact) |
| `touch_imbalance` | `(best_bid_lots − best_ask_lots) / (best_bid_lots + best_ask_lots)` |
| Tick band `ticks:N` | Bids with `p ≥ ⌈(mid_x2 − 2N)/2⌉`; asks with `p ≤ ⌊(mid_x2 + 2N)/2⌋` (that is, within N ticks of mid) |
| Percent band `pct:x` | Bids with `2p ≥ mid_x2·(1 − x/100)`; asks with `2p ≤ mid_x2·(1 + x/100)`, evaluated exactly with rationals |
| `band.bid.lots`, `band.ask.lots`, `levels` | Total resting lots and level count inside the band, per side |
| `band.imbalance` | `(bid_lots − ask_lots) / (bid_lots + ask_lots)`; None if both are 0 |
| `band.*.topk_share` | Concentration: share of the band's lots held by its `top_k` largest levels (default 5) |

**Incomplete bands.** If a band reaches beyond the known depth on a side (`known_bid_floor > ⌈lo⌉` or `known_ask_cap < ⌊hi⌋`), that side's values are **None** and so is the imbalance. They are never extrapolated.

Defaults (`depth.*`): tick bands 10, 50, 100, 500; percent bands 0.1, 0.25, 0.5, 1.0; `top_k` 5.

## 5. Level changes, additions, reductions and depletion

`LevelChange(side, price, old_lots, new_lots)` is produced for every level a diff modifies. `classify_changes` labels them:

| Label | Rule |
|---|---|
| `ADDITION` | `new > old`; lots = `new − old` |
| `REDUCTION_WITH_TRADE` | Part of a reduction matched by passive volume traded at the same side and price in the same interval (aggressive SELL consumes BID, aggressive BUY consumes ASK). This is consistent with a fill but is not proof of one. |
| `REDUCTION_UNATTRIBUTED` | The rest: **"reduction (cancel or hidden fill unknown)"**. L2 deltas cannot tell cancellations from fills. |

Until live trades are wired in (Phase 14), there is no trade input, so every reduction is `REDUCTION_UNATTRIBUTED`.

**Near-touch flow (`NearTouchFlow`):**
- It sums additions and reductions within `touch_ticks` (default 10) of the touch, where the touch is the best price on that side **before** the update.
- It covers the trailing `flow_window_ms` (default 60 s) of exchange time.
- Depletion near the touch = `bid_red` / `ask_red`; replenishment = `bid_add` / `ask_add`.
- A value is produced only when the whole window lies inside one continuous VALID run. After an invalidation, the window must refill before values return.

**`LiquidityTracker`** is the listener that runs all of this in live and in replay. It samples metrics at `metrics_cadence_ms` (default 1 s) of book time, and only from a VALID book.

## 6. Storage

| What | Where |
|---|---|
| Raw capture (every payload exactly as received, written **before** the book sees it) | `data/raw/binance/futures_um/<SYM>/depth_l2/live/date=D/<session>-<YYYYMMDDHH>.jsonl.gz`. Gzip is sync-flushed every second, so a crash loses at most the unflushed tail; the reader turns truncation into a `CAPTURE_TRUNCATED` boundary |
| Canonical events | `data/normalized/…/data_type=depth_l2/date=D/part-000.parquet` (DepthEvent v1); RPI in `data_type=depth_l2_rpi` |
| Validity intervals | `data/derived/…/data_type=depth_validity/book=<kind>/intervals.parquet` |
| Coverage report | `data/reports/depth_coverage.{json,md}` |

Capture line fields: `v, session, seq, kind (ws|snapshot|conn), recv_ms, raw, meta`. Size measured on real XAUUSDT diffs on Saturday 2026-10-31 (spot gold closed): 7.3 events/s, about 370 MB/day raw, about 44 MB/day gzip. Weekdays will be higher (the Phase 0 estimate was about 120 MB/day).

## 7. One code path, one book per venue

**One code path.** The live recorder writes each payload to the capture and then feeds the same `CaptureRecord` to `BookDriver`. `goldflow depth-replay` feeds the records from disk to the same driver. `DepthBookConsumer` (the Phase 4 replay consumer) feeds DepthEvent rows to the same `LocalOrderBook` in exchange-time order, re-deriving every decision rather than trusting the stored labels. Tests prove all three give the same book-state digest and the same validity intervals.

**One book per venue.** `BookKey = (provider, venue, market, symbol, book_kind)`.
- `BookRegistry` hands out one book per key and has no merge operation.
- An event for another symbol raises `MergedBookError`, and so does `assert_same_book` across keys.
- RPI liquidity is a separate key.
- `bookDepth` bands (±0.2 % … ±5 %, ~30 s) are **not L2** and never feed this book.

## 8. Limitations

- **No historical L2.** Depth history exists only from the day the recorder starts (decision D5: record free, or buy Tardis.dev).
- **Recording host.** The recorder must run where REST `/fapi/v1/depth` is reachable: the trading PC or a non-US VPS (decision D6). On a geo-blocked host it stops with status `geo_blocked` (exit 4) and records no book.
- **Reduction attribution** needs live trades (Phase 14), and even then a reduction matched by a trade is "consistent with a fill", not proof.
- **Known depth range.** With a 1,000-level snapshot, percent bands wider than the known range are reported as None.
- **Replay engine.** The `depth_l2` stream is not enabled in `replay.streams` yet: enabling it changes the replay config hash, and there is no recorded data. The RPI dataset has no canonical schema entry, so it is replayed from the capture only.
- **Feed staleness.** Staleness timers and live DQ intervals belong to Phase 14. Phase 8 only guarantees that a book with a broken sequence or connection is never used.
