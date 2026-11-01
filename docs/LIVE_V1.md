# Live ingestion and historical/live parity, v1 (Phase 14)

Code: `goldflow/live/` (`trades.py`, `bus.py`, `runner.py`, `parity.py`). Commands: `live-run`, `parity`.
Config block: `live`. Mode: **paper**. The live pipeline produces signals only; orders are Phase 16.

## 1. Where it runs
Binance's REST API (`fapi.binance.com`) answers **HTTP 451** from US hosts, including this build host. The live runner
must run where REST is reachable: the trading PC (India) or a non-US VPS (decision D6). The WebSocket streams are
reachable from this host, and were measured here (section 8).

**WebSocket routing (verified 2026-10-31).** Binance now serves futures streams on two paths:

| Path | Streams |
|---|---|
| `wss://fstream.binance.com/public/stream` | depth (`@depth@100ms`): the Phase 8 recorder |
| `wss://fstream.binance.com/market/stream` | `@aggTrade` and `@markPrice`: the Phase 14 monitor |

The legacy `wss://fstream.binance.com/stream` still **accepts the connection but delivers no messages**. A client on
the old URL would look connected and stay silent, so the defaults were changed for Phase 8 too
(`DepthCfg.ws_base_url`). The staleness timers in section 5 are what reveal this kind of failure.

## 2. Trade definition (decision D3, option a)
Live uses the same trade definition as the history:
- **Every individual trade, including RPI trades**, from REST `GET /fapi/v1/trades` (the last `rest_limit` trades,
  weight 5), polled every `poll_ms` (default 750 ms);
- `isRPITrade` is counted (`rpi_trades`, `rpi_lots`), so the share of RPI trades in live volume is measured
  (`rpi_share_of_lots` in the run output). That share can only be measured where REST is reachable, so it has no value yet.

The `aggTrade` WebSocket stream **excludes RPI trades** (since 2025-12-31). It is recorded for monitoring and
cross-checks only, **never** used as a trade source. `live.trade_source` accepts only `rest_poll`.

**Normalisation.** A poll response is validated by the **Phase 3 trade validator** (`normalize.validate_trades`): tick
and step multiples, quote notional, positive values, duplicates and conflicting duplicates. The accepted rows become
`TradeEvent` v1 rows:
- `record_origin = LIVE_REST_TRADES`;
- `receive_time_utc` is the local receive time;
- `aggressor_side` comes from `isBuyerMaker`, exactly as in the archive.

Rejected rows are counted by reason. The raw response is kept in the capture.

## 3. Raw capture first
Every REST response is written **before** it is parsed: a `rest_trades` record with the receive time, path and
parameters, in `data/raw/binance/futures_um/XAUUSDT/live/date=YYYY-MM-DD/<session>-<hour>.jsonl.gz`. Every WebSocket
message is written as a `ws` record.

The capture format is Phase 8's: gzip JSONL with sync flushes, a per-session sequence number, and markers for
truncation and gaps. `parity --from-capture` replays these records.

## 4. Gaps and backfill
XAUUSDT trade ids increase but are **not contiguous** (Phase 3), so a skipped id does not prove a trade is missing.
- A gap is **suspected** when a poll returns a full page (`rest_limit` trades) and none of it overlaps the last id
  already seen: more trades may have happened than one page holds.
- The runner then backfills with `GET /fapi/v1/historicalTrades` (API key, MARKET_DATA) from the last seen id, in pages
  of 500 (at most `backfill_max_pages`), until it reaches the new page.
- With no key, or if the backfill cannot reach the new page, the gap is recorded (`suspected_gaps`, `gaps`) and a
  **`GAP` data-quality interval** opens over it.

## 5. The live bus, the clock and feed health
**Same interface.** `LiveBus` delivers `EventChunk`s (streams `quality` and `trades`) to the same consumers as the
Phase 4 replay: the Phase 5 `TradeProcessor` and the Phase 6–12 sinks run unchanged.

**Clock.** The bus clock is the **receive time of the last successful poll minus `grace_ms`** (default 3 s) and never
moves backwards.
- A replay may finalise a bar ending at e once its clock reaches e, because every trade before e has already been
  delivered. Live, a trade arrives only after it was published and fetched. The grace keeps the same guarantee.
- A failed poll **holds the clock**: no bar is finalised before its trades could have been fetched. This was found
  while testing; the first version advanced on wall time.

**Late trades.** A trade that still arrives after its 1 s base bar was finalised is **excluded from every bar and every
window** and counted (`late_after_bar_final` for each interval, `late_after_window_final`). This keeps every aggregate
consistent with the others: a coarse bar is still the sum of its base bars, which Phase 7 checks.

Two more things happen to a late trade:
- the runner counts it (`late_trades`);
- it opens a `LATE` interval from the trade's time to the bar-final time.

A replay never delivers such a trade, so historical results are unchanged.

**Feed health.** Each stream has a staleness limit (`stale_ms`):

| Stream | Limit | Measured on | Dataset |
|---|---|---|---|
| `trades` | 15 s | successful polls (a quiet market is not stale) | `trades` |
| `aggTrade` | 30 s | message receive time | `monitor` |
| `markPrice` | 10 s | message receive time | `monitor` |

- When a stream has been silent longer than its limit, a **`STALE` interval opens** at its last message time with an
  **open end**.
- On the `trades` dataset, the processor then labels the affected bars, and the strategy engine suppresses signals
  (`DQ_OPEN`).
- When the stream answers again, the interval is closed: the same (start, status) is sent with the real end, and
  `merge_dq` replaces the open end.
- `monitor` intervals are reported (`health`, `health_events`) but do not affect the trades analytics.

Live statuses `STALE`, `GAP`, `RECOVERY` and `LATE` are not in the Phase 5 rank table, so the processor labels the
bars they cover `MISSING`. That is the conservative default.

## 6. Checkpoints and restart recovery
Every `checkpoint_s` (default 60 s), and on stop, the runner writes `data/live/<name>.checkpoint.json` atomically. It
holds the state of the processor and every sink (`get_state`, the same as replay checkpoints), plus the poller's last
trade id and time, the bus clock, feed health and the counters.

On restart:
1. The checkpoint is loaded and a **`RECOVERY` interval** opens from the last durable trade time (signals are blocked).
2. The first successful poll backfills every trade since the last durable id (with an API key), or records the
   unrecoverable part as a `GAP`.
3. Only then does the `RECOVERY` interval close and signals resume.

## 7. Parity harness (`parity`)
The same trades are processed twice by the full stack (Phase 5 processor, Phase 6 profile, Phase 7 orderflow, Phase 10
IB, Phase 9 auction, Phase 11 strategies), and every output is compared **exactly**: counts, volumes and every other
field.
1. **Live path:** the trades are packed into REST poll responses as the poller would receive them. There is one poll
   every `poll_ms`, holding the trades published up to `delay_ms` before it, at most 1,000 per page. The responses go
   through the live normaliser (the Phase 3 validator) and the `LiveBus` with its grace clock.
2. **Historical path:** the Phase 4 `ReplayEngine` over the same Phase 3 partitions.

The Phase 3 quality annotations of the trades dataset reach both paths.

There are two sources of input:
- `parity --start --end` uses the archived trades of a historical range. This checks the live code path on real
  trades: the same normaliser, bus and consumers a live session uses, fed the trades a live poller would have received.
- `parity --from-capture` uses the trades a `live-run` recorded. The historical side then needs the Phase 3 partitions
  of the same days, which exist once Binance publishes the daily archive.

Trades that a REST poll could not have produced are refused: trades with an unknown side, or trades recovered from
aggTrades.

**Negative control.** The tests show that the harness detects a difference. With polls delayed past the grace period,
trades arrive late and the comparison fails.

Report: `data/reports/parity_report.{json,md}`.

## 8. Measured on this host (2026-10-31)
- **WebSocket:** 120 s on `/market/stream` gave 123 XAUUSDT aggTrades and 40 markPrice updates. Receive time minus
  event time: median 70 ms, p95 205 ms for aggTrades; median 112 ms for markPrice. XAUUSDT also trades at weekends.
- **REST:** HTTP 451. No live trade poll is possible here.
- **Parity on real data:** see `docs/PHASE14_COMPLETION_REPORT.md`.

## 9. Settings (`live` block)
| Key | Default | Meaning |
|---|---|---|
| `trade_source` | `rest_poll` | the only accepted value (D3 option a) |
| `poll_ms` | 750 | poll interval, 100–5000 |
| `rest_limit` | 1000 | trades per poll (500 or 1000) |
| `grace_ms` | 3000 | bus clock lag behind the last successful poll; ≥ `poll_ms` |
| `stale_ms` | trades 15000, aggTrade 30000, markPrice 10000 | staleness limits |
| `ws_streams` | aggTrade, markPrice | monitor streams |
| `ws_base_url` | `wss://fstream.binance.com/market/stream` | monitor endpoint |
| `checkpoint_s` | 60 | checkpoint interval |
| `backfill_max_pages` | 50 | historicalTrades pages per gap (500 trades each) |

REST weight: one poll every 750 ms at weight 5 is 400 per minute, well inside the 2,400 per minute limit. The
`grace_ms` default (3 s) is a proposal. Measure the receive-minus-trade-time distribution on the live host and set it
above its p99.9.

## 10. Limits
- Not yet run against live REST: this host is geo-blocked (D6).
- `tradingSession` and other streams are not subscribed. Add them to `ws_streams` once their names are verified on the
  live host.
- A live day's trades can be compared with the archive only after Binance publishes that day (next day).
