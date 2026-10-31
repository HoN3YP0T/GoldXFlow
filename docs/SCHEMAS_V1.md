# Canonical schemas, v1

These are the normalised Parquet datasets under `data/normalized/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/data_type=<dataset>/date=<YYYY-MM-DD>/part-000.parquet`. Each file carries its schema name, version and per-field provenance in Parquet metadata (`goldflow.schema`, `goldflow.schema_version`, `goldflow.field_provenance`).

Every field is one of four kinds:
- **reported**: copied from the provider record.
- **derived**: computed deterministically by a documented rule.
- **constant**: provenance shared by the whole dataset.
- **reference**: points back to the raw source. `raw_file_id` points into the catalog `raw_files` table, and `raw_row` is the 0-based data row inside that file's CSV.

## TradeEvent (`trades`): individual executions
| Field | Kind | Notes |
|---|---|---|
| schema_version, provider_id, venue_id, market_type, instrument_id | constant | `binance`, `binance-futures-usdm`, `usdm_perpetual`, `binance:usdm_perpetual:XAUUSDT` |
| source_trade_id | reported | Binance trade id. Ids are increasing but **not contiguous** (unused ids). |
| aggregate_trade_id | reported | Set only on RECOVERED rows (the aggTrade that carried the trade). |
| exchange_event_time_utc, transaction_time_utc | reported | Both hold Binance's single trade `time` (ms, UTC). The archive does not separate them. |
| receive_time_utc | reported | Null for archive data (only a live recorder sets it). |
| price, quantity | reported | USDT per oz; XAU oz (1 contract = 1 oz). |
| quote_notional | reported | `quote_qty`. Null on RECOVERED rows because aggTrades don't report it. |
| buyer_maker_flag | reported | `is_buyer_maker` |
| aggressor_side | derived | `SELL` if buyer_maker_flag else `BUY` (buyer is maker ⇒ seller hit the bid). `UNKNOWN` if the flag is absent. |
| side_evidence | derived | `EXCHANGE_REPORTED` when the flag is present. |
| price_ticks, quantity_lots | derived | price/0.01 and qty/0.001 as exact integers. Use these for price-level aggregation. |
| record_origin | derived | `ARCHIVE_TRADES` or `RECOVERED_FROM_AGGTRADES` |
| data_quality_status | derived | `VALID` or `RECOVERED` |
| raw_file_id, raw_row | reference | |

Rows are sorted by `source_trade_id`, which is also time order.

## AggTradeEvent (`aggTrades`)
These are aggregates of fills that share price, side and taker order. **They are never individual executions.** Fields: `aggregate_trade_id`, `first_trade_id`, `last_trade_id`, `exchange_event_time_utc`, `price`, `quantity`, `buyer_maker_flag` (reported), plus `aggressor_side`, `side_evidence`, `price_ticks`, `quantity_lots`, `data_quality_status` (derived), and the raw reference.

## DepthBandSnapshot (`bookDepth`): **not L2**
Binance publishes a snapshot about every 30 s with 12 bands: ±0.2 % and ±1, 2, 3, 4, 5 % from mid. Each band gives cumulative resting quantity and notional. Fields: `snapshot_time_utc`, `band_pct`, `cumulative_depth_qty`, `cumulative_notional` (reported), and `book_side` (derived: BID if band < 0). This data must not feed order-book reconstruction.

## MarketMetrics (`metrics`)
Five-minute open interest and long/short ratios, as reported. Source rows are unsorted; normalised rows are time-sorted.

## Kline1m (`klines_1m`): reference only
These are Binance-computed candles, used as the **independent completeness reference** for trades: per-minute `trade_count` and `volume` must equal the normalised trades. They are not an analytics input.

## DepthEvent (`depth_l2`, `depth_l2_rpi`): price-level L2 from the Phase 8 live recorder
The capability is **UNAVAILABLE** for the historical period (2026-04-30 → 2026-10-30). Rows exist only for intervals the live recorder captured (`goldflow depth-normalize`; definitions in `docs/METRICS_LIQUIDITY_V1.md`). RPI liquidity, when enabled, goes to `data_type=depth_l2_rpi` and is never mixed into `depth_l2`.

| Field | Kind | Notes |
|---|---|---|
| exchange_event_time_utc, transaction_time_utc | reported | Binance `E` / `T`. Boundary rows (below) are stamped at the last `E` seen for that book + 1 ms. |
| first_update_id, final_update_id, previous_update_id | reported | `U`, `u`, `pu` for deltas. A snapshot has only `final_update_id` = `lastUpdateId`. |
| bids, asks | reported | `(price, quantity)` lists, absolute quantities; quantity 0 removes the level. Converted back to ticks/lots exactly on read. |
| snapshot_or_delta | reported | `SNAPSHOT` (REST response), `DELTA` (diff event), `CONNECTION_LOST` (connection or capture boundary that invalidated the book), `INVALID_PAYLOAD` (unparseable diff that invalidated the book) |
| sequence_validity | derived | The record's final outcome in the book: `APPLIED`, `DROPPED_BEFORE_SNAPSHOT`, `DUPLICATE`, `DISCARDED`, `SNAPSHOT_APPLIED`, `SNAPSHOT_REJECTED`, `INVALID_PAYLOAD`, `CONNECTION_LOST` |
| data_quality_status | derived | `VALID` if the record was applied, else `NOT_APPLIED` |
| raw_file_id, raw_row | reference | `raw_file_id` indexes the partition metadata list `goldflow.capture_sessions`; `raw_row` is the record's `seq` in that session's raw capture |

Partition metadata also carries `goldflow.book_kind` and `goldflow.snapshot_limits` (the snapshot depth limit per snapshot row, which decides how much of the book is known). The live `receive_time` is kept in the raw capture (`recv_ms`). Rows are sorted by exchange time, then capture order.

## Quarantine record
`dataset, reason_code, reason_detail, raw_file_id, raw_row, source_id, event_time_ms, raw_fields_json`. Stored under `data/quarantine/...` with the same partitioning.

Reason codes (first failing rule wins, checked in this order):

| Code | Meaning |
|---|---|
| PARSE_ERROR | Row could not be parsed. |
| NONPOSITIVE_PRICE / NONPOSITIVE_QUANTITY | Value ≤ 0 or not finite. |
| PRICE_NOT_TICK_MULTIPLE / QTY_NOT_STEP_MULTIPLE | Not an exact multiple of 0.01 / 0.001. |
| QUOTE_NOTIONAL_MISMATCH | quote_qty ≠ price × qty beyond rounding. |
| PRICE_OUTSIDE_KLINE_RANGE | Price is outside Binance's own 1m kline [low, high] for that minute. This catches rows from another instrument, as found in the June 2026 monthly file. |
| PRICE_IMPLAUSIBLE | There is no kline for the minute, and the price is more than 10% from the day's median. |
| INVALID_ID_RANGE | aggTrade has last_trade_id < first_trade_id. |
| SYMBOL_MISMATCH | metrics row is for another symbol. |
| INVALID_BAND | bookDepth band is not in the observed set. |
| TS_BEFORE_LISTING / TS_AFTER_ACQUISITION / TS_OUTSIDE_SOURCE_INTERVAL | Timestamp bounds. |
| TS_SEQUENCE_OUTLIER | Breaks id/time monotonicity, and its neighbours identify it as the outlier (this is the June monthly defect pattern). |
| TS_ORDER_AMBIGUOUS | Monotonicity is broken and the outlier can't be identified, so both rows are quarantined. |
| DUPLICATE_EXACT | Identical copy; the first copy is kept. |
| DUPLICATE_CONFLICT | Same id with a different payload; **all** copies are quarantined. |
| UNCLASSIFIED | A check failed with no specific code (detail text explains). |

## Completeness status (catalog `day_status`, `hour_status`)
- **COMPLETE:** at least one independent reference is available (1m klines trade count/volume, or aggTrades quantity per row), and every available reference agrees.
- **PARTIAL:** a reference shows data missing that couldn't be recovered. Affected hours are listed in `hour_status`.
- **UNVERIFIED:** the file checksum is fine, but no independent reference exists for that day.
- **MISSING:** no source file exists.

Time gaps alone never mark data incomplete, because the market can be quiet.
