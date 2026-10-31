# Trade processing v1: classification, price-level aggregation, footprint bars, sessions

Package: `goldflow/processing/`. CLI: `python -m goldflow.cli process` and `python -m goldflow.cli reconcile`.
Input: the Phase 4 replay (`EventChunk`s). The Phase 14 live bus must deliver the same interface, so this code runs unchanged on live data.
Config: the `processing` and `sessions` blocks of `config/binance_xauusdt.json`.

| Module | Role |
|---|---|
| `trade_processor.py` | `TradeProcessor(Consumer)`: dedup, classification, bars, windows, finalisation, snapshot/restore; `ProcessorSink` interface |
| `classify.py` | Exchange-reported aggressor side; optional tick rule (INFERRED, separate) |
| `aggregation.py` | Exact integer structures: `Rows` (bar × price level) and `LevelArrays` (window × price level) |
| `sessions.py` | Versioned session calendar with named time zones (zoneinfo/DST) and weekend schedule |
| `settings.py` | Validation of the `processing`/`sessions` blocks (called by Phase 1 `validate_config` at `Config.load`); settings hash |
| `footprint_store.py` | `FootprintWriter` sink: derived Parquet dataset plus manifest |
| `reconcile.py` | Independent checks against the trades partitions and Binance 1m klines |

## 1. Units: integers only
| Quantity | Unit | Exact conversion |
|---|---|---|
| Price | tick = 0.01 USDT (`price_ticks`) | USDT = ticks × 0.01 |
| Volume | lot = 0.001 oz (`quantity_lots`) | oz = lots × 0.001 (1 contract = 1 troy oz) |
| Notional | tick·lot (`notional_tick_lots` = Σ price_ticks × quantity_lots) | USDT = notional_tick_lots × 10⁻⁵ |
| Time | UTC epoch milliseconds | |

Every sum is an int64 sum of integers, so totals are exact. Six months of notional is about 10¹⁶ tick·lots, well inside int64 (9.2 × 10¹⁸).

## 2. Side classification
| `buyer_maker_flag` (`is_buyer_maker`) | Aggressor | `side_evidence` |
|---|---|---|
| `false` | **BUY** (the buyer took liquidity) | EXCHANGE_REPORTED |
| `true` | **SELL** (the buyer was the maker, so the seller hit the bid) | EXCHANGE_REPORTED |
| missing | **UNKNOWN** | UNKNOWN |

- The processor classifies from the reported flag itself and cross-checks the result against the Phase 3 `aggressor_side` column. Disagreements are counted in `side_mismatch_vs_phase3` (0 on real data).
- UNKNOWN volume is reported in `unknown_lots` and is **never** assigned to buy or sell. `buy + sell + unknown = volume` holds for every bar, level and window.
- **INFERRED side** (`processing.inferred_side.enabled`, default **off**): the tick rule (uptick → buy, downtick → sell, zero tick → previous direction, carried across chunks). It is applied only to UNKNOWN trades and written to the separate `inferred_buy_lots`/`inferred_sell_lots` columns (null when disabled). It never changes `buy_lots`/`sell_lots`. Binance reports a side on every trade in our dataset, so in practice these columns stay zero.
- **Recovered trades** (Phase 3 `RECOVERED_FROM_AGGTRADES`) are included by default (`include_recovered`) and counted per bar in `recovered_count`.

## 3. Idempotency (duplicate-safe delivery)
Per instrument the processor keeps a **watermark** (the highest processed trade id) and a **ring** of the last `dedup_window_ids` (default 131,072) accepted ids.

| Incoming id | Result | Counter |
|---|---|---|
| > watermark | accepted | – |
| ≤ watermark and in the ring (or repeated within the chunk) | **ignored** | `duplicates_ignored` |
| ≤ watermark, not in the ring, inside the ring's span | accepted (out-of-order arrival) | `late_accepted` |
| < oldest id in a full ring | can't be proven new, so dropped | `unverifiable_dropped` |

Trade ids are increasing but not contiguous on Binance, so gaps in ids are not treated as missing data (completeness is proven with klines; see `SCHEMAS_V1.md`). Delivering the same events twice leaves every output unchanged; tested on six real days, where every chunk was delivered twice.

## 4. Aggregation keys
- **Price level:** `level = price_ticks // bin_ticks`. Internally everything is kept at 1 tick. Coarser views are exact re-sums (`LevelArrays.view(bin)`, `Rows.rebin(bin)`). The bin's lowest price is `level × bin_ticks` ticks.
- **Footprint level bin:** `processing.footprint_bin_ticks` (default 1 = $0.01). It is stored with every output, because the bin size changes things like the POC.
- **Per level:** `buy_lots`, `sell_lots`, `unknown_lots`, `trade_count`, `buy_count`, `sell_count`, `notional_tick_lots`, `first/last_trade_time`, `first/last_trade_id`, plus the inferred columns. Every aggregate carries the min/max source trade id, so it links back to its source trades.

## 5. Footprint bars
- **Intervals:** `processing.intervals`, default 1s, 5s, 1m, 5m, 15m, 1h. Each must divide a UTC day and be a multiple of the smallest. Bars are UTC-aligned: `bar_start = t − t mod interval`.
- **Per bar:** OHLC in ticks from trades (open = first delivered trade, close = last), `volume_lots`, `buy_lots`, `sell_lots`, `unknown_lots`, `delta_lots = buy − sell`, counts, `recovered_count`, `notional_tick_lots`, first/last trade time and id, `n_levels_1tick`, plus the per-level footprint (a sparse table).
- **Labels:**
  - `weekend`: the bar lies outside the regular trading week (§7).
  - `dq_status`: the worst Phase 3 data-quality status overlapping the bar for the trades dataset (`OK` < `UNVERIFIED` < `PARTIAL` < `MISSING`). The label is only meant to **suppress** use downstream, never to trigger it.
  - `coverage`: `FULL`; `PARTIAL_START` if processing began after the bar started (replay start or seek); `FILTERED` if the replay used `--dates` or session filters.
- **Finalisation (no look-ahead).** A bar `[s, e)` stays *developing* until the processor clock reaches `e + shift`, where `shift` is the trades stream's `time_offset_ms + latency_ms` (0 by default). Only then is it emitted, with `available_at = e + shift` and `is_final = true`. The rule is exact: replay delivers in key order, so once the clock reaches `e + shift`, every trade with time `< e` has been delivered. `developing_bars(interval)` exposes unfinished bars labelled `is_final = false`.
- **End of a completed replay.** It has delivered every event with availability `< end_ms`, so the clock is treated as `end_ms` and every bar ending by then is finalised. A *stopped* replay (`--until`, `--budget`, Ctrl-C) finalises nothing extra.
- **Late trades.** A trade for a bar that was already finalised is not added to it. It is counted in `late_after_bar_final[interval]` (0 in replay by construction; meaningful for Phase 14 live).

## 6. Windows (price-level aggregates over time ranges)
Each window instance is a half-open UTC interval aggregated per 1-tick level (`LevelArrays`). It is finalised at `end + shift` and emitted with totals, OHLC, `weekend_lots`, `dq_status` and `coverage`. Instances are created from the calendar as the clock passes them, so windows with no trades are still reported.

| Kind | Definition | Label |
|---|---|---|
| `utc_day` | 00:00 → 24:00 UTC | UTC date |
| `trading_day` | 18:00 America/New_York on D−1 → 18:00 on D (CME/COMEX convention; `sessions.trading_day`) | D |
| `trading_week` | start of Monday's trading day → start of the next Monday's | Monday's date |
| `session:<name>` | each configured session on its local weekdays | local start date |
| `custom:<name>` | `TradeProcessor(custom_windows=[...])` or `add_custom_window(name, start_ms, end_ms)`; aligned to the base interval | given label |

Downstream accessors: `window(kind, name)` returns the currently developing instance as (summary, `LevelArrays`). `developing_windows()` lists all open ones (`is_final = false`). Sinks receive finalised windows via `on_window(summary, levels)`.

## 7. Session calendar v1 (status: **PROPOSED**, decision D4 open)
Times are local wall-clock in a named zone, with DST handled by zoneinfo. A configured local time that doesn't exist (a DST gap) is refused rather than shifted. The trading-day roll and the sessions are Mon–Fri local unless configured otherwise.

| Session | Zone | Local hours | UTC, mid-summer (15 Jul 2026) | UTC, 26–30 Oct 2026 (UK on GMT, US still on EDT) | UTC, winter (15 Dec 2026) |
|---|---|---|---|---|---|
| asia | Asia/Tokyo | 08:00–17:00 | 23:00–08:00 | 23:00–08:00 | 23:00–08:00 |
| london | Europe/London | 07:00–16:00 | 06:00–15:00 | 07:00–16:00 | 07:00–16:00 |
| new_york | America/New_York | 08:20–17:00 (COMEX gold) | 12:20–21:00 | 12:20–21:00 | 13:20–22:00 |
| trading day roll | America/New_York | 18:00 | 22:00 | 22:00 | 23:00 |

**Regular trading week** (`sessions.regular_trading_week_utc`): Sun 22:00 → Fri 21:00 UTC, per Binance tradingSchedule COMMODITY REGULAR (to be re-verified from a permitted host). Outside it, the perpetual keeps trading while gold is closed, and bars are flagged `weekend = true`. Every interval must divide the open and close times, so a bar is never half in and half out of the weekend.
- Not in v1: exchange holidays (CME/COMEX calendar, Phase 10), and session-anchored bars (bars are UTC-aligned).
- DST behaviour is tested for UK and US transitions in March and October/November 2026. That includes the 23-hour and 25-hour trading days and the week when the UK and US are on different offsets.

## 8. Snapshot, restore and determinism
- `get_state()` / `set_state()` hold everything:
  - dedup watermark and ring (delta-encoded);
  - counters;
  - developing bar rows per interval, and open windows (sparse levels);
  - DQ intervals, clock, coverage origin, tick-rule state;
  - the finalised-window log;
  - every sink's state.

  It is stored inside the Phase 4 replay checkpoint, so cursor and processor state are always saved together.
- The state holds the **processing settings hash** (the `processing` and `sessions` blocks, the trade availability shift and the instrument spec). A resume under different settings is refused with `ReplayGuardError`.
- `state_digest()` hashes the raw state arrays. It is the same for an uninterrupted run, for runs stopped and resumed at any point, and for any chunk size. This is proven on six real days and in unit tests.
- Every reduction is associative (sums, mins, maxes, and open/close by delivery sequence), which is what makes the results independent of chunking.

## 9. Derived dataset
```
data/derived/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/footprint/interval=1m/date=YYYY-MM-DD/bars.parquet
                                                                                              .../levels.parquet
data/derived/.../footprint/interval=1m/_manifest.json
```
- `processing.persist_intervals` (default `["1m"]`) are written. The other intervals exist in memory for downstream consumers only.
- A date is written once every bar starting on it is final. Between checkpoints, finalised bars are buffered. At each checkpoint the buffer is flushed to `date=D/_staging/` and the checkpoint lists those files, so checkpoints stay small and a crash loses nothing. Staging files are removed once the date is written.
- **Manifest:** per-date bars, level rows, lots by side, trade count, first/last trade id, weekend bars, coverage and DQ counts, plus logical digests (content only) and file SHA-256s. It also records the processing settings hash and the source dataset version (the Phase 4 dataset version).
- Parquet metadata carries `goldflow.schema` (`FootprintBar`/`FootprintLevel`), the version, interval, bin, tick and step, the settings hash and per-field docs.

## 10. Reconciliation (`goldflow.cli reconcile`, also run at the end of `process`)
For each written date:
1. **Internal checks:** `buy + sell + unknown = volume` and `delta = buy − sell` on every bar; Σ levels = bar for every bar.
2. **Against the Phase 3 trades partition:** total lots, trade count, buy/sell/unknown lots, first/last trade id. This applies only when every bar is `coverage = FULL`.
3. **Against Binance's 1m klines**, minute by minute on days whose Phase 3 status is COMPLETE: volume, trade count, **taker-buy volume vs `buy_lots`** (an independent check of the side classification), and OHLC.

Reports: `data/reports/processing_reconciliation_1m.{json,md}`. All checks are exact equalities.

## 11. Sink interface (for Phases 6–10)
```python
class MyAnalytic(ProcessorSink):
    name = "my_analytic"
    def on_bars(self, interval, bars, levels, final_upto_ms): ...   # finalised bars (column dict) + footprint levels
    def on_window(self, summary, levels): ...                       # finalised window totals + 1-tick level columns
    def get_state(self): ...; def set_state(self, s): ...           # saved inside the processor's checkpoint state
proc = TradeProcessor(cfg, sinks=[MyAnalytic()])
ReplayEngine(cfg, [proc]).run()
```
A finalised output may be used at simulated time T only if its `available_at` ≤ T. Developing state must be read through the `developing_*` accessors and treated as provisional.

## 12. Performance
Measured on six real days (6.4 M events, 6.18 M trades) in the cloud container (4 vCPU): about 570–650 k events/s with every interval, every window, the Parquet writer and checkpoints on. Peak RSS was about 1.0 GB. The six-month figure is in `docs/PHASE5_COMPLETION_REPORT.md`.

## 13. Limitations
- **Session times** are the guide's proposals (D4 open). Changing them changes the settings hash, so derived data must be regenerated.
- **No holiday calendar** yet (Phase 10). Weekend detection uses the fixed UTC schedule above.
- **Trade data only.** No order-book or quote-based classification is possible on history (no L2 or quotes; see `L2_ORDERBOOK_AVAILABILITY.md`).
- **Seek:** developing aggregates are discarded, and outputs restart with `coverage = PARTIAL_START`. Finalised outputs already emitted are unaffected, but a date interrupted by a seek is not written by the footprint writer.
