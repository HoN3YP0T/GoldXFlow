# Orderflow metrics v1: delta, CVD, imbalance, Big Trades, absorption, exhaustion, divergence

Package: `goldflow/orderflow/` (Phase 7 modules below). CLI: `orderflow`, `orderflow-verify`. Config: the `orderflow` block of `config/binance_xauusdt.json`.
Input: the Phase 5 `TradeProcessor`. Its finalised bars (base interval 1 s with exact 1-tick footprint, plus every flow interval) and, new in Phase 7, the accepted fills themselves (`ProcessorSink.on_trades`, called after dedup in delivery order). The engine is a `ProcessorSink`, so it runs unchanged on replay and on the Phase 14 live bus.
Instrument: Binance USDⓈ-M `XAUUSDT` only. Nothing here mixes venues or instruments.

| Module | Role |
|---|---|
| `cvd.py` | `PathAgg`: exact running-delta paths per bar (intra-bar extremes) and per reset instance (CVD) |
| `delta.py` | Flow bars: the Phase 5 bar plus delta high/low, unknown share |
| `imbalance.py` | Diagonal / horizontal footprint imbalance and stacked imbalance (pure functions) |
| `big_trades.py` | Taker-order reconstruction from fills; exact size histogram; rolling and time-of-day quantiles |
| `events.py` | Event rows and the lifecycle tracker (CONFIRMED / INVALIDATED / EXPIRED, no back-dating) |
| `absorption.py`, `exhaustion.py`, `divergence.py` | The three detectors |
| `flow_engine.py` | `OrderflowEngine(ProcessorSink)`: wiring, ordering, checkpoint state |
| `flow_store.py` | `OrderflowStore`: Parquet dataset and manifest, staged at replay checkpoints |
| `flow_verify.py` | `orderflow-verify`: independent recomputation from the trades partitions |
| `flow_settings.py` | Validation of the `orderflow` block (called by Phase 1 `validate_config` at `Config.load`); settings hash |

## 1. Units, sides and conventions
- **Units** as in Phase 5: price in ticks (0.01 USDT), volume in lots (0.001 oz), notional in tick·lots (USDT × 10⁵), time in UTC epoch ms, half-open intervals `[start, end)`.
- **Sides.** BUY/SELL are the **exchange-reported aggressor** only (Phase 5 classification). `unknown_lots` is reported next to every output and is **never** assigned to a side: `buy + sell + unknown = volume` on every bar.
- **Side quality gate.** `max_unknown_share` (default 0.05). A bar whose unknown share is larger is not used for imbalances, absorption, exhaustion pushes or divergence pivots; the skip is counted (`*_suppressed_unknown_side`). Big trades with an unknown side are reported with side `UNKNOWN`. (On the real XAUUSDT data every trade has a reported side, so these counters are 0; the synthetic tests have 1 % unknown trades.)
- **Exactness.** Every decision is made in integers or exact rationals. Configured fractions are decimal strings parsed with `Fraction`; floats are refused at startup. Floats appear only in reported magnitudes (`delta_pct`, `unknown_share`, `ratio`, `vwap_ticks`).
- **Bins.** `bin = price_ticks // bin_ticks`; a bin is reported at its lowest price `bin × bin_ticks`.
- **No look-ahead.** Every row has `available_at`, the earliest simulated time it may be used, and is emitted only once the processor clock has reached it.

## 2. Delta and flow bars (`bar_intervals`, default 1m and 5m)
- `delta = buy_lots − sell_lots` per bar (and per level in the Phase 5 footprint; per window in the Phase 5 window summaries).
- `delta_high` / `delta_low`: extremes of the running delta inside the bar, including the open of 0 (so `delta_high ≥ 0 ≥ delta_low`). Resolution is the base interval (1 s): two opposite moves within the same second are not seen.
- A flow bar is available at `bar_end + trade shift` (Phase 5 finalisation).

## 3. CVD (`cvd.resets`, default utc_day, trading_day, session)
- CVD of an instance = running delta of every base bar inside the instance, reset to 0 at its start. Instances come from the Phase 5 calendar (`utc_day`, `trading_day`, `trading_week`, `session:<name>`, `custom:<name>`) plus configured anchors (`cvd.anchors`, open-ended).
- One row per (interval, bar, instance) the bar overlaps: `cvd_open`, `cvd_high`, `cvd_low`, `cvd_close`, clipped to the instance. `straddles = true` when the bar extends beyond the instance (for example a 1 h bar and a session that starts at :20).
- **Coverage.** An instance that started before processing began (`PARTIAL_START`) has an unknown offset: its rows are labelled so and must not be read as absolute CVD.
- Sessions use local time zones (DST through zoneinfo). Example: the London CVD resets at 06:00 UTC on 23 Oct 2026 (BST) and at 07:00 UTC on 26 Oct 2026 (GMT); this is tested.

## 4. Footprint imbalance (`imbalance`, default 1m bars, bin 10 ticks = $0.10)
For bin `p` of one bar (unknown lots never take part):
| | Condition |
|---|---|
| Buy imbalance (diagonal, default) | `buy(p) ≥ ratio × sell(p − 1)` and `buy(p) ≥ min_lots` |
| Sell imbalance (diagonal) | `sell(p) ≥ ratio × buy(p + 1)` and `sell(p) ≥ min_lots` |
| Horizontal (option) | same price: `buy(p)` vs `sell(p)`, `sell(p)` vs `buy(p)` |

- Defaults: `ratio = 3`, `min_lots = 2000` (2 oz), `zero_policy = none`.
- **Empty comparison bin** (no trades, including beyond the bar's range): with `zero_policy = none` there is never an imbalance against it; `any` lets the `min_lots` test alone decide.
- **Stacked imbalance:** ≥ `stack_n` (default 3) adjacent bins with a same-side imbalance in one bar, reported once per run (low and high bin, number of bins, lots).
- Trace: each row carries the trade-id range of the aggressor bin and its comparison bin; the exact trades are those of the bar in those bins.

## 5. Big Trades (`big_trades`)
**Taker-order reconstruction.** Fills are taken in delivery order (exchange time, then trade id). A fill extends the current taker order when (a) its aggressor side is the same, (b) its time is within `cluster_ms` of the previous fill (default 0 = the same millisecond), and (c) its price moves in the aggressor's direction (buy: not lower; sell: not higher) by at most `max_step_ticks` (default 25 = $0.25). Otherwise it starts a new order.
- On real days about 0.1 % of same-ms, same-side fill pairs step against the aggressor's direction: two takers in one millisecond, which rule (c) separates. Two same-side takers in one millisecond whose fills interleave monotonically cannot be distinguished from one order: this is the stated limit of trade-only data.
- aggTrades are **not** used: they are incomplete on 39 days and are grouped by price, not by taker.
- An order is complete once no later fill could extend it: `available_at = last_fill_time + cluster_ms + 1 ms + trade shift`. It is evaluated only then.

**Thresholds** (each can be enabled; `combine` = `any` (default) or `all` of the enabled ones; `all` also requires every enabled quantile to be available):
| Threshold | Definition | Default |
|---|---|---|
| `min_qty_lots` | size ≥ X lots | off |
| `min_notional_usdt` | Σ price × qty ≥ Y USDT (exact) | off |
| `rolling_quantile` | size ≥ the q-quantile of the taker orders that **ended** in the `hours` before the minute in which this order ended | on: q = 0.995, 24 h, ≥ 2000 orders |
| `time_of_day` | size ≥ the q-quantile of the orders in the same 30-min UTC bucket on the previous `lookback_days` days with orders in that bucket and the same weekend status | off: q = 0.995, 20 days, ≥ 200 orders |

- **Quantile definition.** Nearest rank, `rank = ⌈q·n⌉`, on an exact integer histogram: one bin per lot below 1,024 lots (1.024 oz), then 64 bins per doubling, each 1.09 % wide (edges are exact integer roots). The threshold is the **lower edge** of the bin holding that rank, so the threshold is exact below 1.024 oz and within 1.09 % above. With fewer than `min_samples` orders the quantile is **unavailable** (null) and that criterion does not pass.
- Orders ending inside the current minute are never used for that minute's threshold. The engine asserts this.
- Each row stores every fill id (`trade_ids`), the first/last id, the price range and VWAP, which criteria passed, and the thresholds and sample counts that were used.

## 6. Event lifecycle (absorption, exhaustion, divergence)
- An event is first emitted when its evidence is complete: `CANDIDATE` for absorption and exhaustion, `CONFIRMED` for divergence (its swing is confirmed by its right-hand bars). It is then tracked on the finalised **base** bars strictly after `evidence_end`:
  - `CONFIRMED`: price reached `confirm_ticks` (the expected response happened);
  - `INVALIDATED`: price reached `invalidate_ticks` first. If both are touched inside the same base bar, the order within it is unknown: the event is INVALIDATED with `reason = both_in_same_base_bar`. It is never claimed as confirmed on ambiguous evidence;
  - `EXPIRED`: neither, by `deadline`.
- The resolution is stamped at the end of the base bar where it became known (`event_time`); `available_at = event_time + shift`. Therefore `event_time ≥ evidence_end` always: **no back-dating**. Each step is its own row with the same `event_id` (`TYPE:SIDE:interval:detected_at`).
- Events still open when a run ends have only their first row. A completed replay resolves everything whose deadline is inside the range.
- `side` is the aggressor that was absorbed or exhausted (BUY/SELL), or BEARISH/BULLISH for divergences. `direction` of the expected response: BUY/BEARISH → down, SELL/BULLISH → up.

## 7. Detectors (all parameters are v1 defaults, **PROPOSED**)
### 7.1 Absorption (`absorption`, default 1m bars, bin 10 ticks)
Evaluated per finalised bar and per aggressor side, from the bar's exact 1 s × 1 tick footprint:
1. **Zone:** `zone(L)` = aggressive lots of that side in bins `[L − tol, L + tol]` (`tol_bins` 1). `L*` maximises it; ties go to the bin nearest the aggressor's extreme.
2. **Volume:** `zone(L*) ≥ min_zone_lots` (off) and/or `≥` the `zone_quantile` (0.95) of the same side's per-bar maximum zone volume over the previous `quantile_hours` (24 h; bars that **started** before this one; ≥ `min_samples` = 360 bars). Every set criterion must pass; an unavailable quantile fails.
3. **No progress:** the bar's extreme in the aggressor's direction is at most `max_progress_bins` (2) bins beyond the zone.
4. **Not one order:** the zone's lots come from ≥ `min_active_seconds` (3) distinct base bars, and no base bar holds more than `max_second_share` (0.5) of them. A taker order lives in one millisecond (`cluster_ms` 0), so **a single large trade is never labelled absorption** (tested).
5. Candidate at the bar end; tracked for `confirm_window` (5 m). Buyers absorbed: CONFIRMED when price trades `confirm_bins` (3) bins below the zone's low; INVALIDATED when it trades in the bin above the allowed progress (`L* + tol + max_progress + 1`). Sellers: the mirror.
Trace: trades with time in `[evidence_start, evidence_end)`, price in `[level_low, level_high]` and the event's side.

### 7.2 Exhaustion (`exhaustion`, default 1m bars)
1. **Push:** a bar whose high is above every high of the bars that started in the previous `lookback_bars` (5) intervals (at least one such bar must exist). Sellers: lows.
2. **Sequence:** the last `pushes` (3) pushes within `max_span_bars` (30) intervals and inside one CVD instance of `cvd_reset` (trading_day): their extremes rise strictly and their **aggressive lots fall strictly**.
3. **Delta divergence at the extreme:** the last push's CVD close is below the previous push's (buyers; above for sellers).
4. Candidate at the last push's end; CONFIRMED after a pullback of `confirm_bins` (5) bins from the extreme within `confirm_window` (15 m); INVALIDATED if price exceeds the extreme by `inval_bins` (1) bin first.

### 7.3 Divergence (`divergence`, default 5m bars)
- **Pivot high** at bar i: `high(i) >` every high of the R = `swing_bars` (3) bars before and `≥` every high of the R bars after (the first bar of a flat top). Pivot low: the mirror. The 2R + 1 bars must be contiguous in time; a missing bar means no pivot.
- **Confirmation time = end of bar i + R.** Nothing before that moment can use the pivot (no repainting).
- **Bearish:** a confirmed pivot high above the previous confirmed pivot high, at most `max_gap_bars` (48) intervals earlier, with a lower measure. Measure = the CVD close of the pivot bar in the `cvd_reset` instance (both pivots in the same instance) or the bar's delta (`measure = delta`). Bullish: the mirror on lows.
- Emitted CONFIRMED at the confirmation time; tracked for `track_bars` (48) intervals; INVALIDATED once price exceeds the second pivot's extreme by `inval_bins` (3) bins, else EXPIRED.

## 8. Derived dataset
```
data/derived/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/orderflow/date=YYYY-MM-DD/
    bars.parquet  cvd.parquet  imbalances.parquet  stacked.parquet  big_trades.parquet  events.parquet
data/derived/.../orderflow/_manifest.json
```
- A row is stored under the UTC date of `available_at − 1 ms`, so a date is complete once the clock passes its end. Buffers are staged at every replay checkpoint (`date=D/_staging/`), as in Phases 5 and 6.
- The manifest holds per-date row counts, logical digests and SHA-256s, the orderflow and processing settings hashes, the source dataset version and the run's `observe_from`.
- `events.detail` is a JSON string with the detector-specific evidence (zone lots, thresholds, pushes, pivots).

## 9. Determinism and state
- `OrderflowEngine.get_state()` holds every developing path, CVD instance, open taker order, release queue, quantile histograms, detector history, open tracker, pending bar and the store's staged files. It is saved inside the processor's checkpoint and refused if the `orderflow` settings hash changed.
- `state_digest()` is the same for an uninterrupted run, a run stopped and resumed (tested at several cut points, including the DST night) and any chunk size; hand-built live-style chunks give identical rows.
- Adding the orderflow sink does not change the Phase 5 processor state digest (tested).

## 10. Verification (`orderflow-verify`)
Everything is recomputed from the Phase 3 trades partitions with separate, simple code and compared **exactly**: flow bars (all fields including delta extremes), CVD rows (prefix sums), the full imbalance and stacked sets (dict-based), Big Trades (fill-by-fill rebuild, thresholds from scratch, every field and fill id), every event's evidence (absorption zone, exhaustion pushes) and every resolution (re-derived from the base-bar price path), plus the no-back-dating invariants and Phase 5 volume totals. Output: `data/reports/orderflow_verification.{json,md}`, including an event catalogue by session.

## 11. Limitations
- Trade data only: no order-book confirmation of absorption (Phase 8 depth exists only live; `depth_capability` gates it there).
- Delta extremes, CVD high/low and event tracking have base-interval (1 s) resolution.
- Two same-side takers in one millisecond with monotone interleaved fills are merged (§5).
- All detector parameters are PROPOSED v1 defaults chosen for a usable event frequency on real days, **not** by outcome. Outcome statistics are research (Phase 13) and must be judged out of sample.
