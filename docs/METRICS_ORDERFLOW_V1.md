# Orderflow metrics, v1 (Phase 7)

Package: `goldflow/orderflow/`. CLI: `python -m goldflow.cli orderflow` (catalogue) and `orderflow-validate` (independent checks).

Everything runs as one Phase 5 `ProcessorSink` (`OrderflowAnalytics`) on the shared `TradeProcessor`, so replay and the Phase 14 live bus run identical code.

**Inputs.** Phase 5 finalised footprint bars and levels, plus two hooks added to the sink interface in this phase. Neither changes any Phase 5 output or digest:
- `on_trades(t, ids, p, q, side, rec)`: accepted trades after dedup and classification, in delivery order;
- `on_advance(time_floor_ms)`: every trade with time < floor has been delivered.

**Units.** Lots (0.001 oz), ticks (0.01 USDT), UTC ms. All comparisons are exact integer arithmetic. Ratios and quantile levels are `Fraction`s.

**Status of every threshold.** These are **research defaults, not tuned or validated values**. Phase 13 decides which, if any, carry information. Defaults live in code (`goldflow/orderflow/settings.py`). An optional `orderflow` config block overrides them and is validated at `Config.load`. The config file has no such block, so its hash is unchanged. The settings hash is stored in every checkpoint, and a resume under different settings is refused.

## 1. Side uncertainty
Only exchange-reported aggressor sides enter buy, sell and delta.
- UNKNOWN volume is carried next to every output: `unknown_lots` and `cum_unknown_lots`.
- An UNKNOWN trade is its own taker order with side `UNKNOWN`.
- Nothing is ever assigned to bid or ask by inference.

On the Binance dataset every trade has a reported side, so the unknown columns are 0 on real data. The synthetic tests exercise non-zero unknowns.

## 2. Delta and CVD (`delta.py`, `cvd.py`)
- **Delta** = `buy_lots − sell_lots` per bar, per footprint level, or per window. These come straight from the Phase 5 footprint (`bar_delta`, `level_delta`, `window_delta`).
- **CVD(series, t)** = Σ bar delta over the finalised bars of the series' current instance that end by t.

| Series | Reset |
|---|---|
| `utc_day` | 00:00 UTC |
| `trading_day` | 18:00 America/New_York (Phase 5 calendar) |
| `trading_week` | Monday's trading-day start |
| `session:<name>` | each configured session instance; CVD exists only inside the session |
| `anchor:<name>` | from an anchor time (`ResetCVD.add_anchor`), never reset |

- A bar belongs to an instance only if it lies wholly inside it.
- If processing began after an instance started, its rows are `coverage = PARTIAL_START`. Filtered replays are `FILTERED`.
- Output table `cvd`: `bar_delta_lots`, `cvd_lots`, `cum_buy_lots`, `cum_sell_lots`, `cum_unknown_lots`, plus the bar's `dq_status` and `weekend`.
- Defaults: interval 1m; resets `utc_day`, `trading_day` and `session`.

## 3. Footprint imbalance (`imbalance.py`)
Levels are re-binned exactly to bins of `bin_ticks` (default 10 = $0.10). Within one bar, at bin p:

| Mode | Buy imbalance at p | Sell imbalance at p |
|---|---|---|
| `diagonal` (default) | `buy(p) ≥ ratio × sell(p−1)` and `buy(p) ≥ min_lots` | `sell(p) ≥ ratio × buy(p+1)` and `sell(p) ≥ min_lots` |
| `horizontal` | `buy(p) ≥ ratio × sell(p)` and `buy(p) ≥ min_lots` | `sell(p) ≥ ratio × buy(p)` and `sell(p) ≥ min_lots` |

- **Empty comparison bin:** with `zero_policy = "none"` (default) it gives no imbalance. With `"imbalance"` it counts as one if the `min_lots` test passes.
- **Stacked imbalance:** ≥ `stack_n` (default 3) *consecutive* bins with same-side imbalance in one bar. It becomes an event `STACKED_IMBALANCE`, and its member rows are flagged `stacked`.
- **Outputs:** table `imbalances` holds every single-bin imbalance, with `opp_lots`, `unknown_lots` and the trade-id range.
- **Defaults:** interval 1m, ratio 3.0, `min_lots` 1,000 (1 oz).
- **Event time** = the bar's `available_at` (bar end + shift).

## 4. Taker orders and Big Trades (`big_trades.py`)
**Taker-order reconstruction.** An order is a maximal run of consecutive trades (trade-id order) where each next trade:
1. has the same exchange-reported side (UNKNOWN never joins);
2. is ≤ `cluster_ms` later (default 0: the same exchange millisecond);
3. moves in the aggressor's direction or stays (buy: price ≥ previous; sell: price ≤ previous);
4. steps at most one sweep bin (`sweep_bin_ticks`, default 10) from the previous fill.

aggTrades are **not** used as orders. They are incomplete on 39 days, and they aggregate per price, not per order. Binance publishes no order ids, so two taker orders in the same millisecond, on the same side, at adjacent prices merge. `orderflow-validate` measures this against aggTrades (§8).

**Confirmation.** An order is complete when a trade that can't extend it arrives, or when the time floor passes `last fill + cluster_ms`. Its time is fixed at `last_fill_time + cluster_ms + 1 + availability shift`, independent of chunking.

**Thresholds.** Enabled criteria are combined with `any` (default) or `all`:

| Criterion | Rule | Default |
|---|---|---|
| `abs_lots` | size ≥ X lots | off |
| `notional_usdt` | Σ price × qty ≥ Y USDT (exact: tick·lot × 10⁻⁵) | off |
| `quantile` | size ≥ the nearest-rank q-quantile of **all** taker-order sizes confirmed in `[B − lookback, B)`, where `B = floor(t / update_ms) × update_ms` | on: q = 0.995, 4 h, ≥ 2,000 samples, 1-min refresh |
| `time_of_day` | size ≥ the q-quantile of the same 30-min UTC bucket over the previous `days` calendar days (never today) | off: q = 0.995, 20 days, ≥ 5 days |

- **Nearest rank:** the value at rank ⌈q·N⌉ of the N sorted samples. It is always an observed size.
- **No self-influence:** an order's own size never enters its own threshold, because samples are added after evaluation.
- **Warm-up:** a criterion without enough history is **not ready** and cannot pass. Every event records each criterion's threshold, readiness and pass/fail.
- **Exactness of the time-of-day baseline:** each day keeps its count and top `keep_top` sizes, so the threshold is exact or reported as `INEXACT`, never approximated.
- **Event `BIG_TRADE`:** status DETECTED, the order's side, size, VWAP (`ref_price_ticks`), fill count, price path and **the full list of source trade ids** (up to `store_trade_ids_max`).

## 5. Absorption (`absorption.py`): a candidate, never "proven"
Evaluated at each finalised bar end E of `interval` (default 5 s), over the window `W = window_bars × interval` (default 12 → 60 s), in bins of `bin_ticks` (10). For aggressive BUYS absorbed by sellers (SELL mirrors):
1. `zone(b)` = Σ buy lots in bins `[b − tol, b + tol]` over W; L = the arg-max (lowest bin on ties).
2. **Effort:** `zone(L) ≥ abs_lots`, or ≥ the trailing nearest-rank quantile (default q = 0.99, 4 h) of the per-bar maximum zone.
3. **No result:** `highest traded bin in W − L ≤ max_progress_bins` (default 1).
4. **Not one trade:** ≥ `min_orders` (3) reconstructed taker orders of that side touched the zone within W, and the largest of them is ≤ `max_single_order_frac` (0.5) × zone volume. **A single large trade is never absorption.**

→ `ABSORPTION / CANDIDATE` at `E + shift`, with evidence `[E − W, E)`.

Then, on each later bar until `E + confirm_window_ms` (default 5 min):

| Condition | Status |
|---|---|
| low bin ≤ L − `confirm_bins` (3) | `CONFIRMED` |
| high bin > L + `max_progress_bins` + `break_bins` (2) | `FAILED` |
| both in the same bar (order inside the bar unknown) | `AMBIGUOUS`, not confirmed |
| neither by the deadline | `EXPIRED`, at the deadline |

Status events carry `parent_id` = the candidate. **They are never back-dated:** their time is the end of the bar that showed the outcome (+ shift), which is always after the evidence window.

## 6. Exhaustion (`exhaustion.py`)
On finalised bars of `interval` (default 1m), highs side (lows mirror with sell volume):
- A **push** is a bar whose high exceeds every high of the previous `lookback_bars` (30) bars.
- **Exhaustion:** the last `k_pushes` (3) pushes within the lookback have strictly declining aggressive buy volume, **and** the newest push's delta is below the previous push's delta (higher price, weaker delta).

→ `EXHAUSTION / CANDIDATE` at the newest push's bar end. Then, within `confirm_window_bars` (10):

| Condition | Status |
|---|---|
| pullback to ≤ extreme bin − `confirm_bins` (3) | `CONFIRMED` |
| new high above the extreme | `INVALIDATED` |
| both in the same bar | `AMBIGUOUS` |
| neither in time | `EXPIRED` |

## 7. Divergence (`divergence.py`)
On finalised bars of `interval` (default 5m):
- A **swing high** at bar i has a high strictly above the `swing_left` (3) bars before it and the `swing_right` (3) bars after it.
- It is **confirmed only when bar i + R closes**, so the confirmation time is that bar's end. Swings are never used before confirmation (no repainting).

**Bearish divergence** (side BUY): a newly confirmed swing high P2 above the previous confirmed swing high P1 (≤ `max_pivot_gap_bars` = 48 bars earlier) with a lower oscillator at P2. Oscillators:
- `cvd`: CVD at the pivot bar's end. Both pivots must lie in the same `cvd_reset` (`trading_day`) instance, otherwise it isn't comparable and is counted.
- `delta`: the pivot bar's delta.

**Bullish divergence** (side SELL) mirrors on swing lows.

Event types are `DIVERGENCE_CVD` and `DIVERGENCE_DELTA`. Status DETECTED at the confirmation time, with evidence `[P1 start, confirmation bar end)`. It becomes `INVALIDATED` if price later exceeds the P2 extreme by more than `inval_bins` (1) bins.

## 8. Catalogue, traceability, reproducibility
`data/derived/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/orderflow/`:
- `events/date=D/`: every event and status transition (columns below);
- `imbalances/date=D/` and `cvd/date=D/`;
- `_manifest.json`: row counts, logical digests per table and per date, settings hashes, source dataset version, detector statistics, and candidates still open at the end of the data.

**Event columns:**
- identity: `event_id` (deterministic), `event_type`, `status`, `parent_id`, `side`, `interval`, `bin_ticks`;
- timing: `evidence_start_utc`, `evidence_end_utc`, `event_time_utc`;
- values: `ref_price_ticks`, `value_lots`, `threshold_lots`, `unknown_lots`;
- traceability: `first_trade_id`, `last_trade_id`, `n_trades`, `trade_ids`;
- labels: `sessions` (sessions containing the evidence end, `+`-joined, or `none`), `weekend`, `dq_status` (the worst Phase 3 status overlapping the evidence), `coverage`;
- `details` (JSON with every input and threshold behind the decision).

**Invariants**, checked as events are recorded and again in `data/reports/orderflow_catalogue.{md,json}`; all must be 0:
- event time before evidence end;
- status events without a parent;
- initial events without a trade-id range;
- null unknown lots;
- big trades whose id count ≠ fill count;
- duplicate event ids.

**Reproducibility.** Logical digests are identical across runs, across chunk sizes, for interrupted-and-resumed runs, and with duplicate delivery (all tested).

**Independent validation** (`orderflow-validate`, `data/reports/orderflow_validation.{md,json}`). Each check is recomputed from the Phase 3 partitions with separate code:
- utc_day CVD at day end = Σ BUY − Σ SELL of the day's trades (and unknown = Σ UNKNOWN);
- the set of imbalances = a pandas recomputation;
- every BIG_TRADE's ids are consecutive, single-side and sum to its size;
- taker orders against aggTrades:
  - aggTrades split across two reconstructed orders (must be 0);
  - reconstructed orders that contain two same-price aggTrades, i.e. two taker orders merged (measured and reported).

## 9. Limitations
- **Trades only.** Absorption and exhaustion are inferred from executions and price. Without L2 history, resting liquidity can't be seen (Phase 8 records it live from now on).
- **Ambiguity inside a bar.** The order of events within one bar is unknown. A bar that both confirms and breaks a candidate is `AMBIGUOUS`. Smaller intervals reduce this.
- **Taker-order merging.** Same-millisecond orders on the same side merge (measured in §8). Taker orders that rest partially and later fill as maker are not linked.
- **Session dependence.** Session-reset CVD and session labels depend on the **proposed** session times (decision D4).
- **End of data.** Candidates open at the end are not resolved; they are counted in the manifest.
- **No profitability claim.** Outcome rates in the catalogue are descriptive. Whether any label has predictive value is a Phase 12/13 question, on out-of-sample data.
