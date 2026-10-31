# Auction metrics v1: reference levels, acceptance, rejection, failed auctions, auction state, activity, profile shape

Package: `goldflow/orderflow/` (Phase 9 modules below). CLI: `auction`, `auction-verify`, `auction-review`. Config: the `auction` block of `config/binance_xauusdt.json`.
Inputs:
- the Phase 6 `ProfileEngine`: final snapshots → reference levels; developing snapshots → current value and VWAP;
- the Phase 5 `TradeProcessor`: finalised bars with their 1-tick footprint, and finalised windows.

`AuctionEngine` is a `ProcessorSink` placed **after** its `ProfileEngine` in the sink list (enforced at start). It runs unchanged on replay and on the Phase 14 live bus.

| Module | Role |
|---|---|
| `auction.py` | Reference levels from profiles, the acceptance/rejection episode machine, activity classification, profile shape |
| `failed_auction.py` | Failed-auction evidence, the hold confirmation, post-event outcomes |
| `auction_engine.py` | `AuctionEngine(ProcessorSink)` and `AuctionStore` (Parquet dataset) |
| `auction_verify.py` | `auction-verify`: independent re-derivation from the trades partitions, catalogue with outcome statistics |
| `auction_review.py` | `auction-review`: reviewer CSV and per-event SVG charts drawn from raw trades |
| `auction_settings.py` | Validation of the `auction` block (called by Phase 1 `validate_config`); settings hash |

## 1. Conventions
- Prices are in ticks. A reference level is an **exact rational**: VWAP is `sum_qp / sum_q`, a POC bin centre can be a half tick. "Beyond" means strictly beyond (`price > L` above, `price < L` below), decided in integers.
- Bars: `auction.interval` (default 1m) from the Phase 5 processor. "A close beyond" is the bar close, and the volume and delta beyond a level come from the bar's exact 1-tick footprint.
- **No hindsight.** Every row has `available_at`; it is emitted only once the processor clock reaches it, and the engine is replay-tested with a clock check. Bars are processed strictly in time order, interleaved with the state cadence. A level is used for a bar only if it was available at the bar's start.

## 2. Reference levels (`levels`)
- **Source:** the **final** profile (bin `bin_ticks`, default 10) of the previous instance of each source window (`sources`, default every trading day and every session).
- **Active:** from the window's end (`available_at`) until the next instance of the same source series becomes available.
- **Never from a partly observed window.** A window that began before processing started (coverage `PARTIAL_START`), or a filtered run, never supplies levels (`partial_windows_not_used`).

| Type | Price |
|---|---|
| HIGH, LOW | traded high and low (1 tick) |
| POC | centre of the POC bin |
| VAH / VAL | top tick of the value area's highest bin / bottom tick of its lowest bin |
| VWAP | exact `sum_qp / sum_q` |
| VWAP_BAND (option) | VWAP ± kσ (float bands, `band_k`) |
| HVN / LVN (option) | node bin centres |
| USER | `levels.user`: `[{"name": ..., "price": "4000.00"}]` (USDT decimal string → exact ticks), always active |

The previous and current session high/low of the guide: the previous session's high and low are levels. The current session's developing extremes are not used as levels: price makes them, so they cannot be accepted or rejected.

**IB high/low** are added in Phase 10 as another level source.

## 3. Episodes, acceptance, rejection
Per level and side (ABOVE / BELOW), an **episode** starts with the first bar that trades beyond the level and ends with the first bar that **closes** back inside it.

| Event | Rule | Confirmed at |
|---|---|---|
| `ACCEPTANCE` | `accept_time / interval` consecutive closes beyond (default 15 × 1m), **or** a close beyond with volume traded beyond ≥ `accept_vol_frac` (0.10) × the source window's volume | end of that bar |
| `RETURN` | an accepted episode closes back inside (acceptance lost) | end of that bar |
| `REJECTION` | not accepted, probed ≥ `probe_bins` (3) bins beyond, and closed back inside within `reject_time` (30 m) of the episode start | end of the returning bar |

If the level stops being active while an episode is open, the episode is dropped without an event (`episodes_dropped_level_expired`).

## 4. Failed auction (high; the low is the mirror)
All of the following must hold:
1. The episode probed beyond the level by ≥ `probe_bins` bins.
2. Weak participation: volume beyond the level ≤ `fa_vol_frac` (0.25) × the volume of the episode's bars, **or** responsive delta: buy − sell beyond the level < 0 (> 0 for a low).
3. A close back inside within `fa_return_time` (30 m) of the probe start, without acceptance.
4. The return held for `fa_hold_bars` (5) consecutive closes inside, with no trade beyond the probe extreme. A close beyond the level or a trade beyond the extreme drops the candidate (`fa_dropped`).

- Confirmation is at the end of the last hold bar. A wick alone is never enough: rule 4 needs bars after the return, and rule 2 needs evidence of weak participation.
- Recorded:
  - `event_time` = probe start;
  - `confirmed_at`;
  - `invalidation_ticks` = the probe extreme;
  - the evidence (`vol_beyond`, `vol_total`, `delta_beyond`, which of rule 2's tests passed).

## 5. Outcomes (review data, not signals)
For ACCEPTANCE (expected move away from value), REJECTION and FAILED_AUCTION (expected move back), at each horizon in `outcome_minutes` (default 15, 30, 60), an `outcomes` row records:
- `move_ticks`: the close change in the expected direction;
- `mfe_ticks` / `mae_ticks`: the best and worst excursion in that direction;
- `invalidated`: whether price reached the invalidation level. It is `null` where none exists (acceptance).

- The reference price is the close of the confirming bar.
- A horizon only uses bars that ended by `confirmed_at + horizon`.
- The row is available at `confirmed_at + horizon`.

## 6. Auction state (`state`, per `series`, default trading day, every 15 m)
At each cadence point M, for the instance that is open at M (developing snapshot as of M) against the previous final instance:
- **rotations:** alternations between touching the previous VAH (high ≥ VAH) and the previous VAL (low ≤ VAL). If one bar touches both, it counts as a single touch on the side opposite the last one;
- **overlap** = |current VA ∩ previous VA| / previous VA width (exact);
- **POC shift** in bins (current − previous);
- **acceptance** above the previous VAH / below the previous VAL (§3, still standing at M).

| State | Rule (first match) |
|---|---|
| `IMBALANCE_UP` / `IMBALANCE_DOWN` | accepted outside the previous value on that side and POC shifted ≥ `poc_shift_bins` (5) that way |
| `BALANCE` | rotations ≥ `rot_n` (2) and overlap ≥ `ov_frac` (0.5) |
| `TRANSITION` | otherwise |
| `UNDETERMINED` | no previous full instance yet |

## 7. Initiative vs responsive activity (`activity`, every bar)
Location of the bar's close versus the previous value area of the first state series (above / inside / below). Inside value, the developing VWAP is used instead.

| Location | delta > 0 | delta < 0 |
|---|---|---|
| above value, or inside and above VWAP | INITIATIVE_BUY (away) | RESPONSIVE_SELL (back) |
| below value, or inside and below VWAP | RESPONSIVE_BUY (back) | INITIATIVE_SELL (away) |

`NEUTRAL` when delta = 0. `UNDETERMINED` when no previous value or VWAP is known.

## 8. Profile shape (`profile_shape`, final windows, default trading day and sessions)
On the dense binned profile (`bin_ticks`, empty bins included; mean = total / number of bins):

| Event | Rule |
|---|---|
| `EXCESS_HIGH` / `_LOW` | a tail of ≥ `excess_bins` (3) bins at the extreme, each ≤ `tail_frac` (0.25) × mean, traded within `excess_time` (5 m) between the tail's first and last trade |
| `POOR_HIGH` / `_LOW` | no excess, and the extreme bin ≥ `poor_frac` (0.9) × the mean of the 3 bins at the extreme (a flat, unfinished extreme) |
| `SINGLE_PRINT_PROXY` | ≥ `single_print_min_bins` (2) adjacent interior bins each ≤ `single_print_frac` (0.1) × mean. Labelled `LOW_VOLUME_PROXY (no TPO time grid)`: TPO single prints need a time-period grid, which is not built |

Shape events of partly observed windows are emitted with `coverage = PARTIAL_START` and are not verified.

## 9. Dataset, verification, review
```
data/derived/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/auction/date=YYYY-MM-DD/
    levels.parquet  events.parquet  outcomes.parquet  states.parquet  activity.parquet
data/derived/.../auction/_manifest.json        per-date digests, settings hashes, source version, observe_from
data/derived/.../auction/charts/*.svg          auction-review charts (raw-trade candles, level, probe, E and C markers)
data/reports/auction_verification.{json,md}    exact re-derivation + catalogue by session with outcome statistics
data/reports/auction_review.csv                every event with level, session, timing, evidence, outcomes and chart path
```
- **`auction-verify`** rebuilds the bars from trades and re-derives, with plain separate code:
  - every episode event (type, level, side, start, confirmation, volume, delta, probe extreme), including the failed-auction hold;
  - every profile-shape event of fully covered windows;
  - every outcome row.

  It also checks the timing invariants.
- **Determinism.** `state_digest` is identical uninterrupted, resumed and at any chunk size. A resume under changed `auction` settings is refused.

## 10. Limitations
- Trade data only: no TPO grid, so single prints are a volume proxy.
- Levels are limited to the previous instance of each source series.
- IB levels arrive in Phase 10.
- All thresholds are PROPOSED v1 defaults (decision D12).
- Outcome statistics on a few days are anecdotal. They are review data, not evidence of an edge (Phase 13).
