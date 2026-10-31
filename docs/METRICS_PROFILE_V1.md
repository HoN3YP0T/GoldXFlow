# Profile metrics v1: Volume Profile, POC, Value Area, HVN/LVN, VWAP and bands

Package: `goldflow/orderflow/` (shared with the Phase 7 analytics; Phase 6 modules are `volume_profile.py`, `vwap.py` and `profile_*.py`). CLI: `vprofile`, `vprofile-verify`, `vprofile-export`. Config: the `volume_profile` block of `config/binance_xauusdt.json`.
Input: the Phase 5 `TradeProcessor` (finalised 1 s footprint bars with exact 1-tick levels, and finalised windows). The engine is a `ProcessorSink`, so it runs unchanged on replay and on the Phase 14 live bus.
Instrument: Binance USDⓈ-M `XAUUSDT` only. Nothing here mixes venues or instruments.

| Module | Role |
|---|---|
| `volume_profile.py` | Pure functions: binning, POC, Value Area, integer smoothing, HVN/LVN with prominence and width |
| `vwap.py` | Exact VWAP sums (Python integers), VWAP, volume-weighted population sigma, bands |
| `profile_engine.py` | `ProfileEngine(ProcessorSink)`: per-window exact state, developing and final snapshots, anchors, checkpoint state |
| `profile_store.py` | `ProfileStore`: Parquet dataset (snapshots, nodes, final-window levels) and manifest, staged at replay checkpoints |
| `profile_rebuild.py` | From-scratch rebuild from the Phase 3 trades partitions (exact) and an independent float64 VWAP |
| `profile_verify.py` | `vprofile-verify`: parity, VWAP tolerance and Phase 5 totals; writes `data/reports/profile_verification.{json,md}` |
| `profile_export.py` | `vprofile-export`: chart-ready JSON for one window |
| `profile_settings.py` | Validation of the `volume_profile` block (called by Phase 1 `validate_config` at `Config.load`); settings hash |

## 1. Units and conventions
| Quantity | Unit |
|---|---|
| Volume | lot = 0.001 oz (base quantity). Notional profiles are not produced in v1. |
| Price | tick = 0.01 USDT |
| VWAP sums | `sum_q` lots, `sum_qp` tick·lots, `sum_qp2` tick²·lots (exact integers; `sum_qp2` is stored as decimal128(38,0)) |
| Time | UTC epoch ms; every window is half-open `[start, end)` |

- **Bins.** `bin = price_ticks // bin_ticks`. Bin `b` holds ticks `[b·bin_ticks, (b+1)·bin_ticks − 1]` and is reported at its **lowest** price, `b·bin_ticks` ticks. `bin_ticks` is stored with every result, because the bin size changes the POC, the value area and the nodes. Default `bin_ticks = [10]` ($0.10); several sizes can be listed and each gets its own rows.
- **Dense profile.** The profile covers every bin from the lowest traded bin to the highest one. Empty bins inside that range are rows of zero volume (the CBOT convention). They take part in value-area pairs and smoothing.
- **Exactness.** Every decision (which bin is the POC, which side the value area grows, whether a node passes a threshold) is made in integer or rational arithmetic. Configured fractions (`va_pct`, `prom_frac`, …) are decimal strings parsed as exact rationals; floats are refused. Floats appear only in reported magnitudes (VWAP and sigma values, smoothed volume, prominence, node widths), never in a decision. So results are bit-identical across machines and runs.

## 2. Windows
| Kind | Definition (from the Phase 5 calendar) |
|---|---|
| `utc_day` | 00:00 → 24:00 UTC |
| `trading_day` | 18:00 America/New_York on D−1 → 18:00 on D |
| `trading_week` | start of Monday's trading day → start of the next Monday's |
| `session:<name>` | each configured session (Asia/Tokyo, Europe/London, America/New_York local times; DST through zoneinfo) |
| `custom:<name>` | `TradeProcessor(custom_windows=…)` / `add_custom_window`: **fixed-range** and **custom-range** profiles |
| `anchor:<name>` | open-ended from an anchor time: `volume_profile.anchors` or `ProfileEngine.add_anchor(name, start_ms)` |

Session times are still decision **D4 (PROPOSED)**; a change alters the processing settings hash and forces a fresh run. A window resets exactly at its start: it contains only trades with `start ≤ time < end`. Anchors must align to the base interval (1 s). A runtime anchor earlier than the engine's applied point is **refused**, not started late, because the engine no longer holds the rows before that point. Configure it and rerun instead.

## 3. Volume Profile and POC
- **`V(b)`** = Σ lots traded in bin `b` within the window (all aggressor sides, including UNKNOWN; buy and sell are also reported per bin). The engine builds it from exact 1-tick per-level lots.
- **POC** = the bin with maximum `V(b)`. **Tie-break:**
  1. Take the tied bin whose centre (`low + (bin_ticks − 1)/2` ticks) is closest to the window's volume-weighted mean price (`sum_qp / sum_q`, exact).
  2. If still tied, take the lower price.

  The distance is compared as `|2·sum_qp − (2·low + bin_ticks − 1)·sum_q|` in integers.

## 4. Value Area (target `va_pct`, default 0.70; `method = pair`)
1. Start with the POC bin. `acc = V(POC)`.
2. While `acc < va_pct × total` (compared exactly as `acc·den < num·total`):
   - `up` = sum of the next **two** bins above the area (fewer if fewer remain); `down` = the next two below. A side with no bins left cannot be chosen.
   - If `up > down`, add the upper pair. If `down > up`, add the lower pair.
   - If they are **equal**, add the pair on the side where the window's volume-weighted mean price lies relative to the POC centre. If the mean is exactly at the POC centre, add **both** pairs in this step.
3. Stop as soon as `acc ≥ va_pct × total`. Whole pairs are added, so the area may exceed the target. `va_lots` reports what was actually included.
4. **VAL / VAH** are the outermost included bins (they can be zero-volume bins inside a pair). Also reported: `va_bins`, `va_steps` and `va_target`.

`method = single` uses one bin per step with the same rules. `pair` is the default, the CBOT-style convention. This tie rule is the v1 reading of the guide's "side closer to the POC distance-weighted mean". It is written down and tested (`tests/test_profile.py`), and any change needs a new version.

Hand-computed reference (tested): `V = [1, 2, 5, 9, 4, 4, 1, 1, 3]`, total 30, target 21. Start at the POC (9). Step 1: up pair 8 beats down pair 7, so add the upper pair (acc 17). Step 2: down pair 7 beats up pair 2, so add the lower pair (acc 24 ≥ 21). The value area is bins 1–5.

## 5. HVN / LVN
1. **Smoothing.** Exact integer convolution of `V` with an integer Gaussian kernel: `w(x) = round(2^bits · exp(−x²/2σ²))` for `|x| ≤ truncate·σ`, with σ = `smooth_bins` (default 5 bins), `truncate_sigmas = 4`, `kernel_scale_bits = 20`. Outside the traded range the profile is zero. The kernel is stored in the dataset manifest. Smoothed values are in lots × Σw, and are reported divided by Σw.
2. **Extrema.**
   - Strict local maxima (HVN candidates) and minima (LVN candidates) of the smoothed curve.
   - A plateau counts as one extremum, at its middle (the lower middle for even widths).
   - Plateaus touching the profile edge are not extrema.
3. **Prominence.** This is the topographic definition (as in `scipy.signal.peak_prominences`).
   - For a peak, find the lowest point between it and the nearest **strictly higher** peak on each side (or the profile edge). Prominence = height − max(left minimum, right minimum).
   - For a valley, apply the same definition to the negated curve.
   - Keep only nodes with `prominence ≥ prom_frac × max(smoothed)` (default 0.10).
4. **Volume filter** against the mean bin volume `total / n_bins`.
   - HVN: `smoothed ≥ hvn_min_vol_frac × mean` (default 1.0).
   - LVN: `smoothed ≤ lvn_max_vol_frac × mean` (default 0.5).
5. **Separation.** Within each type, take nodes by descending prominence (ties: lower price). Keep a node only if it is at least `min_separation_bins` (default 10) from every node already kept.
6. **Reported per node:**
   - type, bin and `price_ticks`;
   - raw `lots` at the node bin;
   - `smoothed_lots` and `prominence_lots`;
   - `left_ticks`/`right_ticks`: where the curve crosses `height − width_rel_height × prominence` (default ½), linearly interpolated and bounded by the prominence bases;
   - `width_ticks`.

   Nodes are found by these rules only; nothing is picked by eye or by hand.

Hand-computed reference (tested): for `s = [0, 5, 1, 3, 2, 8, 0]` the peak prominences are 4, 1 and 8 at positions 1, 3 and 5, and the valley prominences are 4 and 1 at positions 2 and 4.

## 6. VWAP and bands
- `VWAP = Σ qᵢpᵢ / Σ qᵢ` over trades in the window, using actual trade price and quantity. At 1-tick resolution all trades at a level share its price, so the sums are built exactly from per-level lots.
- **Sigma** (volume-weighted *population* variance): `σ² = Σ qᵢ(pᵢ − VWAP)² / Σ qᵢ = (Q·P2 − P1²) / Q²`, with `Q, P1, P2` exact Python integers. The floats reported are correctly rounded from the exact rationals.
- **Bands:** `VWAP ± k·σ` for `k ∈ band_k` (default 1, 2, 3).
- Sample variance and bar-based deviations are **not** implemented; any other `variance` value is refused at startup.
- **Variants.** Session, daily (`utc_day`, `trading_day`), weekly, custom range and anchored, which is every window kind. Each one resets exactly at its window start.

## 7. Developing state, snapshots and the no-look-ahead rule
- The engine consumes only **finalised** base-interval (1 s) bars. A bar `[s, s + 1 s)` is emitted by the processor only after its clock passed `s + 1 s + shift`.
- A **developing snapshot as of M** is taken at every cadence boundary M (`snapshot_every`, default 1 minute) inside an open window, once the base interval is final up to M.
  - It contains **exactly** the trades with exchange time `< M`; trades at M or later are never included (tested).
  - It is labelled `available_at = M + trade shift` and `is_final = false`.
- A **final snapshot** is taken at the window end, after the processor finalised the window (`is_final = true`, so "plus at session end").
  - The engine's incremental 1-tick state is compared with the processor's own window levels (`final_parity_mismatch`, expected 0).
  - Anchors are open-ended and never final.
- **Labels on every snapshot:**
  - `coverage`: `FULL`, `PARTIAL_START` (processing began inside the window, after a replay start or a seek) or `FILTERED` (replay date/session filters).
  - `covered_from`: the first trade time the snapshot can contain.
  - `dq_status`: the worst Phase 3 status overlapping `[window_start, as_of)`. It is meant only to **suppress** use downstream.
- **Live accessors:** `ProfileEngine.current(kind, name)` returns the latest developing snapshot. Listeners (`ProfileEngine(listeners=[f])`) receive every batch of snapshot and node records as they are produced.
- **Snapshot/restore.**
  - The engine state (per-window exact level arrays, VWAP sums, pending rows, DQ intervals, anchors) is part of the replay checkpoint, and is refused if the `volume_profile` settings hash changed.
  - Restore followed by continue equals an uninterrupted run (state digest and dataset digests; tested at two cut points).

## 8. Dataset
```
data/derived/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/profile/date=YYYY-MM-DD/snapshots.parquet
                                                                                       .../nodes.parquet
                                                                                       .../levels.parquet   (final snapshots only)
data/derived/.../profile/_manifest.json      per-date row counts, logical digests, settings hashes, kernel, source dataset version
data/derived/.../profile/charts/*.json       vprofile-export output
```
- **Key columns** (all three tables): `window_key`, `kind`, `name`, `label`, `window_start`, `window_end` (null for anchors), `covered_from`, `as_of`, `available_at`, `is_final`, `bin_ticks`.
- **Partitioning:** a row is stored under the UTC date of `as_of − 1 ms`, so the 00:00 snapshot that closes a day lives with that day.

| Table | Extra columns |
|---|---|
| `snapshots` | `coverage`, `dq_status`, `total_lots`, `buy_lots`, `sell_lots`, `low_ticks`/`high_ticks` (1-tick traded range), `n_bins`, `low_bin_ticks`, `high_bin_ticks`, `poc_ticks`, `poc_lots`, `val_ticks`, `vah_ticks`, `va_lots`, `va_bins`, `va_steps`, `va_target`, `hvn_count`, `lvn_count`, `sum_q`, `sum_qp`, `sum_qp2`, `vwap_ticks`, `sigma_ticks`, `band_up_k_ticks`/`band_dn_k_ticks` |
| `nodes` | `node_type` (HVN/LVN), `bin`, `price_ticks`, `lots`, `smoothed_lots`, `prominence_lots`, `left_ticks`, `right_ticks`, `width_ticks` |
| `levels` | `bin`, `price_ticks`, `lots`, `buy_lots`, `sell_lots` (non-empty bins of each final profile) |

Writes are atomic. Rows are staged at every checkpoint, so a crash loses nothing. A date is written once every snapshot belonging to it exists.

## 9. Verification (`vprofile-verify`)
1. **Parity (exact).** Every final snapshot, plus developing snapshots whose `as_of` is a multiple of `--sample-hours` (default 6), is rebuilt **from scratch** from the trades partitions over `[covered_from, as_of)`. The rebuild uses no replay and no processor. These must all be identical:
   - totals and the traded range;
   - `sum_q`, `sum_qp`, `sum_qp2`;
   - POC, VAL, VAH, `va_lots`, `va_bins` and `va_steps`;
   - the full node list.
2. **VWAP, independent float.** VWAP from the decimal `price`/`quantity` columns in float64 (two-pass) must agree within **1e-9 relative**. Sigma's relative error is reported too.
3. **Phase 5 totals.** The final `utc_day` profile must equal the Phase 5 footprint manifest (volume, buy and sell lots) and the trades-partition total.

## 10. Parameters (v1 defaults: **PROPOSED**, to be reviewed with Phase 9/10 research)
| Key | Default | Meaning |
|---|---|---|
| `bin_ticks` | `[10]` | profile bin sizes in ticks |
| `value_area.pct` / `method` | `"0.70"` / `pair` | value-area target and expansion rule |
| `nodes.smooth_bins` / `truncate_sigmas` / `kernel_scale_bits` | 5 / 4 / 20 | integer Gaussian kernel |
| `nodes.prom_frac` | `"0.10"` | minimum prominence as a fraction of max(smoothed) |
| `nodes.hvn_min_vol_frac` / `lvn_max_vol_frac` | `"1.0"` / `"0.5"` | volume filters against the mean bin volume |
| `nodes.min_separation_bins` / `width_rel_height` | 10 / `"0.5"` | node spacing; width height |
| `vwap.band_k` / `variance` | `[1, 2, 3]` / `population` | bands |
| `snapshot_every` | `1m` | developing snapshot cadence (a multiple of the base interval) |
| `windows` | all kinds | which window kinds get profiles |
| `anchors` | `[]` | `[{name, start}]` anchored windows |

The `volume_profile` block is validated at `Config.load` (exit 2 on error). It requires `processing.footprint_bin_ticks = 1`.

## 11. Limitations
- **Venue.** This is the profile of Binance's XAUUSDT perpetual. It is not COMEX and not the broker's OTC XAUUSD feed. Weekend perp flow sits inside `utc_day`/`trading_week` windows like any other flow. `weekend` labels are on the Phase 5 bars, so strategies should filter there.
- **Developing levels.** Developing profiles are stored as metrics and nodes per snapshot. The binned profile itself is stored only for final windows. `vprofile-export --as-of` rebuilds a developing profile from trades, and it equals the snapshot exactly.
- **Live late trades.** A trade that reaches the processor after its bar was finalised is not in the bars, so it is not in the profile either. It is counted by the processor (`late_after_bar_final`) and shows up as `final_parity_mismatch` against the processor's window. This is 0 in replay by construction; it matters only for Phase 14.
- **Node parameters** are v1 defaults with no claim of trading relevance. Their usefulness is a Phase 9/10 research question, judged out of sample.
