# Phase 7 completion report: delta, CVD, imbalance, Big Trades, absorption, exhaustion, divergence

Date: 2026-10-31. Built in the cloud workspace on top of Phases 5 and 6. Specification: `docs/METRICS_ORDERFLOW_V1.md`.

## 1. Scope completed / not completed
**Completed** (every item of the build guide's Phase 7 definitions):
- **Delta** per bar, level and window (Phase 5 already carried it per bar, level and window). New: intra-bar running-delta extremes (`delta_high` / `delta_low`) and `unknown_share` on every flow bar.
- **CVD** with configurable resets (UTC day, trading day, sessions, trading week, custom ranges, anchors). Rows are clipped to their instance, flagged when a bar straddles it, and labelled `PARTIAL_START` when the instance began before processing.
- **Footprint imbalance:** diagonal by default, horizontal as an option, an exact rational ratio, `min_lots`, `zero_policy`, and stacked imbalance.
- **Big Trades:**
  - taker-order reconstruction from fills (same side, `cluster_ms`, direction and step rule);
  - absolute and notional thresholds;
  - rolling quantile and time-of-day baseline on an exact histogram, with `any`/`all` combination;
  - every event stored with all its source fill ids.
- **Absorption** (candidate → confirmed/invalidated/expired). It is never labelled on a single large trade.
- **Exhaustion:** new extreme on declining aggressive volume, plus a CVD divergence at the extreme, confirmed by a pullback.
- **Divergence:** swing pivots confirmed only after their right-hand bars (no repainting), against CVD or delta, with invalidation.
- **Infrastructure:**
  - six-table Parquet event catalogue with manifest, staged at replay checkpoints;
  - checkpoint state, guarded by a settings hash;
  - `orderflow` and `orderflow-verify` CLI commands;
  - independent verifier, with an event catalogue by session;
  - validation of the config block at startup.

**Not completed / deferred**
- **Six-month evidence.** The market data lives on the trading PC. The evidence below covers four real days (Fri 23 → Mon 26 Oct 2026, including the weekend and the UK DST change), downloaded and processed in the cloud. On the PC, run `orderflow --name o1` and then `orderflow-verify` over the full range (§4).
- **Depth-confirmed absorption.** Historical L2 does not exist (Phase 8 is live only). Phase 7 absorption is trade-based; a depth check can be layered on once recorded depth exists, gated by `depth_capability`.
- **Parameter review (decision needed).** Every detector parameter is a PROPOSED v1 default (§7).

## 2. Files
**Created**
- `goldflow/orderflow/`:
  - `flow_settings.py`, `cvd.py`, `delta.py`, `imbalance.py`, `big_trades.py`;
  - `events.py`, `absorption.py`, `exhaustion.py`, `divergence.py`;
  - `flow_engine.py`, `flow_store.py`, `flow_verify.py`.
- `tests/test_orderflow.py` (19 tests).
- `docs/METRICS_ORDERFLOW_V1.md` and this report.

**Modified**
- `goldflow/processing/trade_processor.py`: new `ProcessorSink.on_trades` hook, called after dedup. The processor state is unchanged; this is tested.
- `goldflow/settings.py`: the `orderflow` block is validated at `Config.load`.
- `goldflow/cli.py`: `orderflow` and `orderflow-verify`.
- `config/binance_xauusdt.json`: new `orderflow` block.
- `README.md` and `docs/BUILD_GUIDE.md`: status and commands. The guide's stale Phase 6 header and repository layout are also fixed.

**Deleted:** none.

## 3. Architecture and data flow
```
ReplayEngine (Phase 4) ── EventChunk ──► TradeProcessor (Phase 5)
                                            │ on_trades (accepted fills, delivery order) ─► TakerOrderBuilder ─► release queue
                                            │ on_bars(1s)  ─► delta paths, CVD instances, absorption footprints, price path
                                            │ on_bars(1m/5m) ─► flow bars + CVD rows + imbalances ─► pending
                                            └ on_advance ─► detectors (time order) ─► trackers on 1 s path ─► release orders
                                                             └► OrderflowStore (Parquet, staged at checkpoints) / listeners
```
No-look-ahead rules:
- Bars arrive only after Phase 5 finalised them.
- Taker orders are evaluated only once no later fill can extend them.
- Quantiles use only orders and bars that ended before the current minute or bar.
- Events are tracked strictly after their evidence window.
- Every row is emitted at or after its `available_at`.

## 4. Commands
```bash
pip install -e .[dev]
python -m goldflow.cli orderflow --name o1                 # replay → TradeProcessor → OrderflowEngine → Parquet → verification
python -m goldflow.cli orderflow --name o1 --resume        # after --until / --budget / Ctrl-C
python -m goldflow.cli orderflow-verify [--dates D ...]    # independent recomputation; writes data/reports/orderflow_verification.{json,md}
bash scripts/check.sh                                      # ruff, format, mypy --strict (goldflow.orderflow included), pytest
```

## 5. Tests
- `scripts/check.sh`: **all checks passed**. ruff passes; ruff format passes; mypy --strict reports no issues in 51 files; **155 tests pass**, 0 fail. That is the 136 earlier tests plus 19 new ones.
- The new tests cover:
  - hand-computed delta paths;
  - diagonal, horizontal and zero-policy imbalances, and stacked runs;
  - taker-order rules and chunk independence;
  - exact histogram edges and nearest-rank quantiles;
  - rolling and time-of-day quantiles, which only use the past (asserted);
  - the tracker: confirm, invalidate, ambiguous same-bar, expire, no back-dating;
  - the absorption rules, including that a single large order is never absorption and that progress or unknown share suppresses a bar;
  - exhaustion and divergence on hand-built bars, including confirmation only after the right-hand bars and gaps breaking swings;
  - invalid settings refused at startup;
  - on synthetic replay:
    - no look-ahead and side uncertainty;
    - the processor state digest is unchanged by the sink;
    - London CVD resets across DST;
    - verification passes, and tampering with big trades, CVD, bars or imbalances is detected;
    - stop/resume at two cut points gives an identical state and identical dataset digests;
    - chunk size 61 gives the same results;
    - live-style chunks give identical rows;
    - seek discards open state.
- The verifier also caught one real discrepancy during development, on synthetic data with unknown sides: the imbalance side-quality rule was missing from the verifier. It now applies the documented rule.

## 6. Real-data evidence (cloud, 4 days, 2026-10-23 → 2026-10-26, 3,197,220 trades)
Data were downloaded from data.binance.vision. All 20 files have the same SHA-256 as the committed Phase 2 manifest. Every per-day coverage row and every Phase 5 reconciliation row is identical to the committed six-month reports.

| Check | Result |
|---|---|
| Replay sequence digest | `8f528ccd…` (unchanged by Phase 7) |
| Phase 5 processor state digest | `4e235890…` with and without the orderflow sink |
| Orderflow state digest | `e75e52d2…`; **identical** after stopping at 2026-10-25 01:13:07 (DST night) and resuming |
| `orderflow-verify` | **4/4 dates pass**. Exact checks: 6,912 flow bars, 15,504 CVD rows, 19,612 imbalances, 888 stacked runs, 4,708 big trades (every fill id), 334 event rows; evidence of 135 candidates recomputed; 167 resolutions re-derived from the price path; 0 back-dating violations |
| CVD rows skipped | 2,160 `PARTIAL_START`: the trading day and the Asia session that were already running at 00:00 on the first day. Expected by definition |
| Throughput | 296 k events/s (3.34 M events in 11.3 s); peak RSS 668 MB |

**Outputs (4 days)**
- **Taker orders:** 903,624. Of these, **4,708 are Big Trades (0.52 %)**.
  - The rolling 99.5 % threshold adapts to activity: 35–50 oz on weekdays, 17–39 oz around the weekend.
  - Big trade median size is 36–54 oz, the largest 708 oz; the median order has 30–40 fills.
- **Imbalances:** 19,612 (9,455 on Friday, about 1,100–1,400 on weekend days); 888 stacked runs.

**Events**
| Event | First rows | Confirmed | Invalidated | Expired |
|---|---|---|---|---|
| Absorption | 50 candidates | 16 | 28 | 6 |
| Exhaustion | 85 candidates | 52 | 31 | 2 |
| Divergence | 32 confirmed swings | – | 20 | 12 |

**Catalogue by session (first rows)**
| Type | Asia | London | New York | Outside sessions |
|---|---|---|---|---|
| Absorption | 13 | 8 | 3 | 26 |
| Exhaustion | 29 | 18 | 14 | 24 |
| Divergence | 7 | 9 | 5 | 11 |
| Big Trades | 1,398 | 1,735 | 598 | 977 |

"Outside sessions" includes the weekend and the gaps between the configured sessions.

**Absorption sensitivity (4 days, frequency only)**
| `zone_quantile` / `max_progress_bins` | 0.99 / 2 | 0.99 / 5 | **0.95 / 2 (default)** | 0.95 / 5 |
|---|---|---|---|---|
| Candidates | 2 | 8 | 50 | 102 |

The guide's original 0.99 gave 2 candidates in 4 days, so the default was lowered to 0.95, about 12 a day. This was chosen **by frequency only**. Confirmation rates are outcome statistics: they were not used to choose parameters, and must be judged out of sample in Phase 13.

## 7. Limitations and open decisions
- **D-P7 (new): review the v1 detector parameters** (`config/binance_xauusdt.json` → `orderflow`). Changing them changes the settings hash; rerun with a fresh run name.
- Delta extremes, CVD high/low and event tracking have 1 s resolution.
- Two same-side takers in the same millisecond with monotone fills are merged into one taker order.
- Absorption is trade-based only (no historical L2).
- The session times are still D4 (PROPOSED), which affects the session CVD resets and the catalogue.
- Events still open at the end of a run have only their first row.

## 8. Security and operations
- No credentials and no network access are needed: the engine reads local Parquet only.
- The derived data is git-ignored.
- Each checkpoint adds the engine state, a few MB: about 24 h of minute histograms plus open state.
- A full six-month verification should take about 30 minutes, based on 40 s for 4 days.

## 9. Acceptance criteria
| Criterion | Status | Evidence |
|---|---|---|
| Side uncertainty visible in every output | **Passed** | `unknown_lots` / `unknown_share` on bars, imbalances and events; `side = UNKNOWN` big trades; side-quality gate (tested with 1 % unknown synthetic trades) |
| No volume silently assigned to bid or ask | **Passed** | `buy + sell + unknown = volume` on every bar (tested); detectors use exchange-reported sides only |
| Every event traces to its input trade ids | **Passed** | Big trades store every fill id. Bars, imbalances and events carry id ranges plus a documented selection rule; the verifier recomputes the evidence from those trades |
| Events reproducible (same digest across runs) | **Passed** | Same state digest uninterrupted, resumed (real DST night and synthetic cuts) and at chunk size 61; identical manifest digests |
| Confirmation timestamps ≥ evidence window end | **Passed** | Enforced by construction, asserted in tests, 0 violations on real data |
| Six-month event catalogue with counts by session | **Blocked here (data on the PC)** | Built and verified on 4 days; run the two commands in §4 on the PC |

## 10. Recommended next phase
Phase 9: Auction Market Theory and failed auctions. It builds on the Phase 6 profiles and the Phase 7 events.
