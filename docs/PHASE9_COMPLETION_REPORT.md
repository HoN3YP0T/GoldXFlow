# Phase 9 completion report: Auction Market Theory and failed auctions

Date: 2026-10-31. Built in the cloud workspace on top of Phases 5–7. Specification: `docs/METRICS_AUCTION_V1.md`.

## 1. Scope completed / not completed
**Completed**
- **Reference levels.** Previous session and trading-day high, low, POC, VAH, VAL and VWAP (bands, HVN and LVN optional) and user levels. Prices are exact rationals. A level is never taken from a partly observed window.
- **Episodes.** Acceptance (by time or by volume, with closes beyond), return, and rejection (probe, then a return within the time limit, without acceptance).
- **Failed auctions.** All four rules: probe; weak participation or responsive delta; quick return; held return. Each records the event time, confirmation time, invalidation level (the probe extreme) and evidence. Outcome rows at 15, 30 and 60 minutes are kept for review.
- **Auction state.** Balance, imbalance up/down, transition, from rotations, value-area overlap, POC shift and acceptance outside the previous value.
- **Activity.** Initiative vs responsive, per bar, from delta sign relative to value and VWAP.
- **Profile shape.** Poor high/low, excess (thin, fast tail), and a single-print **proxy** explicitly labelled as such (there is no TPO grid).
- **Outputs and tooling.**
  - five-table Parquet dataset with manifest, staged at replay checkpoints;
  - checkpoint state guarded by a settings hash;
  - `auction`, `auction-verify` and `auction-review` CLI commands;
  - reviewer CSV, and SVG charts drawn from raw trades;
  - validation of the config block at startup.

**Not completed / deferred**
- **IB high/low as reference levels.** They belong to Phase 10, which adds them as another level source.
- **Six-month catalogue.** The data lives on the PC. The evidence below covers four real days, 23–26 Oct 2026.
- **TPO single prints.** They need a time-period grid that is not built; a labelled low-volume proxy is provided instead.

## 2. Files
**Created**
- `goldflow/orderflow/`: `auction_settings.py`, `auction.py`, `failed_auction.py`, `auction_engine.py`, `auction_verify.py`, `auction_review.py`.
- `tests/test_auction.py` (12 tests).
- `docs/METRICS_AUCTION_V1.md` and this report.

**Modified**
- `goldflow/orderflow/flow_store.py`: the store is generalised into `DerivedStore`, shared by Phases 7 and 9. Phase 7 behaviour is unchanged and its tests pass.
- `goldflow/settings.py`: the `auction` block is validated.
- `goldflow/cli.py`: three new commands.
- `config/binance_xauusdt.json`: new `auction` block.
- `README.md`, `docs/BUILD_GUIDE.md`.

**Deleted:** none.

## 3. Architecture
```
TradeProcessor ─ sinks: [ProfileEngine, AuctionEngine]
   ProfileEngine ── final snapshots ──► levels registry (active from window end until the next instance)
                 └─ developing snapshots ─► current VA / POC / VWAP per cadence point
   on_bars(1m) + 1-tick footprint ─► pending
   on_window ─► profile shape (excess / poor / single-print proxy)
   on_advance: for bars in time order (state cadence interleaved):
        outcomes → failed-auction holds → rotations → activity → episodes per active level and side
        → ACCEPTANCE / RETURN / REJECTION / failed-auction candidates
   └► AuctionStore (levels, events, outcomes, states, activity) / listeners
```

## 4. Commands
```bash
python -m goldflow.cli auction --name a1                 # replay → TradeProcessor → ProfileEngine → AuctionEngine → Parquet → verification
python -m goldflow.cli auction --name a1 --resume
python -m goldflow.cli auction-verify [--dates D ...]
python -m goldflow.cli auction-review --limit 120        # CSV + SVG charts
bash scripts/check.sh
```

## 5. Tests
- `scripts/check.sh`: **all checks passed**, with **167 tests** (155 earlier plus 12 new) and mypy --strict on 57 files.
- The new tests cover:
  - exact level prices;
  - acceptance by time and by volume, return, rejection, a probe that is too small, a return that is too slow, and the below-side mirror;
  - failed-auction evidence: weak volume, responsive delta, slow return, and the hold confirming or dropping;
  - outcome horizons that never see later bars;
  - the activity table;
  - excess, a slow tail that is not excess, poor extremes and the single-print proxy;
  - invalid settings refused at startup;
  - on synthetic replay:
    - a clock check that no row arrives before its `available_at`;
    - levels used only after they became available, and partial windows never used;
    - the Phase 5 and Phase 6 states unchanged by the auction listener;
    - verification passes, and a deleted event is detected;
    - stop/resume at two points gives an identical state and identical dataset digests;
    - chunk size 61 gives the same results;
    - a settings change refuses resume;
    - wrong sink order is refused;
    - the review CSV and SVG are produced.

## 6. Real-data evidence (cloud, 4 days, 2026-10-23 → 26)
| Check | Result |
|---|---|
| Replay / Phase 5 / Phase 6 digests | `8f528ccd…` / `4e235890…` / `b318578b…`: unchanged with the auction engine attached |
| Auction state digest | `6a515d75…`; identical after stopping at 2026-10-24 13:37 or at 2026-10-25 01:13 (DST night) and resuming |
| `auction-verify` | **4/4 dates pass**. Re-derived exactly from trades: 776 episode events (acceptance, return, rejection, failed auction), 56 shape events (full windows), 1,856 outcome rows. 0 timing violations |
| Throughput | 247 k events/s (13.5 s for 3.34 M events), 628 MB |

**Levels and episodes**
- **Levels:** 48 reference levels. Three partly observed windows were not used: the first trading day and Friday's Asia session, which began before 00:00 on the first day.
- **Episode events:** 180 acceptances, 136 returns, 384 rejections and **76 failed auctions**.
  - The 76 failed auctions came from 224 candidates; 146 were dropped because their return did not hold.

**Other outputs**
- **States** (15-minute cadence, trading day):
  - 182 undetermined, mostly the first day, with no previous full trading day;
  - 107 transition, 82 imbalance up, 9 balance.
- **Activity:** 1,116 initiative buy, 1,011 responsive sell, 457 initiative sell, 414 responsive buy. 2,761 bars are undetermined because there was no previous value yet.
- **Shape:** 4 excess highs, 8 excess lows, 4 poor highs, 1 poor low, 56 single-print proxies.

**Outcomes** (all sessions, expected direction, USD per oz)
| Event | Horizon | n | Mean move | Share > 0 | Mean MFE | Mean MAE | Invalidated |
|---|---|---|---|---|---|---|---|
| Failed auction | 15 m | 76 | −1.05 | 0.45 | 3.30 | −4.35 | 42 % |
| Failed auction | 60 m | 73 | +0.36 | 0.49 | 7.05 | −7.61 | 60 % |
| Rejection | 15 m | 372 | +0.01 | 0.47 | 3.70 | −3.61 | 56 % |
| Acceptance | 15 m | 178 | −0.25 | 0.47 | 2.75 | −2.67 | n/a |

Four days show no edge in any direction. This is review data; any claim needs Phase 13 out-of-sample testing on the full history. Per-session statistics are in `data/reports/auction_verification.md`.

## 7. Limitations and open decisions
- **D12 (new): review the auction thresholds.** These are probe bins, acceptance time and volume share, failed-auction volume share and hold, state rules, and shape fractions.
- IB levels wait for Phase 10.
- There is no TPO grid.
- Levels come from the previous instance of each source window only.
- The session times are still D4.

## 8. Security and operations
- Local Parquet only; no credentials.
- Charts are SVG text files under the derived data folder (git-ignored).
- The verifier rebuilds every level's episodes in plain Python: about 1 min for 4 days. Expect roughly 45 min for six months; it can run per date range with `--dates`.

## 9. Acceptance criteria
| Criterion | Status | Evidence |
|---|---|---|
| Rules documented and reproducible | **Passed** | `docs/METRICS_AUCTION_V1.md`; identical state and dataset digests across uninterrupted, resumed and re-chunked runs; exact independent re-derivation |
| No event emitted before its confirmation time (replay with a clock check) | **Passed** | Listener clock check in tests (0 violations); `available_at = confirmed_at + shift` verified on every real row |
| A sample of events reviewable against charts | **Passed** | `auction-review`: 82 SVG charts from raw trades (level, probe extreme, E/C markers) and a CSV of all 849 events with chart links |

## 10. Recommended next phase
Phase 10: sessions, Initial Balance and IVB. IVB stays disabled until you supply its definition (D1); everything else in Phase 10 can be built.
