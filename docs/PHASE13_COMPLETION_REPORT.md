# Phase 13 completion report: reports, validation, walk-forward and strategy research

Date: 2026-10-31. Specification: `docs/RESEARCH_V1.md`.

## 1. Scope completed / not completed
**Completed**
- **Performance report** from a reconciled ledger. It includes every metric in the guide (net/gross P&L, win rate, PF, expectancy in money and R, average win/loss, R distribution, maximum drawdown and recovery, consecutive losses, duration, frequency), splits by session, side, strategy, auction state and volatility, and **cost impact**. Wilson and bootstrap intervals, the ≥ 100-trade rule, and the trade-sequence Monte Carlo are all included.
- **Chronological splits** fixed in config, and a **test-set lock** (opened once per candidate; reuse refused; overlapping ranges refused).
- **Walk-forward:** rolling 8w/2w/2w windows over development + validation; per-window optimisation, an out-of-sample run, stitched equity and the positive-window share.
- **Robustness:** ±20 % parameter stability, cost stress (spread ×1.5 and slippage ×2 as real reruns), a two-parameter sensitivity heatmap with a plateau score, and Monte Carlo.
- **Research ledger:** hash-chained; hypotheses must be registered before results; rejected variants and decisions are recorded.
- **Approval criteria** evaluated per study. `max_oos_drawdown` must be set by you (D8) before anything can be approved.
- **Commands:** `study` (plan → report with in-sample, validation and out-of-sample kept separate), `perf-report`, `ledger-verify`.
- **Phase 12 additions:**
  - `backtest.spread_mult` for cost stress;
  - `accept_from_ms` for window warm-up without trading;
  - traded signals saved with each run, for regime splits.

**Not completed**
- **Real studies:** they need real backtests, which need D2.
- **Score calibration** (the reliability curve from Phase 11's note): it needs enough out-of-sample signals, which again means D2. The report infrastructure is ready.

## 2. Files
**Created**
- `goldflow/research/`: `__init__.py`, `settings.py`, `metrics.py`, `ledger.py`, `runner.py`, `walkforward.py`, `study.py`;
- `tests/test_research.py` (7 tests);
- `docs/RESEARCH_V1.md` and this report.

**Modified**
- `goldflow/backtesting/`: `settings.py` (`spread_mult`), `engine.py` (`accept_from_ms`, signals output), `simulator.py`.
- `goldflow/cli.py`, `goldflow/settings.py`, `config/binance_xauusdt.json` (new `research` block; `backtest.spread_mult`).
- `pyproject.toml`, `scripts/check.sh` (`goldflow.research` is under mypy --strict).
- `README.md`, `docs/BUILD_GUIDE.md`.

## 3. Tests
- `scripts/check.sh`: **all checks passed**, with **201 tests** (194 earlier plus 7 new) and mypy --strict on 81 files.
- The new tests cover:
  - hand-computed metrics (PF 4.0, drawdown and recovery, losing streak, Wilson interval), deterministic bootstrap and Monte Carlo, and refusal of an unreconciled ledger;
  - ledger ordering, duplicate refusal and tamper detection;
  - the test-lock guard, reuse for the same candidate, and refusal for another candidate;
  - walk-forward window geometry (7 windows, none touching the test period) and refusal of overlapping splits;
  - with a deterministic fake runner:
    - selection of the best parameter set;
    - stitched out-of-sample reconciliation;
    - stability, cost stress and sensitivity;
    - approval with `NOT_SET` and `FAIL`;
    - a full study: separate report sections, a test opened once, a second candidate refused, ledger entries, and a changed hypothesis refused;
  - a **full study through the real pipeline** on synthetic broker ticks (2 windows, in-sample and validation runs, robustness, approval not granted), plus refusal of a run that leaks into the test period.

## 4. Evidence on real data
None yet. Every metric in a study is computed from Phase 12 backtests, which cannot run without your broker's ticks (D2). The pipeline mechanics are exercised end to end in the tests above.

## 5. Open decisions
- **D2:** broker data. Blocks every real result.
- **D8:** approval thresholds. In particular, `max_oos_drawdown` must be set.
- **Splits:** they are fixed now. If you want different dates, change them **before** any study is run; the ledger records the windows each hypothesis was registered with.

## 6. Acceptance criteria
| Criterion | Status | Evidence |
|---|---|---|
| All metrics reconcile with the ledger | **Passed** | `performance` refuses unless Σ trade net = Σ cash flows (tested); Phase 12 reconciles every run to the cent |
| Test set not reused for optimisation (lock file records the opening) | **Passed** | `TestSetLock` refuses overlapping research ranges and any second candidate (tested) |
| Reports separate in-sample, validation and out-of-sample | **Passed** | The study report has distinct sections for in-sample, validation, walk-forward OOS and test (tested) |
| Real research conclusions | **Blocked (D2)** | Needs broker tick history |

## 7. Next phase
Phase 14 (real-time Binance ingestion and historical/live parity). Its REST path is geo-blocked from this US cloud host (HTTP 451); the WebSocket streams are reachable. What can and cannot be tested here is set out at the start of that phase.
