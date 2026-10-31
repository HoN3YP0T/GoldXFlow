# Research, validation and walk-forward v1

Package: `goldflow/research/`. CLI: `perf-report`, `study`, `ledger-verify`. Config: the `research` block.
**Purpose:** decide whether a strategy is robust, not merely fitted. Every number comes from Phase 12 backtests on broker prices, so real studies need decision **D2** (broker tick history).

| Module | Role |
|---|---|
| `settings.py` | Splits, walk-forward geometry, objective, statistics, approval thresholds (D8) |
| `metrics.py` | Performance report from a reconciled ledger, confidence intervals, Monte Carlo |
| `ledger.py` | Research ledger (hash-chained, hypotheses first) and the test-set lock |
| `runner.py` | One full-pipeline backtest over a range with parameter overrides (warm-up, window isolation) |
| `walkforward.py` | Walk-forward, stability, cost stress, sensitivity grid and heatmap |
| `study.py` | A study from a plan: hypothesis → walk-forward → robustness → (once) test → approval → report |

## 1. Splits (fixed before any testing)
| Period | Dates | Use |
|---|---|---|
| Development | 2026-04-30 → 2026-08-31 | walk-forward in-sample, robustness |
| Validation | 2026-09-01 → 2026-09-30 | walk-forward out-of-sample; validation report of the candidate |
| **Test (untouched)** | 2026-10-01 → 2026-10-29 | opened **once** for the final candidate |

- Data after 2026-10-30 is a second out-of-sample period.
- **Test lock** (`data/research/test_lock.json`): the first opening records the candidate hash (strategy, parameters, config hash), time and reason. Later accesses for the same candidate are recorded; any other candidate is refused (`TestSetReused`). Every research run refuses a range that touches the test period unless it comes through the lock.

## 2. Performance report (`metrics.performance`)
**Reconciliation.** It first checks that Σ trade net P&L = Σ cash flows, and refuses otherwise. Phase 12 already reconciled each run to the cent.

**Metrics:**
- net and gross P&L;
- **cost impact** (gross − net: commission and swap) and the slippage paid in points;
- win rate with a Wilson 95 % interval;
- profit factor and expectancy per trade, each with a seeded bootstrap 95 % interval;
- expectancy in R and the R distribution (quantiles);
- average win and loss;
- maximum drawdown with its recovery (in trades), and maximum consecutive losses;
- average duration and trades per day.

**Splits:** by strategy, side, session, Phase 9 auction state and realised-volatility tercile. Regimes are read from each traded signal's evidence.

**Sample size.** Fewer than `min_trades_conclusion` trades (default **100**) marks the report and every split `INSUFFICIENT_SAMPLE`.

**Trade-sequence Monte Carlo:** the trade order is bootstrapped (seeded), giving percentiles of maximum drawdown and of the worst losing streak.

`perf-report --name <backtest>` writes `performance.json` for any Phase 12 run.

## 3. Walk-forward
- **Windows.** Rolling: in-sample `in_sample_days` (56), out-of-sample `out_of_sample_days` (14), step `step_days` (14), over development + validation only. That gives 7 windows on the current dates.
- **Selection.** In each window every parameter set of the plan's grid is backtested in-sample. The best by `objective` (`profit_factor`, `net_cents` or `expectancy_r`) among sets with ≥ `min_trades_in_sample` trades is chosen; ties go to the earlier grid entry. It is then run unchanged out-of-sample.
- **Isolation.** Each run replays from `warmup_days` before its range (analytics and strategy warm-up). Signals before the range are never traded, and positions are closed at the range end.
- **Recorded:** every window's candidates, its choice and its out-of-sample metrics. The out-of-sample trades are stitched into one equity curve, and the share of windows with positive out-of-sample expectancy is reported.

## 4. Robustness (development data)
- **Stability:** each numeric parameter of the final candidate moved by ±`param_change` (20 %), one at a time. Stable when every variant keeps a profit factor ≥ 1.
- **Cost stress:** the broker spread is widened ×`cost_multiplier` (1.5) around the mid, and slippage ×2. These are real reruns (`backtest.spread_mult`), not re-priced trades.
- **Sensitivity:** a two-parameter grid gives the objective and net per cell (`sensitivity.csv`, `sensitivity.svg` heatmap). The plateau score is the share of the best cell's neighbours within `param_change` of the best: a plateau scores high, a lone peak low.

## 5. Research ledger (`data/research/ledger.jsonl`)
- Append-only. Each entry carries the SHA-256 of the previous one, so an edited or removed entry breaks the chain (`ledger-verify`).
- `HYPOTHESIS` entries are written **before** results: id, statement, strategy, grid, windows, success criteria.
- `RESULT` and `REJECTED_VARIANT` entries are refused for a hypothesis that was not registered earlier.
- A study refuses to reuse a hypothesis id with different content.
- `DECISION` entries record what was decided.

## 6. Approval criteria (proposals; you set the final numbers, D8)
| Criterion | Default |
|---|---|
| Out-of-sample profit factor after costs (the test period if opened, else the stitched walk-forward) | ≥ 1.2 |
| Walk-forward windows with positive expectancy | ≥ 70 % |
| Out-of-sample maximum drawdown | **not set: you must set `max_oos_drawdown`; until then the criterion is `NOT_SET` and nothing can be approved** |
| Stable under ±20 % parameter changes | every variant PF ≥ 1 |
| Profitable with costs ×1.5 (spread) and slippage ×2 | PF ≥ 1 |
| Sample size | ≥ 100 out-of-sample trades |

## 7. Study (`study --plan plan.json --name s1`)
```json
{"hypothesis": {"id": "H1", "statement": "Failed auctions at VAH/VAL rotate to the POC", "success_criteria": "OOS PF >= 1.2 after costs, >= 100 trades"},
 "strategy": "fa_value_edge",
 "grid": {"stop_buffer_ticks": [5, 10, 20], "min_rr": ["1", "1.5"]},
 "sensitivity": {"p1": "stop_buffer_ticks", "v1": [5, 10, 20], "p2": "min_rr", "v2": ["1", "1.5", "2"]},
 "open_test": false}
```
Output: `data/research/studies/<name>/{study.json, report.md, sensitivity.csv, sensitivity.svg}`. The report keeps **in-sample, validation, walk-forward out-of-sample and test** in separate sections; only out-of-sample results count for approval. Set `open_test: true` only for the final candidate: it can be done once.

## 8. Limitations
- No real study exists until D2 is supplied.
- Each grid point is a full replay. Six months × grid × windows is a long job: run it on the PC and keep grids small and pre-registered.
- Bootstrap intervals assume trades are exchangeable, which they are not exactly. They are reported as indications, not proofs.
