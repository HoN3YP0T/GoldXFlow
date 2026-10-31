# Backtesting and execution simulation v1

Package: `goldflow/backtesting/`. CLI: `broker-import`, `backtest`. Config: the `broker` block (decision **D2, open**) and the `backtest` block.

**Signal venue ≠ execution venue.** Signals come from Binance XAUUSDT; fills happen on **your MT5 broker's XAUUSD bid/ask**. The backtester refuses to run until the broker is configured and its tick history is imported. Binance prices are never used for fills.

| Module | Role |
|---|---|
| `settings.py` | `BrokerSpec` (contract, costs, server time zone) and `BacktestSettings` (execution model, risk limits); validated at startup |
| `broker.py` | Import and validation of MT5 tick exports into a broker dataset (`provider=broker_<name>`), reader |
| `basis.py` | Basis model (Binance − broker), translation of signal levels |
| `simulator.py` | Orders, fills, exits, costs, risk limits, exact cash ledger |
| `engine.py` | `Backtester(ProcessorSink)`, swap rollovers, statistics with reconciliation, run record, report |

## 1. Broker data (prerequisite)
1. **Specification.** Fill in the `broker` block from MT5 (Symbols → XAUUSD → Specification):
   - name, digits, contract size (oz per lot), lot limits and step;
   - commission per lot per side, swap long and short per lot, swap time and triple-swap day, stops level;
   - the **server time zone**: MT5 tick times are broker server time.
2. **Tick export,** in either format:
   - from the terminal: *Symbols → Ticks → Export*, the tab-separated `<DATE> <TIME> <BID> <ASK> …` format (`--format mt5`);
   - from an MQL5 script with `CopyTicksRange`: a CSV with `time_msc,bid,ask` (`--format csv`).

   Then run `python -m goldflow.cli broker-import --file <export> --format mt5|csv`.
3. **Validation.** Every rejected row is quarantined with a reason (`_quarantine_*.csv`), never dropped silently:
   - a price must be an exact multiple of the point;
   - bid > 0 and ask ≥ bid;
   - time may not run backwards after conversion to UTC;
   - ambiguous or non-existent local times around the server's DST changes are flagged.

   An empty BID or ASK in the terminal format means "unchanged" and is carried forward. The manifest records the source SHA-256, the counts, per-day digests, the spec, a dataset version, and the declared capability `bid_ask_quotes: AVAILABLE`.

## 2. One clock
The backtest runs inside the normal Phase 4 replay: `TradeProcessor → Profile/Orderflow/IB/Auction engines → StrategyEngine → Backtester`. The analytics and strategy code is the same as in research and live.

At each processor advance to clock X, the backtester:
1. reads only the broker ticks with time < X;
2. builds basis samples for the Binance seconds that ended by X;
3. simulates those ticks merged with the new signals in time order.

Broker ticks at or before a signal's time are applied before the signal. An order becomes active at `event_time + latency` and fills on the first broker tick at or after that time.

## 3. Basis
- **Sample, every Binance 1 s bar:** Binance close − broker mid, where the mid is the broker quote in force just before the second ends. Seconds where the broker quote is older than `max_tick_gap` (for example while the broker is closed) are skipped.
- **Estimate at time T:** the lower median of the samples in `(T − window, T]`. It needs `min_samples` samples, otherwise the order is refused with `REJECTED_NO_BASIS`. The model asserts it never receives a sample for a time already queried.
- **Translation:** broker price = Binance price − basis(T_signal), rounded to the nearest point (half up). Entry, stop and target all use the basis frozen at the signal time.
- Binance publishes no historical bid/ask, so its last trade price stands for its price. Integer half-units keep the arithmetic exact.

## 4. Execution model (printed at the top of every report)
| Item | Rule |
|---|---|
| Market entry | buy at **ask**, sell at **bid**, plus slippage = `slippage_points` + ceil(`slippage_vol_mult` × σ of the last `slippage_vol_ticks` mid changes), against the trader |
| Stop loss | market order when bid (long) / ask (short) reaches it; filled at that quote − slippage (gaps fill at the gapped price) |
| Take profit | limit: filled at the target when bid (long) / ask (short) reaches it |
| Stop/target ambiguity | broker ticks decide the first touch. If the target tick comes after a gap > `max_tick_gap` and the Binance path in the gap (translated with the order's basis) reached the stop, the **stop is assumed first** (`GAP_ASSUMED_STOP`, counted) |
| Rejections | deterministic seeded draw < `reject_rate`; stops level: stop and target ≥ `stops_level_points` from the fill, on the correct sides |
| Partial fills | off (retail size) |
| Time exit | `max_hold`; positions open at the end of the data close at the last quote (`END_OF_DATA`) |
| Expiry | an order with no tick between activation and the signal's `expiry_at` expires |
| Commission | per lot per side, at entry and exit |
| Swap | per lot at each rollover (`swap_time`, server time). A rollover belongs to the server day it closes (00:00 Thursday closes Wednesday); charged for Mon–Fri days; triple on `triple_swap_day`. **Confirm with your broker (D2)** |

## 5. Risk simulation (checked at the signal time; suppress only)
| Rule | Refusal |
|---|---|
| sizing: floor(`risk_per_trade` × equity ÷ (stop distance × point × contract size) ÷ step) × step, capped by `volume_max` and the exposure limit | `REJECTED_SIZE_BELOW_MIN` |
| `max_concurrent_positions` (pending orders count) | `REJECTED_MAX_CONCURRENT` |
| daily loss limit on realised balance vs the UTC day's start | `REJECTED_DAILY_LOSS` for the rest of the day |
| `max_consecutive_losses` | `REJECTED_CONSECUTIVE_LOSSES` for the rest of the day |
| `max_drawdown` on equity from its peak | `REJECTED_MAX_DRAWDOWN` for the rest of the run |
| `sessions` filter (empty = all) | `REJECTED_SESSION` |
| broker quote older than `max_tick_gap` / basis unknown | `REJECTED_STALE_QUOTE` / `REJECTED_NO_BASIS` |
| the translated stop or target on the wrong side | `REJECTED_INVALID_TRANSLATION` |

The default limits are PROPOSED (decision D7): 0.5 % per trade, 2 % daily, 8 % maximum drawdown, 4 consecutive losses, 1 position.

## 6. Money and reconciliation
- Every cash flow (commission, swap, P&L) is an exact `Fraction` in USD, rounded **once** to integer cents (round half to even). The balance is the running sum of those cents.
- Before a report is written, `stats()` asserts:
  - final balance = start + Σ cash flows;
  - Σ trade net P&L = Σ cash flows;
  - each trade's net equals its own cash flows.

  Every statistic is computed from those reconciled trades.

## 7. Outputs (`data/backtests/<run>/`)
- `orders`: every order including refusals, with reasons, translated levels, lots and basis.
- `fills`: price, quote, slippage, latency and reason.
- `trades`: gross, commission, swap, net, risk and R multiple.
- `cash`: every balance change.
- `equity`: balance and mark-to-market equity at each fill and each UTC day start.
- `run.json`: the reproducibility record:
  - run, Binance and broker dataset versions, config hash, backtest settings hash, strategies hash, git code version;
  - strategy versions and parameters, broker spec, execution model, starting capital, seed, range;
  - statistics and counters.
- `report.md`: the execution assumptions first, then reproducibility, then the reconciled results. A run on **synthetic** broker data (tests only, refused otherwise) carries `SYNTHETIC BROKER DATA - NOT A RESULT` at the top and bottom.

## 8. Limitations
- No real result exists until D2 is supplied.
- The basis uses Binance last trades (no Binance quotes exist).
- Event tracking and the gap path have 1 s resolution.
- The rollover convention and costs must be confirmed with the broker.
- Market orders only: the example strategies emit `market` entries; limit and stop entries are not simulated yet. A signal with another entry type is refused in Phase 11's schema check.
