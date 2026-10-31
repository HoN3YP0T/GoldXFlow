# Phase 16 completion report: MT5 execution bridge and risk controls

Date: 2026-10-31. Specification: `docs/EXECUTION_V1.md`.

## 1. Scope completed / not completed
**Completed** (`goldflow/execution/`, `mt5/GoldflowExecutor.mq5`)
- **Bridge design as recommended.** The MQL5 EA executor connects as the TCP client: MQL5 sockets cannot listen, so
  Python listens on loopback. A Common Files queue is the fallback.
  - Messages are JSON lines with an HMAC-SHA256 over the exact text.
  - Each one carries a sequence number against replay, a clock-skew check, a session, and a strict schema.
  - Heartbeats run both ways.
- **Instrument validation.** At start-up the broker `SymbolInfo` must equal the `broker` block (D2). The checked fields
  are digits, contract size, volume min/step/max, stops level, trade mode, tick, and the account currency.
- **Order lifecycle.** CREATED → TRANSMITTED → ACCEPTED → PARTIALLY_FILLED → FILLED → CLOSED, or REJECTED, CANCELLED,
  UNKNOWN.
  - **State changes only on broker confirmation** (`OnTradeTransaction` deals, request retcodes).
  - The idempotency key goes in the order comment, with the magic number.
  - The EA refuses a key it has already seen; the engine refuses a signal it has already seen.
- **Risk engine** on every order: emergency stop, trading blocks, **data quality** (cannot be disabled), session,
  spread, slippage, daily loss, drawdown, consecutive losses, positions, stop validity, sizing from the tick value, and
  exposure.
  - The limits default to Phase 12's `backtest.risk` (D7) and may only be stricter.
  - The **emergency stop** latches, flattens, and waits for an operator reset.
- **Reconciliation** on start, on reconnect and periodically. Broker-side changes are adopted and logged; unexplained
  differences block trading.
- **Audit log:** hash-chained, covering every message, decision and response.
- **Deployment gates.**
  - New `demo` profile: orders reach DEMO accounts only, and the bridge blocks any other account mode.
  - `exec-gate` writes the M7 record only from a passing demo audit log and a named operator.
  - Live trading stays off by default: `live_trading_enabled = false`, the live profile is locked, and the EA has
    `InpAllowReal = false`.
- **Commands:**
  - running: `bridge-run --profile demo`, `bridge-status`;
  - safety: `emergency-stop`, `emergency-reset`;
  - deployment: `exec-gate`, `bridge-secret`, `audit-verify`, `ea-vector`.
  - `LiveRunner` gained step hooks so the bridge ticks with the live pipeline.

**Fixes found while testing**
- **In-flight orders.** Reconciliation first marked an unacknowledged order REJECTED when the broker had no trace of
  it, but such an order may still be in flight, for example in the file queue. Absence now never resolves a
  TRANSMITTED order. An UNKNOWN order is re-sent with the same key while its signal is valid, and only expires
  afterwards.
- **Malformed signals.** A signal with its stop on the wrong side was "repaired" by taking the absolute distance. Its
  geometry is now validated and it is refused (`STOP_INVALID`).

**Not completed**
- **The MQL5 EA was not compiled or run.** There is no MetaEditor or MT5 terminal here (Linux build host). It mirrors
  the simulated EA rule for rule and self-tests its HMAC against the Python vector in `OnInit`. It must be compiled,
  and then exercised on a demo account.
- **Demo evidence and the M7 gate.** They need 2–4 weeks on a broker demo account, the broker specification (D2) and
  a host where Binance REST is reachable (D6).
- **Position management.** Break-even, trailing and partial take-profit are not built; the message types for them
  exist.

## 2. Files
**Created**
- `goldflow/execution/`: `__init__.py`, `protocol.py`, `settings.py`, `orders.py`, `risk.py`, `symbol.py`,
  `reconcile.py`, `audit.py`, `transport.py`, `bridge.py`, `gate.py`, `sim.py`.
- `mt5/GoldflowExecutor.mq5`.
- `tests/test_execution.py` (13 tests).
- `docs/EXECUTION_V1.md` and this report.

**Modified**
- `goldflow/settings.py`: `Profile.DEMO`; validation of `bridge`/`risk`.
- `goldflow/profiles.py`: the demo profile and the trading-mode text.
- `goldflow/live/runner.py`: step hooks.
- `goldflow/cli.py`: eight new commands.
- `config/binance_xauusdt.json`: `bridge` and `risk` blocks.
- `pyproject.toml`, `scripts/check.sh`: `goldflow.execution` is under mypy --strict.
- `README.md`, `docs/BUILD_GUIDE.md`.

## 3. Tests
`scripts/check.sh`: **all checks passed**, with **231 tests** (218 earlier plus 13 new) and mypy --strict on 112
files.

Every test runs against the **simulated** EA and broker (labelled synthetic). The simulator follows the MQL5 EA's
rules. The tests inject faults throughout.

**Protocol**
- Bad MAC, wrong secret, replayed line, clock skew, wrong session, nested or mistyped fields, an unknown type and a
  type the sender may not send are all refused.
- HELLO resets the sequence.
- The idempotency key fits MT5's 31-character comment.
- The HMAC vector and the protocol tokens are present in the `.mq5` file.

**Lifecycle**
- Trading stays blocked until HELLO, the symbol check and reconciliation.
- The order is sized at 0.10 lots (0.5 % of 10,000 at a 5.00 stop). Stops and target keep their distances at the ask.
- Exact state history: TRANSMITTED → ACCEPTED → FILLED, then CLOSED by SL; the losing streak is counted.
- The same signal is refused the second time.

**No fill is assumed, and nothing is duplicated (fault injection)**

| Injected fault | Result |
|---|---|
| ORDER_NEW lost | order stays TRANSMITTED, then is retried with the same key: 1 broker order |
| ACK and both fill reports lost while the broker did fill | retry, ACK DUPLICATE, fill adopted by reconciliation: 1 broker order |
| EA restart before handling the order | re-handshake and retry: 1 broker order |
| Engine restart | state restored, reconciled before trading, the old signal refused |
| Network drop | `NO_HEARTBEAT` blocks trading, recovery after re-handshake |
| Link down through every retry | UNKNOWN, then re-sent after reconnect: 1 order; the same with an expired signal gives REJECTED and no order |
| Partial fills | PARTIALLY_FILLED → FILLED, with the filled volume exact |

**Risk**
- `DQ_OPEN`, `SPREAD`, `STOP_INVALID`, `EXPOSURE` (a 1-tick stop would need 50 lots) and `MAX_POSITIONS` all block.
  Nothing refused ever reached the broker.
- A −3 % day latches the emergency stop, positions are flattened, and later orders are refused (`EMERGENCY_STOP`).
- Risk limits looser than the backtest are refused, and `block_on_dq: false` is refused.
- Emergency stop and reset by file work.

**Reconciliation**
- A manual close is adopted (`CLIENT`).
- A close while the link was down is adopted (`RECONCILED_MISSING`).
- An unknown broker position gives `RECONCILE_MISMATCH`, which blocks trading.

**Start-up refusals**
- A demo run against a REAL account is refused (`ACCOUNT_MODE`).
- A 3-digit symbol is refused (`SYMBOL_MISMATCH`).
- A tampered EA message is refused and audited.
- A bridge without the broker block is refused.

**Audit and gate**
- The chain verifies, and an edited line is detected.
- A short demo fails the gate (days, fills, paths), and `record_gate` refuses it.
- The live profile stays locked; the demo profile is allowed.
- A passing evidence record writes `live_gates.json`.

**Transports**
- TCP line framing with a partial line held back, EA reconnection replacing the old connection.
- The file queue is atomic (no `.tmp` left behind) and ordered.

## 4. Evidence on real data
**None, as expected for this phase.**
- Order routing needs an MT5 terminal on Windows, your broker's demo account and its contract specification (D2).
- The live signal path needs a host where Binance REST is reachable (D6).

The CLI was checked here:
- `bridge-run` without the demo profile is refused;
- with the demo profile it stops on the missing secret;
- the live profile is refused by its gates;
- `exec-gate` on a missing run is refused without creating anything.

## 5. Open decisions
- **D2:** the broker contract specification and a demo account. The bridge refuses to start without the `broker`
  block.
- **D6:** the trading host (Windows MT5 plus Binance REST reachable).
- **D7:** risk limits (shared with Phase 12). Spread and slippage filters are in broker points.
- **Gate thresholds (PROPOSED):** ≥ 14 days, ≥ 20 fills, every lifecycle path, slippage ≤ 1.5 × model.
- **Position management:** whether to add break-even, trailing or partial take-profit, which would be a new strategy
  hypothesis for Phase 13.

## 6. Acceptance criteria
| Criterion | Status | Evidence |
|---|---|---|
| No order is assumed filled without broker confirmation | **Passed** | ACK ≠ fill; states move only on TXN/REJECT; lost messages leave TRANSMITTED, then retry or reconcile (tests) |
| Duplicate submissions prevented (retries, restarts, network drops) | **Passed** | Fault-injection tests: 1 broker order in every scenario; EA-side and engine-side de-duplication |
| Risk limits block trades under invalid data or excess exposure | **Passed** | `DQ_OPEN` (cannot be disabled), `EXPOSURE`, `MAX_POSITIONS`, `SPREAD`, `STOP_INVALID`, daily loss → emergency flatten (tests) |
| Live mode off by default | **Passed** | `live_trading_enabled` false; live profile locked without the M7 record, which only `exec-gate` writes from passing evidence; EA refuses REAL accounts by default |
| Demo for 2–4 weeks, lifecycle exercised, slippage vs model, reconciliation clean | **Pending (D2, D6)** | Gate tooling built and tested; needs the demo run |

## 7. Next phase
Phase 17: dashboard, observability, deployment and production hardening.
