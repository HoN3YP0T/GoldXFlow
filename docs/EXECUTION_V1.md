# MT5 execution bridge and risk controls, v1 (Phase 16)

Code: `goldflow/execution/` (`protocol.py`, `settings.py`, `orders.py`, `risk.py`, `symbol.py`, `reconcile.py`,
`audit.py`, `transport.py`, `bridge.py`, `gate.py`, `sim.py`) and the MQL5 executor `mt5/GoldflowExecutor.mq5`.
Config blocks: `bridge`, `risk`.
Commands: `bridge-run`, `bridge-status`, `emergency-stop`, `emergency-reset`, `exec-gate`, `bridge-secret`,
`audit-verify`, `ea-vector`.

**Live trading is off by default.**
- `execution.live_trading_enabled` is false.
- The `live` profile refuses to start until the M7 demo gate is recorded.
- Order routing to a demo account needs the `demo` profile, and even then every order is blocked unless the EA
  reports a **DEMO** account.

## 1. Architecture
```
Binance live (Phase 14) -> analytics + StrategyEngine (Phases 5-11) -> signal
   -> ExecutionBridge: risk engine -> order (idempotency key) -> ORDER_NEW
   -> loopback TCP (Python listens, the EA connects) or a Common Files queue
   -> GoldflowExecutor.mq5: verify, de-duplicate, validate, CTrade -> broker
   <- ACK / REJECT / TXN (OnTradeTransaction) / POSITIONS / HEARTBEAT <- EA
```
The EA keeps execution broker-side, which survives a Python crash. Python never assumes a result. The **EA never
decides to trade**: it validates, de-duplicates, executes and reports.

## 2. Protocol (`protocol.py`)
- **Line format.** `<hmac> <json>\n`. The HMAC is HMAC-SHA256(secret, json bytes) in hex, over the exact text sent,
  so there is no canonicalisation step.
- **Envelope.** `v`, `type`, `seq`, `ts` (ms), `session`, `sender`.
- **Receivers refuse** a bad MAC, a non-increasing `seq` (replay), more than `max_skew_ms` of clock skew, a wrong
  session, an unknown type, and missing or mistyped fields.
- **Python → EA messages are flat**, so the EA's JSON parser stays minimal.
- **HELLO.** It starts a new incarnation of its sender, so its seq may restart. The EA adopts the engine's session
  only from an authenticated, timely HELLO, and ignores everything else until it has one.
- **Secret.** At least 32 bytes, never in the config. It lives in a file both sides read (`bridge-secret --out`, in the
  MT5 Common Files folder) or in an environment variable.
- **Self-test.** The EA proves its HMAC implementation in `OnInit` against the fixed vector printed by `ea-vector`. The
  Python tests check that the vector in the `.mq5` file is the current one.

## 3. Order lifecycle (`orders.py`)
States: `CREATED → TRANSMITTED → ACCEPTED → PARTIALLY_FILLED → FILLED → CLOSED`, or `REJECTED`, `CANCELLED`, or
`UNKNOWN`.
- **Only broker confirmations move an order.** These are `TXN REQUEST` (retcode), `TXN DEAL IN` and `OUT`, and
  `REJECT`. An `ACK` only means the EA received the order; it is not a fill.
- **Retries.** Without an ACK, the order is re-sent after `ack_timeout_ms` with the **same idempotency key**, up to
  `max_retries`. After that it is `UNKNOWN`, trading is blocked, and reconciliation follows.
- **An UNKNOWN order** absent at the broker is re-sent while its signal is valid, and REJECTED once it has expired.
  A TRANSMITTED order is **never** resolved from absence: it may still be in flight, for example in the file queue.
- **Idempotency.**
  - The key is `gf` + 16 hex characters of SHA-256(signal_id). It goes into the order comment as `gf:<key>` (at most
    19 characters) together with the magic number.
  - The EA refuses a key it has seen in this session, or that appears in the comment of an open position, a pending
    order, or a history order of the last `InpHistoryDays`.
  - The engine refuses a `signal_id` it has seen; that set is persisted.

## 4. Risk engine (`risk.py`)
Every order passes every check, in this order:
1. emergency stop;
2. trading blocked (no EA, symbol check, reconciliation, heartbeat, UNKNOWN order, account mode);
3. **data-quality interval open** (it cannot be disabled);
4. session;
5. spread;
6. average slippage;
7. daily loss;
8. drawdown;
9. consecutive losses;
10. maximum positions;
11. stop validity (a malformed signal or a stop inside the stops level);
12. sizing from the tick value (`RISK_TOO_SMALL` below the broker minimum);
13. exposure.

**Limits.** They are decision **D7** and default to Phase 12's `backtest.risk`. The `risk` block may only make them
stricter.

**Emergency stop.** Reaching the daily-loss or drawdown limit, or an operator command, **latches** it:
- `FLATTEN_ALL` is sent, and repeated until the broker reports no positions;
- no order passes until an operator reset.

## 5. Broker checks (`symbol.py`) and reconciliation (`reconcile.py`)
**Symbol check at start-up.** The EA's `SymbolInfo` must equal the `broker` block (D2), the specification the
backtests used. The checked fields are digits, contract size, volume min/step/max, stops level, FULL trade mode,
tick = point, and the account currency. Any difference blocks trading.

**Price translation.** BUY at ask, SELL at bid, with the signal's stop and target distances kept. This is the Phase 12
basis translation at send time.

**Reconciliation** runs on start, on every (re)connect and every `reconcile_every_s`:
- **Broker-side changes are ADOPTED and logged:** a manual or stop-out close, a partial close, SL/TP edits, a fill the
  engine missed, and a close while disconnected.
- **These are MISMATCHES that block trading and raise an alert:** a broker position with our magic and an unknown key,
  and a broker volume larger than the engine's (which is how a duplicate would show).

## 6. Audit log and deployment gate
**Audit log.** `data/execution/<name>/audit.jsonl` is append-only and hash-chained, and `audit-verify` checks it. It
holds every message in or out, refusal, signal, risk decision, order change, reconciliation, alert, emergency, start
and stop.

**Gate.** `exec-gate --name <demo run> --operator <name>` reads the demo audit log and requires:
- only DEMO accounts;
- at least 14 days of fills, and at least 20 fills;
- every lifecycle path exercised: partial fill, rejection, SL, TP, manual or stop-out close, retry, EA reconnect,
  engine restart, and emergency flatten;
- a clean last reconciliation, and no unresolved mismatch;
- mean slippage ≤ 1.5 × the Phase 12 model;
- no refused message left unreviewed.

Only then does it write `data/state/live_gates.json`. The live profile also needs
`execution.live_trading_enabled = true`, and the EA needs `InpAllowReal = true`.

## 7. Running it (Windows trading PC or Windows VPS)
1. Fill in the `broker` block from MT5's XAUUSD specification (D2).
2. Create the shared secret and point the config at it:
   ```bash
   python -m goldflow.cli bridge-secret --out "%APPDATA%\MetaQuotes\Terminal\Common\Files\goldflow\bridge.secret"
   ```
   Then set `bridge.secret_file` to that path.
3. Prepare MT5:
   - copy `mt5/GoldflowExecutor.mq5` to `MQL5\Experts`;
   - compile it in MetaEditor;
   - in Tools → Options → Expert Advisors, allow algorithmic trading and add `127.0.0.1` to the allowed URLs.
4. Attach the EA to an XAUUSD chart **of a demo account**.
5. Start the bridge:
   ```bash
   python -m goldflow.cli bridge-run --profile demo --name demo1
   ```
   This needs a host where Binance REST is reachable (D6).
6. Operate:
   ```bash
   python -m goldflow.cli emergency-stop --name demo1 --reason "..."
   ```
   ```bash
   python -m goldflow.cli emergency-reset --name demo1 --operator NAME
   ```
   ```bash
   python -m goldflow.cli bridge-status --name demo1
   ```
7. After 2–4 weeks, run the gate:
   ```bash
   python -m goldflow.cli exec-gate --name demo1 --operator NAME
   ```

## 8. Limits of v1
- **The MQL5 EA was not compiled or run here.** There is no MetaEditor or MT5 in the build environment. Its behaviour
  is specified by `sim.py`, which every test drives under fault injection. Compile it, then exercise it on a demo
  account before any gate.
- Market orders only. Pending entries are refused upstream, as in Phase 12.
- No partial take-profits, break-even or trailing in v1. `ORDER_MODIFY` and `ORDER_CLOSE` exist for them and for
  operator use.
- Netting accounts: the EA reports the margin mode. With `max_positions` = 1 the behaviour is the same as hedging.
  Pyramiding on netting is not supported.
