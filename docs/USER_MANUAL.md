# GoldXFlow user manual

This manual explains how to use GoldXFlow step by step, from installation to signals, backtests, live paper trading and
MT5 demo trading. Commands are shown for **Windows** (your trading PC). On Linux or macOS, write `python3` instead of
`python` and use `/` in paths.

> **What GoldXFlow is, and what it is not.** It analyses Binance's XAUUSDT perpetual (individual trades, orderflow,
> volume profile, auction behaviour), turns that evidence into explainable trade signals, backtests them on your
> broker's prices, and can route orders to MT5. It does **not** guarantee profits, and it never trades a real account
> unless you deliberately pass every safety gate in Part 9.

**Contents**
1. Install
2. Configure
3. Download the history
4. Check the data
5. Run the analytics and signals
6. Look at the results
7. Other exchanges (optional)
8. Backtest on your broker's prices
9. Research a strategy properly
10. Live paper mode
11. MT5 demo trading
12. Going live (only after the gate)
13. Daily operation: dashboard, alerts, backups
14. Troubleshooting
15. Command reference
16. Decisions you own

---

## Part 1. Install

**You need:**
- Windows 10/11 (or Linux/macOS) with **Python 3.10 or newer**;
- about **15 GB of free disk** for six months of data and its derived datasets;
- an internet connection where `fapi.binance.com` is reachable. India is fine; US hosts get **HTTP 451**.

**Steps**
1. Get the code. Either download the repository as a ZIP from GitHub and unpack it, or clone it:
   ```bash
   git clone https://github.com/HoN3YP0T/GoldXFlow.git
   ```
2. Open a terminal in the project folder (for example `C:\Users\you\Documents\python\goldflow`).
3. (Recommended) Create a virtual environment and activate it:
   ```bash
   python -m venv .venv
   ```
   ```bash
   .venv\Scripts\activate
   ```
4. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```
5. Check that it works:
   ```bash
   python -m goldflow.cli status
   ```
   You should see JSON with `"trading_mode": "DISABLED ..."` and the provider capabilities.

> **Shortcut.** `run_windows.bat` performs Parts 1, 3, 4 and 5 in one go. Re-run it any time: finished steps are
> skipped or resumed.

---

## Part 2. Configure

Everything is in **`config\binance_xauusdt.json`**. Every block is checked at start; a wrong value stops the program
with a message naming it. Most of the file is already set. What you may want to touch:

| Block | What it is | When to change it |
|---|---|---|
| `range` | how many months to download (6) | only before the first download |
| `sessions` | Asia, London, New York session times (D4) | if you trade other session definitions |
| `volume_profile`, `orderflow`, `auction`, `initial_balance` | analytics parameters (D11, D12) | research only; record changes in the research ledger |
| `strategies` | which strategies run, and their parameters (D13) | research only |
| `broker` | your MT5 XAUUSD contract specification (**D2**) | before backtesting or MT5 (Part 8) |
| `backtest.risk` | risk limits (**D7**) | before backtesting or MT5 |
| `bridge`, `risk` | MT5 connection; live risk filters (stricter only) | before MT5 demo (Part 11) |
| `ops` | dashboard port, alert thresholds, log size | optional |

**Secrets never go in the config.** Set them as environment variables. Windows keeps a `setx` value for **new**
terminals, so open a new terminal afterwards.
```bash
setx BINANCE_API_KEY "your-key"
```
`BINANCE_API_KEY` is optional: it lets the program recover individual trades and backfill gaps (read-only, MARKET_DATA
permission). The other secrets (bridge, dashboard token) come up in Parts 11 and 13.

---

## Part 3. Download the history

1. *(Optional, India/PC only)* Record the live instrument specification:
   ```bash
   python -m goldflow.cli discover
   ```
2. Download the archives:
   ```bash
   python -m goldflow.cli acquire
   ```
   - It downloads trades, aggTrades, klines, bookDepth and metrics from `data.binance.vision`, verifies every SHA-256
     checksum, and keeps the raw files under `data\raw\binance\`.
   - **The range is frozen** in `data\state\range.json` so that every rerun covers exactly the same period. It is
     2026-04-30 → 2026-10-30 as shipped. To download a different period:
     ```bash
     python -m goldflow.cli acquire --new-range --start 2026-05-01T00:00:00Z --end 2026-11-01T00:00:00Z
     ```
   - Interrupted? Run the same command again; verified files are skipped.
   - On a host where REST is blocked, add `--no-rest`.
3. Write the coverage manifest:
   ```bash
   python -m goldflow.cli manifest
   ```

**Result:** `data\manifest.json`. Exit code 1 means a file failed or an interval of trades is uncovered: rerun
`acquire`.

---

## Part 4. Check the data

1. Validate and convert everything into clean Parquet partitions:
   ```bash
   python -m goldflow.cli normalize
   ```
   - Bad rows are quarantined, never silently fixed.
   - Each day gets a status per dataset: COMPLETE, PARTIAL, UNVERIFIED or MISSING.
   - Long runs can be split with `--budget 3600` (seconds); run it again to continue.
2. Re-check everything independently:
   ```bash
   python -m goldflow.cli validate
   ```
   It must report `"identical_to_previous_run": 1` and no `digest_differences_vs_catalog`.
3. Write the reports:
   ```bash
   python -m goldflow.cli report
   ```

**Result:** `data\reports\validation_report.md`, `quarantine_report.md` and the coverage CSVs. Read the validation
report once: it lists every day that is not COMPLETE and why. Signals are automatically blocked on those intervals.

---

## Part 5. Run the analytics and signals

One command runs everything from trade processing to signals, in one pass, and then checks every result with an
independent recomputation:
```bash
python -m goldflow.cli pipeline --name main
```

| It computes | Phase |
|---|---|
| footprint bars (1 s … 1 h), session and day windows | 5 |
| volume profiles: POC, value area, HVN/LVN, VWAP and bands | 6 |
| delta, CVD, imbalances, Big Trades, absorption, exhaustion, divergence | 7 |
| Initial Balance, extensions, failed breaks | 10 |
| auction levels, acceptance and rejection, failed auctions, auction state | 9 |
| strategy signals (and why others were suppressed) | 11 |

- **Long run?** Add `--budget 3600` to stop after an hour, then continue with `--resume`.
- **Finished:** exit code 0 and `"ok": true` in `data\reports\pipeline_main.json`. Each `verification` entry must have
  an empty `fail` list.
- **The same steps one at a time**, if you ever need them: `process`, `vprofile`, `orderflow`, `ib`, `auction`,
  `signals`, each with `--name`. They write exactly the same files.

---

## Part 6. Look at the results

**The dashboard** (easiest):
```bash
python -m goldflow.cli dashboard
```
Then open **http://127.0.0.1:8765/** in a browser.
- It shows the latest profiles, CVD, orderflow and auction events, signals, data coverage, backtests and system
  health.
- **Every value comes from a file on disk**: hover over a value to see which one. Anything missing says "not
  available" and why.

**Files** (all under `data\`):

| What | Where |
|---|---|
| Verification reports | `reports\*_verification.md`, `reports\processing_reconciliation_1m.md` |
| One profile as JSON (for charts) | run `python -m goldflow.cli vprofile-export --window session:london:2026-10-26 --bin 10` (the command prints the file path) |
| Charts of auction events | run `python -m goldflow.cli auction-review --limit 120`, then open the SVG files under `derived\...\auction\charts\` |
| Signals with their evidence | `derived\provider=binance\...\signals\date=*\signals.parquet` (and `suppressed.parquet`) |
| All datasets | `derived\provider=binance\market=usdm_perpetual\instrument=XAUUSDT\{footprint,profile,orderflow,initial_balance,auction,signals}\` |

The Parquet files open in Python (pandas), Excel's Power Query, or any Parquet viewer.

---

## Part 7. Other exchanges (optional)

OKX and Bybit gold perpetuals are kept as **separate markets**: they are never merged with Binance.
1. Download and validate their trades:
   ```bash
   python -m goldflow.cli venue-import --provider okx --start 2026-10-01 --end 2026-11-01
   ```
   ```bash
   python -m goldflow.cli venue-import --provider bybit --start 2026-10-01 --end 2026-11-01
   ```
2. Run the same analytics on a venue:
   ```bash
   python -m goldflow.cli venue-run --provider okx --name v1
   ```
3. Compare prices between venues (1 s basis series):
   ```bash
   python -m goldflow.cli basis --pair binance:okx --start 2026-10-01 --end 2026-11-01
   ```
4. Compare traded ounces (a labelled aggregate):
   ```bash
   python -m goldflow.cli volume-oz --venues binance okx bybit --start 2026-10-01 --end 2026-11-01
   ```
5. See what each provider can and cannot do:
   ```bash
   python -m goldflow.cli providers-report
   ```

**Completeness.** OKX days can be proven complete. Bybit days stay UNVERIFIED, which means its signals are blocked.
Bitget is limited to instrument lookup from the build host.

---

## Part 8. Backtest on your broker's prices

Backtests fill on **your MT5 broker's XAUUSD bid/ask**, never on Binance prices. Until you complete step 1 the
`backtest` command refuses to run.

1. **Fill in the `broker` block** in the config. In MT5: *Market Watch → right-click XAUUSD → Specification*. Copy:

   | Field | Content |
   |---|---|
   | `name` | the broker's name |
   | `symbol` | e.g. `XAUUSD` |
   | `digits`, `contract_size` | from the specification |
   | `volume_min`, `volume_step`, `volume_max` | from the specification |
   | `commission_per_lot_side` | the broker's commission |
   | `swap_long_per_lot`, `swap_short_per_lot`, `swap_time`, `triple_swap_day` | the swap terms |
   | `stops_level_points` | the minimum stop distance |
   | `server_tz` | the server's time zone, e.g. `"Europe/Athens"` |
   | `account_currency` | the account currency |
2. **Export the broker's ticks from MT5.** *View → Symbols → Ticks*, pick XAUUSD and the same date range as your
   history, *Request*, then *Export*. Save the file, for example as `XAUUSD_ticks.csv`.
3. Import the ticks:
   ```bash
   python -m goldflow.cli broker-import --file XAUUSD_ticks.csv --format mt5
   ```
4. **Check the risk limits** in `backtest.risk` (D7). The proposed values are 0.5 % per trade, 2 % daily loss, 8 %
   drawdown and 1 position at a time.
5. Run the backtest (analytics, signals and backtest in one pass):
   ```bash
   python -m goldflow.cli pipeline --name bt1 --backtest
   ```
6. Write the full performance report:
   ```bash
   python -m goldflow.cli perf-report --name bt1
   ```

**Result:** `data\backtests\bt1\` holds `report.md`, `performance.json`, and `orders`, `fills`, `trades`, `cash` and
`equity` as Parquet. The report states the execution model at the top: latency, slippage, spread, commission and swap.

---

## Part 9. Research a strategy properly

A backtest that looks good is not yet evidence. The `study` command enforces fair testing:
- walk-forward (optimise on the past, test on the next weeks);
- robustness (±20 % parameters, 1.5× costs);
- a **test period that can be opened only once**;
- a tamper-evident ledger in which your hypothesis is recorded **before** any results.

1. **Before any study:**
   - check the date splits in the `research` block (development, validation, test);
   - set `research.approval.max_oos_drawdown` (D8). Nothing can be approved until it is set.
2. Write a plan file, for example `plan.json`:
   ```json
   {"hypothesis": {"id": "H1", "statement": "Failed auctions at VAH/VAL rotate to the POC",
                   "success_criteria": "OOS PF >= 1.2 after costs, >= 100 trades"},
    "strategy": "fa_value_edge",
    "grid": {"stop_buffer_ticks": [5, 10, 20], "min_rr": ["1", "1.5"]},
    "sensitivity": {"p1": "stop_buffer_ticks", "v1": [5, 10, 20], "p2": "min_rr", "v2": ["1", "1.5", "2"]},
    "open_test": false}
   ```
3. Run it:
   ```bash
   python -m goldflow.cli study --plan plan.json --name s1
   ```
4. Read `data\research\studies\s1\report.md`. Only the **out-of-sample** sections count.
5. Only for your **final** candidate, set `"open_test": true` and run the study once more. It cannot be repeated.
6. Check the ledger:
   ```bash
   python -m goldflow.cli ledger-verify
   ```

---

## Part 10. Live paper mode

Paper mode follows the live market and produces signals, but places **no orders**. It needs `fapi.binance.com` to be
reachable (your PC in India, or a non-US VPS).

1. Start it:
   ```bash
   python -m goldflow.cli live-run --name live1
   ```
   - It polls Binance's individual trades every 750 ms (RPI trades included, as in the history) and watches the
     WebSocket feeds.
   - It runs the same analytics and strategies as Part 5, and writes signals under `data\live\live1\`.
   - Stop with **Ctrl-C**. Run the same command to resume: it backfills what it missed and blocks signals until it
     has caught up.
2. Watch it on the dashboard (Part 13). If a feed goes stale, a STALE alert opens and signals are blocked until the
   feed recovers.
3. **Next day:** once Binance has published the day's archive, download it (Part 3) and compare live with history:
   ```bash
   python -m goldflow.cli parity --start 2026-11-02T00:00:00Z --end 2026-11-03T00:00:00Z --from-capture
   ```
   It must report `"all_equal": true`.

**Tip:** measure the delay between a trade and its arrival over the first days, and set `live.grace_ms` above it
(3000 ms by default).

---

## Part 11. MT5 demo trading

Orders go to an **MT5 demo account** through the GoldXFlow Expert Advisor (EA). The EA reports every broker event
back. A REAL account is refused at every level in this mode.

**One-time setup**
1. Complete Part 8 step 1 (the `broker` block). The bridge refuses to start without it.
2. Create the shared secret in MT5's common folder:
   ```bash
   python -m goldflow.cli bridge-secret --out "%APPDATA%\MetaQuotes\Terminal\Common\Files\goldflow\bridge.secret"
   ```
   Then set `"secret_file"` in the `bridge` block to that same path.
3. Copy `mt5\GoldflowExecutor.mq5` into MT5's data folder (*File → Open Data Folder*) under `MQL5\Experts\`.
4. Open it in **MetaEditor** and compile it (**F7**). Fix anything the compiler reports. *This EA has not been
   compiled before; tell me the exact compiler messages if you get stuck.*
5. In MT5, *Tools → Options → Expert Advisors*:
   - tick **Allow algorithmic trading**;
   - add **`127.0.0.1`** to *Allow WebRequest for listed URL*.
6. Log in to your **demo** account. Drag **GoldflowExecutor** onto an **XAUUSD** chart.
   - Leave `InpAllowReal = false`.
   - `InpMagic` must equal `bridge.magic`.
   - The Experts tab should show `goldflow: executor ready (... DEMO account)`. If it shows "HMAC self-test FAILED",
     do not continue.

**Every session**
1. Start the bridge (it includes live paper mode):
   ```bash
   python -m goldflow.cli bridge-run --profile demo --name demo1
   ```
2. Before any order it:
   - connects to the EA;
   - checks the account is DEMO;
   - checks the broker symbol against your `broker` block;
   - reconciles positions.
3. Each signal then passes the risk engine before being sent:
   - data quality, session, spread, slippage;
   - loss limits, position count, stop validity, exposure.

   Orders change state only on the broker's confirmation.
4. **Emergency stop** (closes every GoldXFlow position and blocks new orders):
   ```bash
   python -m goldflow.cli emergency-stop --name demo1 --reason "why"
   ```
   Check in MT5 that no position with the GoldXFlow magic number is left. Resume only when you know the cause:
   ```bash
   python -m goldflow.cli emergency-reset --name demo1 --operator "your name"
   ```
5. Status and audit:
   ```bash
   python -m goldflow.cli bridge-status --name demo1
   ```
   ```bash
   python -m goldflow.cli audit-verify --name demo1
   ```

Run the demo for **2–4 weeks**. Make sure every situation happens at least once: a partial fill, a rejection, an SL
close, a TP close, a manual close, an EA restart, an engine restart and an emergency stop.

---

## Part 12. Going live (only after the gate)

Real-money trading needs **all** of the following, in this order. A missing step refuses the start.
1. Passed backtests and research (Parts 8–9), or your explicit, written decision to trade without them.
2. The demo period. Then run the gate:
   ```bash
   python -m goldflow.cli exec-gate --name demo1
   ```
   It must pass: DEMO only, at least 14 days, at least 20 fills, every lifecycle path exercised, clean reconciliation,
   and slippage within 1.5× the backtest model.
3. Record your approval:
   ```bash
   python -m goldflow.cli exec-gate --name demo1 --operator "your name"
   ```
   This writes `data\state\live_gates.json`.
4. Set `execution.live_trading_enabled` to `true` in the config.
5. In the EA inputs, set `InpAllowReal = true`, on the **real** account's chart.
6. Start with the live profile:
   ```bash
   python -m goldflow.cli bridge-run --profile live --name live-real
   ```

Read `docs\PRODUCTION_READINESS.md` §4 (the checklist you sign) before step 6.

---

## Part 13. Daily operation: dashboard, alerts, backups

**Dashboard**
- Start it with `python -m goldflow.cli dashboard` and open http://127.0.0.1:8765/.
- To require a password, set an environment variable before starting it:
  ```bash
  setx GOLDFLOW_DASHBOARD_TOKEN "a-long-random-text"
  ```
  Then open `http://127.0.0.1:8765/?token=...`.
- The page is read-only; nothing on it can place or close a trade.

**Alerts.** `python -m goldflow.cli ops-check` evaluates them once; it exits with 2 if a critical alert is open. The
dashboard also evaluates them on every refresh.

| Alert | Meaning | What to do |
|---|---|---|
| STALE_FEED | a feed is silent, or the live runner is down | check the runner; it resumes from its checkpoint |
| DQ_OPEN | a data gap or recovery is open; signals are blocked | wait, or backfill (API key) |
| BRIDGE_DOWN | the EA is silent | check MT5 is running and the EA is attached |
| RECONCILE_MISMATCH | the broker's positions differ from GoldXFlow's | compare `bridge-status` with MT5; trading stays blocked until they match |
| RISK_LIMIT | the emergency stop is latched | find the cause; then `emergency-reset` |
| AUDIT_BROKEN | an audit or ledger file was altered | stop; restore from backup |
| DISK_LOW | free space is low | delete old backups or `data\cache` |

To get alerts pushed to you, set `ops.webhook_env` to the name of an environment variable that holds your webhook URL
(Slack, Telegram relay, etc.).

**Backups** (daily recommended; use another disk, D9):
1. Make a snapshot:
   ```bash
   python -m goldflow.cli backup --dest D:\goldflow-backup
   ```
2. Check it:
   ```bash
   python -m goldflow.cli backup-verify --src D:\goldflow-backup\<snapshot>
   ```
3. To restore, use an **empty** folder, then point `paths.data_root` at it:
   ```bash
   python -m goldflow.cli restore --src D:\goldflow-backup\<snapshot> --target D:\goldflow-restored\data
   ```

**Automatic start on Windows.** From an administrator PowerShell in the project folder:
```bash
powershell -ExecutionPolicy Bypass -File deploy\windows\install_tasks.ps1
```
It registers tasks for the bridge (demo profile), the dashboard, the one-minute alert check and the daily backup. On a
Linux VPS, use `deploy\systemd\`. Full procedures are in `docs\RUNBOOK.md`.

---

## Part 14. Troubleshooting

| Message or symptom | Cause | Fix |
|---|---|---|
| `HTTP 451 ... service unavailable from this location` | Binance REST blocks US hosts | run on your PC (India) or a non-US VPS; for history only, add `--no-rest` |
| `the acquisition range is frozen at ...` | you passed `--start/--end/--months` but a range is already frozen | add `--new-range` to use your range, or drop the arguments |
| `invalid configuration: - <block>.<key>: ...` | a config value is wrong | fix the named key; the message says what is allowed |
| `broker.name is not set (decision D2)` | backtest or bridge without the broker specification | Part 8, step 1 |
| `no backtest run at ...` | `perf-report` for a run that does not exist | run `pipeline --name X --backtest` first |
| `bridge secret missing or shorter than 32 bytes` | no secret file and no environment variable | Part 11, step 2 |
| `bridge-run routes orders: use --profile demo` | started without a trading profile | add `--profile demo` |
| `live profile refused: ...` | a live gate is missing | Part 12 |
| EA: `HMAC-SHA256 self-test FAILED` | the compiled EA's hashing differs from Python's | send me the compiler version and EA log |
| EA: `connect 127.0.0.1:47011 failed` | the bridge is not running, or 127.0.0.1 is not allowed | start `bridge-run`; Part 11, step 5 |
| EA: `REAL account and InpAllowReal = false` | the EA is on a real account | use the demo account (or Part 12) |
| Dashboard alert `SYMBOL_MISMATCH` | the broker's XAUUSD differs from your `broker` block | update the block, then re-run backtests before trading |
| `checkpoint ... was taken with processing settings ...` | you changed analytics settings and tried to resume | start a new run with another `--name` (or without `--resume`) |
| A run stops with exit code 3 | it was stopped by `--budget` or Ctrl-C | run it again with `--resume` |
| `raw archive problem: unaccounted=[...]` | an unknown file inside `data\raw\binance\` | remove the stray file |

---

## Part 15. Command reference

Run any command with `python -m goldflow.cli <command> [options]`. Options shared by most commands:
- `--config FILE` (default `config\binance_xauusdt.json`);
- `--name N` (a run name; resumable runs are keyed by it);
- `--resume`;
- `--budget SECONDS`;
- `--start/--end ISO` and `--dates YYYY-MM-DD ...`.

| Area | Commands |
|---|---|
| Data (Phases 2–4) | `discover`, `acquire`, `rest`, `manifest`, `normalize`, `validate`, `report`, `replay` |
| Analytics and signals (5–11) | **`pipeline`**, `process`, `reconcile`, `vprofile`, `vprofile-verify`, `vprofile-export`, `orderflow`, `orderflow-verify`, `ib`, `ib-verify`, `auction`, `auction-verify`, `auction-review`, `signals`, `signals-verify` |
| Order book (8) | `depth-record`, `depth-replay`, `depth-normalize` |
| Backtest and research (12–13) | `broker-import`, `backtest`, `perf-report`, `study`, `ledger-verify` |
| Live (14) | `live-run`, `parity` |
| Other venues (15) | `venue-import`, `venue-run`, `basis`, `volume-oz`, `providers-report` |
| MT5 execution (16) | `bridge-secret`, `bridge-run`, `bridge-status`, `emergency-stop`, `emergency-reset`, `exec-gate`, `audit-verify`, `ea-vector` |
| Operations (17) | `dashboard`, `ops-status`, `ops-check`, `backup`, `backup-verify`, `restore`, `status`, `providers` |

**Exit codes:** 0 = success; 1 = a check failed (read the report); 2 = refused (the message says why); 3 = stopped
early (use `--resume`).

---

## Part 16. Decisions you own

The program refuses or marks results as PROPOSED until you decide:

| Decision | What it covers |
|---|---|
| **D2** | broker specification and tick history (unlocks backtests, research and MT5) |
| **D6** | where live runs (your PC, or a non-US VPS) |
| **D7** | risk limits |
| **D8** | approval thresholds (`max_oos_drawdown`) |
| **D9** | backup location |
| D1 | IVB definition |
| D4 | sessions |
| D11–D13 | analytics and strategy parameters |
| D3 | confirm live trades = REST individual trades including RPI |

Status of each one: `docs\PRODUCTION_READINESS.md`. Technical details of each phase:
`docs\PHASE<N>_COMPLETION_REPORT.md`. Procedures: `docs\RUNBOOK.md`.
