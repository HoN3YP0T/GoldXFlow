# Phase 17 completion report: dashboard, observability, deployment and production hardening

Date: 2026-10-31. Procedures: `docs/RUNBOOK.md`. Readiness status: `docs/PRODUCTION_READINESS.md`.

## 1. Scope completed / not completed
**Completed** (`goldflow/ops/`, `deploy/`)
- **Status snapshot** (`ops-status`), built from engine state on disk only. Every value carries its source file and
  time. A missing value is `null` with the reason, never a default. It has five sections:
  - **market:** live feed freshness and counters, sessions now;
  - **orderflow:** latest POC, VAL/VAH, VWAP ±1σ and HVN/LVN per window type, CVD, Phase 7 and Phase 9 events, depth
    availability;
  - **trading:** signals; per execution run, orders, positions, P&L, risk state and blocks, emergency latch, EA
    message age, audit chain;
  - **research:** coverage, venues, backtests (synthetic runs labelled), ledger, test lock, studies, reports;
  - **system:** disk, logs, recent errors, catalog integrity, version.
- **Alerts** (`ops-check`; also evaluated by every dashboard refresh):
  - rules: stale feed, DQ open, reconciliation mismatch, risk limit or emergency, bridge down, audit chain broken, disk
    low, catalog integrity, errors in the log, failed status section;
  - an open/close lifecycle in `data/ops/alerts.jsonl`;
  - a log notifier, and an opt-in webhook.
- **Dashboard** (`dashboard`):
  - **read-only** and **loopback-only**, with an optional bearer token;
  - built on the standard library, so it adds no dependency on the trading PC (FastAPI is not installed there);
  - one HTML page with no external resources and a strict content-security policy;
  - `/api/status`, `/api/alerts`, and `/api/health` (503 while a critical alert is open);
  - control actions stay CLI-only.
- **Backups** (`backup`, `backup-verify`, `restore`):
  - SQLite online-backup copies, each checked with `integrity_check`;
  - SHA-256 manifest;
  - incremental hard links for unchanged files;
  - verification re-hashes every file and re-checks the databases, audit chains and research ledger;
  - restore goes into an empty folder only, and is verified again after copying.
- **Logs:** rotating JSON log (`ops.log_max_mb` × `ops.log_backups`). Audit logs and ledgers are never pruned.
- **Deployment templates:**
  - systemd units for live ingestion and the dashboard;
  - timers for alert checks (every minute) and backups (daily);
  - an env-file example for secrets;
  - a Windows scheduled-task script (bridge in the demo profile, dashboard, ops-check, backup).
- **Hardening:**
  - a failing live checkpoint is counted and retried, and ingestion continues;
  - the execution bridge **blocks trading** (`PERSISTENCE_FAILED`) when it cannot write its state or audit log, and
    such a failure never propagates into the analytics pipeline.
- **Runbook and production-readiness document.**

**Fixes found in this phase**
- **Phase 3 regression from Phase 15 (real).** Phase 3's raw registry scanned all of `data/raw/` and refused to run
  ("unaccounted") once OKX/Bybit raw archives existed. It now registers only its own provider's tree. Regression test
  added. `validate` on the real 4 days then gave `identical_to_previous_run: 1`, so Phase 3 outputs are unchanged by
  the Phase 15–17 code.
- **The status page showed "no errors" when no log file existed.** It now says "not available: no log file yet". The
  empty-root test found this.
- **A synthetic backtest appeared unlabelled.** It is now marked "SYNTHETIC broker data (test, not a result)".
- `goldflow.__version__` was still 0.2.0 and is now 0.17.0.

**Not completed**
- **Services installed on a real host.** The templates were not installed or run on a VPS or the Windows PC; that
  needs D6.
- **Alerts reaching a human.** This needs a webhook URL or a monitored log on the production host.
- **PostgreSQL/TimescaleDB.** Not needed while each SQLite database has a single writer; the trigger for migrating is
  documented.

## 2. Files
**Created**
- `goldflow/ops/`: `__init__.py`, `status.py`, `alerts.py`, `dashboard.py`, `backup.py`, `static/dashboard.html`.
- `deploy/`: `README.md`, `systemd/*.service|*.timer|goldflow.env.example`, `windows/install_tasks.ps1`.
- `tests/test_ops.py` (7 tests); one regression test in `tests/test_venues.py`.
- `docs/RUNBOOK.md`, `docs/PRODUCTION_READINESS.md` and this report.

**Modified**
- `goldflow/catalog.py`: raw registry scoped to the provider.
- `goldflow/observability.py`: rotating log file.
- `goldflow/live/runner.py`: checkpoint failure tolerated.
- `goldflow/execution/bridge.py`: persistence failure blocks trading.
- `goldflow/settings.py`: validation of the `ops` block.
- `goldflow/cli.py`: six new commands; log rotation settings.
- `goldflow/__init__.py`: version.
- `config/binance_xauusdt.json`: `ops` block.
- `pyproject.toml`: `goldflow.ops` under mypy --strict; dashboard HTML as package data.
- `scripts/check.sh`, `README.md`, `docs/BUILD_GUIDE.md`.

## 3. Tests
`scripts/check.sh`: **all checks passed**, with **239 tests** (231 earlier plus 8 new) and mypy --strict on 117 files.

- **Status honesty.** On an empty root every value is unavailable with a reason, and no value exists without a source.
- **Alerts.**
  - Fixtures with a stale live checkpoint, an open STALE interval, suspected gaps, a latched emergency,
    reconciliation-mismatch and no-heartbeat blocks, and an edited audit log raise STALE_FEED (critical), DQ_OPEN,
    RECONCILE_MISMATCH, RISK_LIMIT, BRIDGE_DOWN and AUDIT_BROKEN.
  - A repeated evaluation gives no duplicate transition, and removing the causes closes every alert.
  - The webhook is opt-in, and a non-loopback dashboard host is refused.
- **Dashboard.** The API and page are served; POST gives 501 (no write endpoints); the token is enforced (401 without
  it, 200 with it).
- **Backup.**
  - A destination inside the data root is refused.
  - Databases are copied and checked; the incremental snapshot hard-links everything but the databases.
  - The audit chain is verified inside the snapshot.
  - **Restore, then replay: the processor state digest equals the original.**
  - A non-empty target is refused; tampering is detected; restoring a tampered snapshot is refused.
- **Stress and faults.**
  - 25 consecutive REST 429 responses: errors counted, clock held, STALE then RECOVERED.
  - A checkpoint path that cannot be written: errors counted, ingestion continues.
  - Persistence failure: trading blocked, nothing sent, unblocked once the disk is writable.
  - Engine crash after the EA executed: restart, reconciliation adopts the fill, 1 broker order.
  - Broker outage (retcode 10031): REJECTED, 1 attempt, no retry storm; the next signal trades.

## 4. Evidence on real data (the 4-day root: 2026-10-23 → 2026-10-27)
- **Dashboard and status.**
  - Served from the real root and rendered in headless Chromium with 0 console errors.
  - It shows the latest profiles (e.g. UTC day 2026-10-26: POC 4023.50, VA 4009.70–4034.10, VWAP 4016.23), CVD,
    exhaustion and rejection events, the 4 Phase 11 signals, coverage (4 COMPLETE + 1 MISSING), venues (OKX 4
    COMPLETE, Bybit 4 UNVERIFIED) and the backtest labelled synthetic.
  - The live feed, execution, ledger, test lock and depth show "not available" with the reason.
  - One real alert opened: `DISK_LOW` warning (this host has 11.6 % free).
- **Backup and restore of the real data root** (242 MB):

  | Step | Result |
  |---|---|
  | Snapshot | 284 files, 252 MB in 3.8 s; 4 SQLite databases (Phase 3 catalog, validation scratch, OKX and Bybit catalogs) all `ok` |
  | Incremental snapshot | 1.4 s; 278 files hard-linked, 5 copied |
  | Verify | ok, 0 problems |
  | Restore | 283 files restored and re-verified |
  | Restore into a non-empty target | refused |

  The full stack replayed from the **restored** copy gives digests **identical** to the originals: processor
  `4e235890…`, profile `b318578b…`, orderflow `e75e52d2…`, IB `e6d2c1e6…`, auction `e66e56a8…`, strategies
  `bc3e06ab…`.
- **Phase 3 after the Phase 15–17 code changes.** `validate`: 4 days, 3,197,220 trades, 0 quarantined,
  `digest_differences_vs_catalog: []`, `identical_to_previous_run: 1`.
- **High event rate.** The live path ran at maximum speed on real data: 278,688 REST-shaped polls (750 ms) carrying
  3,197,220 trades, through the normaliser, the `LiveBus` and the full Phase 5–11 stack.
  - **3,162 trades/s and 276 polls/s, 342× real time.** The busiest real day averaged about 17.5 trades/s.
  - **Slowest single poll: 54.6 ms**, against a 750 ms poll interval.
  - Peak RSS was 2.19 GB, but most of that is the 4 days of pre-built polls held in memory by the harness; a live run
    holds one poll at a time.
  - The run shared the CPU with the test suite, so these numbers are conservative.

## 5. Open decisions
- **D6: production host.** It is needed to install the services, run live ingestion and the bridge, and route alerts.
- **D9: backup location.** Another disk or off-site sync, and the retention period for snapshots.
- **Alert channel:** which webhook (Slack, Telegram, email relay), if any.
- **Dashboard exposure:** loopback plus SSH tunnel is the default; anything wider needs an authenticated reverse
  proxy.

## 6. Acceptance criteria
| Criterion | Status | Evidence |
|---|---|---|
| No fabricated dashboard values | **Passed** | Every value is sourced; missing values say why (tested on an empty root); synthetic results are labelled; checked on the real root |
| Critical errors and stale feeds are visible | **Passed** | Alert rules and lifecycle tested; real `DISK_LOW` warning raised here; `/api/health` gives 503 while a critical alert is open |
| Backup restoration is tested | **Passed** | Real 242 MB root: restore verified, and the six state digests rebuilt from it are identical |
| Production readiness is documented, not assumed | **Passed** | `docs/PRODUCTION_READINESS.md` marks each item verified, built-but-not-on-host, or blocked, with evidence; `docs/RUNBOOK.md` |

## 7. After Phase 17
All 18 phases of the build guide (0–17) are built. What remains is external:
- **D2:** broker specification and ticks, which unlock backtests, research and the bridge.
- **D6:** a production host, for live REST, services and the demo run.
- **The demo period and the M7 gate.**
- Your decisions on risk (D7), approval thresholds (D8) and backups (D9).
