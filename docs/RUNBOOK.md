# GoldXFlow runbook (Phase 17)

Every command is `python -m goldflow.cli <command> [--config config/binance_xauusdt.json]`, run from the project
folder. "Data root" means `paths.data_root` (default `data/`).

## 1. Daily operation

### Start (Linux VPS)
```bash
systemctl start goldflow-live goldflow-dashboard
```
```bash
systemctl enable --now goldflow-ops-check.timer goldflow-backup.timer
```

### Start (Windows trading PC)
1. Start MT5 and log in to the **demo** account.
2. Check that `GoldflowExecutor` is attached to XAUUSD and "algo trading" is on.
3. Run `deploy\windows\install_tasks.ps1` once to register the tasks. After that they start at logon.

### Stop
- Linux:
  ```bash
  systemctl stop goldflow-live
  ```
- Windows: end the `GoldXFlow-Bridge` task, or press Ctrl-C in its console.

A normal stop writes a final checkpoint. The next start resumes with a RECOVERY interval: signals stay blocked until
the trades since the last durable id are backfilled.

### Watch
- **Dashboard:** `http://127.0.0.1:8765/`. From a VPS, reach it through an SSH tunnel:
  ```bash
  ssh -L 8765:127.0.0.1:8765 vps
  ```
- **Alerts:** open alerts are in `data/ops/alerts_open.json`, every transition is in `data/ops/alerts.jsonl`, and
  each one is also logged at CRITICAL/WARNING. To get them pushed somewhere, set `ops.webhook_env`.
- **Health:** `GET /api/health` returns 503 while any critical alert is open, which suits uptime monitors.

## 2. Emergency stop
```bash
python -m goldflow.cli emergency-stop --name demo1 --reason "what happened"
```
1. The running bridge picks this up within one tick: it latches the stop, sends `FLATTEN_ALL`, and repeats until the
   broker reports no positions.
2. **Verify in MT5** that no position with the GoldXFlow magic number is left.
3. If the bridge itself is down, close the positions in MT5 by hand. Reconciliation adopts those closes on the next
   start (`CLOSE_ADOPTED`).

**Resume** only after the cause is understood:
```bash
python -m goldflow.cli emergency-reset --name demo1 --operator YOUR_NAME
```

A daily-loss or drawdown latch stays latched until reset. The daily-loss check itself rearms at `risk.day_reset_utc`.

## 3. Recovery procedures

| Situation | What you see | What to do |
|---|---|---|
| **Process crash** (live or bridge) | `STALE_FEED` (checkpoint age), `BRIDGE_DOWN` | The supervisor restarts it. Live resumes from its checkpoint with RECOVERY and backfill. The bridge reloads its state, re-handshakes and **reconciles before any order**; an order in flight is adopted from the broker or re-sent with its idempotency key, never duplicated. Check the dashboard: `blocked` must become empty and `last reconcile` must be clean. |
| **Data gap** (feed stale, suspected gap) | `STALE_FEED`, `DQ_OPEN`, `suspected_gaps` > 0 | Signals are already blocked while the interval is open. With an API key, gaps are backfilled automatically. Otherwise, after Binance publishes the day, re-acquire and normalise it (`acquire`, `normalize`); the Phase 3 catalog then records the real coverage. |
| **Broker outage** (EA reports rejects or no connection) | orders REJECTED with retcode 10031/10018, `BRIDGE_DOWN` | Nothing is retried automatically after a broker rejection. Wait for MT5 to reconnect. Reconciliation runs on reconnect. Do not reset an emergency stop during an outage. |
| **EA restart or MT5 restart** | `EA_CONNECTED` alert, then trading resumes | Automatic: handshake, symbol check, reconciliation. If `SYMBOL_MISMATCH` appears, the broker changed the contract: update the `broker` block, then re-run the Phase 12 backtests before trading. |
| **Reconciliation mismatch** | `RECONCILE_MISMATCH` (critical) | Trading is blocked. Compare `bridge-status` with MT5. An unknown position with our magic number means someone placed a manual order with the EA's magic, or something was duplicated: close it in MT5 if it should not exist. Blocks clear on the next clean reconciliation. |
| **Audit or ledger broken** | `AUDIT_BROKEN` | Stop trading. Restore the file from the last verified backup (§5) and investigate how it changed. Never edit the audit log by hand. |
| **Disk low** | `DISK_LOW` | Free space (old backups, `data/cache`). The live runner keeps going if a checkpoint fails (`checkpoint_errors`). The bridge **blocks trading** if it cannot persist (`PERSISTENCE_FAILED`). |
| **Catalog damaged** | `CATALOG` (integrity not ok) | Stop the jobs, restore `data/state/acquisition.sqlite` from the backup, then run `validate` (it must report `identical_to_previous_run`). |

## 4. Changing things safely
- **Configuration.** Every change is validated at start. Risk limits can only be made stricter than `backtest.risk`.
  Changing analytics or strategy parameters invalidates comparisons with earlier results; record the change in the
  research ledger.
- **Adding a provider.**
  1. Add an adapter under `goldflow/providers/<venue>/` that implements the Phase 1 contract with honest capabilities.
  2. Add its identity to `schemas.VENUES` and its mapping record to `venues/mapping.py`.
  3. Add a parser to `venues/archives.py`, with a completeness rule (provable or UNVERIFIED).
  4. Add tests with synthetic archive files.
  5. Run `venue-import`, `venue-run` and `providers-report`. The venue keeps its own partitions and catalog; nothing
     is merged.
- **Going live.** It needs, in order:
  1. a demo run of 2–4 weeks;
  2. `exec-gate --name demo1 --operator NAME` passing, which writes `data/state/live_gates.json`;
  3. `execution.live_trading_enabled = true`;
  4. the EA input `InpAllowReal = true`;
  5. `--profile live`.

  Any one missing refuses the start.

## 5. Backups
**Daily** (timer or scheduled task):
```bash
python -m goldflow.cli backup --dest /mnt/backup/goldflow
```
```bash
python -m goldflow.cli backup-verify --src /mnt/backup/goldflow/<snapshot>
```
- Snapshots are complete directories. Unchanged files are hard links to the previous snapshot, and databases are
  copied with SQLite's online backup.
- Keep the backup destination on another disk, or sync it off-site (decision D9). Prune old snapshots by deleting
  whole directories; hard links keep the shared files alive.

**Restore**, into a new, empty folder only:
```bash
python -m goldflow.cli restore --src /mnt/backup/goldflow/<snapshot> --target /opt/goldflow/data.restored
```
Then point `paths.data_root` (and `state_db`) at it, or swap the folders while everything is stopped. Run `validate`
and check that the dashboard shows the expected coverage.

**Restore drill** (monthly): restore to a scratch folder and rebuild something with a known digest, for example a
`process` run with its `state_digest`. It must be identical. Phase 17 did this with the 4-day archive (see the
report).

## 6. Logs and retention
- `data/state/goldflow.jsonl` is JSON lines and rotates at `ops.log_max_mb` (default 50 MB), keeping
  `ops.log_backups` files (default 10).
- Audit logs and the research ledger are **never rotated or pruned**: they are evidence. They are small, and they go
  into every backup.
- Alerts are in `data/ops/alerts.jsonl` (append-only).
