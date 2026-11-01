# Production readiness (Phase 17): documented, not assumed

Status as of 2026-10-31. Each line says what is **verified, and how**, or what is **pending, and on what**.

| Status | Meaning |
|---|---|
| ✅ | verified here, with evidence |
| 🟡 | built and tested in the build environment; not yet run on the production host |
| ⛔ | blocked on a decision or an external dependency |

## 1. Data and analytics
| Item | Status | Evidence |
|---|---|---|
| History acquisition, normalisation and validation (Phases 2–3) | ✅ | 4-day archive: `validate` result digest identical across code changes (most recently with the Phase 15/17 changes) |
| Deterministic replay, processing and analytics (Phases 4–11) | ✅ | Six state digests (processor, profile, orderflow, IB, auction, strategies) unchanged since Phase 13. They are **also identical when rebuilt from a restored backup** |
| Live path = historical path (Phase 14) | ✅ | Parity: 26 outputs exactly equal on 3,197,220 real trades |
| Live REST trades on the production host | ⛔ D6 | HTTP 451 from the build host. Run `live-run` on the non-US host, then `parity --from-capture` the next day |
| Other venues (Phase 15) | ✅ | OKX 4/4 days COMPLETE; Bybit UNVERIFIED by design; basis verified point for point |

## 2. Execution and risk
| Item | Status | Evidence |
|---|---|---|
| Order lifecycle only on broker confirmation; no duplicates under faults | 🟡 | 13 fault-injection tests against the simulated EA, plus the Phase 17 crash-mid-order and broker-outage tests |
| MQL5 EA compiled and running | ⛔ | Needs MetaEditor and an MT5 terminal on Windows. Its HMAC self-test vector is shared with Python and test-checked |
| Broker contract specification | ⛔ D2 | The bridge refuses to start without the `broker` block |
| Risk limits | ⛔ D7 | Proposed values in `backtest.risk`; live limits can only be stricter |
| Demo period and M7 gate | ⛔ D2/D6 | `exec-gate` built and tested (it refuses short or incomplete evidence) |
| Live trading off by default | ✅ | `live_trading_enabled` false; live profile locked without the M7 record (tested); EA refuses REAL accounts by default |

## 3. Operations
| Item | Status | Evidence |
|---|---|---|
| Dashboard shows real state only | ✅ | Every value carries its source file; missing values say why. The test on an empty root asserts no value without a source. Rendered on the real 4-day root in headless Chromium with 0 console errors. A synthetic backtest is labelled SYNTHETIC |
| Critical errors and stale feeds visible | ✅ | Alert rules (stale feed, DQ, reconciliation mismatch, risk limit, bridge down, audit broken, disk, catalog, errors) open and close with a logged lifecycle (tested). On this host the real `DISK_LOW` warning (11.6 % free) opened |
| Backup restoration tested | ✅ | 242 MB real data root: snapshot (284 files; 4 databases with integrity ok), incremental snapshot (278 hard links), verify, restore. **All six digests rebuilt from the restored copy are identical.** Tamper detection and unsafe restore targets are tested |
| Log rotation and retention | ✅ | Rotating JSON log (50 MB × 11 files by default). Audit logs and ledgers are never pruned |
| Secrets | ✅ design / 🟡 host | Never in the config: environment file, Windows user environment or the MT5 Common Files secret (≥ 32 bytes, refused otherwise) |
| Service supervision | 🟡 | systemd units and timers, and a Windows scheduled-task script, in `deploy/`. Not installed from the build environment |
| Stress: high event rate | ✅ | The live path at maximum speed on 3.2 M real trades: 3,162 trades/s (342× real time); slowest poll 54.6 ms against a 750 ms interval (Phase 17 report §4) |
| Stress: disconnections, rate limits, crashes, broker outage, disk/persistence failure | ✅ (simulated) | Tests: REST 429 bursts hold the clock and raise STALE then RECOVERED; a failed checkpoint is counted while ingestion continues; the bridge blocks on persistence failure; crash mid-order recovers without duplication; a broker outage gives no retry storm |
| Database failure | ✅ (partial) | SQLite integrity is checked by the status page (`CATALOG` alert) and by every backup. A failing live checkpoint does not stop ingestion. A PostgreSQL/TimescaleDB migration is **not** needed yet: there is a single writer per database |
| Runbook | ✅ | `docs/RUNBOOK.md`: start/stop, emergency stop, recovery after a crash, data gap or broker outage, adding a provider, backups and restore drill |

## 4. Before real money (the checklist the operator signs)
1. D2: broker specification and tick history imported. Phase 12 backtests and Phase 13 studies run, with an approval
   (D8), or an explicit decision to trade without one.
2. D6: production host running `live-run` for at least a week. Its first day passes `parity --from-capture`, and RPI
   share and receive latency are measured (`grace_ms` set above the p99.9).
3. EA compiled; demo run for 2–4 weeks; `exec-gate` passing; the operator approves in writing.
4. A restore drill was done on the production backup destination in the last 30 days.
5. Emergency stop rehearsed on demo: positions flattened, then reset by a named operator.
6. Alerts reach a human (webhook or monitored log) and were tested by forcing one, for example by stopping
   `goldflow-live`.
