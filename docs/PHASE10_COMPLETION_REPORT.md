# Phase 10 completion report: sessions, Initial Balance and IVB

Date: 2026-10-31. Specification: `docs/METRICS_IB_V1.md`.

## 1. Scope completed / not completed
**Completed**
- **Session calendar.** Still the single Phase 5 source of truth, with a new **holiday calendar** (`holidays`, PROPOSED). CLOSED dates skip the IB; MODIFIED_HOURS dates are labelled. The calendar only labels, so the Phase 5 session instances are unchanged and so are every earlier digest and dataset.
- **Initial Balance.** Configurable per session (offset or local start, length). Developing snapshots are built only from completed bars; the final row has high, low, mid, range, volume, sides and the k × range extensions.
- **IB behaviour.**
  - breakouts, with the documented volume and delta filters (reported only);
  - failed breaks;
  - first touches of each extension (range expansion);
  - a session summary.
- **Acceptance outside the IB.** IB levels feed the Phase 9 auction engine as `IB_HIGH`, `IB_LOW` and `IB_MID`, and `ib` is now a default level source.
- **IVB.** Ships **disabled**. It is refused at startup until a full, signed-off definition and its implementation exist, and it is never replaced by another metric.
- **Tooling.** `ib` and `ib-verify` CLI commands, the Parquet dataset with manifest, and checkpoint state guarded by a settings hash.

**Not completed**
- **IVB itself.** Needs your definition (D1).
- **Holiday calendar check.** It must be checked against CME's published schedule (the entries are PROPOSED).
- **Six-month run.** Run it on the PC.

## 2. Files
**Created**
- `goldflow/processing/holidays.py`;
- `goldflow/orderflow/initial_balance.py`, `ib_verify.py`, `ivb.py`;
- `tests/test_ib.py` (12 tests);
- `docs/METRICS_IB_V1.md` and this report.

**Modified**
- `goldflow/orderflow/auction_settings.py` and `auction_engine.py`: the `ib` level source; IB levels arrive through a listener; the sink order is checked.
- `goldflow/settings.py`: validation of `initial_balance`, `holidays` and `ivb`.
- `goldflow/cli.py`: `ib` and `ib-verify`; `auction` now wires the `IBEngine` in.
- `config/binance_xauusdt.json`: new `holidays`, `initial_balance` and `ivb` blocks, and `ib` added to `auction.levels.sources`.
- `tests/test_auction.py`: IB engine wiring.
- `README.md`, `docs/BUILD_GUIDE.md`, `docs/METRICS_AUCTION_V1.md`.

## 3. Architecture
```
TradeProcessor ─ sinks: [ProfileEngine, IBEngine, AuctionEngine]
  IBEngine: on_bars(1s) → per-minute IB aggregates; the first bar at/after the IB end finalises the IB
            → final row, and IB levels to the AuctionEngine (listener)
            on_bars(1s) after the IB end → extension touches;  on_bars(5m) → breakouts / failed breaks
            on_advance → developing snapshots, session summaries
```

## 4. Commands
```bash
python -m goldflow.cli ib --name i1 [--resume]
python -m goldflow.cli ib-verify
python -m goldflow.cli auction --name a1      # now includes IB_HIGH / IB_LOW / IB_MID levels
```

## 5. Tests
- `scripts/check.sh`: **all checks passed**, with **179 tests** (167 earlier plus 12 new) and mypy --strict on 61 files.
- The new tests cover:
  - IB windows across the US and UK DST changes, including the gap weeks in March and late October;
  - invalid settings refused: an IB outside its session, offset and start both given, an unknown session, a bad length, duplicate extensions, a bad interval;
  - the holiday calendar;
  - IVB refused with an incomplete definition and with a complete one, and `ivb()` raising;
  - on synthetic replay:
    - a clock check;
    - developing snapshots before the IB end only;
    - every event type produced;
    - verification passes;
    - stop/resume at two points, chunk size 61 and live-style chunks give identical state and rows;
    - a settings change refuses resume;
    - a CLOSED holiday skips the IB and a MODIFIED one is labelled;
    - IB levels reach the auction engine at the right time (London 08:00 GMT + IB length).

## 6. Real-data evidence (cloud, 4 days, 2026-10-23 → 26)
| Check | Result |
|---|---|
| Replay / Phase 5 digests | `8f528ccd…` / `4e235890…`: unchanged |
| IB state digest | `e6d2c1e6…`; identical after stopping on Saturday 13:37 and resuming |
| `ib-verify` | **Pass**: 4 final IBs, 240 snapshot rows, 44 events recomputed exactly from trades; 0 missing; 0 timing violations |
| Auction with IB levels | 60 levels (12 from IBs); `auction-verify` passes 4/4 days (1,115 episode events, 2,654 outcomes); state digest `e66e56a8…` reproduced |
| Throughput | `ib` 743 k events/s (4.5 s for 3.34 M events) |

**IBs**
| IB | UTC window | Range | Notes |
|---|---|---|---|
| London, Fri 23 Oct | 07:00–08:00 (BST) | $9.95 | breakout down at 08:05 failed by 08:15; breakout up at 08:45; extensions up to 1× |
| New York, Fri 23 Oct | 12:20–13:20 (EDT) | $10.49 | broke down at 13:50, failed at 13:55, broke down again at 14:05 and ran to the 3× extension by 14:21 |
| London, Mon 26 Oct | 08:00–09:00 (GMT, after the UK change) | $12.40 | |
| New York, Mon 26 Oct | 12:20–13:20 (US still EDT) | $15.46 | |

**IB events (4 days):** 14 breakouts, 9 failed breaks and 17 extension touches.

**Through the auction engine:** IB levels produced 82 acceptances, 146 rejections and 41 failed auctions.

## 7. Limitations and open decisions
- **D1 (IVB definition):** still open. IVB is unavailable until you supply its definition.
- **D4 (sessions and IB windows):** the defaults are PROPOSED.
- **Holiday calendar:** PROPOSED; verify it against CME.
- Extension touches have 1 s resolution.

## 8. Acceptance criteria
| Criterion | Status | Evidence |
|---|---|---|
| Session boundaries identical in replay and live | **Passed** | One code path (`TradeProcessor` sinks); hand-built live-style chunks give identical IB rows, events and levels |
| DST transition weeks tested (US and UK change on different dates) | **Passed** | `test_ib_windows_across_us_and_uk_dst_changes` (March and October/November 2026); real data across 25 Oct |
| IVB implemented only after formula sign-off | **Passed (blocked by design)** | `ivb.enabled` is refused at startup; `IVBUnavailable`; documented in `METRICS_IB_V1.md` §4 |

## 9. Next phase
Phase 11: the strategy interface and signal generation. It can be built now. Its three example research strategies use the Phase 7, 9 and 10 outputs: a failed auction at VAH/VAL with delta, absorption at an HVN or VWAP band, and an IB failed break.
