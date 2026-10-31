# Phase 2: Binance XAUUSDT six-month acquisition report

Acquisition run: 2026-10-30. Tool: `goldflow` v0.2.0. All times are UTC.

## Instrument
Binance USDⓈ-M **XAUUSDT**, contract type `TRADIFI_PERPETUAL`, underlying `COMMODITY` (spot gold). Margin USDT, tick 0.01, step 0.001 oz, min notional 5 USDT. Listed 2025-12-11. It trades 24/7, with no zero-trade stretch longer than 5 minutes in six months.
Bybit `XAUTUSDT` / Binance `XAUTUSDT` and `PAXGUSDT` are token perps, not XAU, and are excluded.
The instrument was discovered and verified from the archive listing. The REST `exchangeInfo` spec check is blocked from the acquisition host (HTTP 451) and must be re-run from a permitted location (`python -m goldflow.cli discover`).

## Requested range
`[2026-04-30T08:42:51.101Z, 2026-10-30T08:42:51.101Z)`. That is six calendar months ending at the acquisition timestamp, half-open, frozen in `data/state/range.json`.

## What was downloaded (all SHA-256 verified against Binance `.CHECKSUM`; 497/497 pass `sha256sum -c`)

| Dataset | Files used | Records in files | Archive coverage | Uncovered (reason) |
|---|---|---|---|---|
| trades (primary) | 64 (4 monthly + 60 daily) | 149,511,048 (149,273,397 inside range) | 2026-04-30 00:00 → 2026-10-29 23:59:59.381 | 10-30 00:00 → 08:42:51 (day not yet archived; REST 451) |
| aggTrades | 34 (5 monthly + 29 daily) | 60,365,927 | same | 10-28 whole day (not published, 404); 10-30 tail |
| klines 1m (cross-check) | 33 | 260,640 | → 2026-10-27 23:59 | 10-28 and 10-29 not yet published; 10-30 tail |
| bookDepth | 182 daily | 6,165,348 | same as trades | 10-08 (not published, 404); 10-30 tail |
| metrics | 183 daily | 52,704 | same as trades | 10-30 tail |

Raw data is 2.3 GB, kept byte-for-byte as published. Trade volume inside the range is 55,137,092.7 oz.

## Verification results
- **Trades vs Binance 1m klines** (2026-04-30 08:43 → 2026-10-27 23:59; 260,117 minutes): klines count 145,904,847 trades and the trades files hold 145,904,846. The single missing trade (2026-09-03 09:07:28.516, id 156621458, 0.353 oz) was recovered from aggTrades.
- **Trades id gaps:** 732,500 gaps (834,376 ids). Because the klines count matches, these are **ids Binance never used for public trades**, not lost data. They are recorded per file in `_verify/*.idgaps.jsonl.gz`.
- **Trades vs aggTrades (quantity per aggTrade row):** 60,256,218 / 60,267,805 rows match exactly. Every mismatch sits on two days:
  - **2026-10-29: TRADES_INCOMPLETE.** The trades file (re-published by Binance 2026-10-30 08:27) is missing ~37k trades (19,344 oz), almost all between **22:00 and 24:00 UTC**. 4,767 single trades were fully recovered from aggTrades. The rest are known only as aggregates. Treat 10-29 22:00–24:00 as incomplete until Binance republishes; the downloader detects republishing via ETag.
  - **2026-10-27: AGGTRADES_INCOMPLETE.** 21,346 trades are absent from aggTrades, in one hole from 00:02:47 to 00:30:05. The trades file is complete here (it matches the klines count).
  - Minor: across 38 days, 2,437 trades are absent from aggTrades. Trades remains the source of truth.
- **Monthly file defect:** `XAUUSDT-trades-2026-06.zip` (monthly) contains 76 duplicated ids, each with a wrong timestamp several days later. It was rejected, kept on disk as evidence, and June was rebuilt from 30 verified daily files.
- There are 0 duplicate ids, 0 backwards timestamps and 0 invalid prices or quantities in the files that were accepted. The longest no-trade interval in accepted trades files is 81.8 s.
- **bookDepth snapshot holes:** 845 holes longer than 2 minutes (65.4 h in total). Two of them, together with the missing 10-08 file, form one continuous bookDepth outage from **2026-10-07 02:49 to 2026-10-09 04:05** (~49 h). The other 843 fall between 2026-04-30 and 2026-06-03, at roughly 25 per day and about 4 minutes each. Depth context is unreliable for that stretch.
- 4 one-id discontinuities fall at day boundaries. They are consistent with unused ids, since klines counts match.

## Open gaps (not silently filled)
1. 2026-10-30 00:00 → 08:42:51 for all datasets. It isn't archived yet. Recovery: rerun `acquire` after Binance publishes the 10-30 files, or run `python -m goldflow.cli rest` from a host where `fapi.binance.com` is allowed. Individual trades via REST additionally need `BINANCE_API_KEY`.
2. aggTrades 2026-10-28 (not published). The REST unit is pending a permitted host.
3. bookDepth 2026-10-07 02:49 → 2026-10-09 04:05 (10-08 file not published, and the neighbouring files are truncated; no REST equivalent exists for history).
4. trades 2026-10-29 22:00–24:00 is incomplete upstream (see above).

## Level 2
There is no free historical price-level L2 for XAUUSDT. See `L2_ORDERBOOK_AVAILABILITY.md`. No artificial book was generated.
