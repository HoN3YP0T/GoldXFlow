# Historical Level 2 order book: availability for Binance XAUUSDT (USDⓈ-M)

Investigated 2026-10-30. Decision: **no artificial order book history is generated.**

## What exists in the free public archive (data.binance.vision, futures/um)

| Dataset | XAUUSDT availability | Is it price-level L2? |
|---|---|---|
| `bookDepth` (daily) | 2025-12-11 → latest closed day | **No.** ~30 s snapshots of *cumulative* depth and notional at ±0.2 % and ±1…5 % bands from mid (12 rows per snapshot, verified across all 182 daily files in Phase 3). Useful for coarse liquidity context only. |
| `bookTicker` (daily/monthly) | **Not published** for XAUUSDT (empty listing) | Top of book only, and absent anyway. |
| `trades` / `aggTrades` | Available | Trades, not book. |
| Price-level depth snapshots / diffs | **Not published** for USDⓈ-M futures | — |

The ±% bands are far too coarse for gold. Even the narrowest band, ±0.2 % of $4,000, is ±$8, and the next is ±1 % = ±$40, so tick-level book behaviour (absorption at a level, spoofing, depletion) cannot be reconstructed from them.

## Other routes

1. **Binance REST `/fapi/v1/depth`.** This is the *current* snapshot only (≤1000 levels). It has no history.
2. **Binance WebSocket `<symbol>@depth@100ms`** diff stream plus a REST snapshot. This is the correct way to build *our own* L2 history going forward. It needs `U`/`u`/`pu` sequence tracking and resync on any break. That fits Phase 7's zero-silent-loss rules. Note that `fapi.binance.com` returns HTTP 451 from US cloud IPs, so the recorder must run from a permitted location (e.g. the trading PC in India or a non-US VPS).
3. **Binance account-gated historical data** (tick-level order book via the SAPI data-download service). This historically required an API key and an application. Its availability for TradFi perpetuals like XAUUSDT is **unverified** and was not tested here.
4. **Paid vendors** (e.g. Tardis.dev `incremental_book_L2` for binance-futures). They are likely to cover XAUUSDT since its 2025-12-11 listing. Cost and coverage are to be confirmed before purchase.

## Implication for later phases

- Orderflow metrics that need only trades (volume profile, VWAP, delta, CVD, big trades, footprint imbalances, absorption *inferred from trades*) can be backtested on the six-month dataset now.
- Metrics that need the book (liquidity depletion, book imbalance, queue behaviour) **cannot be backtested historically** from free data. They should be flagged `requires_l2_history` and enabled only on periods covered by our own recorder or by purchased data.
