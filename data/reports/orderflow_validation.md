# Orderflow independent validation (Phase 7)

Each check is recomputed from the Phase 3 partitions by separate code and compared exactly.

| Date | Trades | CVD day = Σbuy−Σsell | Imbalances ref / catalogue / missing / extra | aggTrades split, same ms (must be 0) / multi-ms (info) | aggTrade span max ms | Big-trade ids ok | OK |
|---|---|---|---|---|---|---|---|
| 2026-04-30 | 620,360 | = -6,953,836 | 10,641 / 10,641 / 0 / 0 | 0 / 31472 | 100 | 1049/1049 | yes |
| 2026-06-27 | 93,584 | = -2,006,847 | 826 / 826 / 0 / 0 | 0 / 6476 | 100 | 238/238 | yes |
| 2026-07-17 | 1,050,936 | = -20,641,944 | 10,836 / 10,836 / 0 / 0 | 0 / 37520 | 100 | 1504/1504 | yes |
| 2026-08-06 | 1,198,571 | = 2,113,980 | 13,170 / 13,170 / 0 / 0 | 0 / 46996 | 100 | 1607/1607 | yes |
| 2026-08-26 | 1,257,674 | = 9,649,268 | 11,939 / 11,939 / 0 / 0 | 0 / 40767 | 100 | 1535/1535 | yes |
| 2026-09-15 | 1,093,228 | = -2,127,650 | 9,358 / 9,358 / 0 / 0 | 0 / 41219 | 100 | 1548/1548 | yes |
| 2026-10-05 | 1,185,437 | = -6,048,027 | 8,386 / 8,386 / 0 / 0 | 0 / 42460 | 100 | 1514/1514 | yes |

## Taker-order clustering sensitivity (2026-04-30)

aggTrades split = Binance aggTrades spread over several reconstructed orders; orders merging aggTrades = reconstructed orders holding two same-price aggTrades (two taker orders glued together).

| cluster_ms | orders | aggTrades split | orders merging aggTrades |
|---|---|---|---|
| 0 | 209,456 | 31,472 | 0 |
| 10 | 149,066 | 17,303 | 0 |
| 50 | 133,773 | 7,386 | 71 |
| 100 | 123,586 | 0 | 1,289 |
| 250 | 107,893 | 0 | 11,644 |
| 1000 | 80,532 | 0 | 22,207 |

Result: **PASS** (7/7 days)
