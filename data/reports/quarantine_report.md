# Quarantine report

Quarantined records are stored as Parquet under data/quarantine/... (same partitioning) with
reason_code, reason_detail, raw_file_id, raw_row and the original fields. Nothing is dropped silently.

- No records quarantined.

## Reason code reference

- `PARSE_ERROR`: row could not be parsed into the source schema
- `NONPOSITIVE_PRICE`: price <= 0 or not finite
- `NONPOSITIVE_QUANTITY`: quantity <= 0 or not finite
- `PRICE_NOT_TICK_MULTIPLE`: price is not an exact multiple of the instrument tick size
- `QTY_NOT_STEP_MULTIPLE`: quantity is not an exact multiple of the instrument step size
- `PRICE_OUTSIDE_KLINE_RANGE`: price outside Binance's 1m kline [low, high] for the trade's minute (foreign/corrupt row)
- `PRICE_IMPLAUSIBLE`: no kline for the minute and price more than 10% from the day's median
- `QUOTE_NOTIONAL_MISMATCH`: reported quote_qty differs from price*qty beyond rounding tolerance
- `TS_OUTSIDE_SOURCE_INTERVAL`: timestamp outside the UTC day/month the source file covers
- `TS_BEFORE_LISTING`: timestamp before the instrument onboard date
- `TS_AFTER_ACQUISITION`: timestamp after the acquisition timestamp
- `TS_SEQUENCE_OUTLIER`: timestamp breaks id/time monotonicity and its neighbours identify it as the outlier
- `TS_ORDER_AMBIGUOUS`: id/time monotonicity broken and the outlier cannot be identified
- `DUPLICATE_EXACT`: identical copy of an already accepted record (same id and payload)
- `DUPLICATE_CONFLICT`: same id as another record with a different payload; all copies quarantined
- `INVALID_ID_RANGE`: aggTrade last_trade_id < first_trade_id
- `SYMBOL_MISMATCH`: row symbol differs from the dataset instrument
- `INVALID_BAND`: bookDepth percentage band outside the observed set {+-0.2, +-1..5}
- `UNCLASSIFIED`: invalid by a check without a specific reason code
