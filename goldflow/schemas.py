"""Canonical event schemas, version 1.

Every column is labelled in the Parquet schema metadata as one of:
  reported  - value copied from the provider record (no transformation beyond type parsing)
  derived   - computed deterministically from reported values by a documented rule
  constant  - provenance constant for the dataset (provider, venue, market, instrument, schema version)
  reference - pointer to the immutable raw source (raw_file_id + raw_row)
Reported and derived values are never stored in the same column.
"""
from __future__ import annotations
import json
import pyarrow as pa

SCHEMA_VERSION = 1
PROVIDER_ID = "binance"
VENUE_ID = "binance-futures-usdm"
MARKET_TYPE = "usdm_perpetual"

def instrument_id(symbol: str) -> str:
    return f"{PROVIDER_ID}:{MARKET_TYPE}:{symbol}"

TS = pa.timestamp("ms", tz="UTC")
DICT = pa.dictionary(pa.int8(), pa.string())

def _const_fields():
    return [("schema_version", pa.int16(), "constant"), ("provider_id", DICT, "constant"), ("venue_id", DICT, "constant"),
            ("market_type", DICT, "constant"), ("instrument_id", DICT, "constant")]

def _ref_fields():
    return [("raw_file_id", pa.int32(), "reference"), ("raw_row", pa.int64(), "reference")]

def _schema(name: str, fields: list, doc: dict) -> pa.Schema:
    prov = {f[0]: f[2] for f in fields}
    md = {"goldflow.schema": name, "goldflow.schema_version": str(SCHEMA_VERSION),
          "goldflow.field_provenance": json.dumps(prov), "goldflow.doc": json.dumps(doc)}
    return pa.schema([pa.field(n, t, nullable=True) for n, t, _ in fields], metadata=md)

# --------------------------------------------------------------------------- TradeEvent (individual executions)
TRADE_FIELDS = _const_fields() + [
    ("source_trade_id", pa.int64(), "reported"),            # Binance trade id
    ("aggregate_trade_id", pa.int64(), "reported"),         # only for RECOVERED rows: the aggTrade that carried it
    ("exchange_event_time_utc", TS, "reported"),            # Binance trade `time` (the only timestamp the archive gives)
    ("transaction_time_utc", TS, "reported"),               # same reported `time`; Binance does not separate the two
    ("receive_time_utc", TS, "reported"),                   # null for archive data (not captured live)
    ("price", pa.float64(), "reported"),
    ("quantity", pa.float64(), "reported"),                 # base quantity, troy ounces of XAU
    ("quote_notional", pa.float64(), "reported"),           # `quote_qty`; null for RECOVERED rows (not reported there)
    ("buyer_maker_flag", pa.bool_(), "reported"),           # `is_buyer_maker`
    ("aggressor_side", DICT, "derived"),                    # BUY if buyer_maker_flag is false, SELL if true, else UNKNOWN
    ("side_evidence", DICT, "derived"),                     # EXCHANGE_REPORTED when buyer_maker_flag present, else UNKNOWN
    ("price_ticks", pa.int64(), "derived"),                 # round(price / tick_size); validated exact
    ("quantity_lots", pa.int64(), "derived"),               # round(quantity / step_size); validated exact
    ("record_origin", DICT, "derived"),                     # ARCHIVE_TRADES | RECOVERED_FROM_AGGTRADES
    ("data_quality_status", DICT, "derived"),               # VALID | RECOVERED
] + _ref_fields()
TRADE_SCHEMA = _schema("TradeEvent", TRADE_FIELDS, {
    "aggressor_side": "Binance documents is_buyer_maker=true as: the buyer was the maker, so the seller was the aggressor (SELL).",
    "record_origin": "RECOVERED_FROM_AGGTRADES rows come from an aggTrade covering exactly one trade id (first==last) "
                     "that is absent from the trades file; price/qty/time/side are the aggTrade's reported values.",
    "units": "quantity in XAU troy ounces (1 contract = 1 oz); price in USDT per oz"})

# --------------------------------------------------------------------------- AggTradeEvent (aggregated; never individual)
AGG_FIELDS = _const_fields() + [
    ("aggregate_trade_id", pa.int64(), "reported"), ("first_trade_id", pa.int64(), "reported"),
    ("last_trade_id", pa.int64(), "reported"), ("exchange_event_time_utc", TS, "reported"),
    ("price", pa.float64(), "reported"), ("quantity", pa.float64(), "reported"), ("buyer_maker_flag", pa.bool_(), "reported"),
    ("aggressor_side", DICT, "derived"), ("side_evidence", DICT, "derived"),
    ("price_ticks", pa.int64(), "derived"), ("quantity_lots", pa.int64(), "derived"),
    ("data_quality_status", DICT, "derived"),
] + _ref_fields()
AGG_SCHEMA = _schema("AggTradeEvent", AGG_FIELDS, {"note": "Aggregate of one or more same-price, same-side, same-taker-order "
                     "fills. Must never be treated as individual executions."})

# --------------------------------------------------------------------------- DepthBandSnapshot (bookDepth; NOT L2)
DEPTHBAND_FIELDS = _const_fields() + [
    ("snapshot_time_utc", TS, "reported"), ("band_pct", pa.float64(), "reported"),
    ("cumulative_depth_qty", pa.float64(), "reported"), ("cumulative_notional", pa.float64(), "reported"),
    ("book_side", DICT, "derived"),                         # BID if band_pct < 0, ASK if > 0
    ("data_quality_status", DICT, "derived"),
] + _ref_fields()
DEPTHBAND_SCHEMA = _schema("DepthBandSnapshot", DEPTHBAND_FIELDS, {
    "note": "Binance bookDepth: ~30s snapshots of cumulative resting quantity within +-0.2% and +-1..5% of mid (12 bands). "
            "Not price-level L2; must not feed order-book reconstruction."})

# --------------------------------------------------------------------------- MarketMetrics (5-min OI / ratios)
METRICS_FIELDS = _const_fields() + [
    ("create_time_utc", TS, "reported"), ("sum_open_interest", pa.float64(), "reported"),
    ("sum_open_interest_value", pa.float64(), "reported"), ("count_toptrader_long_short_ratio", pa.float64(), "reported"),
    ("sum_toptrader_long_short_ratio", pa.float64(), "reported"), ("count_long_short_ratio", pa.float64(), "reported"),
    ("sum_taker_long_short_vol_ratio", pa.float64(), "reported"), ("data_quality_status", DICT, "derived"),
] + _ref_fields()
METRICS_SCHEMA = _schema("MarketMetrics", METRICS_FIELDS, {"note": "Source file rows are unsorted; normalised rows are sorted by time."})

# --------------------------------------------------------------------------- Kline1m (reference / cross-check only)
KLINE_FIELDS = _const_fields() + [
    ("open_time_utc", TS, "reported"), ("close_time_utc", TS, "reported"), ("open", pa.float64(), "reported"),
    ("high", pa.float64(), "reported"), ("low", pa.float64(), "reported"), ("close", pa.float64(), "reported"),
    ("volume", pa.float64(), "reported"), ("quote_volume", pa.float64(), "reported"), ("trade_count", pa.int64(), "reported"),
    ("taker_buy_volume", pa.float64(), "reported"), ("taker_buy_quote_volume", pa.float64(), "reported"),
    ("volume_lots", pa.int64(), "derived"), ("data_quality_status", DICT, "derived"),
] + _ref_fields()
KLINE_SCHEMA = _schema("Kline1m", KLINE_FIELDS, {"note": "Binance-computed candles; used as an independent completeness "
                       "reference for trades. Not an input to orderflow analytics."})

# --------------------------------------------------------------------------- DepthEvent (L2) - defined, no data
DEPTH_FIELDS = _const_fields() + [
    ("exchange_event_time_utc", TS, "reported"), ("transaction_time_utc", TS, "reported"),
    ("first_update_id", pa.int64(), "reported"), ("final_update_id", pa.int64(), "reported"),
    ("previous_update_id", pa.int64(), "reported"),
    ("bids", pa.list_(pa.struct([("price", pa.float64()), ("quantity", pa.float64())])), "reported"),
    ("asks", pa.list_(pa.struct([("price", pa.float64()), ("quantity", pa.float64())])), "reported"),
    ("snapshot_or_delta", DICT, "reported"), ("sequence_validity", DICT, "derived"),
    ("data_quality_status", DICT, "derived"),
] + _ref_fields()
DEPTH_SCHEMA = _schema("DepthEvent", DEPTH_FIELDS, {"note": "Price-level L2. No historical source available; "
                       "capability UNAVAILABLE for 2026-04-30..2026-10-30. Populated only by a future live recorder."})

SCHEMAS = {"trades": TRADE_SCHEMA, "aggTrades": AGG_SCHEMA, "bookDepth": DEPTHBAND_SCHEMA,
           "metrics": METRICS_SCHEMA, "klines_1m": KLINE_SCHEMA, "depth_l2": DEPTH_SCHEMA}

# Capability flags per dataset for this provider/instrument (consumed by later phases for gating).
CAPABILITIES = {
    "historical_trades": "AVAILABLE", "aggressor_side": "EXCHANGE_REPORTED", "historical_aggtrades": "AVAILABLE",
    "historical_l2_depth": "UNAVAILABLE", "historical_depth_bands": "AVAILABLE_COARSE",
    "historical_bid_ask_quotes": "UNAVAILABLE", "trade_sequence_numbers": "IDS_NOT_CONTIGUOUS",
    "completeness_reference": "KLINES_1M_COUNT_AND_AGGTRADES_QTY",
}

QUARANTINE_SCHEMA = pa.schema([
    ("dataset", pa.string()), ("reason_code", pa.string()), ("reason_detail", pa.string()),
    ("raw_file_id", pa.int32()), ("raw_row", pa.int64()), ("source_id", pa.int64()),
    ("event_time_ms", pa.int64()), ("raw_fields_json", pa.string())],
    metadata={"goldflow.schema": "QuarantineRecord", "goldflow.schema_version": str(SCHEMA_VERSION)})

REASON_CODES = {
    "PARSE_ERROR": "row could not be parsed into the source schema",
    "NONPOSITIVE_PRICE": "price <= 0 or not finite",
    "NONPOSITIVE_QUANTITY": "quantity <= 0 or not finite",
    "PRICE_NOT_TICK_MULTIPLE": "price is not an exact multiple of the instrument tick size",
    "QTY_NOT_STEP_MULTIPLE": "quantity is not an exact multiple of the instrument step size",
    "PRICE_OUTSIDE_KLINE_RANGE": "price outside Binance's 1m kline [low, high] for the trade's minute (foreign/corrupt row)",
    "PRICE_IMPLAUSIBLE": "no kline for the minute and price more than 10% from the day's median",
    "QUOTE_NOTIONAL_MISMATCH": "reported quote_qty differs from price*qty beyond rounding tolerance",
    "TS_OUTSIDE_SOURCE_INTERVAL": "timestamp outside the UTC day/month the source file covers",
    "TS_BEFORE_LISTING": "timestamp before the instrument onboard date",
    "TS_AFTER_ACQUISITION": "timestamp after the acquisition timestamp",
    "TS_SEQUENCE_OUTLIER": "timestamp breaks id/time monotonicity and its neighbours identify it as the outlier",
    "TS_ORDER_AMBIGUOUS": "id/time monotonicity broken and the outlier cannot be identified",
    "DUPLICATE_EXACT": "identical copy of an already accepted record (same id and payload)",
    "DUPLICATE_CONFLICT": "same id as another record with a different payload; all copies quarantined",
    "INVALID_ID_RANGE": "aggTrade last_trade_id < first_trade_id",
    "SYMBOL_MISMATCH": "row symbol differs from the dataset instrument",
    "INVALID_BAND": "bookDepth percentage band outside the observed set {+-0.2, +-1..5}",
    "UNCLASSIFIED": "invalid by a check without a specific reason code",
}
