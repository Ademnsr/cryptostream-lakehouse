select
    event_id,
    schema_version,
    source,
    product_id,
    trade_id,
    price,
    size,
    quote_value,
    maker_side,
    trade_time,
    ingested_at,
    ingestion_latency_ms
from {{ ref('int_crypto_trades_deduplicated') }}
