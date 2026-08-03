with base as (
    select * from {{ source('bronze', 'market_trades') }}
),

casted as (
    select
        event_id,
        schema_version,
        source,
        product_id,
        trade_id,
        cast(price as decimal(20, 8)) as price,
        cast(size as decimal(20, 8)) as size,
        maker_side,
        cast(try(from_iso8601_timestamp(trade_time)) as timestamp) as trade_time,
        cast(try(from_iso8601_timestamp(ingested_at)) as timestamp) as ingested_at
    from base
)

select
    *,
    price * size as quote_value,
    date_diff('millisecond', trade_time, ingested_at) as ingestion_latency_ms
from casted
