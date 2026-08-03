with ranked as (
    select
        *,
        row_number() over (partition by event_id order by ingested_at desc) as rn
    from {{ ref('stg_coinbase_market_trades') }}
)

select
    event_id,
    schema_version,
    source,
    product_id,
    trade_id,
    price,
    size,
    maker_side,
    trade_time,
    ingested_at,
    quote_value,
    ingestion_latency_ms
from ranked
where rn = 1
