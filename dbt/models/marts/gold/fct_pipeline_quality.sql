with bronze as (
    select * from {{ source('bronze', 'market_trades') }}
),

validated as (
    select
        *,
        try_cast(price as decimal(20, 8)) as price_cast,
        try_cast(size as decimal(20, 8)) as size_cast,
        cast(try(from_iso8601_timestamp(trade_time)) as timestamp) as trade_time_cast,
        cast(try(from_iso8601_timestamp(ingested_at)) as timestamp) as ingested_at_cast
    from bronze
),

flagged as (
    select
        *,
        case
            when price_cast is null or price_cast <= 0 then true
            when size_cast is null or size_cast <= 0 then true
            when maker_side not in ('BUY', 'SELL') then true
            when trade_time_cast is null then true
            when ingested_at_cast is null then true
            else false
        end as is_invalid
    from validated
)

select
    count(*) as bronze_record_count,
    count(distinct event_id) as distinct_event_count,
    count(*) - count(distinct event_id) as duplicate_count,
    sum(case when is_invalid then 1 else 0 end) as invalid_count,
    avg(date_diff('millisecond', trade_time_cast, ingested_at_cast)) as avg_ingestion_latency_ms,
    max(trade_time_cast) as latest_trade_time
from flagged
