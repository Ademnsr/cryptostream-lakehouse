with trades as (
    select
        product_id,
        date_trunc('minute', trade_time) as minute,
        price,
        size,
        quote_value,
        trade_time
    from {{ ref('fct_crypto_trades') }}
),

ordered as (
    select
        *,
        row_number() over (partition by product_id, minute order by trade_time asc) as rn_asc,
        row_number() over (partition by product_id, minute order by trade_time desc) as rn_desc
    from trades
)

select
    product_id,
    minute,
    max(case when rn_asc = 1 then price end) as open,
    max(price) as high,
    min(price) as low,
    max(case when rn_desc = 1 then price end) as close,
    count(*) as trade_count,
    sum(size) as base_volume,
    sum(quote_value) as quote_volume,
    sum(quote_value) / nullif(sum(size), 0) as vwap
from ordered
group by product_id, minute
