select *
from {{ ref('fct_crypto_trades') }}
where size <= 0
