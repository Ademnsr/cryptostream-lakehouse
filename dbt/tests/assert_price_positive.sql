select *
from {{ ref('fct_crypto_trades') }}
where price <= 0
