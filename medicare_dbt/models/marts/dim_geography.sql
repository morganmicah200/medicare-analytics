with stg as (
    select * from {{ ref('stg_claims_clean') }}
)

select distinct
    provider_state,
    provider_city,
    provider_zip,
    provider_state_fips,
    provider_ruca,
    provider_ruca_desc,
    provider_country
from stg
where provider_state is not null