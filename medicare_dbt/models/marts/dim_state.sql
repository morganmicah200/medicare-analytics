with stg as (
    select * from {{ ref('stg_claims_clean') }}
)

select distinct
    provider_state,
    provider_state_fips
from stg
where provider_state is not null