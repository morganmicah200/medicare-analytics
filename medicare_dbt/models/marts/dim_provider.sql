with stg as (
    select * from {{ ref('stg_claims_clean') }}
),

ranked as (
    select
        provider_npi,
        provider_last_org_name,
        provider_first_name,
        provider_type,
        provider_state,
        medicare_participating,
        ROW_NUMBER() OVER (PARTITION BY provider_npi ORDER BY provider_npi) as rn
    from stg
    where provider_npi is not null
)

select
    provider_npi,
    provider_last_org_name,
    provider_first_name,
    provider_type,
    provider_state,
    medicare_participating
from ranked
where rn = 1