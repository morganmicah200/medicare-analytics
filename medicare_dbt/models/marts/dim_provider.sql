with stg as (
    select * from {{ ref('stg_claims_clean') }}
)

select distinct
     provider_npi,
    provider_last_org_name,
    provider_first_name,
    provider_mi,
    provider_credentials,
    provider_entity_code,
    provider_street1,
    provider_street2,
    provider_state,
    provider_type,
    medicare_participating
from stg
where provider_npi is not null