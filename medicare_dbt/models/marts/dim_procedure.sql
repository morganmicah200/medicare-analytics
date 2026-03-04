with stg as (
    select * from {{ ref('stg_claims_clean') }}
)

select distinct
    hcpcs_code,
    hcpcs_description,
    drug_indicator
from stg
where hcpcs_code is not null