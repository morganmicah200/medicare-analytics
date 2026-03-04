with stg as (
    select * from {{ ref('stg_claims_clean') }}
)

select
    -- Foreign keys
    provider_npi,
    hcpcs_code,
    provider_state,
    place_of_service,
    year,

    -- Metrics
    total_beneficiaries,
    total_services,
    total_beneficiary_day_services,
    avg_submitted_charge,
    avg_medicare_allowed_amt,
    avg_medicare_payment,
    avg_medicare_standardized_amt,

    -- Derived metrics
    round(avg_submitted_charge - avg_medicare_payment, 2) as payment_gap
from stg
where provider_npi is not null
and hcpcs_code is not null
and avg_medicare_payment is not null