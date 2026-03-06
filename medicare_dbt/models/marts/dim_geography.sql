with stg as (
    select * from {{ ref('stg_claims_clean') }}
),

ranked as (
    select
        provider_state,
        provider_city,
        provider_zip,
        provider_state_fips,
        provider_ruca,
        provider_ruca_desc,
        provider_country,
        ROW_NUMBER() OVER (PARTITION BY provider_state, provider_city, provider_zip ORDER BY provider_state) as rn
    from stg
    where provider_state is not null
),

deduped as (
    select
        provider_state,
        provider_city,
        provider_zip,
        provider_state_fips,
        provider_ruca,
        provider_ruca_desc,
        provider_country
    from ranked
    where rn = 1
)

select * from deduped