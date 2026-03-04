with source as (
    select * from public.stg_claims
),

renamed as (
    select
        -- Provider identifiers
        provider_npi,
        provider_last_org_name,
        provider_first_name,
        provider_mi,
        provider_credentials,
        provider_entity_code,
        provider_street1,
        provider_street2,
        provider_city,
        provider_state,
        provider_state_fips,
        provider_zip,
        provider_ruca,
        provider_ruca_desc,
        provider_country,
        provider_type,
        medicare_participating,

        -- Procedure identifiers
        hcpcs_code,
        hcpcs_description,
        drug_indicator,
        place_of_service,

        -- Metrics cast to proper types
        cast(total_beneficiaries as numeric)            as total_beneficiaries,
        cast(total_services as numeric)                 as total_services,
        cast(total_beneficiary_day_services as numeric) as total_beneficiary_day_services,
        cast(avg_submitted_charge as numeric)           as avg_submitted_charge,
        cast(avg_medicare_allowed_amt as numeric)       as avg_medicare_allowed_amt,
        cast(avg_medicare_payment as numeric)           as avg_medicare_payment,
        cast(avg_medicare_standardized_amt as numeric)  as avg_medicare_standardized_amt,

        -- Year
        cast(year as integer) as year

    from source
)

select * from renamed