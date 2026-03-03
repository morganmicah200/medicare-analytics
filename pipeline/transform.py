import pandas as pd

def transform_data(df):
    print("Starting transformation...")

    # Rename columns to clean readable names
    df = df.rename(columns={
        'Rndrng_NPI': 'provider_npi',
        'Rndrng_Prvdr_Last_Org_Name': 'provider_last_org_name',
        'Rndrng_Prvdr_First_Name': 'provider_first_name',
        'Rndrng_Prvdr_MI': 'provider_mi',
        'Rndrng_Prvdr_Crdntls': 'provider_credentials',
        'Rndrng_Prvdr_Ent_Cd': 'provider_entity_code',
        'Rndrng_Prvdr_St1': 'provider_street1',
        'Rndrng_Prvdr_St2': 'provider_street2',
        'Rndrng_Prvdr_City': 'provider_city',
        'Rndrng_Prvdr_State_Abrvtn': 'provider_state',
        'Rndrng_Prvdr_State_FIPS': 'provider_state_fips',
        'Rndrng_Prvdr_Zip5': 'provider_zip',
        'Rndrng_Prvdr_RUCA': 'provider_ruca',
        'Rndrng_Prvdr_RUCA_Desc': 'provider_ruca_desc',
        'Rndrng_Prvdr_Cntry': 'provider_country',
        'Rndrng_Prvdr_Type': 'provider_type',
        'Rndrng_Prvdr_Mdcr_Prtcptg_Ind': 'medicare_participating',
        'HCPCS_Cd': 'hcpcs_code',
        'HCPCS_Desc': 'hcpcs_description',
        'HCPCS_Drug_Ind': 'drug_indicator',
        'Place_Of_Srvc': 'place_of_service',
        'Tot_Benes': 'total_beneficiaries',
        'Tot_Srvcs': 'total_services',
        'Tot_Bene_Day_Srvcs': 'total_beneficiary_day_services',
        'Avg_Sbmtd_Chrg': 'avg_submitted_charge',
        'Avg_Mdcr_Alowd_Amt': 'avg_medicare_allowed_amt',
        'Avg_Mdcr_Pymt_Amt': 'avg_medicare_payment',
        'Avg_Mdcr_Stdzd_Amt': 'avg_medicare_standardized_amt'
    })

    # Convert numeric columns
    numeric_cols = [
        'total_beneficiaries',
        'total_services',
        'total_beneficiary_day_services',
        'avg_submitted_charge',
        'avg_medicare_allowed_amt',
        'avg_medicare_payment',
        'avg_medicare_standardized_amt'
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Clean string columns
    string_cols = [
        'provider_npi',
        'provider_last_org_name',
        'provider_first_name',
        'provider_type',
        'provider_state',
        'provider_city',
        'provider_zip',
        'hcpcs_code',
        'hcpcs_description'
    ]
    for col in string_cols:
        df[col] = df[col].str.strip().str.upper()

    # Drop rows missing critical fields
    df = df.dropna(subset=['provider_npi', 'hcpcs_code', 'avg_medicare_payment'])

    # Convert year to integer
    df['year'] = df['year'].astype(int)

    print(f"Transformation complete. {len(df)} rows remaining.")
    return df