import pandas as pd
import pytest
import sys
import os

# Allow importing from pipeline/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from pipeline.transform import transform_data


def make_raw_df(**overrides):
    """Build a minimal raw CMS-style DataFrame for testing."""
    row = {
        'Rndrng_NPI': '1234567890',
        'Rndrng_Prvdr_Last_Org_Name': '  SMITH  ',
        'Rndrng_Prvdr_First_Name': 'john',
        'Rndrng_Prvdr_MI': 'A',
        'Rndrng_Prvdr_Crdntls': 'MD',
        'Rndrng_Prvdr_Ent_Cd': 'I',
        'Rndrng_Prvdr_St1': '123 Main St',
        'Rndrng_Prvdr_St2': '',
        'Rndrng_Prvdr_City': 'indianapolis',
        'Rndrng_Prvdr_State_Abrvtn': 'IN',
        'Rndrng_Prvdr_State_FIPS': '18',
        'Rndrng_Prvdr_Zip5': '46201',
        'Rndrng_Prvdr_RUCA': '1',
        'Rndrng_Prvdr_RUCA_Desc': 'Urban',
        'Rndrng_Prvdr_Cntry': 'US',
        'Rndrng_Prvdr_Type': 'Internal Medicine',
        'Rndrng_Prvdr_Mdcr_Prtcptg_Ind': 'Y',
        'HCPCS_Cd': '99214',
        'HCPCS_Desc': 'Office visit',
        'HCPCS_Drug_Ind': 'N',
        'Place_Of_Srvc': 'O',
        'Tot_Benes': '50',
        'Tot_Srvcs': '120',
        'Tot_Bene_Day_Srvcs': '120',
        'Avg_Sbmtd_Chrg': '250.00',
        'Avg_Mdcr_Alowd_Amt': '90.00',
        'Avg_Mdcr_Pymt_Amt': '75.00',
        'Avg_Mdcr_Stdzd_Amt': '72.00',
        'year': '2023',
    }
    row.update(overrides)
    return pd.DataFrame([row])


# ── Column renaming ────────────────────────────────────────────────────────────

def test_columns_renamed():
    df = transform_data(make_raw_df())
    assert 'provider_npi' in df.columns
    assert 'hcpcs_code' in df.columns
    assert 'avg_medicare_payment' in df.columns
    assert 'Rndrng_NPI' not in df.columns


# ── Numeric conversion ─────────────────────────────────────────────────────────

def test_numeric_columns_are_numeric():
    df = transform_data(make_raw_df())
    for col in ['total_services', 'total_beneficiaries', 'avg_medicare_payment',
                'avg_submitted_charge', 'avg_medicare_allowed_amt',
                'avg_medicare_standardized_amt']:
        assert pd.api.types.is_numeric_dtype(df[col]), f"{col} should be numeric"


def test_invalid_numeric_becomes_nan():
    df = transform_data(make_raw_df(Tot_Srvcs='not_a_number'))
    assert pd.isna(df['total_services'].iloc[0])


# ── Whitespace stripping and uppercasing ───────────────────────────────────────

def test_provider_name_stripped_and_uppercased():
    df = transform_data(make_raw_df(Rndrng_Prvdr_Last_Org_Name='  smith  '))
    assert df['provider_last_org_name'].iloc[0] == 'SMITH'


def test_hcpcs_code_uppercased():
    df = transform_data(make_raw_df(HCPCS_Cd='  a0425  '))
    assert df['hcpcs_code'].iloc[0] == 'A0425'


def test_city_uppercased():
    df = transform_data(make_raw_df(Rndrng_Prvdr_City='indianapolis'))
    assert df['provider_city'].iloc[0] == 'INDIANAPOLIS'


# ── Row dropping ───────────────────────────────────────────────────────────────

def test_row_dropped_when_npi_missing():
    df = transform_data(make_raw_df(Rndrng_NPI=None))
    assert len(df) == 0


def test_row_dropped_when_hcpcs_missing():
    df = transform_data(make_raw_df(HCPCS_Cd=None))
    assert len(df) == 0


def test_row_dropped_when_payment_missing():
    df = transform_data(make_raw_df(Avg_Mdcr_Pymt_Amt=None))
    assert len(df) == 0


def test_valid_row_is_kept():
    df = transform_data(make_raw_df())
    assert len(df) == 1


# ── Year column ────────────────────────────────────────────────────────────────

def test_year_is_integer():
    df = transform_data(make_raw_df())
    assert df['year'].dtype in ['int32', 'int64']


def test_year_value_correct():
    df = transform_data(make_raw_df(year='2023'))
    assert df['year'].iloc[0] == 2023