import pandas as pd
import pytest
import psycopg2
import sys
import os

# Allow importing from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


# ── Database connectivity ──────────────────────────────────────────────────────

def test_database_connection():
    """Verify we can connect to the medicare_analytics database."""
    conn = get_connection()
    assert conn is not None
    conn.close()


def test_correct_database():
    """Verify we are connected to the right database."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT current_database();")
    db_name = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    assert db_name == DB_NAME


# ── Staging table ──────────────────────────────────────────────────────────────

def test_stg_claims_exists():
    """Verify stg_claims table was created by the pipeline."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'stg_claims'
        );
    """)
    exists = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    assert exists, "stg_claims table does not exist"


def test_stg_claims_row_count():
    """Verify stg_claims has the expected number of rows (9.6M)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stg_claims;")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    assert count > 9_000_000, f"Expected 9M+ rows, got {count}"


def test_stg_claims_has_expected_columns():
    """Verify all expected columns exist in stg_claims."""
    expected_columns = [
        'provider_npi', 'provider_last_org_name', 'provider_type',
        'provider_state', 'hcpcs_code', 'avg_medicare_payment',
        'total_services', 'year'
    ]
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'stg_claims';
    """)
    actual_columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    for col in expected_columns:
        assert col in actual_columns, f"Missing column: {col}"


def test_stg_claims_no_null_npis():
    """Verify no NULL provider_npi values exist after transform."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stg_claims WHERE provider_npi IS NULL;")
    null_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    assert null_count == 0, f"Found {null_count} NULL NPIs in stg_claims"


# ── dbt mart tables ────────────────────────────────────────────────────────────

def test_fact_claims_exists():
    """Verify dbt built fact_claims."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'fact_claims'
        );
    """)
    exists = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    assert exists, "fact_claims table does not exist — run dbt"


def test_dim_provider_unique_npi():
    """Verify dim_provider has no duplicate NPIs."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM (
            SELECT provider_npi, COUNT(*)
            FROM dim_provider
            GROUP BY provider_npi
            HAVING COUNT(*) > 1
        ) dupes;
    """)
    dupe_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    assert dupe_count == 0, f"dim_provider has {dupe_count} duplicate NPIs"


def test_dim_procedure_unique_hcpcs():
    """Verify dim_procedure has no duplicate HCPCS codes."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM (
            SELECT hcpcs_code, COUNT(*)
            FROM dim_procedure
            GROUP BY hcpcs_code
            HAVING COUNT(*) > 1
        ) dupes;
    """)
    dupe_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    assert dupe_count == 0, f"dim_procedure has {dupe_count} duplicate HCPCS codes"


def test_fact_claims_row_count_matches_stg():
    """Verify fact_claims row count is close to stg_claims (within 1%)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stg_claims;")
    stg_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM fact_claims;")
    fact_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    diff_pct = abs(stg_count - fact_count) / stg_count
    assert diff_pct < 0.01, f"Row count mismatch: stg={stg_count}, fact={fact_count}"