# Architecture: Medicare Analytics Pipeline

## Overview

This project ingests 9.66 million rows of 2023 CMS Medicare Physician & Other Practitioners claims data into a PostgreSQL data warehouse, transforms it into a star schema using dbt, and visualizes it in Power BI. The pipeline is designed to be reproducible, testable, and extensible to additional years of CMS data.

---

## Stack

| Layer | Tool | Reason |
|---|---|---|
| Ingestion & Transform | Python / Pandas | Flexible column renaming, type coercion, and data cleaning |
| Data Warehouse | PostgreSQL 18 | Reliable, local, handles 9M+ rows without issue |
| Transformation & Modeling | dbt | Version-controlled SQL, layered modeling, reproducible builds |
| Visualization | Power BI Desktop | Industry-standard BI tool, connects natively to PostgreSQL |
| Testing | pytest | Unit and integration tests for pipeline validation |
| Version Control | Git / GitHub | Full project history, portfolio visibility |

---

## Pipeline Design

### Stage 1 — Ingest (`pipeline/ingest.py`)
Raw CSV data is loaded entirely as strings using `dtype=str`. This avoids silent data loss that can occur when Pandas infers types on mixed or malformed columns. A `year` tag is added to each row for future multi-year support.

### Stage 2 — Transform (`pipeline/transform.py`)
The transform step handles three concerns:
- **Column renaming** — CMS column codes (e.g. `Rndrng_NPI`, `Avg_Mdcr_Pymt_Amt`) are renamed to readable snake_case names
- **Type coercion** — Numeric columns are converted using `pd.to_numeric(errors='coerce')`, turning unparseable values into NaN rather than crashing
- **Data cleaning** — String columns are stripped of whitespace and uppercased for consistency. Rows missing `provider_npi`, `hcpcs_code`, or `avg_medicare_payment` are dropped as they cannot be meaningfully analyzed

### Stage 3 — Load (`pipeline/load.py`)
Data is streamed into PostgreSQL using `psycopg2`'s `COPY` command via a `StringIO` buffer. This approach was chosen over SQLAlchemy's `to_sql()` for performance — `COPY` is PostgreSQL's native bulk load mechanism and is significantly faster at this row count.

All columns are stored as `TEXT` in the staging table. Type casting is intentionally deferred to the dbt staging layer, keeping the raw staging table as a faithful, unmodified representation of the source data.

---

## Star Schema Design

The dbt project builds a star schema on top of the `stg_claims` staging table. The schema follows Kimball dimensional modeling principles.

```
                    ┌─────────────────┐
                    │   dim_provider  │
                    │  (1.17M rows)   │
                    └────────┬────────┘
                             │ provider_npi
                             │
┌──────────────┐    ┌────────┴────────┐    ┌──────────────────┐
│ dim_procedure│    │   fact_claims   │    │    dim_state     │
│  (6,405 rows)│────│  (9.66M rows)   │────│   (62 rows)      │
└──────────────┘    └────────┬────────┘    └──────────────────┘
     hcpcs_code              │ provider_state
                             │
                    ┌────────┴────────┐
                    │  dim_geography  │
                    │ (26,356 rows)   │
                    └─────────────────┘
```

### Dimension Tables

**dim_provider** — One row per provider NPI. Providers occasionally appear in the source data with multiple state registrations. A `ROW_NUMBER()` window function deduplicates on NPI, keeping one canonical row per provider.

**dim_procedure** — One row per HCPCS procedure code. Deduplicated on `hcpcs_code`. Includes procedure description and drug indicator flag.

**dim_geography** — City and zip code level geographic detail. Deduplicated on `(provider_state, provider_city, provider_zip)`.

**dim_state** — One row per state. Created separately from `dim_geography` to provide a clean Many-to-one relationship key for Power BI. State-level aggregation in Power BI connects through this table.

**fact_claims** — One row per provider/procedure/year combination. Contains all financial and volume metrics plus a derived `payment_gap` column (`avg_submitted_charge - avg_medicare_payment`) that quantifies the difference between what providers bill and what Medicare pays.

### dbt Layer Structure

```
models/
├── staging/
│   └── stg_claims_clean.sql   ← casts TEXT columns to proper numeric types
└── marts/
    ├── fact_claims.sql
    ├── dim_provider.sql
    ├── dim_procedure.sql
    ├── dim_geography.sql
    └── dim_state.sql
```

Staging models are materialized as **views** (no storage cost, always fresh).
Mart models are materialized as **tables** (pre-computed for Power BI query performance).

---

## Key Design Decisions

### Why psycopg2 COPY instead of SQLAlchemy to_sql?
SQLAlchemy's `to_sql()` inserts rows one batch at a time using INSERT statements. At 9.66M rows this would take 20-30 minutes. PostgreSQL's `COPY` command streams data directly into the table in bulk — the full load completes in under 5 minutes.

### Why store everything as TEXT in stg_claims?
The staging table is a raw landing zone. Storing as TEXT preserves exactly what came from the source without any implicit type coercion by PostgreSQL. All type casting happens explicitly in the dbt staging layer where it is version-controlled and testable.

### Why a separate dim_state table?
`dim_geography` operates at city/zip level, meaning `provider_state` is not unique within it. Power BI requires a unique key on the "one" side of a Many-to-one relationship. `dim_state` provides a clean 62-row state lookup table that satisfies this constraint.

### Why ROW_NUMBER() for deduplication?
Several providers in the CMS data appear with multiple rows due to registrations in different states. Using `SELECT DISTINCT` on a subset of columns risks non-deterministic results. `ROW_NUMBER() OVER (PARTITION BY provider_npi)` deterministically selects exactly one row per NPI on every dbt run.

---

## Reproducibility

The full pipeline can be reproduced from scratch:

1. Download source data from CMS
2. Run `python main.py` to ingest, transform, and load into PostgreSQL
3. Run `dbt run` inside `medicare_dbt/` to build the star schema
4. Run `pytest tests/ -v` to validate the pipeline (22 tests, all passing)
5. Connect Power BI to the `medicare_analytics` database

The raw CSV is gitignored due to file size (3GB). All pipeline code, dbt models, SQL queries, and tests are version-controlled.