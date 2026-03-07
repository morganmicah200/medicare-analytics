# Medicare Analytics Pipeline

A full-stack data engineering portfolio project ingesting, modeling, and visualizing 9.66 million rows of 2023 CMS Medicare claims data.

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-blue?logo=postgresql)
![dbt](https://img.shields.io/badge/dbt-1.11-orange?logo=dbt)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow?logo=powerbi)
![pytest](https://img.shields.io/badge/pytest-22%20passing-green?logo=pytest)

---

## Overview

This project builds an end-to-end analytics pipeline on top of the CMS Medicare Physician & Other Practitioners dataset — one of the largest publicly available healthcare datasets in the United States. The pipeline ingests raw CSV data into PostgreSQL, models it into a star schema using dbt, and delivers insights through an interactive Power BI dashboard.

**Data source:** [CMS Medicare Physician & Other Practitioners by Provider and Service, 2023](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service)

---

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion & Transform | Python 3.12, Pandas |
| Data Warehouse | PostgreSQL 18 |
| Data Modeling | dbt 1.11 |
| Visualization | Power BI Desktop |
| Testing | pytest |
| Version Control | Git / GitHub |

---

## Architecture

```
CMS Raw CSV (3GB)
      │
      ▼
pipeline/ingest.py        ← Load as strings, tag with year
pipeline/transform.py     ← Rename columns, cast types, clean data
pipeline/load.py          ← Bulk load via PostgreSQL COPY command
      │
      ▼
PostgreSQL: stg_claims    ← 9,660,647 rows, all columns as TEXT
      │
      ▼
dbt staging layer         ← Cast TEXT columns to proper numeric types
      │
      ▼
dbt marts (star schema)
├── fact_claims           ← 9.66M rows, financial & volume metrics
├── dim_provider          ← 1.17M unique providers
├── dim_procedure         ← 6,405 unique HCPCS procedure codes
├── dim_state             ← 62 states/territories
└── dim_geography         ← 26,356 city/zip combinations
      │
      ▼
Power BI Dashboard        ← Interactive state-level analysis
```

For full design details see [docs/architecture.md](docs/architecture.md).

---

## Key Findings

**1. Alaska has the highest average Medicare reimbursement at $101/service**
Alaska ranks #1 nationally, well above the ~$80 national average, driven by geographic cost adjustments built into Medicare's payment formula.

**2. Providers bill roughly 4x more than Medicare pays**
Nationally, the average submitted charge exceeds $300 while Medicare pays around $80 — an 80% billing gap. This gap is visible across every state in the dashboard.

**3. COVID testing was the #1 most billed procedure in 2023**
HCPCS code K1034 (COVID-19 diagnostic test) topped procedure volume nationally, reflecting the continued impact of the pandemic on Medicare claims two years after peak COVID.

**4. Ambulatory Surgical Centers receive the highest average Medicare payment at $1,100+/service**
This is nearly 15x the average office visit reimbursement, highlighting the significant cost differential between care settings.

**5. Facility-based services pay 64% more than office-based services**
Medicare pays an average of $110 for facility-based services vs $67 for office-based services — a deliberate site-of-service payment differential in Medicare's reimbursement policy.

---

## Project Structure

```
medicare-analytics/
├── pipeline/
│   ├── ingest.py             # Load raw CSV as strings
│   ├── transform.py          # Rename, cast, clean
│   └── load.py               # Bulk load via COPY
├── medicare_dbt/
│   └── models/
│       ├── staging/
│       │   └── stg_claims_clean.sql
│       └── marts/
│           ├── fact_claims.sql
│           ├── dim_provider.sql
│           ├── dim_procedure.sql
│           ├── dim_state.sql
│           └── dim_geography.sql
├── sql/
│   └── analysis_queries.sql  # 5 analytical queries
├── tests/
│   ├── test_transform.py     # 11 unit tests
│   └── test_load.py          # 11 integration tests
├── docs/
│   └── architecture.md       # Full design documentation
├── config.py
├── main.py
└── requirements.txt
```

---

## Setup & Reproduction

### Prerequisites
- Python 3.12
- PostgreSQL 18
- dbt-postgres

### 1. Clone the repo
```bash
git clone https://github.com/morganmicah200/medicare-analytics.git
cd medicare-analytics
```

### 2. Create virtual environment
```bash
py -3.12 -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure environment variables
Create a `.env` file in the project root:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=medicare_analytics
DB_USER=postgres
DB_PASSWORD=your_password
```

### 4. Download the data
Download the 2023 dataset from CMS and save as:
```
data/raw/claims_2023.csv
```

### 5. Run the pipeline
```bash
python main.py
```
Loads 9.66M rows into PostgreSQL via bulk COPY (~5 minutes).

### 6. Build the star schema
```bash
cd medicare_dbt
dbt run
```
Builds all 5 dimension and fact tables (~3.5 minutes).

### 7. Run the tests
```bash
pytest tests/ -v
```
22 tests, all passing.

### 8. Connect Power BI
Connect Power BI Desktop to your PostgreSQL instance:
- Server: `localhost`
- Database: `medicare_analytics`
- Tables: `fact_claims`, `dim_provider`, `dim_procedure`, `dim_state`

---

## Dashboard

The Power BI dashboard features an interactive state filter that updates all visuals dynamically.

**Visuals:**
- Medicare payment map by state
- Medicare paid vs billing gap (donut chart)
- Top procedures by volume
- Avg Medicare payment by provider specialty
- KPI cards: avg reimbursement, state rank, total services, total beneficiaries, avg submitted charge

> Dashboard screenshots coming soon.

---

## Data Source

Centers for Medicare & Medicaid Services (CMS)
Medicare Physician & Other Practitioners — by Provider and Service, 2023
https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service