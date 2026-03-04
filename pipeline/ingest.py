import pandas as pd
import os

# Path to raw CSV files
RAW_DATA_PATH = "data/raw"

# Only loading 2023 data
FILES = {
    "2023": "claims_2023.csv"
}

def load_raw_data(filename, year):
    filepath = os.path.join(RAW_DATA_PATH, filename)
    print(f"Loading {year} data from {filepath}...")

    # Load everything as strings first to avoid data loss during ingestion
    # Type conversion happens in transform.py
    df = pd.read_csv(filepath, dtype=str, low_memory=False)

    # Tag each row with its source year for year over year analysis
    df["year"] = year
    print(f"{year}: {len(df)} rows loaded")
    return df