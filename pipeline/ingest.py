import pandas as pd
import os

# Path to raw CSV files
RAW_DATA_PATH = "data/raw"

# Map each year to its corresponding CSV file
FILES = {
    "2021": "claims_2021.csv",
    "2022": "claims_2022.csv",
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