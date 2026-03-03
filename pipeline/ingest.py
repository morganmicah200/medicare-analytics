import pandas as pd
import os

# Path to raw CSV files
RAW_DATA_PATH = "data/raw"

def load_raw_data():
    # Map each year to its corresponding CSV file
    files = {
        "2021": "claims_2021.csv",
        "2022": "claims_2022.csv",
        "2023": "claims_2023.csv"
    }

    dataframes = []

    for year, filename in files.items():
        filepath = os.path.join(RAW_DATA_PATH, filename)
        print(f"Loading {year} data from {filepath}...")

        # Load everything as strings first to avoid data loss during ingestion
        # Type conversion happens in transform.py
        df = pd.read_csv(filepath, dtype=str, low_memory=False)

        # Tag each row with its source year for year over year analysis
        df["year"] = year
        dataframes.append(df)
        print(f"{year}: {len(df)} rows loaded")

    # Combine all three years into a single dataframe
    combined = pd.concat(dataframes, ignore_index=True)
    print(f"\nTotal rows loaded: {len(combined)}")
    return combined