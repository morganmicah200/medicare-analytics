import pandas as pd
import os

RAW_DATA_PATH = "data/raw"

def load_raw_data():
    files = {
        "2021": "claims_2021.csv",
        "2022": "claims_2022.csv",
        "2023": "claims_2023.csv"
    }

    dataframes = []

    for year, filename in files.items():
        filepath = os.path.join(RAW_DATA_PATH, filename)
        print(f"Loading {year} data from {filepath}...")
        df = pd.read_csv(filepath, dtype=str, low_memory=False)
        df["year"] = year
        dataframes.append(df)
        print(f"{year}: {len(df)} rows loaded")

    combined = pd.concat(dataframes, ignore_index=True)
    print(f"\nTotal rows loaded: {len(combined)}")
    return combined