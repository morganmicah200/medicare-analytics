from pipeline.ingest import load_raw_data
from pipeline.transform import transform_data
from pipeline.load import load_data

def run_pipeline():
    print("="*50)
    print("Starting Medicare Analytics Pipeline")
    print("="*50)

    # Step 1: Ingest raw CSV files
    print("\n[1/3] Ingesting raw data...")
    df = load_raw_data()

    # Step 2: Transform and clean the data
    print("\n[2/3] Transforming data...")
    df = transform_data(df)

    # Step 3: Load into Postgres staging table
    print("\n[3/3] Loading into database...")
    load_data(df)

    print("\n" + "="*50)
    print("Pipeline complete!")
    print("="*50)

if __name__ == "__main__":
    run_pipeline()