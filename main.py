from pipeline.ingest import load_raw_data, FILES
from pipeline.transform import transform_data
from pipeline.load import load_data

def run_pipeline():
    print("="*50)
    print("Starting Medicare Analytics Pipeline")
    print("="*50)

    first_load = True

    for year, filename in FILES.items():
        print(f"\nProcessing {year}...")

        # Step 1: Ingest one year at a time to manage memory
        print(f"[1/3] Ingesting {year} data...")
        df = load_raw_data(filename, year)

        # Step 2: Transform and clean the data
        print(f"[2/3] Transforming {year} data...")
        df = transform_data(df)

        # Step 3: Load into Postgres staging table
        print(f"[3/3] Loading {year} data into database...")
        load_data(df, first_load)
        first_load = False

    print("\n" + "="*50)
    print("Pipeline complete!")
    print("="*50)

if __name__ == "__main__":
    run_pipeline()