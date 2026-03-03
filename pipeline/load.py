import pandas as pd
from sqlalchemy import create_engine
from config import DATABASE_URL

def load_data(df):
    print("Connecting to database...")
    engine = create_engine(DATABASE_URL)

    print("Loading data into staging table...")

    # Load into staging table, replacing it on each run
    # chunksize prevents memory issues when inserting millions of rows
    df.to_sql(
        name='stg_claims',
        con=engine,
        if_exists='replace',
        index=False,
        chunksize=10000
    )

    print(f"Successfully loaded {len(df)} rows into stg_claims")

    # Close all database connections cleanly
    engine.dispose()