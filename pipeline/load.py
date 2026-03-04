from sqlalchemy import create_engine
from config import DATABASE_URL
import io

def load_data(df, first_load=True):
    print("Connecting to database...")
    engine = create_engine(DATABASE_URL)

    with engine.connect() as conn:
        if first_load:
            # Drop and recreate table on first load
            print("Creating staging table...")
            df.head(0).to_sql(
                name='stg_claims',
                con=conn,
                if_exists='replace',
                index=False
            )
            conn.commit()

        # Convert dataframe to in-memory CSV buffer
        print("Streaming data into Postgres via COPY...")
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        # Use PostgreSQL's native COPY command for fast bulk insert
        with conn.connection.cursor() as cursor:
            cursor.copy_expert(
                "COPY stg_claims FROM STDIN WITH CSV",
                buffer
            )
        conn.commit()

    print(f"Successfully loaded {len(df)} rows into stg_claims")

    # Close all database connections cleanly
    engine.dispose()