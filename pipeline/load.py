from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
import io
import psycopg2

def load_data(df, first_load=True):
    print("Connecting to database...")
    
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

    if first_load:
        print("Creating staging table...")
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS stg_claims;")
        
        # Build CREATE TABLE from dataframe columns
        cols = ", ".join([f'"{col}" TEXT' for col in df.columns])
        cursor.execute(f"CREATE TABLE stg_claims ({cols});")
        conn.commit()
        cursor.close()

    print("Streaming data into Postgres via COPY...")
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()
    cursor.copy_expert(
        "COPY stg_claims FROM STDIN WITH CSV",
        buffer
    )
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Successfully loaded {len(df)} rows into stg_claims")