import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ---------------------------
# LOAD ENV VARIABLES
# ---------------------------
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

BASE_PATH = os.getenv("BASE_PATH")
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH")
DEID_DATA_PATH = os.getenv("DEID_DATA_PATH")

RAW_PATH = os.path.join(BASE_PATH, RAW_DATA_PATH)
DEID_PATH = os.path.join(BASE_PATH, DEID_DATA_PATH)

# ---------------------------
# CREATE DB ENGINE
# ---------------------------
engine = create_engine(
    f"postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ---------------------------
# CREATE SCHEMAS (IMPORTANT)
# ---------------------------
def create_schemas():
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS deidentified"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
        conn.commit()
    print("Schemas ensured: raw, deidentified, analytics")

# ---------------------------
# LOAD TABLE FUNCTION
# ---------------------------
def load_table(file_path, table_name, schema_name):
    print(f"Loading {table_name} into {schema_name} schema...")

    df = pd.read_csv(file_path)

    df.to_sql(
        table_name,
        engine,
        schema=schema_name,
        if_exists="replace",   # overwrite for pipeline runs
        index=False,
        method="multi"
    )

    print(f"Loaded: {schema_name}.{table_name} ({len(df)} rows)")

# ---------------------------
# MAIN LOAD PIPELINE
# ---------------------------
def main():
    print("Starting data load to PostgreSQL...")

    create_schemas()

    # ---------------------------
    # RAW (restricted / PHI)
    # ---------------------------
    load_table(
        os.path.join(RAW_PATH, "patients.csv"),
        "patients",
        "raw"
    )

    # ---------------------------
    # DE-IDENTIFIED (safe)
    # ---------------------------
    load_table(
        os.path.join(DEID_PATH, "patients_deid.csv"),
        "patients",
        "deidentified"
    )

    load_table(
        os.path.join(DEID_PATH, "encounters_deid.csv"),
        "encounters",
        "deidentified"
    )

    print("Data successfully loaded into PostgreSQL!")

# ---------------------------
# ENTRY POINT
# ---------------------------
if __name__ == "__main__":
    main()
