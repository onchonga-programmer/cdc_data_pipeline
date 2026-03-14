"""
Stage 3: Silver Layer Transformation — CDC Deaths
==================================================
Senior principle: Read from bronze, never from the API.
                  Write to silver with full idempotency.

This script:
1. Reads raw JSONB records from raw.cdc_chronic_disease (bronze)
2. Casts and cleans each field
3. Derives the is_provisional flag
4. Upserts into transformed.cdc_deaths (silver)
"""

import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

# ─────────────────────────────────────────────
# LOGGING SETUP — same pattern as Stage 2
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("transform.log"),
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG — same DB credentials as Stage 2
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     os.environ.get("POSTGRES_PORT", 5434),
    "database": os.environ.get("POSTGRES_DB", "health_pipeline"),
    "user":     os.environ.get("POSTGRES_USER", "pipeline_user"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
}

# ─────────────────────────────────────────────
# SILVER TABLE DEFINITION
# ─────────────────────────────────────────────
CREATE_SILVER_TABLE_SQL = """
CREATE SCHEMA IF NOT EXISTS transformed;

CREATE TABLE IF NOT EXISTS transformed.cdc_deaths (
    jurisdiction            VARCHAR(100)    NOT NULL,
    year                    INTEGER         NOT NULL,
    month                   INTEGER         NOT NULL,
    start_date              DATE,
    end_date                DATE,
    data_as_of              DATE,
    all_cause               INTEGER,
    natural_cause           INTEGER,
    diseases_of_heart       INTEGER,
    malignant_neoplasms     INTEGER,
    diabetes_mellitus       INTEGER,
    alzheimer_disease       INTEGER,
    drug_overdose           INTEGER,
    covid_19_underlying     INTEGER,
    accidents_unintentional INTEGER,
    is_provisional          BOOLEAN         NOT NULL DEFAULT FALSE,
    loaded_at               TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Grain: one row per jurisdiction + year + month
    PRIMARY KEY (jurisdiction, year, month)
);
"""

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def safe_int(value):
    """
    Safely cast any value to integer.
    Returns None (NULL) instead of crashing on bad data.
    """
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def safe_date(value):
    """
    Safely cast a CDC timestamp string to a Python date.
    Input example: "2020-01-01T00:00:00.000"
    """
    if not value:
        return None
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


# ─────────────────────────────────────────────
# STEP 1: EXTRACT FROM BRONZE
# ─────────────────────────────────────────────
def extract(conn):
    """
    Read raw records from the bronze layer.
    
    Important: We only read the LATEST batch to avoid
    reprocessing old ingestion runs.
    
    Because raw_data is stored as JSONB, psycopg2 
    automatically returns it as a Python dict — 
    no json.loads() needed.
    """
    log.info("Reading from bronze layer (raw.cdc_chronic_disease)...")

    with conn.cursor() as cur:

        # First — find the latest batch_id
        cur.execute("""
            SELECT batch_id 
            FROM raw.cdc_chronic_disease 
            ORDER BY ingested_at DESC 
            LIMIT 1;
        """)
        result = cur.fetchone()

        if not result:
            raise ValueError(
                "Bronze layer is empty. Run fetch_cdc_data.py first."
            )

        latest_batch_id = result[0]
        log.info(f"  → Latest batch: {latest_batch_id}")

        # Then — read all records from that batch
        cur.execute("""
            SELECT raw_data
            FROM raw.cdc_chronic_disease
            WHERE batch_id = %s;
        """, (latest_batch_id,))

        rows = cur.fetchall()

    # Each row is (raw_data,) where raw_data is already a dict (JSONB magic)
    records = [row[0] for row in rows]
    log.info(f"  → Extracted {len(records)} records from bronze")
    return records


# ─────────────────────────────────────────────
# STEP 2: TRANSFORM
# ─────────────────────────────────────────────
def transform(raw_records):
    """
    Clean and cast each raw record into a structured silver row.

    Rules:
    - Cast all numeric strings to integers
    - Cast all date strings to proper dates
    - Derive is_provisional from CDC flag columns
    - Drop rows that are missing year or month (unidentifiable)
    - Never crash the pipeline on a single bad record
    """
    log.info("Transforming records...")

    transformed = []
    dropped     = 0

    for record in raw_records:

        # Derive is_provisional
        # If ANY CDC flag column is populated → data is not yet final
        is_provisional = any([
            record.get("flag_accid"),
            record.get("flag_mva"),
            record.get("flag_suic"),
            record.get("flag_homic"),
            record.get("flag_drugod"),
        ])

        row = {
            "jurisdiction":            record.get("jurisdiction_of_occurrence"),
            "year":                    safe_int(record.get("year")),
            "month":                   safe_int(record.get("month")),
            "start_date":              safe_date(record.get("start_date")),
            "end_date":                safe_date(record.get("end_date")),
            "data_as_of":              safe_date(record.get("data_as_of")),
            "all_cause":               safe_int(record.get("all_cause")),
            "natural_cause":           safe_int(record.get("natural_cause")),
            "diseases_of_heart":       safe_int(record.get("diseases_of_heart")),
            "malignant_neoplasms":     safe_int(record.get("malignant_neoplasms")),
            "diabetes_mellitus":       safe_int(record.get("diabetes_mellitus")),
            "alzheimer_disease":       safe_int(record.get("alzheimer_disease")),
            "drug_overdose":           safe_int(record.get("drug_overdose")),
            "covid_19_underlying":     safe_int(record.get("covid_19_underlying_cause")),
            "accidents_unintentional": safe_int(record.get("accidents_unintentional")),
            "is_provisional":          is_provisional,
        }

        # Data quality gate
        # Year + month are the minimum required to identify a row
        if row["year"] is None or row["month"] is None:
            log.warning(f"Dropping unidentifiable row: {record}")
            dropped += 1
            continue

        transformed.append(row)

    log.info(f"  → {len(transformed)} rows transformed | {dropped} rows dropped")
    return transformed


# ─────────────────────────────────────────────
# STEP 3: LOAD INTO SILVER (IDEMPOTENT)
# ─────────────────────────────────────────────
def load(transformed_records, conn):
    """
    Upsert transformed records into the silver layer.

    Uses INSERT ... ON CONFLICT DO UPDATE (upsert) to ensure
    idempotency — running this twice produces the same result.

    Primary key: (jurisdiction, year, month)
    """
    log.info("Loading into silver layer (transformed.cdc_deaths)...")

    upsert_sql = """
        INSERT INTO transformed.cdc_deaths (
            jurisdiction, year, month, start_date, end_date,
            data_as_of, all_cause, natural_cause, diseases_of_heart,
            malignant_neoplasms, diabetes_mellitus, alzheimer_disease,
            drug_overdose, covid_19_underlying, accidents_unintentional,
            is_provisional
        ) VALUES %s
        ON CONFLICT (jurisdiction, year, month)
        DO UPDATE SET
            all_cause               = EXCLUDED.all_cause,
            natural_cause           = EXCLUDED.natural_cause,
            diseases_of_heart       = EXCLUDED.diseases_of_heart,
            malignant_neoplasms     = EXCLUDED.malignant_neoplasms,
            diabetes_mellitus       = EXCLUDED.diabetes_mellitus,
            alzheimer_disease       = EXCLUDED.alzheimer_disease,
            drug_overdose           = EXCLUDED.drug_overdose,
            covid_19_underlying     = EXCLUDED.covid_19_underlying,
            accidents_unintentional = EXCLUDED.accidents_unintentional,
            is_provisional          = EXCLUDED.is_provisional,
            data_as_of              = EXCLUDED.data_as_of,
            loaded_at               = NOW();
    """

    rows = [(
        r["jurisdiction"],  r["year"],          r["month"],
        r["start_date"],    r["end_date"],       r["data_as_of"],
        r["all_cause"],     r["natural_cause"],  r["diseases_of_heart"],
        r["malignant_neoplasms"],                r["diabetes_mellitus"],
        r["alzheimer_disease"],                  r["drug_overdose"],
        r["covid_19_underlying"],                r["accidents_unintentional"],
        r["is_provisional"]
    ) for r in transformed_records]

    with conn.cursor() as cur:
        execute_values(cur, upsert_sql, rows)

    conn.commit()
    log.info(f"  → {len(rows)} rows upserted into transformed.cdc_deaths ✅")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    log.info("=" * 55)
    log.info("Stage 3 — Silver Layer Transformation")
    log.info("=" * 55)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        log.info("Database connection established")
    except Exception as e:
        log.error(f"Failed to connect to database: {e}")
        raise

    try:
        # Create silver table if it doesn't exist
        with conn.cursor() as cur:
            cur.execute(CREATE_SILVER_TABLE_SQL)
        conn.commit()

        # ETL
        raw   = extract(conn)
        clean = transform(raw)
        load(clean, conn)

        log.info("✅ Stage 3 complete.")

    except Exception as e:
        log.error(f"Pipeline failed: {e}")
        raise

    finally:
        conn.close()
        log.info("Database connection closed")


if __name__ == "__main__":
    main()