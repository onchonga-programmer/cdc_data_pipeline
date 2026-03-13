"""
Stage 2: Data Ingestion — CDC Chronic Disease Indicators
=========================================================
Senior principle: Store raw, ingest exactly as-is, never transform here.

This script:
1. Connects to CDC's public API
2. Fetches chronic disease data (full load)
3. Saves the raw JSON response into PostgreSQL — no changes
4. Logs ingestion metadata (when, how many rows, success/fail)
"""

import requests
import json
import psycopg2
import logging
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")
# ─────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("ingestion.log"),
    ]
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
API_URL = "https://data.cdc.gov/resource/g4ie-h725.json"
API_PARAMS = {
    "$limit": 1000,
    "$offset": 0,
}
CDC_APP_TOKEN = os.environ.get("CDC_APP_TOKEN")  
DB_CONFIG = {
    "host": os.environ.get('POSTGRES_HOST', 'localhost'),
    "port": os.environ.get('POSTGRES_PORT', 5434),
    "database": os.environ.get('POSTGRES_DB', 'health_pipeline'),
    "user": os.environ.get('POSTGRES_USER', 'pipeline_user'),
    "password": os.environ.get('POSTGRES_PASSWORD'),
}

# ─────────────────────────────────────────────
# STEP 1: CREATE THE RAW TABLE
# ─────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.cdc_chronic_disease (
    id            SERIAL PRIMARY KEY,
    raw_data      JSONB        NOT NULL,
    ingested_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    source_url    TEXT         NOT NULL,
    batch_id      TEXT         NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cdc_raw_ingested_at
    ON raw.cdc_chronic_disease (ingested_at);
"""

# ─────────────────────────────────────────────
# STEP 2: FETCH DATA FROM THE API
# ─────────────────────────────────────────────
def fetch_from_api(url: str, params: dict) -> list[dict]:
    """
    Fetch data from a REST API endpoint.

    Returns a list of records (each record is a raw dict).
    Raises an exception if the request fails — we handle that in main().
    """
    log.info(f"Fetching from API: {url} with params: {params}")

    response = requests.get(
        url,
        params=params,
        timeout=30,
        headers={"Accept": "application/json", "X-App-Token": CDC_APP_TOKEN}
    )

    response.raise_for_status()

    records = response.json()
    log.info(f"Received {len(records)} records from API")

    return records


# ─────────────────────────────────────────────
# STEP 3: STORE RAW DATA INTO POSTGRES
# ─────────────────────────────────────────────
def store_raw_records(conn, records: list[dict], source_url: str, batch_id: str):
    """
    Insert raw JSON records into the bronze layer table.
    """
    if not records:
        log.warning("No records to insert. Skipping.")
        return 0

    insert_sql = """
        INSERT INTO raw.cdc_chronic_disease (raw_data, source_url, batch_id)
        VALUES (%s, %s, %s)
    """

    rows = [
        (json.dumps(record), source_url, batch_id)
        for record in records
    ]

    with conn.cursor() as cur:
        cur.executemany(insert_sql, rows)

    conn.commit()
    log.info(f"Inserted {len(rows)} raw records into raw.cdc_chronic_disease")
    return len(rows)


# ─────────────────────────────────────────────
# STEP 4: LOG THE INGESTION RUN
# ─────────────────────────────────────────────
CREATE_LOG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.ingestion_log (
    id            SERIAL PRIMARY KEY,
    batch_id      TEXT         NOT NULL,
    source        TEXT         NOT NULL,
    status        TEXT         NOT NULL,
    rows_ingested INT,
    error_message TEXT,
    started_at    TIMESTAMPTZ  NOT NULL,
    finished_at   TIMESTAMPTZ
);
"""

def log_ingestion_run(conn, batch_id, source, status, rows, error_msg, started_at):
    sql = """
        INSERT INTO raw.ingestion_log
            (batch_id, source, status, rows_ingested, error_message, started_at, finished_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
    """
    with conn.cursor() as cur:
        cur.execute(sql, (batch_id, source, status, rows, error_msg, started_at))
    conn.commit()


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    batch_id = f"cdc_full_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    started_at = datetime.now(timezone.utc)

    log.info(f"=== Starting ingestion run | batch_id={batch_id} ===")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        log.info("Database connection established")
    except Exception as e:
        log.error(f"Failed to connect to database: {e}")
        raise  # Can't continue without a DB connection

    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            cur.execute(CREATE_TABLE_SQL)
            cur.execute(CREATE_LOG_TABLE_SQL)
        conn.commit()

        records = fetch_from_api(API_URL, API_PARAMS)
        rows_inserted = store_raw_records(conn, records, API_URL, batch_id)
        log_ingestion_run(
            conn, batch_id, "cdc_chronic_disease",
            status="success", rows=rows_inserted,
            error_msg=None, started_at=started_at
        )
        log.info(f"=== Ingestion complete | {rows_inserted} rows | batch={batch_id} ===")

    except requests.exceptions.Timeout:
        log.error("API request timed out after 30 seconds")
        log_ingestion_run(
            conn, batch_id, "cdc_chronic_disease",
            status="failed", rows=0,
            error_msg="API timeout", started_at=started_at
        )

    except requests.exceptions.HTTPError as e:
        log.error(f"API HTTP error: {e}")
        log_ingestion_run(
            conn, batch_id, "cdc_chronic_disease",
            status="failed", rows=0,
            error_msg=str(e), started_at=started_at
        )

    except requests.exceptions.ConnectionError:
        log.error("Could not reach the API — check your network connection")
        log_ingestion_run(
            conn, batch_id, "cdc_chronic_disease",
            status="failed", rows=0,
            error_msg="Connection error", started_at=started_at
        )

    except Exception as e:
        log.error(f"Unexpected error during ingestion: {e}")
        log_ingestion_run(
            conn, batch_id, "cdc_chronic_disease",
            status="failed", rows=0,
            error_msg=str(e), started_at=started_at
        )
        raise  # Re-raise so the system knows the job failed

    finally:
        conn.close()
        log.info("Database connection closed")


if __name__ == "__main__":
    main()