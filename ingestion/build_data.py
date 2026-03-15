

import psycopg2
import logging
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("build.log"),
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     os.environ.get("POSTGRES_PORT", 5434),
    "database": os.environ.get("POSTGRES_DB", "health_pipeline"),
    "user":     os.environ.get("POSTGRES_USER", "pipeline_user"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
}

# ─────────────────────────────────────────────
# STEP 1: CREATE GOLD TABLES
# ─────────────────────────────────────────────
CREATE_GOLD_TABLES_SQL = """
CREATE SCHEMA IF NOT EXISTS reporting;

CREATE TABLE IF NOT EXISTS reporting.monthly_death_trends (
    year                    INTEGER         NOT NULL,
    month                   INTEGER         NOT NULL,
    total_deaths            INTEGER,
    heart_disease_deaths    INTEGER,
    cancer_deaths           INTEGER,
    covid_deaths            INTEGER,
    drug_overdose_deaths    INTEGER,
    heart_disease_pct       NUMERIC(5,2),
    cancer_pct              NUMERIC(5,2),
    covid_pct               NUMERIC(5,2),
    drug_overdose_pct       NUMERIC(5,2),
    is_provisional          BOOLEAN,
    loaded_at               TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (year, month)
);

CREATE TABLE IF NOT EXISTS reporting.cause_of_death_summary (

    year                    INTEGER         NOT NULL,
    cause                   VARCHAR(100)    NOT NULL,
    total_deaths            INTEGER,
    avg_monthly_deaths      NUMERIC(10,2),
    pct_of_all_deaths       NUMERIC(5,2),
    loaded_at               TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (year, cause)
);

CREATE TABLE IF NOT EXISTS reporting.covid_impact (
    year                    INTEGER         NOT NULL,
    month                   INTEGER         NOT NULL,
    total_deaths            INTEGER,
    covid_deaths            INTEGER,
    non_covid_deaths        INTEGER,
    covid_pct               NUMERIC(5,2),
    excess_vs_prev_year     INTEGER,
    loaded_at               TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (year, month)
);
"""

# ─────────────────────────────────────────────
# STEP 2: TRANSFORM QUERIES
# ─────────────────────────────────────────────

# Table 1 — Monthly trends with percentages
MONTHLY_TRENDS_SQL = """
INSERT INTO reporting.monthly_death_trends (
    year, month, total_deaths,
    heart_disease_deaths, cancer_deaths,
    covid_deaths, drug_overdose_deaths,
    heart_disease_pct, cancer_pct,
    covid_pct, drug_overdose_pct,
    is_provisional
)
SELECT
    year,
    month,
    all_cause                                                       AS total_deaths,
    diseases_of_heart                                               AS heart_disease_deaths,
    malignant_neoplasms                                             AS cancer_deaths,
    covid_19_underlying                                             AS covid_deaths,
    drug_overdose                                                   AS drug_overdose_deaths,

    -- Percentages — cast to float to avoid integer division
    ROUND((diseases_of_heart::FLOAT   / NULLIF(all_cause,0) * 100)::NUMERIC, 2)  AS heart_disease_pct,
    ROUND((malignant_neoplasms::FLOAT / NULLIF(all_cause,0) * 100)::NUMERIC, 2)  AS cancer_pct,
    ROUND((covid_19_underlying::FLOAT / NULLIF(all_cause,0) * 100)::NUMERIC, 2)  AS covid_pct,
    ROUND((drug_overdose::FLOAT       / NULLIF(all_cause,0) * 100)::NUMERIC, 2)  AS drug_overdose_pct,
    is_provisional

FROM transformed.cdc_deaths         -- ← reads from silver, never bronze

ON CONFLICT (year, month)
DO UPDATE SET
    total_deaths         = EXCLUDED.total_deaths,
    heart_disease_deaths = EXCLUDED.heart_disease_deaths,
    cancer_deaths        = EXCLUDED.cancer_deaths,
    covid_deaths         = EXCLUDED.covid_deaths,
    drug_overdose_deaths = EXCLUDED.drug_overdose_deaths,
    heart_disease_pct    = EXCLUDED.heart_disease_pct,
    cancer_pct           = EXCLUDED.cancer_pct,
    covid_pct            = EXCLUDED.covid_pct,
    drug_overdose_pct    = EXCLUDED.drug_overdose_pct,
    is_provisional       = EXCLUDED.is_provisional,
    loaded_at            = NOW();
"""


CAUSE_SUMMARY_SQL = """
INSERT INTO reporting.cause_of_death_summary (
    year, cause, total_deaths, avg_monthly_deaths, pct_of_all_deaths
)

-- We use UNION ALL to turn each wide column into its own row
-- This is called UNPIVOTING — wide format → long format
SELECT
    year,
    'Heart Disease'                                         AS cause,
    SUM(diseases_of_heart)                                  AS total_deaths,
    ROUND(AVG(diseases_of_heart)::NUMERIC, 2)               AS avg_monthly_deaths,
    ROUND((SUM(diseases_of_heart)::FLOAT 
        / NULLIF(SUM(all_cause), 0) * 100)::NUMERIC, 2)    AS pct_of_all_deaths
FROM transformed.cdc_deaths
GROUP BY year

UNION ALL

SELECT
    year,
    'Cancer'                                                AS cause,
    SUM(malignant_neoplasms),
    ROUND(AVG(malignant_neoplasms)::NUMERIC, 2),
    ROUND((SUM(malignant_neoplasms)::FLOAT 
        / NULLIF(SUM(all_cause), 0) * 100)::NUMERIC, 2)
FROM transformed.cdc_deaths
GROUP BY year

UNION ALL

SELECT
    year,
    'COVID-19'                                              AS cause,
    SUM(covid_19_underlying),
    ROUND(AVG(covid_19_underlying)::NUMERIC, 2),
    ROUND((SUM(covid_19_underlying)::FLOAT 
        / NULLIF(SUM(all_cause), 0) * 100)::NUMERIC, 2)
FROM transformed.cdc_deaths
GROUP BY year

UNION ALL

SELECT
    year,
    'Drug Overdose'                                         AS cause,
    SUM(drug_overdose),
    ROUND(AVG(drug_overdose)::NUMERIC, 2),
    ROUND((SUM(drug_overdose)::FLOAT 
        / NULLIF(SUM(all_cause), 0) * 100)::NUMERIC, 2)
FROM transformed.cdc_deaths
GROUP BY year

UNION ALL

SELECT
    year,
    'Alzheimer Disease'                                     AS cause,
    SUM(alzheimer_disease),
    ROUND(AVG(alzheimer_disease)::NUMERIC, 2),
    ROUND((SUM(alzheimer_disease)::FLOAT 
        / NULLIF(SUM(all_cause), 0) * 100)::NUMERIC, 2)
FROM transformed.cdc_deaths
GROUP BY year

UNION ALL

SELECT
    year,
    'Diabetes'                                              AS cause,
    SUM(diabetes_mellitus),
    ROUND(AVG(diabetes_mellitus)::NUMERIC, 2),
    ROUND((SUM(diabetes_mellitus)::FLOAT 
        / NULLIF(SUM(all_cause), 0) * 100)::NUMERIC, 2)
FROM transformed.cdc_deaths
GROUP BY year

ON CONFLICT (year, cause)
DO UPDATE SET
    total_deaths        = EXCLUDED.total_deaths,
    avg_monthly_deaths  = EXCLUDED.avg_monthly_deaths,
    pct_of_all_deaths   = EXCLUDED.pct_of_all_deaths,
    loaded_at           = NOW();
"""

# Table 3 — COVID excess mortality
# Uses a self-join to compare same month across years
COVID_IMPACT_SQL = """
INSERT INTO reporting.covid_impact (
    year, month, total_deaths, covid_deaths,
    non_covid_deaths, covid_pct, excess_vs_prev_year
)
SELECT
    current.year,
    current.month,
    current.all_cause                                               AS total_deaths,
    current.covid_19_underlying                                     AS covid_deaths,

    -- Non-covid deaths = total minus covid
    (current.all_cause - COALESCE(current.covid_19_underlying, 0)) AS non_covid_deaths,

    -- COVID as percentage of all deaths
    ROUND((current.covid_19_underlying::FLOAT 
        / NULLIF(current.all_cause, 0) * 100)::NUMERIC, 2)        AS covid_pct,

    -- Excess deaths vs same month previous year
    -- If January 2021 had 50k more deaths than January 2020
    -- that excess is what we measure here
    (current.all_cause - COALESCE(prev.all_cause, current.all_cause)) 
                                                                    AS excess_vs_prev_year

FROM transformed.cdc_deaths current

-- Self join: match current row with same month, previous year
LEFT JOIN transformed.cdc_deaths prev
    ON  prev.year  = current.year - 1
    AND prev.month = current.month

ON CONFLICT (year, month)
DO UPDATE SET
    total_deaths        = EXCLUDED.total_deaths,
    covid_deaths        = EXCLUDED.covid_deaths,
    non_covid_deaths    = EXCLUDED.non_covid_deaths,
    covid_pct           = EXCLUDED.covid_pct,
    excess_vs_prev_year = EXCLUDED.excess_vs_prev_year,
    loaded_at           = NOW();
"""

# ─────────────────────────────────────────────
# STEP 3: EXECUTE
# ─────────────────────────────────────────────
def build_gold_layer(conn):
    queries = [
        ("monthly_death_trends",    MONTHLY_TRENDS_SQL),
        ("cause_of_death_summary",  CAUSE_SUMMARY_SQL),
        ("covid_impact",            COVID_IMPACT_SQL),
    ]

    with conn.cursor() as cur:
        for table_name, sql in queries:
            log.info(f"Building reporting.{table_name}...")
            cur.execute(sql)
            log.info(f"  → reporting.{table_name} ✅")

    conn.commit()

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    log.info("=" * 55)
    log.info("Stage 4 — Gold Layer Build")
    log.info("=" * 55)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        log.info("Database connection established")
    except Exception as e:
        log.error(f"Failed to connect to database: {e}")
        raise

    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_GOLD_TABLES_SQL)
        conn.commit()
        log.info("Gold tables ready")

        build_gold_layer(conn)
        log.info("✅ Stage 4 complete.")

    except Exception as e:
        log.error(f"Gold layer build failed: {e}")
        conn.rollback()  # ← undo everything if anything fails
        raise

    finally:
        conn.close()
        log.info("Database connection closed")


if __name__ == "__main__":
    main()
