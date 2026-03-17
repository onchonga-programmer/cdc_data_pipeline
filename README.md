# CDC Health Data Pipeline

A production-grade data engineering pipeline that ingests US mortality data from the CDC API, transforms it through a Medallion architecture (Bronze → Silver → Gold), and serves it via an automated dashboard.

Built as a learning project to demonstrate core data engineering principles: idempotency, layer isolation, data quality testing, and pipeline orchestration.

---

## What It Does

Every night at midnight, this pipeline automatically:

1. Fetches monthly US death data from the CDC's public API
2. Stores it raw and untouched in a bronze layer
3. Cleans, casts, and validates it into a silver layer
4. Builds three reporting tables in a gold layer
5. Runs 15 data quality checks to certify the gold layer is trustworthy
6. Makes the results available in a Metabase dashboard

---

## Architecture

```
CDC API  (https://data.cdc.gov/resource/9dzk-mvmi.json)
    │
    ▼
Bronze Layer — raw.cdc_chronic_disease
    Raw JSONB stored exactly as received. Never modified.
    │
    ▼
Silver Layer — transformed.cdc_deaths
    Cleaned, typed, and validated. Idempotent upserts.
    │
    ▼
Gold Layer — reporting.*
    Business-ready reporting tables with calculated metrics.
    │
    ▼
Metabase Dashboard (localhost:3000)
    Visual charts powered by gold layer tables.
```

---

## Project Structure

```
cdc-pipeline/
├── ingestion/
│   ├── fetch_cdc_data.py       # Stage 2 — Bronze ingestion
│   ├── transform_data.py       # Stage 3 — Silver transformation
│   ├── build_data.py           # Stage 4 — Gold layer build
│   ├── test_gold_layer.py      # Stage 6 — Data quality tests
│   ├── ingestion.log           # Stage 2 run logs
│   ├── transform.log           # Stage 3 run logs
│   ├── gold.log                # Stage 4 run logs
│   └── quality.log             # Stage 6 test logs
├── airflow/
│   └── dags/
│       └── health_pipeline.py  # Stage 5 — Airflow DAG
├── logs/                       # Airflow scheduler logs
├── .env                        # Environment variables (never committed)
├── .gitignore
├── docker-compose.yml          # Full infrastructure definition
└── requirements.txt            # Python dependencies
```

---

## Database Schema

### Bronze Layer — `raw`

| Table | Description |
|---|---|
| `raw.cdc_chronic_disease` | Raw JSONB records from CDC API, one row per API record |
| `raw.ingestion_log` | Audit log of every ingestion run — batch ID, status, row count |

### Silver Layer — `transformed`

| Table | Description |
|---|---|
| `transformed.cdc_deaths` | Cleaned and typed death records. Grain: one row per jurisdiction + year + month |

### Gold Layer — `reporting`

| Table | Grain | Description |
|---|---|---|
| `reporting.monthly_death_trends` | One row per year + month | Death counts and percentages by cause per month |
| `reporting.cause_of_death_summary` | One row per year + cause | Yearly totals, averages and percentages per cause |
| `reporting.covid_impact` | One row per year + month | COVID deaths, non-COVID deaths, and excess mortality vs prior year |

---

## Key Engineering Decisions

**Idempotency via UPSERT**
All load operations use `INSERT ... ON CONFLICT DO UPDATE`. Running the pipeline twice produces the same result as running it once — no duplicates, no data loss.

**Layer Isolation**
Each layer reads only from the layer directly behind it. Silver reads from bronze. Gold reads from silver. No layer ever skips or goes backwards. This makes the pipeline debuggable and safe to rerun.

**Raw Vault Bronze Pattern**
The bronze layer stores data as JSONB exactly as the API returned it. No transformation happens at ingestion. This means any past data can always be reprocessed if business logic changes.

**NULL over Zero**
Where data is unavailable (e.g. `excess_vs_prev_year` for the earliest year in the dataset), we store NULL rather than 0. A NULL honestly means "unknown." A 0 falsely implies "no change."

**Provisional Data Flagging**
The CDC suppresses recent data for 6 months while counts are being finalised. We detect this via CDC flag columns and derive an `is_provisional` boolean rather than dropping or silently misrepresenting these rows.

**Data Quality Tests**
15 automated tests run after every gold layer build covering uniqueness, validity, completeness, and consistency. If any test fails, Airflow marks the pipeline as failed before bad data can reach the dashboard.

---

## Technology Stack

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.12 | Pipeline scripting |
| PostgreSQL | 15 | Data warehouse |
| Apache Airflow | 2.8.1 | Pipeline orchestration |
| Metabase | Latest | Data visualisation |
| Docker | — | Infrastructure containerisation |
| psycopg2 | 2.9.7 | PostgreSQL connectivity |
| requests | 2.31.0 | CDC API calls |
| python-dotenv | 1.0.0 | Environment variable management |
| dbt-postgres | 1.7.0 | Reserved for future dbt models |

---

## Infrastructure

All services run in Docker. One command starts the entire stack:

```bash
docker-compose up -d
```

| Container | Image | Port | Purpose |
|---|---|---|---|
| `health_postgres` | postgres:15 | 5434 | Data warehouse |
| `health_airflow_webserver` | apache/airflow:2.8.1 | 8080 | Airflow UI |
| `health_airflow_scheduler` | apache/airflow:2.8.1 | — | DAG scheduler |
| `health_metabase` | metabase/metabase | 3000 | Dashboard UI |

---

## Setup & Running

### Prerequisites
- Docker and Docker Compose installed
- Python 3.12+
- A CDC App Token (optional but recommended for higher API rate limits)

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd cdc-pipeline
```

### 2. Create your environment file
```bash
cp .env.example .env
```

Edit `.env` with your values:
```
POSTGRES_HOST=localhost
POSTGRES_PORT=5434
POSTGRES_DB=health_pipeline
POSTGRES_USER=pipeline_user
POSTGRES_PASSWORD=your_password
CDC_APP_TOKEN=your_cdc_token
```

### 3. Start the infrastructure
```bash
docker-compose up -d
```

Wait for all containers to show healthy:
```bash
docker-compose ps
```

### 4. Set up your Python environment
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 5. Run the pipeline manually
```bash
# Stage 2 — Ingest from CDC API
python ingestion/fetch_cdc_data.py

# Stage 3 — Transform to silver
python ingestion/transform_data.py

# Stage 4 — Build gold layer
python ingestion/build_data.py

# Stage 6 — Run quality tests
python ingestion/test_gold_layer.py
```

### 6. Access the services
| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Metabase | http://localhost:3000 | Set during first login |
| PostgreSQL | localhost:5434 | See .env file |

---

## Airflow DAG

The pipeline is orchestrated by a single DAG: `cdc_health_pipeline`

```
fetch_cdc_data → transform_silver → build_gold → test_gold_quality
```

**Schedule:** Daily at midnight UTC

**Failure behaviour:** If any task fails, all downstream tasks are automatically skipped. Bad data never propagates to the next layer.

**Retry policy:** Each task retries once after a 5-minute delay before marking as failed.

To trigger a manual run:
1. Open http://localhost:8080
2. Find `cdc_health_pipeline`
3. Click the ▶ play button

---

## Data Quality Tests

15 automated tests run against the gold layer after every pipeline execution:

| Table | Tests |
|---|---|
| `monthly_death_trends` | No duplicate year+month, all full years have 12 months, deaths between 100k-500k, no negative counts, cause deaths don't exceed total, all percentages between 0-100 |
| `cause_of_death_summary` | No duplicate year+cause, exactly 6 causes per year, percentages between 0-100, no negative values |
| `covid_impact` | No duplicate year+month, covid + non_covid = total, covid_pct between 0-100, excess NULL only for earliest year, no negative counts |

Any test failure exits with code 1, which Airflow interprets as a task failure.

---

## Data Source

**CDC National Center for Health Statistics**
Weekly Counts of Deaths by State and Select Causes
- API endpoint: `https://data.cdc.gov/resource/9dzk-mvmi.json`
- Coverage: United States, 2020 to present
- Update frequency: Weekly
- Note: Data for the most recent 6 months is provisional and subject to revision

---

## Lessons Learned

**Idempotency is not optional.** The first version of this pipeline used plain `INSERT` statements. Running it twice created duplicate rows. Switching to `ON CONFLICT DO UPDATE` fixed this — and the grain check query (`HAVING COUNT(*) > 1`) became a permanent part of validation.

**NULL is honest. Zero is not.** When 2019 baseline data was unavailable for excess mortality calculations, replacing NULL with 0 would have made every 2020 row appear to have zero excess deaths — a dangerously wrong conclusion. NULL correctly communicates "we don't have enough data to calculate this."

**Read the log before guessing.** A Python 3.9+ type hint (`list[dict]`) caused a silent failure in Airflow's Python 3.8 container. The fix took 2 minutes once the log was read. Without the log it could have taken hours.

**Test the data, not just the pipeline.** A pipeline can run successfully and produce wrong data. The 15 data quality tests exist precisely because "all green boxes in Airflow" does not mean "data is correct."