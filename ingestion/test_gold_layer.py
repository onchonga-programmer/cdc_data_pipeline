

import psycopg2
import logging
import os
import sys
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
        logging.FileHandler("quality.log"),
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
# TEST RUNNER
# Keeps track of passes and failures
# ─────────────────────────────────────────────
class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.failures = []

    def check(self, test_name, condition, failure_message):
        """
        Runs a single test.
        condition = True  → test passes
        condition = False → test fails, records the message
        """
        if condition:
            log.info(f"  ✅ PASS  {test_name}")
            self.passed += 1
        else:
            log.error(f"  ❌ FAIL  {test_name} — {failure_message}")
            self.failed += 1
            self.failures.append(f"{test_name}: {failure_message}")

    def summary(self):
        total = self.passed + self.failed
        log.info(f"\n{'='*55}")
        log.info(f"Results: {self.passed}/{total} tests passed")
        if self.failures:
            log.error("Failed tests:")
            for f in self.failures:
                log.error(f"  → {f}")
        return self.failed == 0  # True if all passed


# ─────────────────────────────────────────────
# TESTS — monthly_death_trends
# ─────────────────────────────────────────────
def test_monthly_death_trends(conn, runner):
    log.info("\nTesting reporting.monthly_death_trends...")

    with conn.cursor() as cur:

        # 1. UNIQUENESS — no duplicate year+month
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT year, month
                FROM reporting.monthly_death_trends
                GROUP BY year, month
                HAVING COUNT(*) > 1
            ) duplicates;
        """)
        duplicate_count = cur.fetchone()[0]
        runner.check(
            "monthly_trends: no duplicate year+month",
            duplicate_count == 0,
            f"Found {duplicate_count} duplicate year+month combinations"
        )

        # 2. COMPLETENESS — full years have 12 months
        cur.execute("""
            SELECT year, COUNT(*) as month_count
            FROM reporting.monthly_death_trends
            GROUP BY year
            HAVING COUNT(*) < 12
              AND year < (SELECT MAX(year) FROM reporting.monthly_death_trends);
        """)
        incomplete_years = cur.fetchall()
        runner.check(
            "monthly_trends: all full years have 12 months",
            len(incomplete_years) == 0,
            f"Incomplete years found: {incomplete_years}"
        )

        # 3. VALIDITY — total deaths in realistic range
        cur.execute("""
            SELECT COUNT(*)
            FROM reporting.monthly_death_trends
            WHERE total_deaths < 100000
               OR total_deaths > 500000;
        """)
        out_of_range = cur.fetchone()[0]
        runner.check(
            "monthly_trends: total_deaths between 100k and 500k",
            out_of_range == 0,
            f"{out_of_range} rows have unrealistic death counts"
        )

        # 4. VALIDITY — no negative death counts
        cur.execute("""
            SELECT COUNT(*)
            FROM reporting.monthly_death_trends
            WHERE heart_disease_deaths < 0
               OR cancer_deaths < 0
               OR covid_deaths < 0
               OR drug_overdose_deaths < 0;
        """)
        negative_count = cur.fetchone()[0]
        runner.check(
            "monthly_trends: no negative death counts",
            negative_count == 0,
            f"{negative_count} rows have negative death counts"
        )

        # 5. CONSISTENCY — cause deaths don't exceed total
        cur.execute("""
            SELECT COUNT(*)
            FROM reporting.monthly_death_trends
            WHERE (COALESCE(heart_disease_deaths, 0) +
                   COALESCE(cancer_deaths, 0) +
                   COALESCE(covid_deaths, 0) +
                   COALESCE(drug_overdose_deaths, 0)) > total_deaths;
        """)
        inconsistent = cur.fetchone()[0]
        runner.check(
            "monthly_trends: cause deaths don't exceed total",
            inconsistent == 0,
            f"{inconsistent} rows where causes exceed total deaths"
        )

        # 6. VALIDITY — percentages between 0 and 100
        cur.execute("""
            SELECT COUNT(*)
            FROM reporting.monthly_death_trends
            WHERE heart_disease_pct < 0 OR heart_disease_pct > 100
               OR cancer_pct        < 0 OR cancer_pct        > 100
               OR covid_pct         < 0 OR covid_pct         > 100
               OR drug_overdose_pct < 0 OR drug_overdose_pct > 100;
        """)
        bad_pct = cur.fetchone()[0]
        runner.check(
            "monthly_trends: all percentages between 0 and 100",
            bad_pct == 0,
            f"{bad_pct} rows have percentages outside 0-100 range"
        )


# ─────────────────────────────────────────────
# TESTS — cause_of_death_summary
# ─────────────────────────────────────────────
def test_cause_summary(conn, runner):
    log.info("\nTesting reporting.cause_of_death_summary...")

    with conn.cursor() as cur:

        # 1. UNIQUENESS — no duplicate year+cause
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT year, cause
                FROM reporting.cause_of_death_summary
                GROUP BY year, cause
                HAVING COUNT(*) > 1
            ) duplicates;
        """)
        duplicate_count = cur.fetchone()[0]
        runner.check(
            "cause_summary: no duplicate year+cause",
            duplicate_count == 0,
            f"Found {duplicate_count} duplicate year+cause combinations"
        )

        # 2. COMPLETENESS — 6 causes per year
        cur.execute("""
            SELECT year, COUNT(*) as cause_count
            FROM reporting.cause_of_death_summary
            GROUP BY year
            HAVING COUNT(*) != 6;
        """)
        wrong_cause_count = cur.fetchall()
        runner.check(
            "cause_summary: exactly 6 causes per year",
            len(wrong_cause_count) == 0,
            f"Years with wrong cause count: {wrong_cause_count}"
        )

        # 3. VALIDITY — percentages between 0 and 100
        cur.execute("""
            SELECT COUNT(*)
            FROM reporting.cause_of_death_summary
            WHERE pct_of_all_deaths < 0
               OR pct_of_all_deaths > 100;
        """)
        bad_pct = cur.fetchone()[0]
        runner.check(
            "cause_summary: pct_of_all_deaths between 0 and 100",
            bad_pct == 0,
            f"{bad_pct} rows have percentages outside 0-100 range"
        )

        # 4. VALIDITY — no negative deaths
        cur.execute("""
            SELECT COUNT(*)
            FROM reporting.cause_of_death_summary
            WHERE total_deaths < 0
               OR avg_monthly_deaths < 0;
        """)
        negative_count = cur.fetchone()[0]
        runner.check(
            "cause_summary: no negative values",
            negative_count == 0,
            f"{negative_count} rows have negative values"
        )


# ─────────────────────────────────────────────
# TESTS — covid_impact
# ─────────────────────────────────────────────
def test_covid_impact(conn, runner):
    log.info("\nTesting reporting.covid_impact...")

    with conn.cursor() as cur:

        # 1. UNIQUENESS — no duplicate year+month
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT year, month
                FROM reporting.covid_impact
                GROUP BY year, month
                HAVING COUNT(*) > 1
            ) duplicates;
        """)
        duplicate_count = cur.fetchone()[0]
        runner.check(
            "covid_impact: no duplicate year+month",
            duplicate_count == 0,
            f"Found {duplicate_count} duplicate year+month combinations"
        )

        # 2. CONSISTENCY — covid + non_covid = total
        cur.execute("""
            SELECT COUNT(*)
            FROM reporting.covid_impact
            WHERE (covid_deaths + non_covid_deaths) != total_deaths;
        """)
        inconsistent = cur.fetchone()[0]
        runner.check(
            "covid_impact: covid + non_covid = total_deaths",
            inconsistent == 0,
            f"{inconsistent} rows where covid + non_covid != total"
        )

        # 3. VALIDITY — covid_pct between 0 and 100
        cur.execute("""
            SELECT COUNT(*)
            FROM reporting.covid_impact
            WHERE covid_pct < 0 OR covid_pct > 100;
        """)
        bad_pct = cur.fetchone()[0]
        runner.check(
            "covid_impact: covid_pct between 0 and 100",
            bad_pct == 0,
            f"{bad_pct} rows have covid_pct outside 0-100"
        )

        # 4. PRECISION — excess_vs_prev_year NULL only for earliest year
        cur.execute("""
            SELECT COUNT(*)
            FROM reporting.covid_impact
            WHERE excess_vs_prev_year IS NULL
              AND year != (SELECT MIN(year) FROM reporting.covid_impact);
        """)
        unexpected_nulls = cur.fetchone()[0]
        runner.check(
            "covid_impact: excess_vs_prev_year NULL only for earliest year",
            unexpected_nulls == 0,
            f"{unexpected_nulls} unexpected NULLs in excess_vs_prev_year"
        )

        # 5. VALIDITY — no negative covid deaths
        cur.execute("""
            SELECT COUNT(*)
            FROM reporting.covid_impact
            WHERE covid_deaths < 0
               OR non_covid_deaths < 0
               OR total_deaths < 0;
        """)
        negative_count = cur.fetchone()[0]
        runner.check(
            "covid_impact: no negative death counts",
            negative_count == 0,
            f"{negative_count} rows have negative values"
        )


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    log.info("=" * 55)
    log.info("Stage 6 — Gold Layer Data Quality Tests")
    log.info("=" * 55)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        log.info("Database connection established")
    except Exception as e:
        log.error(f"Failed to connect to database: {e}")
        raise

    runner = TestRunner()

    try:
        test_monthly_death_trends(conn, runner)
        test_cause_summary(conn, runner)
        test_covid_impact(conn, runner)

        all_passed = runner.summary()

        if not all_passed:
            log.error("❌ Quality checks failed — review errors above")
            sys.exit(1)      # ← exit code 1 = Airflow marks task as FAILED
        else:
            log.info("✅ All quality checks passed — gold layer is trusted")
            sys.exit(0)      # ← exit code 0 = Airflow marks task as SUCCESS

    except Exception as e:
        log.error(f"Test runner failed unexpectedly: {e}")
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()