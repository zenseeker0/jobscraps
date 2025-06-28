import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import sql

from .config import DatabaseConfig
from .backup import BackupMixin

SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger = logging.getLogger(__name__)


class JobDatabase(BackupMixin):
    """Class to handle database operations for job data."""

    def __init__(self, config_path: str | None = None, database_type: str = "production"):
        if config_path is None:
            config_path = os.path.join(SCRIPT_DIR, "configs", "db", "db_config.json")
        self.db_config = DatabaseConfig(config_path, database_type)
        self.database_type = database_type
        self.conn = None
        self._connect_with_retry()
        self.create_tables()
        logger.info("PostgreSQL database initialized (%s)", database_type)

    def _connect_with_retry(self) -> None:
        retry_config = self.db_config.get_retry_config()
        max_attempts = retry_config.get("retry_attempts", 3)
        retry_delay = retry_config.get("retry_delay", 5)
        for attempt in range(max_attempts):
            try:
                conn_params = self.db_config.get_connection_params()
                self.conn = psycopg2.connect(**conn_params)
                self.conn.autocommit = False
                logger.info("Connected to PostgreSQL database successfully")
                return
            except psycopg2.Error as exc:
                logger.warning("Database connection attempt %s failed: %s", attempt + 1, exc)
                if attempt < max_attempts - 1:
                    time.sleep(retry_delay)
                else:
                    raise psycopg2.Error(f"Failed to connect after {max_attempts} attempts: {exc}")

    def _ensure_connection(self) -> None:
        try:
            if self.conn.closed:
                logger.warning("Database connection is closed, reconnecting...")
                self._connect_with_retry()
        except (psycopg2.Error, AttributeError):
            logger.warning("Database connection error, reconnecting...")
            self._connect_with_retry()

    def create_tables(self) -> None:
        self._ensure_connection()
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS scraped_jobs (
                id TEXT PRIMARY KEY,
                site TEXT,
                job_url TEXT,
                job_url_direct TEXT,
                title TEXT,
                company TEXT,
                location TEXT,
                date_posted TEXT,
                job_type TEXT,
                salary_source TEXT,
                interval TEXT,
                min_amount DECIMAL(12,2),
                max_amount DECIMAL(12,2),
                currency TEXT,
                is_remote BOOLEAN,
                job_level TEXT,
                job_function TEXT,
                listing_type TEXT,
                emails TEXT,
                description TEXT,
                company_industry TEXT,
                company_url TEXT,
                company_logo TEXT,
                company_url_direct TEXT,
                company_addresses TEXT,
                company_num_employees TEXT,
                company_revenue TEXT,
                company_description TEXT,
                skills TEXT,
                experience_range TEXT,
                company_rating TEXT,
                company_reviews_count TEXT,
                vacancy_count TEXT,
                work_from_home_type TEXT,
                date_scraped TIMESTAMP,
                search_query TEXT
            )
            """
            )
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS search_sessions (
                id SERIAL PRIMARY KEY,
                start_time TIMESTAMP
            )
            """
            )
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS search_history (
                id SERIAL PRIMARY KEY,
                session_id INTEGER REFERENCES search_sessions(id),
                search_query TEXT,
                parameters TEXT,
                new_jobs_inserted INTEGER,
                duration_seconds NUMERIC,
                site_breakdown TEXT,
                duplicate_breakdown TEXT,
                remote_jobs_count INTEGER,
                avg_salary NUMERIC(12,2),
                timestamp TIMESTAMP,
                jobs_found INTEGER
            )
            """
            )
            cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
            index_queries = [
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_title ON scraped_jobs(title)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_company ON scraped_jobs(company)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_location ON scraped_jobs(location)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_site ON scraped_jobs(site)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_is_remote ON scraped_jobs(is_remote)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_date_posted ON scraped_jobs(date_posted)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_date_scraped ON scraped_jobs(date_scraped)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_search_query ON scraped_jobs(search_query)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_search_query_lower ON scraped_jobs(LOWER(search_query))",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_title_company ON scraped_jobs(title, company)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_company_location ON scraped_jobs(company, location)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_description_gin ON scraped_jobs USING gin (description gin_trgm_ops)",
                "CREATE INDEX IF NOT EXISTS idx_search_history_search_query ON search_history(search_query)",
                "CREATE INDEX IF NOT EXISTS idx_search_history_timestamp ON search_history(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_search_history_session_id ON search_history(session_id)",
                "CREATE INDEX IF NOT EXISTS idx_search_history_site_breakdown ON search_history USING gin (site_breakdown)",
                "CREATE INDEX IF NOT EXISTS idx_search_history_duplicate_breakdown ON search_history USING gin (duplicate_breakdown)",
            ]
            for query in index_queries:
                try:
                    cursor.execute(query)
                except Exception as exc:  # pylint: disable=broad-except
                    logger.warning("Index creation warning: %s", exc)
            self.conn.commit()
            logger.info("Database tables and indexes created/verified successfully")

    def insert_jobs(
        self, jobs_df: pd.DataFrame, search_query: str
    ) -> tuple[int, Dict[str, int], Dict[str, int], int, float]:
        """Insert jobs into the database.

        Returns the number of new jobs inserted along with a breakdown of
        inserted jobs per site, duplicate counts per site, the number of remote
        jobs inserted and the average salary of the inserted jobs.
        """

        self._ensure_connection()
        jobs_df["date_scraped"] = datetime.now()
        jobs_df["search_query"] = search_query

        with self.conn.cursor() as cursor:
            cursor.execute("SELECT id FROM scraped_jobs")
            existing_ids = {row[0] for row in cursor.fetchall()}

        if "id" in jobs_df.columns:
            new_jobs_df = jobs_df[~jobs_df["id"].isin(existing_ids)]
        else:
            jobs_df["id"] = jobs_df.apply(
                lambda row: f"{row.get('site', 'unknown')}_{row.get('job_url', '')[-20:]}",
                axis=1,
            )
            new_jobs_df = jobs_df[~jobs_df["id"].isin(existing_ids)]

        if "is_remote" in new_jobs_df.columns:
            new_jobs_df["is_remote"] = new_jobs_df["is_remote"].astype(bool)

        new_jobs_count = len(new_jobs_df)
        site_counts = new_jobs_df["site"].fillna("unknown").value_counts().to_dict()

        duplicate_df = jobs_df[jobs_df["id"].isin(existing_ids)]
        duplicate_counts = duplicate_df["site"].fillna("unknown").value_counts().to_dict()

        remote_jobs_count = int(new_jobs_df.get("is_remote", pd.Series(dtype=bool)).sum())

        if not new_jobs_df.empty:
            salary_cols = new_jobs_df[["min_amount", "max_amount"]].apply(
                pd.to_numeric, errors="coerce"
            )
            salary_mean = salary_cols.mean(axis=1)
            avg_salary = float(salary_mean.mean()) if not salary_mean.empty else 0.0
        else:
            avg_salary = 0.0
        if new_jobs_count > 0:
            columns = [
                "id",
                "site",
                "job_url",
                "job_url_direct",
                "title",
                "company",
                "location",
                "date_posted",
                "job_type",
                "salary_source",
                "interval",
                "min_amount",
                "max_amount",
                "currency",
                "is_remote",
                "job_level",
                "job_function",
                "listing_type",
                "emails",
                "description",
                "company_industry",
                "company_url",
                "company_logo",
                "company_url_direct",
                "company_addresses",
                "company_num_employees",
                "company_revenue",
                "company_description",
                "skills",
                "experience_range",
                "company_rating",
                "company_reviews_count",
                "vacancy_count",
                "work_from_home_type",
                "date_scraped",
                "search_query",
            ]
            for col in columns:
                if col not in new_jobs_df.columns:
                    new_jobs_df[col] = None
            data_to_insert = []
            for _, row in new_jobs_df.iterrows():
                row_data = []
                for col in columns:
                    value = row[col]
                    if pd.isna(value):
                        row_data.append(None)
                    else:
                        row_data.append(value)
                data_to_insert.append(tuple(row_data))
            insert_query = sql.SQL(
                "INSERT INTO scraped_jobs (" + ",".join(columns) + ") VALUES (" + ",".join(["%s"] * len(columns)) + ")"
            )
            with self.conn.cursor() as cursor:
                psycopg2.extras.execute_batch(cursor, insert_query, data_to_insert)
                self.conn.commit()
            logger.info("Inserted %s new jobs into database", new_jobs_count)
        else:
            logger.info("No new jobs to insert")

        return new_jobs_count, site_counts, duplicate_counts, remote_jobs_count, avg_salary

    def start_session(self) -> int:
        self._ensure_connection()
        with self.conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO search_sessions (start_time) VALUES (%s) RETURNING id",
                (datetime.now(),),
            )
            session_id = cursor.fetchone()[0]
            self.conn.commit()
        return session_id

    def log_search(
        self,
        session_id: int,
        search_query: str,
        parameters: Dict,
        jobs_found: int,
        new_jobs_inserted: int,
        duration_seconds: float,
        site_breakdown: Dict[str, int],
        duplicate_breakdown: Dict[str, int],
        remote_jobs_count: int,
        avg_salary: float,
    ) -> None:
        """Log a search operation with extended metrics."""

        self._ensure_connection()
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO search_history (
                    session_id,
                    search_query,
                    parameters,
                    new_jobs_inserted,
                    duration_seconds,
                    site_breakdown,
                    duplicate_breakdown,
                    remote_jobs_count,
                    avg_salary,
                    timestamp,
                    jobs_found
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    session_id,
                    search_query,
                    json.dumps(parameters),
                    new_jobs_inserted,
                    duration_seconds,
                    json.dumps(site_breakdown),
                    json.dumps(duplicate_breakdown),
                    remote_jobs_count,
                    avg_salary,
                    datetime.now(),
                    jobs_found,
                ),
            )
            self.conn.commit()

    def get_all_jobs(self) -> pd.DataFrame:
        self._ensure_connection()
        return pd.read_sql("SELECT * FROM scraped_jobs", self.conn)

    def get_jobs_by_query(self, search_query: str) -> pd.DataFrame:
        self._ensure_connection()
        query = "SELECT * FROM scraped_jobs WHERE search_query = %s"
        return pd.read_sql(query, self.conn, params=(search_query,))

    def get_duplicate_groups(self) -> List[List[Dict]]:
        self._ensure_connection()
        query = (
            "SELECT id, site, title, company, description, min_amount, max_amount, job_url, is_remote, location, search_query, date_posted "
            "FROM scraped_jobs WHERE title IS NOT NULL AND company IS NOT NULL ORDER BY title, company, site"
        )
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
        groups: Dict[tuple, List[Dict]] = defaultdict(list)
        for row in rows:
            job_dict = dict(row)
            key = (
                row["title"].strip().lower() if row["title"] else "",
                row["company"].strip().lower() if row["company"] else "",
            )
            groups[key].append(job_dict)
        duplicate_groups = [group for group in groups.values() if len(group) > 1]
        return duplicate_groups

    def clear_all_jobs(self) -> int:
        self._ensure_connection()
        with self.conn.cursor() as cursor:
            cursor.execute("DELETE FROM scraped_jobs")
            rows_deleted = cursor.rowcount
            self.conn.commit()
        logger.info("Cleared %s rows from scraped_jobs table", rows_deleted)
        return rows_deleted

    def delete_jobs_before_date(self, date_str: str) -> int:
        self._ensure_connection()
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            with self.conn.cursor() as cursor:
                cursor.execute("DELETE FROM scraped_jobs WHERE date_scraped < %s", (date_obj,))
                rows_deleted = cursor.rowcount
                self.conn.commit()
            logger.info("Deleted %s jobs scraped before %s", rows_deleted, date_str)
            return rows_deleted
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Error deleting jobs by date: %s", exc)
            return 0

    def delete_jobs_by_ids(self, ids_file: str) -> int:
        self._ensure_connection()
        try:
            if not os.path.exists(ids_file):
                logger.warning("IDs file %s not found", ids_file)
                return 0
            with open(ids_file, "r") as f:
                job_ids = [line.strip() for line in f if line.strip()]
            if not job_ids:
                logger.warning("No IDs found in %s", ids_file)
                return 0
            with self.conn.cursor() as cursor:
                cursor.execute("DELETE FROM scraped_jobs WHERE id = ANY(%s)", (job_ids,))
                rows_deleted = cursor.rowcount
                self.conn.commit()
            logger.info("Deleted %s jobs by ID from %s", rows_deleted, ids_file)
            return rows_deleted
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Error deleting jobs by IDs: %s", exc)
            return 0

    def delete_jobs_by_salary(self, min_threshold: int = 70000, max_threshold: int = 90000) -> int:
        self._ensure_connection()
        try:
            query = (
                "DELETE FROM scraped_jobs WHERE (min_amount != 0 AND min_amount < %s AND max_amount < %s) "
                "OR (min_amount >= %s AND max_amount < %s)"
            )
            with self.conn.cursor() as cursor:
                cursor.execute(query, (min_threshold, max_threshold, min_threshold, max_threshold))
                rows_deleted = cursor.rowcount
                self.conn.commit()
            logger.info(
                "Deleted %s jobs with salaries below thresholds (min: %s, max: %s)",
                rows_deleted,
                min_threshold,
                max_threshold,
            )
            return rows_deleted
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Error deleting jobs by salary: %s", exc)
            return 0

    def delete_jobs_by_field(self, field: str, patterns_file: str) -> int:
        self._ensure_connection()
        try:
            if not os.path.exists(patterns_file):
                logger.warning("Patterns file %s not found", patterns_file)
                return 0
            with open(patterns_file, "r") as f:
                patterns = [line.strip() for line in f if line.strip()]
            if not patterns:
                logger.warning("No patterns found in %s", patterns_file)
                return 0
            valid_fields = {"company", "title"}
            if field not in valid_fields:
                logger.error("Invalid field name: %s", field)
                return 0
            rows_deleted = 0
            with self.conn.cursor() as cursor:
                for pattern in patterns:
                    pattern_lower = pattern.lower()
                    query = sql.SQL("DELETE FROM scraped_jobs WHERE LOWER({}) LIKE %s").format(sql.Identifier(field))
                    cursor.execute(query, (pattern_lower,))
                    pattern_deleted = cursor.rowcount
                    rows_deleted += pattern_deleted
                self.conn.commit()
            logger.info("Deleted %s jobs matching %s patterns from %s", rows_deleted, field, patterns_file)
            return rows_deleted
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Error deleting jobs by %s: %s", field, exc)
            return 0

    def close(self) -> None:
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Database connection closed")


class ConnectionPool:
    """Manage shared JobDatabase instances."""

    _instances: Dict[Tuple[str, str], JobDatabase] = {}

    @classmethod
    def get_connection(
        cls, config_path: Optional[str] = None, database_type: str = "production"
    ) -> JobDatabase:
        """Return a pooled JobDatabase instance."""
        key = (config_path or "", database_type)
        db = cls._instances.get(key)
        if db is None or db.conn is None or getattr(db.conn, "closed", True):
            db = JobDatabase(config_path, database_type)
            cls._instances[key] = db
        return db


def get_connection(
    config_path: Optional[str] = None, database_type: str = "production"
) -> JobDatabase:
    """Convenience wrapper for ConnectionPool.get_connection."""
    return ConnectionPool.get_connection(config_path, database_type)

