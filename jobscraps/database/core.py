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

    def get_jobs_count(self) -> int:
        """Get count of jobs without loading all data."""
        self._ensure_connection()
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM scraped_jobs")
            return cursor.fetchone()[0]

    def create_tables(self) -> None:
        """Create all database tables, indexes, and views for complete schema."""
        self._ensure_connection()
        
        # Helper function to execute with rollback protection
        def safe_execute(cursor, query, description="operation"):
            try:
                cursor.execute(query)
                return True
            except Exception as exc:
                logger.warning("%s warning: %s", description, exc)
                try:
                    self.conn.rollback()
                except:
                    pass
                return False
        
        with self.conn.cursor() as cursor:
            # Create main tables - these are fast operations
            logger.info("Creating/verifying main tables...")
            
            safe_execute(cursor, """
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
            """, "scraped_jobs table creation")
            
            safe_execute(cursor, """
                CREATE TABLE IF NOT EXISTS search_sessions (
                    id SERIAL PRIMARY KEY,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    status TEXT
                )
            """, "search_sessions table creation")
            
            safe_execute(cursor, """
                CREATE TABLE IF NOT EXISTS search_history (
                    id SERIAL PRIMARY KEY,
                    session_id INTEGER REFERENCES search_sessions(id),
                    search_query TEXT,
                    parameters TEXT,
                    new_jobs_inserted INTEGER,
                    duration_seconds NUMERIC,
                    site_breakdown JSONB,
                    duplicate_breakdown JSONB,
                    remote_jobs_count INTEGER,
                    avg_salary NUMERIC(12,2),
                    timestamp TIMESTAMP,
                    jobs_found INTEGER
                )
            """, "search_history table creation")
            
            # Create user metadata tables
            safe_execute(cursor, """
                CREATE TABLE IF NOT EXISTS job_user_metadata (
                    job_id TEXT PRIMARY KEY,
                    reviewed BOOLEAN DEFAULT false,
                    status VARCHAR(20),
                    user_notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    excluded BOOLEAN DEFAULT false,
                    exclusion_reason TEXT
                )
            """, "job_user_metadata table creation")
            
            safe_execute(cursor, """
                CREATE TABLE IF NOT EXISTS company_user_metadata (
                    company_name TEXT PRIMARY KEY,
                    status VARCHAR(20),
                    notes TEXT,
                    appeal_factors TEXT,
                    application_history JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """, "company_user_metadata table creation")
            
            # Create extensions
            safe_execute(cursor, "CREATE EXTENSION IF NOT EXISTS pg_trgm", "pg_trgm extension")
            
            self.conn.commit()
            logger.info("Main tables created/verified successfully")
            
            # Create basic indexes (fast operations)
            logger.info("Creating/verifying basic indexes...")
            basic_indexes = [
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_id ON scraped_jobs(id)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_title ON scraped_jobs(title)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_company ON scraped_jobs(company)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_location ON scraped_jobs(location)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_site ON scraped_jobs(site)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_is_remote ON scraped_jobs(is_remote)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_date_posted ON scraped_jobs(date_posted)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_date_scraped ON scraped_jobs(date_scraped)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_search_query ON scraped_jobs(search_query)",
                "CREATE INDEX IF NOT EXISTS idx_search_history_search_query ON search_history(search_query)",
                "CREATE INDEX IF NOT EXISTS idx_search_history_timestamp ON search_history(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_search_history_session_id ON search_history(session_id)",
                "CREATE INDEX IF NOT EXISTS idx_job_metadata_status ON job_user_metadata(status)",
                "CREATE INDEX IF NOT EXISTS idx_job_metadata_reviewed ON job_user_metadata(reviewed)",
                "CREATE INDEX IF NOT EXISTS idx_job_metadata_excluded ON job_user_metadata(excluded)",
                "CREATE INDEX IF NOT EXISTS idx_company_metadata_status ON company_user_metadata(status)",
            ]
            
            for query in basic_indexes:
                safe_execute(cursor, query, "basic index creation")
            
            self.conn.commit()
            logger.info("Basic indexes created/verified successfully")
            
            # Create expensive indexes only if they don't exist (these can timeout on large datasets)
            logger.info("Checking expensive indexes...")
            expensive_indexes = [
                ("idx_scraped_jobs_description_gin", "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_description_gin ON scraped_jobs USING gin (description gin_trgm_ops)"),
                ("scraped_jobs_title_idx", "CREATE INDEX IF NOT EXISTS scraped_jobs_title_idx ON scraped_jobs USING gin (title gin_trgm_ops)"),
                ("idx_scraped_jobs_search_query_lower", "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_search_query_lower ON scraped_jobs(LOWER(search_query))"),
                ("idx_scraped_jobs_title_company", "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_title_company ON scraped_jobs(title, company)"),
                ("idx_scraped_jobs_company_location", "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_company_location ON scraped_jobs(company, location)"),
                ("idx_search_history_site_breakdown", "CREATE INDEX IF NOT EXISTS idx_search_history_site_breakdown ON search_history USING gin (site_breakdown)"),
                ("idx_search_history_duplicate_breakdown", "CREATE INDEX IF NOT EXISTS idx_search_history_duplicate_breakdown ON search_history USING gin (duplicate_breakdown)"),
            ]
            
            for index_name, query in expensive_indexes:
                # Check if index already exists
                cursor.execute("""
                    SELECT 1 FROM pg_indexes 
                    WHERE indexname = %s AND tablename IN ('scraped_jobs', 'search_history')
                """, (index_name,))
                
                if cursor.fetchone() is None:
                    logger.info("Creating expensive index: %s (this may take a while...)", index_name)
                    if safe_execute(cursor, query, f"expensive index {index_name}"):
                        try:
                            self.conn.commit()
                            logger.info("Successfully created index: %s", index_name)
                        except Exception as e:
                            logger.warning("Failed to commit index %s: %s", index_name, e)
                            try:
                                self.conn.rollback()
                            except:
                                pass
                    else:
                        logger.warning("Skipped problematic index: %s", index_name)
                else:
                    logger.info("Index %s already exists, skipping", index_name)
            
            # Create all views
            logger.info("Creating/updating database views...")
            
            # Main job board view with metadata
            safe_execute(cursor, """
                CREATE OR REPLACE VIEW job_board_main AS
                SELECT 
                    s.id,
                    s.site,
                    s.job_url,
                    s.job_url_direct,
                    s.title,
                    s.company,
                    s.location,
                    s.date_posted,
                    s.job_type,
                    s.salary_source,
                    s.interval,
                    s.min_amount,
                    s.max_amount,
                    s.currency,
                    s.is_remote,
                    s.job_level,
                    s.job_function,
                    s.listing_type,
                    s.emails,
                    s.description,
                    s.company_industry,
                    s.company_url,
                    s.company_logo,
                    s.company_url_direct,
                    s.company_addresses,
                    s.company_num_employees,
                    s.company_revenue,
                    s.company_description,
                    s.skills,
                    s.experience_range,
                    s.company_rating,
                    s.company_reviews_count,
                    s.vacancy_count,
                    s.work_from_home_type,
                    s.date_scraped,
                    s.search_query,
                    COALESCE(jum.status, 'unreviewed') AS status,
                    jum.user_notes,
                    COALESCE(jum.reviewed, false) AS reviewed,
                    COALESCE(jum.excluded, false) AS excluded,
                    jum.exclusion_reason
                FROM scraped_jobs s
                LEFT JOIN job_user_metadata jum ON s.id = jum.job_id
                WHERE COALESCE(jum.excluded, false) = false
            """, "job_board_main view creation")
            
            # Job details view (same as main but doesn't filter exclusions)
            safe_execute(cursor, """
                CREATE OR REPLACE VIEW job_details AS
                SELECT 
                    s.id,
                    s.site,
                    s.job_url,
                    s.job_url_direct,
                    s.title,
                    s.company,
                    s.location,
                    s.date_posted,
                    s.job_type,
                    s.salary_source,
                    s.interval,
                    s.min_amount,
                    s.max_amount,
                    s.currency,
                    s.is_remote,
                    s.job_level,
                    s.job_function,
                    s.listing_type,
                    s.emails,
                    s.description,
                    s.company_industry,
                    s.company_url,
                    s.company_logo,
                    s.company_url_direct,
                    s.company_addresses,
                    s.company_num_employees,
                    s.company_revenue,
                    s.company_description,
                    s.skills,
                    s.experience_range,
                    s.company_rating,
                    s.company_reviews_count,
                    s.vacancy_count,
                    s.work_from_home_type,
                    s.date_scraped,
                    s.search_query,
                    COALESCE(jum.status, 'unreviewed') AS status,
                    jum.user_notes,
                    COALESCE(jum.reviewed, false) AS reviewed,
                    COALESCE(jum.excluded, false) AS excluded,
                    jum.exclusion_reason
                FROM scraped_jobs s
                LEFT JOIN job_user_metadata jum ON s.id = jum.job_id
                WHERE COALESCE(jum.excluded, false) = false
            """, "job_details view creation")
            
            # Filtered views
            safe_execute(cursor, """
                CREATE OR REPLACE VIEW job_board_applied AS
                SELECT * FROM job_board_main
                WHERE status = 'applied'
            """, "job_board_applied view creation")
            
            safe_execute(cursor, """
                CREATE OR REPLACE VIEW job_board_needs_review AS
                SELECT * FROM job_board_main
                WHERE reviewed = false
            """, "job_board_needs_review view creation")
            
            safe_execute(cursor, """
                CREATE OR REPLACE VIEW job_board_remote AS
                SELECT * FROM job_board_main
                WHERE is_remote = true
            """, "job_board_remote view creation")
            
            safe_execute(cursor, """
                CREATE OR REPLACE VIEW job_board_with_salary AS
                SELECT * FROM job_board_main
                WHERE min_amount IS NOT NULL OR max_amount IS NOT NULL
            """, "job_board_with_salary view creation")
            
            # Export view (includes all columns)
            safe_execute(cursor, """
                CREATE OR REPLACE VIEW job_board_export AS
                SELECT * FROM job_board_main
            """, "job_board_export view creation")
            
            # Create updated_at trigger function and triggers
            logger.info("Creating/updating database triggers...")
            
            safe_execute(cursor, """
                CREATE OR REPLACE FUNCTION update_updated_at_column()
                RETURNS TRIGGER AS 
                $$
                BEGIN
                    NEW.updated_at = CURRENT_TIMESTAMP;
                    RETURN NEW;
                END;
                $$ 
                LANGUAGE plpgsql
            """, "update_updated_at_column function creation")
            
            safe_execute(cursor, """
                DROP TRIGGER IF EXISTS update_job_user_metadata_updated_at ON job_user_metadata
            """, "job_user_metadata trigger drop")
            
            safe_execute(cursor, """
                CREATE TRIGGER update_job_user_metadata_updated_at
                    BEFORE UPDATE ON job_user_metadata
                    FOR EACH ROW
                    EXECUTE FUNCTION update_updated_at_column()
            """, "job_user_metadata trigger creation")
            
            safe_execute(cursor, """
                DROP TRIGGER IF EXISTS update_company_user_metadata_updated_at ON company_user_metadata
            """, "company_user_metadata trigger drop")
            
            safe_execute(cursor, """
                CREATE TRIGGER update_company_user_metadata_updated_at
                    BEFORE UPDATE ON company_user_metadata
                    FOR EACH ROW
                    EXECUTE FUNCTION update_updated_at_column()
            """, "company_user_metadata trigger creation")
            
            self.conn.commit()
            logger.info("Database tables, indexes, views, and triggers created/verified successfully")

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
    
        # Handle empty DataFrame or missing columns
        if jobs_df.empty:
            logger.info("No jobs to process - empty DataFrame")
            return 0, {}, {}, 0, 0.0
    
        # Ensure required columns exist
        required_columns = ['site', 'id', 'is_remote']
        for col in required_columns:
            if col not in jobs_df.columns:
                if col == 'site':
                    jobs_df[col] = 'unknown'
                elif col == 'id':
                    jobs_df[col] = jobs_df.apply(
                        lambda row: f"unknown_{row.name}_{int(time.time())}", axis=1
                    )
                elif col == 'is_remote':
                    jobs_df[col] = False
    
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
    
        # Ensure is_remote column is boolean
        if "is_remote" in new_jobs_df.columns:
            new_jobs_df["is_remote"] = new_jobs_df["is_remote"].astype(bool)
    
        new_jobs_count = len(new_jobs_df)
        
        # Safe site counts calculation
        if "site" in new_jobs_df.columns and not new_jobs_df.empty:
            site_counts = new_jobs_df["site"].fillna("unknown").value_counts().to_dict()
        else:
            site_counts = {"unknown": new_jobs_count} if new_jobs_count > 0 else {}
    
        # Safe duplicate counts calculation
        duplicate_df = jobs_df[jobs_df["id"].isin(existing_ids)]
        if "site" in duplicate_df.columns and not duplicate_df.empty:
            duplicate_counts = duplicate_df["site"].fillna("unknown").value_counts().to_dict()
        else:
            duplicate_counts = {}
    
        # Safe remote jobs count calculation
        if "is_remote" in new_jobs_df.columns and not new_jobs_df.empty:
            remote_jobs_count = int(new_jobs_df["is_remote"].sum())
        else:
            remote_jobs_count = 0
    
        # Safe average salary calculation
        if not new_jobs_df.empty:
            salary_cols = new_jobs_df[["min_amount", "max_amount"]].apply(
                pd.to_numeric, errors="coerce"
            )
            salary_mean = salary_cols.mean(axis=1)
            avg_salary = float(salary_mean.mean()) if not salary_mean.empty and not salary_mean.isna().all() else 0.0
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
            
            # Ensure all required columns exist in DataFrame
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
            
            try:
                with self.conn.cursor() as cursor:
                    psycopg2.extras.execute_batch(cursor, insert_query, data_to_insert)
                    self.conn.commit()
                logger.info("Inserted %s new jobs into database", new_jobs_count)
            except Exception as e:
                logger.error("Error inserting jobs into database: %s", e)
                self.conn.rollback()
                return 0, {}, {}, 0, 0.0
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

    def end_session(self, session_id: int, status: str) -> None:
        """Mark a search session as completed."""
        self._ensure_connection()
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE search_sessions
                SET end_time = %s, status = %s
                WHERE id = %s
                """,
                (datetime.now(), status, session_id),
            )
            self.conn.commit()

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
        try:
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
        except psycopg2.Error as exc:
            self.conn.rollback()
            logger.error(
                "Failed to log search history (possible schema mismatch): %s",
                exc,
            )

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

    def mark_jobs_excluded_by_ids(self, job_ids: List[str], reason: str = "manual") -> int:
        """Mark jobs as excluded by their IDs (preserves data) - OPTIMIZED."""
        self._ensure_connection()
        try:
            if not job_ids:
                logger.warning("No job IDs provided")
                return 0

            with self.conn.cursor() as cursor:
                # Batch operation using execute_values
                batch_data = [(job_id, True, reason, True) for job_id in job_ids]
                
                psycopg2.extras.execute_values(
                    cursor,
                    """
                    INSERT INTO job_user_metadata (job_id, excluded, exclusion_reason, reviewed, created_at, updated_at)
                    VALUES %s
                    ON CONFLICT (job_id) DO UPDATE SET
                        excluded = EXCLUDED.excluded,
                        exclusion_reason = EXCLUDED.exclusion_reason,
                        reviewed = EXCLUDED.reviewed,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    batch_data,
                    template="(%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                    page_size=1000
                )
                
                rows_marked = cursor.rowcount
                self.conn.commit()
            
            logger.info("Marked %s jobs as excluded by ID (reason: %s)", rows_marked, reason)
            return rows_marked
            
        except Exception as exc:
            self.conn.rollback()
            logger.error("Error marking jobs as excluded by IDs: %s", exc)
            return 0

    def mark_jobs_excluded_by_field(self, field: str, patterns: List[str], reason: str) -> int:
        """Mark jobs as excluded by field patterns (preserves data) - OPTIMIZED."""
        self._ensure_connection()
        try:
            if not patterns:
                logger.warning("No patterns provided")
                return 0

            valid_fields = {"company", "title"}
            if field not in valid_fields:
                logger.error("Invalid field name: %s", field)
                return 0

            # Separate wildcard patterns from exact matches
            wildcard_patterns = [p for p in patterns if '%' in p]
            exact_patterns = [p for p in patterns if '%' not in p]

            all_matching_ids = set()
            
            with self.conn.cursor() as cursor:
                
                # Process wildcard patterns in batch using ANY()
                if wildcard_patterns:
                    query = sql.SQL("SELECT DISTINCT id FROM scraped_jobs WHERE {} ILIKE ANY(%s)").format(
                        sql.Identifier(field)
                    )
                    cursor.execute(query, (wildcard_patterns,))
                    wildcard_matches = {row[0] for row in cursor.fetchall()}
                    all_matching_ids.update(wildcard_matches)
                    logger.info("Found %d jobs matching %d wildcard patterns", len(wildcard_matches), len(wildcard_patterns))

                # Process exact patterns in batch using ANY() with LOWER()
                if exact_patterns:
                    # Convert to lowercase for comparison
                    exact_patterns_lower = [p.lower() for p in exact_patterns]
                    query = sql.SQL("SELECT DISTINCT id FROM scraped_jobs WHERE LOWER({}) = ANY(%s)").format(
                        sql.Identifier(field)
                    )
                    cursor.execute(query, (exact_patterns_lower,))
                    exact_matches = {row[0] for row in cursor.fetchall()}
                    all_matching_ids.update(exact_matches)
                    logger.info("Found %d jobs matching %d exact patterns", len(exact_matches), len(exact_patterns))

                # Batch insert/update all matching jobs at once
                if all_matching_ids:
                    # Convert to list for batch processing
                    matching_ids_list = list(all_matching_ids)
                    
                    # Process in batches of 1000 to avoid memory issues
                    batch_size = 1000
                    total_processed = 0
                    
                    for i in range(0, len(matching_ids_list), batch_size):
                        batch = matching_ids_list[i:i + batch_size]
                        
                        # Prepare batch data for INSERT...ON CONFLICT
                        batch_data = [(job_id, True, reason, True) for job_id in batch]
                        
                        # Single batch operation
                        psycopg2.extras.execute_values(
                            cursor,
                            """
                            INSERT INTO job_user_metadata (job_id, excluded, exclusion_reason, reviewed, created_at, updated_at)
                            VALUES %s
                            ON CONFLICT (job_id) DO UPDATE SET
                                excluded = EXCLUDED.excluded,
                                exclusion_reason = EXCLUDED.exclusion_reason,
                                reviewed = EXCLUDED.reviewed,
                                updated_at = CURRENT_TIMESTAMP
                            """,
                            batch_data,
                            template="(%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                            page_size=1000
                        )
                        
                        total_processed += len(batch)
                        if len(matching_ids_list) > batch_size:
                            logger.info("Processed %d/%d jobs...", total_processed, len(matching_ids_list))

                self.conn.commit()
            
            rows_marked = len(all_matching_ids)
            logger.info("Marked %s jobs as excluded by %s patterns (reason: %s)", rows_marked, field, reason)
            logger.info("  - Wildcard patterns: %d", len(wildcard_patterns))
            logger.info("  - Exact patterns: %d", len(exact_patterns))
            return rows_marked
            
        except Exception as exc:
            self.conn.rollback()
            logger.error("Error marking jobs as excluded by %s: %s", field, exc)
            return 0

    def mark_jobs_excluded_by_salary(self, min_threshold: int = 70000, max_threshold: int = 90000, reason: str = "salary_filter") -> int:
        """Mark jobs as excluded with salaries below thresholds (preserves data) - OPTIMIZED."""
        self._ensure_connection()
        try:
            with self.conn.cursor() as cursor:
                # Single query to find all matching jobs and insert/update in one operation
                cursor.execute("""
                    INSERT INTO job_user_metadata (job_id, excluded, exclusion_reason, reviewed, created_at, updated_at)
                    SELECT 
                        s.id,
                        true,
                        %s,
                        true,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP
                    FROM scraped_jobs s
                    LEFT JOIN job_user_metadata jum ON s.id = jum.job_id
                    WHERE (s.min_amount IS NOT NULL AND s.min_amount != 0 AND s.min_amount < %s AND s.max_amount < %s) 
                       OR (s.min_amount IS NOT NULL AND s.min_amount >= %s AND s.max_amount IS NOT NULL AND s.max_amount < %s)
                    ON CONFLICT (job_id) DO UPDATE SET
                        excluded = true,
                        exclusion_reason = EXCLUDED.exclusion_reason,
                        reviewed = true,
                        updated_at = CURRENT_TIMESTAMP
                """, (reason, min_threshold, max_threshold, min_threshold, max_threshold))
                
                rows_marked = cursor.rowcount
                self.conn.commit()
            
            logger.info(
                "Marked %s jobs as excluded with salaries below thresholds (min: %s, max: %s, reason: %s)",
                rows_marked, min_threshold, max_threshold, reason
            )
            return rows_marked
            
        except Exception as exc:
            self.conn.rollback()
            logger.error("Error marking jobs as excluded by salary: %s", exc)
            return 0

    def unmark_jobs_excluded(self, reason: Optional[str] = None) -> int:
        """Remove exclusion marks from jobs (makes them visible again)."""
        self._ensure_connection()
        try:
            rows_unmarked = 0
            with self.conn.cursor() as cursor:
                if reason:
                    # Unmark only jobs excluded for specific reason
                    cursor.execute("""
                        UPDATE job_user_metadata 
                        SET excluded = false, exclusion_reason = NULL, updated_at = CURRENT_TIMESTAMP
                        WHERE excluded = true AND exclusion_reason = %s
                    """, (reason,))
                else:
                    # Unmark all excluded jobs
                    cursor.execute("""
                        UPDATE job_user_metadata 
                        SET excluded = false, exclusion_reason = NULL, updated_at = CURRENT_TIMESTAMP
                        WHERE excluded = true
                    """)
                
                rows_unmarked = cursor.rowcount
                self.conn.commit()
            
            if reason:
                logger.info("Unmarked %s jobs excluded for reason: %s", rows_unmarked, reason)
            else:
                logger.info("Unmarked %s jobs (all exclusions removed)", rows_unmarked)
            return rows_unmarked
            
        except Exception as exc:
            self.conn.rollback()
            logger.error("Error unmarking excluded jobs: %s", exc)
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