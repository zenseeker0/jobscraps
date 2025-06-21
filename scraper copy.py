#!/usr/bin/env python3
# /Users/jonesy/gitlocal/jobscrape/scraper.py
#
# JobScraps Scraper with intelligent backup management:
# - Backups are created AFTER scraping (production only) to capture new data
# - Working database operations (data cleaning) skip backups to save time/space
# - No backup before creating working copy (read-only operation, no risk)
# - Users are warned when attempting data cleaning operations on production database

import os
import sys
import json
import logging
import shutil
import argparse
import time
import subprocess
import glob
from datetime import datetime
from typing import Dict, List, Optional, Any, Union, Tuple
from collections import defaultdict

import psycopg2
import psycopg2.extras
from psycopg2 import sql
import pandas as pd
from jobspy import scrape_jobs

import warnings
# Suppress pandas SQLAlchemy warnings for psycopg2 connections
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Or more specifically, suppress only the exact warning you're seeing:
warnings.filterwarnings('ignore', 
                       message='pandas only supports SQLAlchemy connectable.*', 
                       category=UserWarning, 
                       module='pandas')

# Get the directory where scraper.py is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Setup logging
LOG_DIR = os.path.join(SCRIPT_DIR, "outputs", "logs")
os.makedirs(LOG_DIR, exist_ok=True)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "jobscraper.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DatabaseConfig:
    """Class to handle database configuration loading."""
    
    def __init__(self, config_path: str = None, 
                 database_type: str = "production"):
        """Initialize with configuration file path and database type.
        
        Args:
            config_path: Path to the database configuration file
            database_type: Either 'production' or 'working' to select database config
        """
        if config_path is None:
            config_path = os.path.join(SCRIPT_DIR, "configs", "db_config.json")
        self.config_path = config_path
        self.database_type = database_type
        self.config = self._load_config()
        
    def _load_config(self) -> Dict:
        """Load database configuration from file.
        
        Returns:
            Dictionary containing database configuration
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Database config file not found: {self.config_path}")
            
        with open(self.config_path, 'r') as f:
            return json.load(f)
    
    def get_connection_params(self) -> Dict:
        """Get database connection parameters.
        
        Returns:
            Dictionary of connection parameters for psycopg2
        """
        # Determine which database config to use
        if self.database_type == "working" and 'working_database' in self.config:
            db_config = self.config['working_database']
        elif self.database_type == "production" and 'production_database' in self.config:
            db_config = self.config['production_database']
        elif 'database' in self.config:
            # Fallback to old format for backwards compatibility
            db_config = self.config['database']
        else:
            raise KeyError(f"No database configuration found for type: {self.database_type}")
            
        conn_config = self.config.get('connection', {})
        
        return {
            'host': db_config['host'],
            'port': db_config['port'],
            'database': db_config['database'],
            'user': db_config['username'],
            'password': db_config['password'],
            'connect_timeout': conn_config.get('connect_timeout', 30),
            'application_name': 'jobscraps_scraper'
        }
    
    def get_retry_config(self) -> Dict:
        """Get retry configuration.
        
        Returns:
            Dictionary with retry settings
        """
        return self.config.get('connection', {})


class JobDatabase:
    """Class to handle database operations for job data."""
    
    def __init__(self, config_path: str = None,
                 database_type: str = "production"):
        """Initialize the database connection.
        
        Args:
            config_path: Path to the database configuration file
            database_type: Either 'production' or 'working' to select database config
        """
        if config_path is None:
            config_path = os.path.join(SCRIPT_DIR, "configs", "db_config.json")
        self.db_config = DatabaseConfig(config_path, database_type)
        self.database_type = database_type
        self.conn = None
        self._connect_with_retry()
        self.create_tables()
        logger.info(f"PostgreSQL database initialized ({database_type})")
        
    def _connect_with_retry(self) -> None:
        """Connect to database with retry logic."""
        retry_config = self.db_config.get_retry_config()
        max_attempts = retry_config.get('retry_attempts', 3)
        retry_delay = retry_config.get('retry_delay', 5)
        
        for attempt in range(max_attempts):
            try:
                conn_params = self.db_config.get_connection_params()
                self.conn = psycopg2.connect(**conn_params)
                self.conn.autocommit = False
                logger.info(f"Connected to PostgreSQL database successfully")
                return
            except psycopg2.Error as e:
                logger.warning(f"Database connection attempt {attempt + 1} failed: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(retry_delay)
                else:
                    raise psycopg2.Error(f"Failed to connect after {max_attempts} attempts: {e}")
    
    def _ensure_connection(self) -> None:
        """Ensure database connection is active, reconnect if needed."""
        try:
            if self.conn.closed:
                logger.warning("Database connection is closed, reconnecting...")
                self._connect_with_retry()
        except (psycopg2.Error, AttributeError):
            logger.warning("Database connection error, reconnecting...")
            self._connect_with_retry()

    def create_backup(self, backup_type: str = 'auto', reason: str = '') -> Dict:
        """Create a PostgreSQL backup using pg_dump.
        
        Args:
            backup_type: Type of backup ('auto' or 'manual')
            reason: Reason for backup (e.g., 'scraping', 'deletion', 'working_copy')
            
        Returns:
            Dictionary with backup information
        """
        try:
            # Create backup directory
            backup_dir = os.path.join(SCRIPT_DIR, "backups", "DatabaseBackups")
            os.makedirs(backup_dir, exist_ok=True)
            
            # Get connection parameters
            conn_params = self.db_config.get_connection_params()
            
            # Create filename with timestamp and reason
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            reason_suffix = f"_{reason}" if reason else ""
            filename = f"jobscraps_{timestamp}_{backup_type}{reason_suffix}.sql.gz"
            backup_path = os.path.join(backup_dir, filename)
            
            # Build pg_dump command
            cmd = [
                'pg_dump',
                '-h', conn_params['host'],
                '-p', str(conn_params['port']),
                '-U', conn_params['user'],
                '-d', conn_params['database'],
                '--compress=9',
                '--verbose',
                '--file', backup_path
            ]
            
            # Set password environment variable
            env = os.environ.copy()
            env['PGPASSWORD'] = conn_params['password']
            
            logger.info(f"Creating backup: {filename}")
            start_time = time.time()
            
            # Execute pg_dump with retry logic
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    result = subprocess.run(
                        cmd, 
                        env=env, 
                        capture_output=True, 
                        text=True, 
                        timeout=300  # 5 minute timeout
                    )
                    
                    if result.returncode == 0:
                        backup_time = time.time() - start_time
                        file_size = os.path.getsize(backup_path)
                        file_size_mb = file_size / (1024 * 1024)
                        
                        backup_info = {
                            'filename': filename,
                            'path': backup_path,
                            'size_bytes': file_size,
                            'size_mb': round(file_size_mb, 1),
                            'duration_seconds': round(backup_time, 1),
                            'timestamp': timestamp,
                            'reason': reason,
                            'backup_type': backup_type
                        }
                        
                        # Update backup manifest
                        self._update_backup_manifest(backup_info)
                        
                        logger.info(f"Backup created successfully: {filename} ({file_size_mb:.1f} MB in {backup_time:.1f}s)")
                        return backup_info
                        
                    else:
                        error_msg = result.stderr or result.stdout
                        logger.warning(f"pg_dump attempt {attempt + 1} failed: {error_msg}")
                        if attempt < max_attempts - 1:
                            time.sleep(5)  # Wait before retry
                        
                except subprocess.TimeoutExpired:
                    logger.warning(f"pg_dump attempt {attempt + 1} timed out")
                    if attempt < max_attempts - 1:
                        time.sleep(5)
                except Exception as e:
                    logger.warning(f"pg_dump attempt {attempt + 1} error: {e}")
                    if attempt < max_attempts - 1:
                        time.sleep(5)
            
            # If all attempts failed
            if os.path.exists(backup_path):
                os.remove(backup_path)  # Clean up partial file
            raise Exception(f"pg_dump failed after {max_attempts} attempts")
            
        except Exception as e:
            logger.error(f"Backup creation failed: {e}")
            raise
    
    def _update_backup_manifest(self, backup_info: Dict) -> None:
        """Update the backup manifest file.
        
        Args:
            backup_info: Dictionary containing backup information
        """
        manifest_path = os.path.join(SCRIPT_DIR, "backups", "DatabaseBackups", "backup_manifest.json")
        
        try:
            # Load existing manifest
            if os.path.exists(manifest_path):
                with open(manifest_path, 'r') as f:
                    manifest = json.load(f)
            else:
                manifest = {"backups": []}
            
            # Add new backup info
            manifest["backups"].append(backup_info)
            
            # Update summary statistics
            manifest["total_backups"] = len(manifest["backups"])
            total_size = sum(backup['size_bytes'] for backup in manifest["backups"])
            manifest["total_size_gb"] = round(total_size / (1024**3), 2)
            
            if manifest["backups"]:
                manifest["oldest_backup"] = min(backup['timestamp'] for backup in manifest["backups"])
                manifest["newest_backup"] = max(backup['timestamp'] for backup in manifest["backups"])
                manifest["last_updated"] = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save updated manifest
            with open(manifest_path, 'w') as f:
                json.dump(manifest, f, indent=2)
                
        except Exception as e:
            logger.warning(f"Failed to update backup manifest: {e}")
    
    def manage_backup_retention(self) -> Dict:
        """Manage backup retention policy.
        
        Returns:
            Dictionary with retention summary
        """
        backup_dir = os.path.join(SCRIPT_DIR, "backups", "DatabaseBackups")
        manifest_path = os.path.join(backup_dir, "backup_manifest.json")
        
        try:
            if not os.path.exists(manifest_path):
                return {"action": "no_manifest", "message": "No backup manifest found"}
            
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
            
            backups = manifest.get("backups", [])
            total_size_gb = manifest.get("total_size_gb", 0)
            
            # Check if cleanup needed
            needs_cleanup = len(backups) > 48 or total_size_gb > 4.8
            
            if not needs_cleanup:
                return {
                    "action": "no_cleanup_needed",
                    "total_backups": len(backups),
                    "total_size_gb": total_size_gb
                }
            
            # Sort backups by timestamp (oldest first)
            backups.sort(key=lambda x: x['timestamp'])
            
            # Calculate how many to remove
            target_count = 40
            to_remove = max(0, len(backups) - target_count)
            
            if total_size_gb > 4.8:
                # Remove additional backups to get under size limit
                size_removed = 0
                while len(backups) > to_remove and total_size_gb > 4.5:
                    oldest_backup = backups[to_remove]
                    size_removed += oldest_backup['size_bytes']
                    total_size_gb -= oldest_backup['size_bytes'] / (1024**3)
                    to_remove += 1
            
            # Remove old backup files
            removed_backups = []
            for i in range(to_remove):
                backup = backups[i]
                backup_path = backup['path']
                
                if os.path.exists(backup_path):
                    os.remove(backup_path)
                    removed_backups.append(backup['filename'])
                    logger.info(f"Removed old backup: {backup['filename']}")
            
            # Update manifest
            manifest["backups"] = backups[to_remove:]
            manifest["total_backups"] = len(manifest["backups"])
            
            if manifest["backups"]:
                total_size = sum(backup['size_bytes'] for backup in manifest["backups"])
                manifest["total_size_gb"] = round(total_size / (1024**3), 2)
                manifest["oldest_backup"] = min(backup['timestamp'] for backup in manifest["backups"])
                manifest["last_cleanup"] = datetime.now().strftime("%Y%m%d_%H%M%S")
            else:
                manifest["total_size_gb"] = 0
            
            with open(manifest_path, 'w') as f:
                json.dump(manifest, f, indent=2)
            
            return {
                "action": "cleanup_performed",
                "removed_count": len(removed_backups),
                "removed_files": removed_backups,
                "remaining_backups": len(manifest["backups"]),
                "total_size_gb": manifest["total_size_gb"]
            }
            
        except Exception as e:
            logger.error(f"Backup retention management failed: {e}")
            return {"action": "error", "message": str(e)}
    
    def list_backups(self) -> List[Dict]:
        """List available backups.
        
        Returns:
            List of backup information dictionaries
        """
        manifest_path = os.path.join(SCRIPT_DIR, "backups", "DatabaseBackups", "backup_manifest.json")
        
        try:
            if not os.path.exists(manifest_path):
                return []
            
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
            
            backups = manifest.get("backups", [])
            # Sort by timestamp (newest first)
            backups.sort(key=lambda x: x['timestamp'], reverse=True)
            
            return backups
            
        except Exception as e:
            logger.error(f"Failed to list backups: {e}")
            return []
    
    def restore_backup(self, backup_filename: str) -> bool:
        """Restore database from backup file.
        
        Args:
            backup_filename: Name of the backup file to restore
            
        Returns:
            True if restore was successful, False otherwise
        """
        backup_path = os.path.join(SCRIPT_DIR, "backups", "DatabaseBackups", backup_filename)
        
        if not os.path.exists(backup_path):
            logger.error(f"Backup file not found: {backup_path}")
            return False
        
        try:
            conn_params = self.db_config.get_connection_params()
            
            # Close current connection
            if self.conn and not self.conn.closed:
                self.conn.close()
            
            # Build psql restore command
            cmd = [
                'psql',
                '-h', conn_params['host'],
                '-p', str(conn_params['port']),
                '-U', conn_params['user'],
                '-d', conn_params['database'],
                '-f', backup_path,
                '--quiet'
            ]
            
            env = os.environ.copy()
            env['PGPASSWORD'] = conn_params['password']
            
            logger.info(f"Restoring from backup: {backup_filename}")
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=600)
            
            if result.returncode == 0:
                logger.info(f"Database restored successfully from {backup_filename}")
                # Reconnect after restore
                self._connect_with_retry()
                return True
            else:
                logger.error(f"Restore failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Restore operation failed: {e}")
            return False
    
    def test_backup(self, backup_filename: str) -> bool:
        """Test backup file integrity.
        
        Args:
            backup_filename: Name of the backup file to test
            
        Returns:
            True if backup is valid, False otherwise
        """
        backup_path = os.path.join(SCRIPT_DIR, "backups", "DatabaseBackups", backup_filename)
        
        if not os.path.exists(backup_path):
            logger.error(f"Backup file not found: {backup_path}")
            return False
        
        try:
            # Test if file can be read and is valid gzip
            if backup_filename.endswith('.gz'):
                import gzip
                with gzip.open(backup_path, 'rt') as f:
                    # Read first few lines to verify it's a valid SQL dump
                    lines = [f.readline() for _ in range(10)]
                    
                # Check for PostgreSQL dump headers
                content = ''.join(lines)
                if 'PostgreSQL database dump' in content or 'CREATE TABLE' in content:
                    logger.info(f"Backup file {backup_filename} appears to be valid")
                    return True
                else:
                    logger.error(f"Backup file {backup_filename} does not appear to be a valid PostgreSQL dump")
                    return False
            else:
                logger.error(f"Backup file {backup_filename} is not a compressed file")
                return False
                
        except Exception as e:
            logger.error(f"Backup test failed: {e}")
            return False

    def create_tables(self) -> None:
        """Create necessary tables if they don't exist."""
        self._ensure_connection()
        
        with self.conn.cursor() as cursor:
            # Create scraped_jobs table
            cursor.execute('''
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
            ''')
            
            # Create search_history table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS search_history (
                id SERIAL PRIMARY KEY,
                search_query TEXT,
                parameters TEXT,
                timestamp TIMESTAMP,
                jobs_found INTEGER
            )
            ''')
            
            # Create indexes
            index_queries = [
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_title ON scraped_jobs(title)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_company ON scraped_jobs(company)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_search_query ON scraped_jobs(search_query)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_search_query_lower ON scraped_jobs(LOWER(search_query))",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_date_posted ON scraped_jobs(date_posted)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_location ON scraped_jobs(location)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_is_remote ON scraped_jobs(is_remote)",
                "CREATE INDEX IF NOT EXISTS idx_scraped_jobs_date_scraped ON scraped_jobs(date_scraped)"
            ]
            
            for query in index_queries:
                cursor.execute(query)
            
            self.conn.commit()
        
    def insert_jobs(self, jobs_df: pd.DataFrame, search_query: str) -> int:
        """Insert new jobs into the database, maintaining uniqueness by ID.
        
        Args:
            jobs_df: DataFrame containing job data
            search_query: The search query used to find these jobs
            
        Returns:
            Number of new jobs inserted
        """
        self._ensure_connection()
        
        # Add date_scraped and search_query columns
        jobs_df['date_scraped'] = datetime.now()
        jobs_df['search_query'] = search_query
        
        # Get existing job IDs to check for duplicates
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT id FROM scraped_jobs")
            existing_ids = {row[0] for row in cursor.fetchall()}
        
        # Filter out jobs that already exist in the database
        if 'id' in jobs_df.columns:
            new_jobs_df = jobs_df[~jobs_df['id'].isin(existing_ids)]
        else:
            # If no ID column, create one
            jobs_df['id'] = jobs_df.apply(
                lambda row: f"{row.get('site', 'unknown')}_{row.get('job_url', '')[-20:]}",
                axis=1
            )
            new_jobs_df = jobs_df[~jobs_df['id'].isin(existing_ids)]
        
        # Convert boolean columns properly for PostgreSQL
        if 'is_remote' in new_jobs_df.columns:
            new_jobs_df['is_remote'] = new_jobs_df['is_remote'].astype(bool)
        
        # Insert new jobs
        new_jobs_count = len(new_jobs_df)
        if new_jobs_count > 0:
            # Convert DataFrame to list of tuples for batch insert
            columns = [
                'id', 'site', 'job_url', 'job_url_direct', 'title', 'company', 'location',
                'date_posted', 'job_type', 'salary_source', 'interval', 'min_amount', 'max_amount',
                'currency', 'is_remote', 'job_level', 'job_function', 'listing_type', 'emails',
                'description', 'company_industry', 'company_url', 'company_logo', 'company_url_direct',
                'company_addresses', 'company_num_employees', 'company_revenue', 'company_description',
                'skills', 'experience_range', 'company_rating', 'company_reviews_count', 'vacancy_count',
                'work_from_home_type', 'date_scraped', 'search_query'
            ]
            
            # Ensure all columns exist in DataFrame
            for col in columns:
                if col not in new_jobs_df.columns:
                    new_jobs_df[col] = None
            
            # Prepare data for insertion
            data_to_insert = []
            for _, row in new_jobs_df.iterrows():
                row_data = []
                for col in columns:
                    value = row[col]
                    # Handle NaN values
                    if pd.isna(value):
                        row_data.append(None)
                    else:
                        row_data.append(value)
                data_to_insert.append(tuple(row_data))
            
            # Build insert query
            insert_query = sql.SQL(
                "INSERT INTO scraped_jobs ({}) VALUES ({})"
            ).format(
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.SQL(', ').join(sql.Placeholder() * len(columns))
            )
            
            with self.conn.cursor() as cursor:
                psycopg2.extras.execute_batch(cursor, insert_query, data_to_insert)
                self.conn.commit()
                
            logger.info(f"Inserted {new_jobs_count} new jobs into database")
        else:
            logger.info("No new jobs to insert")
            
        return new_jobs_count
    
    def log_search(self, search_query: str, parameters: Dict, jobs_found: int) -> None:
        """Log search history.
        
        Args:
            search_query: The search query used
            parameters: The parameters used for the search
            jobs_found: Number of jobs found in this search
        """
        self._ensure_connection()
        
        with self.conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO search_history (search_query, parameters, timestamp, jobs_found) VALUES (%s, %s, %s, %s)",
                (search_query, json.dumps(parameters), datetime.now(), jobs_found)
            )
            self.conn.commit()
        
    def get_all_jobs(self) -> pd.DataFrame:
        """Get all jobs from the database.
        
        Returns:
            DataFrame containing all jobs
        """
        self._ensure_connection()
        return pd.read_sql("SELECT * FROM scraped_jobs", self.conn)
    
    def get_jobs_by_query(self, search_query: str) -> pd.DataFrame:
        """Get jobs matching a specific search query.
        
        Args:
            search_query: The search query to filter by
            
        Returns:
            DataFrame containing matching jobs
        """
        self._ensure_connection()
        query = "SELECT * FROM scraped_jobs WHERE search_query = %s"
        return pd.read_sql(query, self.conn, params=(search_query,))
    
    def get_duplicate_groups(self) -> List[List[Dict]]:
        """Identify groups of duplicate jobs based on title and company.
        
        Returns:
            List of groups, where each group is a list of job dictionaries
        """
        self._ensure_connection()
        
        query = """
        SELECT id, site, title, company, description, min_amount, max_amount, job_url, is_remote, location, search_query, date_posted
        FROM scraped_jobs 
        WHERE title IS NOT NULL AND company IS NOT NULL
        ORDER BY title, company, site
        """
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
        
        # Group jobs by (title, company) combination
        groups = defaultdict(list)
        for row in rows:
            job_dict = dict(row)
            key = (row['title'].strip().lower() if row['title'] else '', 
                   row['company'].strip().lower() if row['company'] else '')
            groups[key].append(job_dict)
        
        # Filter out groups with only one job (not duplicates)
        duplicate_groups = [group for group in groups.values() if len(group) > 1]
        
        return duplicate_groups
    
    def clear_all_jobs(self) -> int:
        """Clear all data from the scraped_jobs table without deleting the table.
        
        Returns:
            Number of rows deleted
        """
        self._ensure_connection()
        
        with self.conn.cursor() as cursor:
            cursor.execute("DELETE FROM scraped_jobs")
            rows_deleted = cursor.rowcount
            self.conn.commit()
            
        logger.info(f"Cleared {rows_deleted} rows from scraped_jobs table")
        return rows_deleted
    
    def delete_jobs_before_date(self, date_str: str) -> int:
        """Delete jobs scraped before a specified date.
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            
        Returns:
            Number of rows deleted
        """
        self._ensure_connection()
        
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM scraped_jobs WHERE date_scraped < %s", 
                    (date_obj,)
                )
                rows_deleted = cursor.rowcount
                self.conn.commit()
                
            logger.info(f"Deleted {rows_deleted} jobs scraped before {date_str}")
            return rows_deleted
        except Exception as e:
            logger.error(f"Error deleting jobs by date: {str(e)}")
            return 0
    
    def delete_jobs_by_ids(self, ids_file: str) -> int:
        """Delete jobs by their IDs from a file.
        
        Args:
            ids_file: Path to a file containing job IDs, one per line
            
        Returns:
            Number of rows deleted
        """
        self._ensure_connection()
        
        try:
            if not os.path.exists(ids_file):
                logger.warning(f"IDs file {ids_file} not found")
                return 0
                
            with open(ids_file, 'r') as f:
                job_ids = [line.strip() for line in f if line.strip()]
                
            if not job_ids:
                logger.warning(f"No IDs found in {ids_file}")
                return 0
                
            with self.conn.cursor() as cursor:
                cursor.execute("DELETE FROM scraped_jobs WHERE id = ANY(%s)", (job_ids,))
                rows_deleted = cursor.rowcount
                self.conn.commit()
                
            logger.info(f"Deleted {rows_deleted} jobs by ID from {ids_file}")
            return rows_deleted
        except Exception as e:
            logger.error(f"Error deleting jobs by IDs: {str(e)}")
            return 0
    
    def delete_jobs_by_salary(self, min_threshold: int = 70000, max_threshold: int = 90000) -> int:
        """Delete jobs with salaries below specified thresholds.
        
        Args:
            min_threshold: Minimum salary threshold (default: 70000)
            max_threshold: Maximum salary threshold (default: 90000)
            
        Returns:
            Number of rows deleted
        """
        self._ensure_connection()
        
        try:
            query = """
            DELETE FROM scraped_jobs
            WHERE 
              (
                min_amount != 0 
                AND min_amount < %s 
                AND max_amount < %s
              )
              OR 
              (
                min_amount >= %s 
                AND max_amount < %s
              )
            """
            
            with self.conn.cursor() as cursor:
                cursor.execute(query, (min_threshold, max_threshold, min_threshold, max_threshold))
                rows_deleted = cursor.rowcount
                self.conn.commit()
                
            logger.info(f"Deleted {rows_deleted} jobs with salaries below thresholds (min: {min_threshold}, max: {max_threshold})")
            return rows_deleted
        except Exception as e:
            logger.error(f"Error deleting jobs by salary: {str(e)}")
            return 0
    
    def delete_jobs_by_field(self, field: str, patterns_file: str) -> int:
        """Delete jobs by matching patterns on a specific field.
        
        Args:
            field: The database field to match on (e.g., 'company', 'title')
            patterns_file: Path to a file containing patterns, one per line
            
        Returns:
            Number of rows deleted
        """
        self._ensure_connection()
        
        try:
            if not os.path.exists(patterns_file):
                logger.warning(f"Patterns file {patterns_file} not found")
                return 0
                
            with open(patterns_file, 'r') as f:
                patterns = [line.strip() for line in f if line.strip()]
                
            if not patterns:
                logger.warning(f"No patterns found in {patterns_file}")
                return 0
                
            # Validate field name to prevent SQL injection
            valid_fields = {'company', 'title'}
            if field not in valid_fields:
                logger.error(f"Invalid field name: {field}")
                return 0
                
            rows_deleted = 0
            
            with self.conn.cursor() as cursor:
                for pattern in patterns:
                    # Convert pattern to lowercase for case-insensitive matching
                    pattern_lower = pattern.lower()
                    query = sql.SQL("DELETE FROM scraped_jobs WHERE LOWER({}) LIKE %s").format(
                        sql.Identifier(field)
                    )
                    cursor.execute(query, (pattern_lower,))
                    pattern_deleted = cursor.rowcount
                    rows_deleted += pattern_deleted
                    
                    if pattern_deleted > 0:
                        logger.info(f"Pattern '{pattern}' deleted {pattern_deleted} rows")
                    else:
                        logger.debug(f"Pattern '{pattern}' found no matches")
                
                self.conn.commit()
                
            logger.info(f"Deleted {rows_deleted} jobs matching {field} patterns from {patterns_file}")
            return rows_deleted
        except Exception as e:
            logger.error(f"Error deleting jobs by {field}: {str(e)}")
            return 0
    
    def backup_and_reset(self) -> bool:
        """Create a backup of the database and clear all data.
        
        Returns:
            True if operation was successful, False otherwise
        """
        try:
            # Create pg_dump backup first
            backup_info = self.create_backup('manual', 'backup_and_reset')
            print(f"✓ Database backup created: {backup_info['filename']} ({backup_info['size_mb']} MB)")
            
            # Also create CSV backup for backwards compatibility
            backup_dir = os.path.join(SCRIPT_DIR, "backups")
            os.makedirs(backup_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Backup scraped_jobs
            jobs_df = self.get_all_jobs()
            jobs_backup_path = os.path.join(backup_dir, f"scraped_jobs_{timestamp}.csv")
            jobs_df.to_csv(jobs_backup_path, index=False)
            logger.info(f"Jobs CSV backup created at {jobs_backup_path}")
            
            # Backup search_history
            search_df = pd.read_sql("SELECT * FROM search_history", self.conn)
            search_backup_path = os.path.join(backup_dir, f"search_history_{timestamp}.csv")
            search_df.to_csv(search_backup_path, index=False)
            logger.info(f"Search history CSV backup created at {search_backup_path}")
            
            # Clear all data
            rows_deleted = self.clear_all_jobs()
            
            with self.conn.cursor() as cursor:
                cursor.execute("DELETE FROM search_history")
                search_rows_deleted = cursor.rowcount
                self.conn.commit()
            
            logger.info(f"Database reset completed. Jobs: {rows_deleted}, Search history: {search_rows_deleted}")
            
            return True
        except Exception as e:
            logger.error(f"Error during database backup and reset: {str(e)}")
            return False
    
    def close(self) -> None:
        """Close the database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Database connection closed")


class DuplicateManager:
    """Class to handle duplicate job detection and management."""
    
    def __init__(self, db: JobDatabase):
        """Initialize with database connection.
        
        Args:
            db: JobDatabase instance
        """
        self.db = db
        self.site_preference = ['linkedin', 'indeed', 'google']
    
    def identify_duplicates(self) -> Tuple[List[List[Dict]], List[str], List[str]]:
        """Identify duplicate jobs and determine which ones to keep/delete.
        
        Returns:
            Tuple of (duplicate_groups, ids_to_delete, ids_to_keep)
        """
        duplicate_groups = self.db.get_duplicate_groups()
        ids_to_delete = []
        ids_to_keep = []
        
        for group in duplicate_groups:
            best_job = self._select_best_job(group)
            ids_to_keep.append(best_job['id'])
            
            for job in group:
                if job['id'] != best_job['id']:
                    ids_to_delete.append(job['id'])
        
        return duplicate_groups, ids_to_delete, ids_to_keep
    
    def _select_best_job(self, jobs: List[Dict]) -> Dict:
        """Select the best job from a group of duplicates using the ranking algorithm.
        
        Args:
            jobs: List of job dictionaries
            
        Returns:
            The best job dictionary
        """
        candidates = jobs.copy()
        
        # Step 1: Filter by description (keep jobs with descriptions)
        jobs_with_desc = [job for job in candidates if job.get('description') and job['description'].strip()]
        if jobs_with_desc and len(jobs_with_desc) < len(candidates):
            candidates = jobs_with_desc
        
        if len(candidates) == 1:
            return candidates[0]
        
        # Step 2: Filter by Colorado location preference
        colorado_jobs = [job for job in candidates 
                        if job.get('location') and 
                        (', CO' in job['location'] or 
                         'Colorado' in job['location'] or
                         ', co' in job['location'].lower() or
                         'colorado' in job['location'].lower())]
        if colorado_jobs and len(colorado_jobs) < len(candidates):
            candidates = colorado_jobs
        
        if len(candidates) == 1:
            return candidates[0]
        
        # Step 3: Filter by salary (keep jobs with min_amount > 0)
        jobs_with_salary = [job for job in candidates 
                           if job.get('min_amount') and job['min_amount'] > 0]
        if jobs_with_salary and len(jobs_with_salary) < len(candidates):
            candidates = jobs_with_salary
        
        if len(candidates) == 1:
            return candidates[0]
        
        # Step 4: Select by highest min_amount
        if len(candidates) > 1:
            candidates_with_salary = [job for job in candidates 
                                    if job.get('min_amount') and job['min_amount'] > 0]
            if candidates_with_salary:
                max_salary = max(job['min_amount'] for job in candidates_with_salary)
                highest_salary_jobs = [job for job in candidates_with_salary 
                                     if job['min_amount'] == max_salary]
                if len(highest_salary_jobs) < len(candidates):
                    candidates = highest_salary_jobs
        
        if len(candidates) == 1:
            return candidates[0]
        
        # Step 5: Filter by remote status (if mixed, prefer remote=1)
        remote_values = {job.get('is_remote', False) for job in candidates}
        if False in remote_values and True in remote_values:
            remote_jobs = [job for job in candidates if job.get('is_remote') == True]
            if remote_jobs:
                candidates = remote_jobs
        
        if len(candidates) == 1:
            return candidates[0]
        
        # Step 6: Filter out United States search queries (prefer others)
        non_us_jobs = [job for job in candidates 
                      if not (job.get('search_query') and 
                             'united states' in job['search_query'].lower())]
        if non_us_jobs and len(non_us_jobs) < len(candidates):
            candidates = non_us_jobs
        
        if len(candidates) == 1:
            return candidates[0]
        
        # Step 7: Select by site preference
        for preferred_site in self.site_preference:
            site_jobs = [job for job in candidates if job.get('site', '').lower() == preferred_site]
            if site_jobs:
                candidates = site_jobs
                break
        
        if len(candidates) == 1:
            return candidates[0]
        
        # Step 8: Select by most recent date_posted
        if len(candidates) > 1:
            # Filter jobs that have valid dates
            jobs_with_dates = [job for job in candidates 
                             if job.get('date_posted') and job['date_posted'].strip()]
            if jobs_with_dates:
                # Sort by date_posted (assuming ISO format or comparable string format)
                try:
                    jobs_with_dates.sort(key=lambda x: x['date_posted'], reverse=True)
                    return jobs_with_dates[0]
                except:
                    # If date sorting fails, continue to fallback
                    pass
        
        # Fallback: return first job if no other criteria distinguishes them
        return candidates[0]
    
    def delete_duplicate_jobs_directly(self, ids_to_delete: List[str]) -> int:
        """Delete duplicate jobs directly from database without creating files.
        
        Args:
            ids_to_delete: List of job IDs to delete
            
        Returns:
            Number of jobs deleted
        """
        if not ids_to_delete:
            return 0
            
        try:
            with self.db.conn.cursor() as cursor:
                cursor.execute("DELETE FROM scraped_jobs WHERE id = ANY(%s)", (ids_to_delete,))
                rows_deleted = cursor.rowcount
                self.db.conn.commit()
                
            logger.info(f"Deleted {rows_deleted} duplicate jobs directly")
            return rows_deleted
        except Exception as e:
            logger.error(f"Error deleting duplicate jobs directly: {str(e)}")
            return 0
    
    def create_delete_ids_file(self, ids_to_delete: List[str], filename: str = None) -> None:
        """Create/overwrite file with IDs to delete.
        
        Args:
            ids_to_delete: List of job IDs to delete
            filename: Name of the file to create/overwrite
        """
        if filename is None:
            filename = os.path.join(SCRIPT_DIR, "configs", "delete_ids.txt")
        try:
            config_dir = os.path.dirname(filename)
            if config_dir and not os.path.exists(config_dir):
                os.makedirs(config_dir)
            
            with open(filename, 'w') as f:
                for job_id in ids_to_delete:
                    f.write(f"{job_id}\n")
            
            logger.info(f"Created {filename} with {len(ids_to_delete)} IDs")
            print(f"Created {filename} with {len(ids_to_delete)} IDs to delete")
        except Exception as e:
            logger.error(f"Error creating delete IDs file: {str(e)}")
            print(f"Error creating delete IDs file: {str(e)}")


class JobSearchConfig:
    """Class to handle job search configuration from file."""
    
    def __init__(self, config_path: str = None):
        """Initialize with configuration file path.
        
        Args:
            config_path: Path to the configuration file
        """
        if config_path is None:
            config_path = os.path.join(SCRIPT_DIR, "configs", "job_search_config.json")
        self.config_path = config_path
        self.config = self._load_config()
        
    def _load_config(self) -> Dict:
        """Load configuration from file.
        
        Returns:
            Dictionary containing configuration
        """
        config_dir = os.path.dirname(self.config_path)
        if config_dir and not os.path.exists(config_dir):
            os.makedirs(config_dir)
            
        if not os.path.exists(self.config_path):
            logger.warning(f"Config file {self.config_path} not found. Creating default configuration.")
            default_config = {
                "jobs": [
                    {
                        "name": "Software Engineer",
                        "enabled": True,
                        "parameters": {
                            "site_name": ["indeed", "linkedin", "glassdoor"],
                            "search_term": "software engineer",
                            "location": "San Francisco, CA",
                            "results_wanted": 100,
                            "hours_old": 72,
                            "country_indeed": "USA"
                        }
                    }
                ],
                "global": {
                    "description_format": "markdown",
                    "enforce_annual_salary": True,
                    "verbose": 1
                }
            }
            with open(self.config_path, 'w') as f:
                json.dump(default_config, f, indent=4)
            return default_config
            
        with open(self.config_path, 'r') as f:
            return json.load(f)
    
    def get_job_configs(self) -> List[Dict]:
        """Get all enabled job configurations.
        
        Returns:
            List of enabled job configurations
        """
        return [job for job in self.config.get("jobs", []) if job.get("enabled", True)]
    
    def get_global_params(self) -> Dict:
        """Get global parameters that apply to all searches.
        
        Returns:
            Dictionary of global parameters
        """
        return self.config.get("global", {})


class JobScraper:
    """Main class for scraping jobs using JobSpy."""
    
    def __init__(self, config_path: str = None, 
                 db_config_path: str = None,
                 database_type: str = "production"):
        """Initialize the job scraper.
        
        Args:
            config_path: Path to the job search configuration file
            db_config_path: Path to the database configuration file
            database_type: Either 'production' or 'working' to select database config
        """
        if config_path is None:
            config_path = os.path.join(SCRIPT_DIR, "configs", "job_search_config.json")
        if db_config_path is None:
            db_config_path = os.path.join(SCRIPT_DIR, "configs", "db_config.json")
        self.config = JobSearchConfig(config_path)
        self.db = JobDatabase(db_config_path, database_type)
        self.duplicate_manager = DuplicateManager(self.db)
        
        self.proxies = [
        ]

    def _create_backup_with_prompt(self, reason: str) -> bool:
        """Create backup with user prompt if backup fails.
        Only creates backups when operating on production database.
        
        Args:
            reason: Reason for the backup
            
        Returns:
            True if should continue operation, False if should abort
        """
        # Only create backups when operating on production database
        if self.db.database_type != "production":
            print(f"Skipping backup (operating on {self.db.database_type} database)")
            return True
            
        try:
            print("Creating backup before operation on production database...")
            backup_info = self.db.create_backup('auto', reason)
            print(f"✓ Backup created: {backup_info['filename']} ({backup_info['size_mb']} MB in {backup_info['duration_seconds']}s)")
            
            # Manage retention after successful backup
            retention_result = self.db.manage_backup_retention()
            if retention_result['action'] == 'cleanup_performed':
                print(f"Backup retention: {retention_result['remaining_backups']} backups, {retention_result['total_size_gb']} GB")
            
            return True
            
        except Exception as e:
            print(f"⚠️  Backup failed: {e}")
            response = input("Continue with operation without backup? (y/n): ")
            if response.lower() != 'y':
                print("Operation aborted for safety")
                return False
            print("Proceeding without backup...")
            return True
        
    def run(self) -> None:
        """Run the job scraper for all enabled job configurations."""
        global_params = self.config.get_global_params()
        job_configs = self.config.get_job_configs()
        
        if not job_configs:
            logger.warning("No enabled job configurations found")
            return
        
        total_new_jobs = 0
        
        for job_config in job_configs:
            job_name = job_config.get("name", "Unnamed Job")
            params = job_config.get("parameters", {})
            
            # Merge with global parameters
            for key, value in global_params.items():
                if key not in params:
                    params[key] = value
                    
            # Add proxies if available
            if self.proxies:
                params["proxies"] = self.proxies
                
            logger.info(f"Starting search for: {job_name}")
            logger.info(f"Parameters: {params}")
            
            try:
                # Perform job search
                jobs_df = scrape_jobs(**params)
                
                # Log the search
                self.db.log_search(job_name, params, len(jobs_df))
                
                # Insert results into database
                new_jobs = self.db.insert_jobs(jobs_df, job_name)
                total_new_jobs += new_jobs
                
                logger.info(f"Search completed for {job_name}. Found {len(jobs_df)} jobs, {new_jobs} new.")
                
            except Exception as e:
                logger.error(f"Error searching for {job_name}: {str(e)}", exc_info=True)
        
        # Create backup after scraping to capture new data (only for production database)
        if total_new_jobs > 0 and self.db.database_type == "production":
            print(f"\nScraping completed with {total_new_jobs} new jobs added.")
            print("Creating backup to capture new data...")
            try:
                backup_info = self.db.create_backup('auto', 'post_scraping')
                print(f"✓ Post-scraping backup created: {backup_info['filename']} ({backup_info['size_mb']} MB)")
                
                # Manage retention after backup
                retention_result = self.db.manage_backup_retention()
                if retention_result['action'] == 'cleanup_performed':
                    print(f"Backup retention: {retention_result['remaining_backups']} backups, {retention_result['total_size_gb']} GB")
                    
            except Exception as e:
                logger.warning(f"Post-scraping backup failed: {e}")
                print(f"⚠️  Post-scraping backup failed: {e}")
        elif total_new_jobs == 0:
            logger.info("No new jobs found, skipping post-scraping backup")
        else:
            logger.info("Working database scraping completed (no backup needed)")
    
    def _process_duplicates_auto(self) -> int:
        """Process duplicates for auto-clean workflow (in-memory, no file creation).
        
        Returns:
            Number of duplicate jobs deleted
        """
        try:
            duplicate_groups, ids_to_delete, ids_to_keep = self.duplicate_manager.identify_duplicates()
            
            if not duplicate_groups:
                logger.info("No duplicate groups found during auto-clean")
                return 0
            
            # Delete duplicates directly without creating files
            deleted_count = self.duplicate_manager.delete_duplicate_jobs_directly(ids_to_delete)
            
            logger.info(f"Auto-clean duplicate processing: {len(duplicate_groups)} groups, {deleted_count} duplicates removed")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error during auto-clean duplicate processing: {e}")
            return 0
    
    def process_duplicates(self) -> None:
        """Process duplicates manually (creates delete_ids.txt file only)."""
        # Warn if running data cleaning against production database
        if self.db.database_type == "production":
            print("⚠️  WARNING: Running duplicate processing against PRODUCTION database!")
            print("   Consider using --create-working-copy for data cleaning operations.")
            response = input("   Continue with production database processing? (y/n): ")
            if response.lower() != 'y':
                print("Operation cancelled for safety")
                return
        
        # Create backup before processing duplicates (only for production database)
        if not self._create_backup_with_prompt('duplicates'):
            return
            
        print("Processing duplicates...")
        
        duplicate_groups, ids_to_delete, ids_to_keep = self.duplicate_manager.identify_duplicates()
        
        if not duplicate_groups:
            print("No duplicate groups found.")
            return
        
        # Only create delete_ids.txt file (no Excel report)
        self.duplicate_manager.create_delete_ids_file(ids_to_delete)
        
        print(f"\n=== PROCESSING SUMMARY ===")
        print(f"Duplicate groups found: {len(duplicate_groups)}")
        print(f"IDs targeted for deletion: {len(ids_to_delete)}")
        print(f"IDs to keep (best from each group): {len(ids_to_keep)}")
        print(f"Delete IDs file: {os.path.join('configs', 'delete_ids.txt')}")
    
    def clear_jobs(self) -> None:
        """Clear all data from the scraped_jobs table."""
        # Warn if running data clearing against production database
        if self.db.database_type == "production":
            print("⚠️  WARNING: About to CLEAR ALL DATA from PRODUCTION database!")
            print("   This will permanently delete all job records.")
            response = input("   Are you absolutely sure? (y/n): ")
            if response.lower() != 'y':
                print("Operation cancelled for safety")
                return
        
        # Create backup before clearing (only for production database)
        if not self._create_backup_with_prompt('clear_all'):
            return
            
        rows_deleted = self.db.clear_all_jobs()
        logger.info(f"Cleared {rows_deleted} jobs from database")
    
    def delete_jobs_before_date(self, date_str: str) -> None:
        """Delete jobs scraped before a specified date."""
        # Warn if running data deletion against production database
        if self.db.database_type == "production":
            print("⚠️  WARNING: Running data deletion against PRODUCTION database!")
            print("   Consider using --create-working-copy for data cleaning operations.")
            response = input("   Continue with production database deletion? (y/n): ")
            if response.lower() != 'y':
                print("Operation cancelled for safety")
                return
        
        # Create backup before deletion (only for production database)
        if not self._create_backup_with_prompt('delete_by_date'):
            return
            
        rows_deleted = self.db.delete_jobs_before_date(date_str)
        logger.info(f"Deleted {rows_deleted} jobs scraped before {date_str}")
    
    def delete_jobs_by_ids(self, ids_file: str = None) -> None:
        """Delete jobs by their IDs from a file."""
        if ids_file is None:
            ids_file = os.path.join(SCRIPT_DIR, "configs", "delete_ids.txt")
        # Warn if running data deletion against production database
        if self.db.database_type == "production":
            print("⚠️  WARNING: Running data deletion against PRODUCTION database!")
            print("   Consider using --create-working-copy for data cleaning operations.")
            response = input("   Continue with production database deletion? (y/n): ")
            if response.lower() != 'y':
                print("Operation cancelled for safety")
                return
        
        # Create backup before deletion (only for production database)
        if not self._create_backup_with_prompt('delete_by_ids'):
            return
            
        rows_deleted = self.db.delete_jobs_by_ids(ids_file)
        logger.info(f"Deleted {rows_deleted} jobs using IDs from {ids_file}")
    
    def delete_jobs_by_salary(self, min_threshold: int = 70000, max_threshold: int = 90000) -> None:
        """Delete jobs with salaries below specified thresholds."""
        # Warn if running data cleaning against production database
        if self.db.database_type == "production":
            print("⚠️  WARNING: Running data cleaning against PRODUCTION database!")
            print("   Consider using --create-working-copy for data cleaning operations.")
            response = input("   Continue with production database cleaning? (y/n): ")
            if response.lower() != 'y':
                print("Operation cancelled for safety")
                return
        
        # Create backup before deletion (only for production database)
        if not self._create_backup_with_prompt('delete_by_salary'):
            return
            
        rows_deleted = self.db.delete_jobs_by_salary(min_threshold, max_threshold)
        logger.info(f"Deleted {rows_deleted} jobs with low salaries")
    
    def delete_jobs_by_company(self, companies_file: str = None) -> None:
        """Delete jobs by company names from a file."""
        if companies_file is None:
            companies_file = os.path.join(SCRIPT_DIR, "configs", "delete_companies.txt")
        # Warn if running data cleaning against production database
        if self.db.database_type == "production":
            print("⚠️  WARNING: Running data cleaning against PRODUCTION database!")
            print("   Consider using --create-working-copy for data cleaning operations.")
            response = input("   Continue with production database cleaning? (y/n): ")
            if response.lower() != 'y':
                print("Operation cancelled for safety")
                return
        
        # Create backup before deletion (only for production database)
        if not self._create_backup_with_prompt('delete_by_company'):
            return
            
        rows_deleted = self.db.delete_jobs_by_field('company', companies_file)
        logger.info(f"Deleted {rows_deleted} jobs matching companies from {companies_file}")
    
    def delete_jobs_by_title(self, titles_file: str = None) -> None:
        """Delete jobs by job titles from a file."""
        if titles_file is None:
            titles_file = os.path.join(SCRIPT_DIR, "configs", "delete_titles.txt")
        # Warn if running data cleaning against production database
        if self.db.database_type == "production":
            print("⚠️  WARNING: Running data cleaning against PRODUCTION database!")
            print("   Consider using --create-working-copy for data cleaning operations.")
            response = input("   Continue with production database cleaning? (y/n): ")
            if response.lower() != 'y':
                print("Operation cancelled for safety")
                return
        
        # Create backup before deletion (only for production database)
        if not self._create_backup_with_prompt('delete_by_title'):
            return
            
        rows_deleted = self.db.delete_jobs_by_field('title', titles_file)
        logger.info(f"Deleted {rows_deleted} jobs matching titles from {titles_file}")
    
    def backup_and_reset_db(self) -> None:
        """Create a backup of the database and clear all data."""
        success = self.db.backup_and_reset()
        if success:
            logger.info("Database successfully backed up and reset")
        else:
            logger.error("Failed to backup and reset database")

    def manual_backup(self) -> None:
        """Create a manual backup."""
        try:
            print("Creating manual backup...")
            backup_info = self.db.create_backup('manual', 'manual')
            print(f"✓ Manual backup created: {backup_info['filename']} ({backup_info['size_mb']} MB)")
            
            # Manage retention
            retention_result = self.db.manage_backup_retention()
            if retention_result['action'] == 'cleanup_performed':
                print(f"Backup retention: {retention_result['remaining_backups']} backups, {retention_result['total_size_gb']} GB")
            
        except Exception as e:
            print(f"✗ Manual backup failed: {e}")
            
    def list_backups(self) -> None:
        """List available backups."""
        backups = self.db.list_backups()
        
        if not backups:
            print("No backups found.")
            return
        
        print(f"\n=== AVAILABLE BACKUPS ===")
        print(f"{'Filename':<50} {'Size (MB)':<10} {'Created':<20} {'Reason'}")
        print("-" * 100)
        
        for backup in backups:
            created = datetime.strptime(backup['timestamp'], "%Y%m%d_%H%M%S").strftime("%Y-%m-%d %H:%M")
            reason = backup.get('reason', 'unknown')
            print(f"{backup['filename']:<50} {backup['size_mb']:<10} {created:<20} {reason}")
        
        total_size = sum(backup['size_mb'] for backup in backups)
        print(f"\nTotal: {len(backups)} backups, {total_size:.1f} MB")
    
    def restore_backup(self, backup_filename: str) -> None:
        """Restore from a backup file."""
        print(f"Restoring from backup: {backup_filename}")
        
        # Confirm restore operation
        response = input("This will overwrite all current data. Are you sure? (y/n): ")
        if response.lower() != 'y':
            print("Restore cancelled.")
            return
        
        success = self.db.restore_backup(backup_filename)
        if success:
            print(f"✓ Database restored successfully from {backup_filename}")
        else:
            print(f"✗ Restore failed from {backup_filename}")
    
    def test_backup(self, backup_filename: str) -> None:
        """Test backup file integrity."""
        print(f"Testing backup: {backup_filename}")
        
        is_valid = self.db.test_backup(backup_filename)
        if is_valid:
            print(f"✓ Backup {backup_filename} is valid")
        else:
            print(f"✗ Backup {backup_filename} is invalid or corrupted")
    
    def cleanup_backups(self) -> None:
        """Force cleanup of old backups."""
        print("Cleaning up old backups...")
        
        retention_result = self.db.manage_backup_retention()
        
        if retention_result['action'] == 'cleanup_performed':
            print(f"✓ Cleanup completed:")
            print(f"  Removed {retention_result['removed_count']} old backups")
            print(f"  Remaining: {retention_result['remaining_backups']} backups ({retention_result['total_size_gb']} GB)")
        elif retention_result['action'] == 'no_cleanup_needed':
            print(f"✓ No cleanup needed:")
            print(f"  Current: {retention_result['total_backups']} backups ({retention_result['total_size_gb']} GB)")
        else:
            print(f"✗ Cleanup failed: {retention_result.get('message', 'Unknown error')}")
            
    def create_working_copy(self, auto_clean: bool = True) -> None:
        """Create a working copy of the database for data cleaning.
        
        Args:
            auto_clean: If True, automatically run data cleaning workflows
        """
        # No backup needed - we're just copying the production database to create working copy
        try:
            # Get current database config - we need production database info
            current_config = self.db.db_config.config
            
            # Determine source database name
            if 'production_database' in current_config:
                source_db = current_config['production_database']['database']
            elif 'database' in current_config:
                source_db = current_config['database']['database']
            else:
                raise ValueError("No database configuration found")
            
            working_db = "jobscraps_working"  # Fixed name for Retool consistency
            
            # IMPORTANT: Close the current connection to the source database first
            logger.info("Closing current database connection to allow template copy")
            self.db.close()
            
            # Connect to a maintenance database (not the source database)
            conn_params = self.db.db_config.get_connection_params()
            
            # Try connecting to maintenance databases in order of preference
            maintenance_databases = ['template1', 'postgres', 'template0']
            conn = None
            
            for maintenance_db in maintenance_databases:
                try:
                    conn_params['database'] = maintenance_db
                    conn = psycopg2.connect(**conn_params)
                    conn.autocommit = True
                    logger.info(f"Connected to maintenance database: {maintenance_db}")
                    break
                except psycopg2.Error as e:
                    logger.debug(f"Cannot connect to {maintenance_db}: {e}")
                    continue
            
            if not conn:
                raise psycopg2.Error("Cannot connect to any maintenance database (template1, postgres, template0)")
            
            with conn.cursor() as cursor:
                # Drop existing working database if exists
                cursor.execute(f"DROP DATABASE IF EXISTS {working_db}")
                logger.info(f"Dropped existing working database if it existed")
                
                # Create new working database from template
                cursor.execute(f"CREATE DATABASE {working_db} WITH TEMPLATE {source_db} OWNER jonesy")
                logger.info(f"Created working database: {working_db}")
            
            conn.close()
            
            # Reconnect to the original database for normal operations
            self.db._connect_with_retry()
            
            # Update the config in memory to include working_database section
            if 'working_database' not in current_config:
                # If production_database exists, copy it to working_database
                if 'production_database' in current_config:
                    current_config['working_database'] = current_config['production_database'].copy()
                elif 'database' in current_config:
                    current_config['working_database'] = current_config['database'].copy()
                
                current_config['working_database']['database'] = working_db
                
                # Save updated config
                with open(self.db.db_config.config_path, 'w') as f:
                    json.dump(current_config, f, indent=2)
            
            print(f"\n=== WORKING COPY CREATED ===")
            print(f"Working database: {working_db}")
            print(f"Config updated: {self.db.db_config.config_path}")
            
            if auto_clean:
                print("Running automatic data cleaning workflows...")
                start_time = time.time()
                
                # Create a temporary scraper instance for the working database
                # Note: Since this operates on working database, no backups will be created during cleaning
                working_scraper = JobScraper(
                    config_path=self.config.config_path,
                    db_config_path=self.db.db_config.config_path,
                    database_type="working"
                )
                
                try:
                    # Get initial count
                    initial_count = working_scraper.db.get_all_jobs().shape[0]
                    print(f"Initial job count in working database: {initial_count}")
                    
                    # Run cleaning workflows in optimized order (fastest deletions first)
                    # No backups created during these operations since we're working on working database copy
                    step_start = time.time()
                    print("1. Deleting jobs by salary thresholds (fastest, removes most jobs)...")
                    working_scraper.delete_jobs_by_salary()
                    
                    remaining_after_salary = working_scraper.db.get_all_jobs().shape[0]
                    step_time = time.time() - step_start
                    print(f"   Jobs remaining after salary filter: {remaining_after_salary:,} ({step_time:.1f}s)")
                    
                    step_start = time.time()
                    print("2. Deleting jobs by company patterns...")
                    working_scraper.delete_jobs_by_company()
                    
                    remaining_after_company = working_scraper.db.get_all_jobs().shape[0]
                    step_time = time.time() - step_start
                    print(f"   Jobs remaining after company filter: {remaining_after_company:,} ({step_time:.1f}s)")
                    
                    step_start = time.time()
                    print("3. Deleting jobs by title patterns...")
                    working_scraper.delete_jobs_by_title()
                    
                    remaining_after_title = working_scraper.db.get_all_jobs().shape[0]
                    step_time = time.time() - step_start
                    print(f"   Jobs remaining after title filter: {remaining_after_title:,} ({step_time:.1f}s)")
                    
                    step_start = time.time()
                    print("4. Processing duplicates (in-memory processing)...")
                    duplicates_deleted = working_scraper._process_duplicates_auto()
                    
                    # Get final counts
                    final_count = working_scraper.db.get_all_jobs().shape[0]
                    removed_count = initial_count - final_count
                    removal_percentage = (removed_count / initial_count * 100) if initial_count > 0 else 0
                    duplicate_time = time.time() - step_start
                    total_time = time.time() - start_time
                    
                    print(f"   Duplicate processing completed: {duplicates_deleted} duplicates removed ({duplicate_time:.1f}s)")
                    
                    print(f"\n=== CLEANING COMPLETE ===")
                    print(f"Initial jobs: {initial_count:,}")
                    print(f"Jobs removed: {removed_count:,} ({removal_percentage:.1f}%)")
                    print(f"Jobs remaining: {final_count:,}")
                    print(f"Total cleaning time: {total_time:.1f} seconds")
                    print(f"Working database ready for analysis and Retool")
                    
                except Exception as e:
                    logger.error(f"Error during auto-cleaning: {e}")
                    print(f"Error during auto-cleaning: {e}")
                finally:
                    working_scraper.close()
            else:
                print("Use with: python scraper.py --working [command]")
            
        except psycopg2.Error as e:
            logger.error(f"Error creating working copy: {e}")
            print(f"Error creating working copy: {e}")
            
            # Provide helpful troubleshooting
            if "is being accessed by other users" in str(e):
                print("\nThis error occurs when there are active connections to the source database.")
                print("The script tried to close its connection, but there may be other active connections.")
                print("\nSolutions:")
                print("1. Check for other connections and close them")
                print("2. Restart your PostgreSQL container if needed")
            elif "permission denied" in str(e).lower():
                print("Permission denied - make sure your 'jonesy' user has CREATEDB privileges")
                print("Run this as a superuser:")
                print("ALTER USER jonesy CREATEDB;")
            elif "does not exist" in str(e).lower():
                print("Template database not found. Available maintenance databases may be limited.")
            else:
                print("Try creating the database manually:")
                print(f"psql -h 192.168.1.31 -p 5432 -U jonesy -d template1 -c \"CREATE DATABASE jobscraps_working WITH TEMPLATE {source_db} OWNER jonesy;\"")
                
        except Exception as e:
            logger.error(f"Unexpected error creating working copy: {e}")
            print(f"Unexpected error: {e}")
        
        finally:
            # Ensure we're reconnected to the original database
            try:
                self.db._ensure_connection()
            except:
                pass
            
    def close(self) -> None:
        """Close database connection and perform cleanup."""
        self.db.close()


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Job Scraper with PostgreSQL support and data management options")
    
    action_group = parser.add_mutually_exclusive_group()
    action_group.add_argument('--scrape', action='store_true', help='Scrape jobs based on configuration')
    action_group.add_argument('--clear', action='store_true', help='Clear all data from the scraped_jobs table')
    action_group.add_argument('--delete-before-date', metavar='YYYY-MM-DD', 
                             help='Delete jobs scraped before specified date (YYYY-MM-DD)')
    action_group.add_argument('--delete-by-ids', nargs='?', const='default', metavar='FILE', 
                             help=f'Delete jobs using IDs from specified file (default: {os.path.join("configs", "delete_ids.txt")})')
    action_group.add_argument('--delete-by-company', nargs='?', const='default', metavar='FILE', 
                             help=f'Delete jobs matching company patterns from specified file (default: {os.path.join("configs", "delete_companies.txt")})')
    action_group.add_argument('--delete-by-title', nargs='?', const='default', metavar='FILE', 
                             help=f'Delete jobs matching title patterns from specified file (default: {os.path.join("configs", "delete_titles.txt")})')
    action_group.add_argument('--delete-by-salary', nargs='?', const='default', metavar='MIN,MAX',
                             help='Delete jobs with low salaries (default: 70000,90000)')
    action_group.add_argument('--backup-reset', action='store_true', 
                             help='Backup the database and clear all data')
    action_group.add_argument('--process-duplicates', action='store_true',
                             help='Process duplicate jobs across sites and manage them')
    action_group.add_argument('--create-working-copy', action='store_true',
                             help='Create a working copy of the database with automatic data cleaning')
    
    # New backup-related arguments
    action_group.add_argument('--backup', action='store_true',
                             help='Create a manual backup')
    action_group.add_argument('--list-backups', action='store_true',
                             help='List available backups')
    action_group.add_argument('--restore-backup', metavar='FILENAME',
                             help='Restore database from specified backup file')
    action_group.add_argument('--test-backup', metavar='FILENAME',
                             help='Test backup file integrity')
    action_group.add_argument('--cleanup-backups', action='store_true',
                             help='Force cleanup of old backups')
    
    parser.add_argument('--config', metavar='FILE',
                       help=f'Path to job search configuration file (default: {os.path.join("configs", "job_search_config.json")})')
    parser.add_argument('--db-config', metavar='FILE',
                       help=f'Path to database configuration file (default: {os.path.join("configs", "db_config.json")})')
    parser.add_argument('--working', action='store_true',
                       help='Use working database configuration (working_database section in config)')
    parser.add_argument('--no-auto-clean', action='store_true',
                       help='Skip automatic data cleaning when creating working copy')
    
    return parser.parse_args()


def main():
    """Main function to run the job scraper."""
    logger.info("Starting JobSpy Scraper with PostgreSQL")
    
    args = parse_args()
    
    # Determine database type based on --working flag
    database_type = "working" if args.working else "production"
    if args.working:
        logger.info("Using working database configuration")
    
    scraper = JobScraper(
        config_path=args.config, 
        db_config_path=args.db_config,
        database_type=database_type
    )
    
    try:
        if args.backup:
            scraper.manual_backup()
        elif args.list_backups:
            scraper.list_backups()
        elif args.restore_backup:
            scraper.restore_backup(args.restore_backup)
        elif args.test_backup:
            scraper.test_backup(args.test_backup)
        elif args.cleanup_backups:
            scraper.cleanup_backups()
        elif args.create_working_copy:
            scraper.create_working_copy(auto_clean=not args.no_auto_clean)
        elif args.scrape or not any([args.clear, args.delete_before_date, args.delete_by_ids is not None, 
                                  args.delete_by_company is not None, args.delete_by_title is not None, 
                                  args.delete_by_salary is not None, args.backup_reset, args.process_duplicates]):
            scraper.run()
        elif args.clear:
            scraper.clear_jobs()
        elif args.delete_before_date:
            scraper.delete_jobs_before_date(args.delete_before_date)
        elif args.delete_by_ids is not None:
            if args.delete_by_ids == 'default':
                ids_file = None  # Will use default
            else:
                ids_file = args.delete_by_ids
            scraper.delete_jobs_by_ids(ids_file)
        elif args.delete_by_company is not None:
            if args.delete_by_company == 'default':
                companies_file = None  # Will use default
            else:
                companies_file = args.delete_by_company
            scraper.delete_jobs_by_company(companies_file)
        elif args.delete_by_title is not None:
            if args.delete_by_title == 'default':
                titles_file = None  # Will use default
            else:
                titles_file = args.delete_by_title
            scraper.delete_jobs_by_title(titles_file)
        elif args.delete_by_salary is not None:
            if args.delete_by_salary == 'default':
                scraper.delete_jobs_by_salary()  # Use defaults: 70000, 90000
            else:
                try:
                    min_sal, max_sal = map(int, args.delete_by_salary.split(','))
                    scraper.delete_jobs_by_salary(min_sal, max_sal)
                except ValueError:
                    logger.error("Invalid salary format. Use: --delete-by-salary 70000,90000")
                    sys.exit(1)
        elif args.backup_reset:
            scraper.backup_and_reset_db()
        elif args.process_duplicates:
            scraper.process_duplicates()
            
    except KeyboardInterrupt:
        logger.info("Scraper interrupted by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
    finally:
        scraper.close()
        logger.info("JobSpy Scraper finished")


if __name__ == "__main__":
    main()