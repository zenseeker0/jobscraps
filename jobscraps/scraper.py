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
from .duplicate_manager import DuplicateManager
from .config import JobSearchConfig

from .database import DatabaseConfig, JobDatabase
from .console_interface import console
from .backup_manager import BackupManager
from .data_cleaner import DataCleaner
import warnings
# Suppress pandas SQLAlchemy warnings for psycopg2 connections
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Or more specifically, suppress only the exact warning you're seeing:
warnings.filterwarnings('ignore', 
                       message='pandas only supports SQLAlchemy connectable.*', 
                       category=UserWarning, 
                       module='pandas')

# Get the directory where scraper.py is located
SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

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
            config_path = os.path.join(SCRIPT_DIR, "configs", "search", "job_search_config.json")
        if db_config_path is None:
            db_config_path = os.path.join(SCRIPT_DIR, "configs", "db", "db_config.json")
        self.config = JobSearchConfig(config_path)
        self.db = JobDatabase(db_config_path, database_type)
        self.duplicate_manager = DuplicateManager(self.db)

        self.session_id = self.db.start_session()
        logger.info("Started search session %s", self.session_id)
        
        self.proxies = [
        ]

        
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
                self.db.log_search(self.session_id, job_name, params, len(jobs_df))
                
                # Insert results into database
                new_jobs = self.db.insert_jobs(jobs_df, job_name)
                total_new_jobs += new_jobs
                
                logger.info(f"Search completed for {job_name}. Found {len(jobs_df)} jobs, {new_jobs} new.")
                
            except Exception as e:
                logger.error(f"Error searching for {job_name}: {str(e)}", exc_info=True)
        
        # Create backup after scraping to capture new data (only for production database)
        if total_new_jobs > 0 and self.db.database_type == "production":
            console.info(f"\nScraping completed with {total_new_jobs} new jobs added.")
            console.info("Creating backup to capture new data...")
            try:
                backup_info = self.db.create_backup('auto', 'post_scraping')
                console.info(f"✓ Post-scraping backup created: {backup_info['filename']} ({backup_info['size_mb']} MB)")
                
                # Manage retention after backup
                retention_result = self.db.manage_backup_retention()
                if retention_result['action'] == 'cleanup_performed':
                    console.info(f"Backup retention: {retention_result['remaining_backups']} backups, {retention_result['total_size_gb']} GB")
                    
            except Exception as e:
                logger.warning(f"Post-scraping backup failed: {e}")
                console.info(f"⚠️  Post-scraping backup failed: {e}")
        elif total_new_jobs == 0:
            logger.info("No new jobs found, skipping post-scraping backup")
        else:
            logger.info("Working database scraping completed (no backup needed)")
    
    
    
    def backup_and_reset_db(self) -> None:
        """Create a backup of the database and clear all data."""
        success = self.db.backup_and_reset()
        if success:
            logger.info("Database successfully backed up and reset")
        else:
            logger.error("Failed to backup and reset database")

            
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
            user = conn_params.get("user", "")
            
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
                cursor.execute(
                    f"CREATE DATABASE {working_db} WITH TEMPLATE {source_db} OWNER {user}"
                )
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
            
            console.info(f"\n=== WORKING COPY CREATED ===")
            console.info(f"Working database: {working_db}")
            console.info(f"Config updated: {self.db.db_config.config_path}")
            
            if auto_clean:
                console.info("Running automatic data cleaning workflows...")
                start_time = time.time()
                
                # Create a temporary scraper instance for the working database
                # Note: Since this operates on working database, no backups will be created during cleaning
                working_scraper = JobScraper(
                    config_path=self.config.config_path,
                    db_config_path=self.db.db_config.config_path,
                    database_type="working"
                )
                working_cleaner = DataCleaner(
                    working_scraper.db,
                    working_scraper.duplicate_manager,
                    BackupManager(working_scraper.db)
                )
                
                try:
                    # Get initial count
                    initial_count = working_scraper.db.get_all_jobs().shape[0]
                    console.info(f"Initial job count in working database: {initial_count}")
                    
                    # Run cleaning workflows in optimized order (fastest deletions first)
                    # No backups created during these operations since we're working on working database copy
                    step_start = time.time()
                    console.info("1. Deleting jobs by salary thresholds (fastest, removes most jobs)...")
                    working_cleaner.delete_jobs_by_salary()
                    
                    remaining_after_salary = working_scraper.db.get_all_jobs().shape[0]
                    step_time = time.time() - step_start
                    console.info(f"   Jobs remaining after salary filter: {remaining_after_salary:,} ({step_time:.1f}s)")
                    
                    step_start = time.time()
                    console.info("2. Deleting jobs by company patterns...")
                    working_cleaner.delete_jobs_by_company()
                    
                    remaining_after_company = working_scraper.db.get_all_jobs().shape[0]
                    step_time = time.time() - step_start
                    console.info(f"   Jobs remaining after company filter: {remaining_after_company:,} ({step_time:.1f}s)")
                    
                    step_start = time.time()
                    console.info("3. Deleting jobs by title patterns...")
                    working_cleaner.delete_jobs_by_title()
                    
                    remaining_after_title = working_scraper.db.get_all_jobs().shape[0]
                    step_time = time.time() - step_start
                    console.info(f"   Jobs remaining after title filter: {remaining_after_title:,} ({step_time:.1f}s)")
                    
                    step_start = time.time()
                    console.info("4. Processing duplicates (in-memory processing)...")
                    duplicates_deleted = working_cleaner._process_duplicates_auto()
                    
                    # Get final counts
                    final_count = working_scraper.db.get_all_jobs().shape[0]
                    removed_count = initial_count - final_count
                    removal_percentage = (removed_count / initial_count * 100) if initial_count > 0 else 0
                    duplicate_time = time.time() - step_start
                    total_time = time.time() - start_time
                    
                    console.info(f"   Duplicate processing completed: {duplicates_deleted} duplicates removed ({duplicate_time:.1f}s)")
                    
                    console.info(f"\n=== CLEANING COMPLETE ===")
                    console.info(f"Initial jobs: {initial_count:,}")
                    console.info(f"Jobs removed: {removed_count:,} ({removal_percentage:.1f}%)")
                    console.info(f"Jobs remaining: {final_count:,}")
                    console.info(f"Total cleaning time: {total_time:.1f} seconds")
                    console.info(f"Working database ready for analysis and Retool")
                    
                except Exception as e:
                    logger.error(f"Error during auto-cleaning: {e}")
                    console.info(f"Error during auto-cleaning: {e}")
                finally:
                    working_scraper.close()
            else:
                console.info("Use with: python scraper.py --working [command]")
            
        except psycopg2.Error as e:
            logger.error(f"Error creating working copy: {e}")
            console.info(f"Error creating working copy: {e}")
            
            # Provide helpful troubleshooting
            if "is being accessed by other users" in str(e):
                console.info("\nThis error occurs when there are active connections to the source database.")
                console.info("The script tried to close its connection, but there may be other active connections.")
                console.info("\nSolutions:")
                console.info("1. Check for other connections and close them")
                console.info("2. Restart your PostgreSQL container if needed")
            elif "permission denied" in str(e).lower():
                user = self.db.db_config.get_connection_params().get("user", "")
                console.info(f"Permission denied - make sure user '{user}' has CREATEDB privileges")
                console.info("Run this as a superuser:")
                console.info(f"ALTER USER {user} CREATEDB;")
            elif "does not exist" in str(e).lower():
                console.info("Template database not found. Available maintenance databases may be limited.")
            else:
                conn_params = self.db.db_config.get_connection_params()
                host = conn_params.get("host", "localhost")
                port = conn_params.get("port", 5432)
                user = conn_params.get("user", "")
                console.info("Try creating the database manually:")
                console.info(
                    f"psql -h {host} -p {port} -U {user} -d template1 -c \"CREATE DATABASE jobscraps_working WITH TEMPLATE {source_db} OWNER {user};\""
                )
                
        except Exception as e:
            logger.error(f"Unexpected error creating working copy: {e}")
            console.info(f"Unexpected error: {e}")
        
        finally:
            # Ensure we're reconnected to the original database
            try:
                self.db._ensure_connection()
            except:
                pass
            
    def close(self) -> None:
        """Close database connection and perform cleanup."""
        self.db.close()


if __name__ == "__main__":
    from .cli import app
    app()
