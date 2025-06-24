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

from database import DatabaseConfig, JobDatabase
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
            filename = os.path.join(SCRIPT_DIR, "configs", "filters", "delete_ids.txt")
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
            config_path = os.path.join(SCRIPT_DIR, "configs", "search", "job_search_config.json")
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
            config_path = os.path.join(SCRIPT_DIR, "configs", "search", "job_search_config.json")
        if db_config_path is None:
            db_config_path = os.path.join(SCRIPT_DIR, "configs", "db", "db_config.json")
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
        print(f"Delete IDs file: {os.path.join('configs', 'filters', 'delete_ids.txt')}")
    
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
            ids_file = os.path.join(SCRIPT_DIR, "configs", "filters", "delete_ids.txt")
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
            companies_file = os.path.join(SCRIPT_DIR, "configs", "filters", "delete_companies.txt")
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
            titles_file = os.path.join(SCRIPT_DIR, "configs", "filters", "delete_titles.txt")
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
                user = self.db.db_config.get_connection_params().get("user", "")
                print(f"Permission denied - make sure user '{user}' has CREATEDB privileges")
                print("Run this as a superuser:")
                print(f"ALTER USER {user} CREATEDB;")
            elif "does not exist" in str(e).lower():
                print("Template database not found. Available maintenance databases may be limited.")
            else:
                conn_params = self.db.db_config.get_connection_params()
                host = conn_params.get("host", "localhost")
                port = conn_params.get("port", 5432)
                user = conn_params.get("user", "")
                print("Try creating the database manually:")
                print(
                    f"psql -h {host} -p {port} -U {user} -d template1 -c \"CREATE DATABASE jobscraps_working WITH TEMPLATE {source_db} OWNER {user};\""
                )
                
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


if __name__ == "__main__":
    from cli import app
    app()
