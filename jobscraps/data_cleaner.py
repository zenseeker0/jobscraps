from __future__ import annotations

import os
from typing import Optional

from .database import JobDatabase
from .duplicate_manager import DuplicateManager
from .console_interface import console
from .backup_manager import BackupManager


class DataCleaner:
    """Data deletion and duplicate processing utilities."""

    def __init__(self, db: JobDatabase, duplicate_manager: DuplicateManager, backup_manager: BackupManager) -> None:
        self.db = db
        self.duplicate_manager = duplicate_manager
        self.backup_manager = backup_manager

    def _process_duplicates_auto(self) -> int:
        """Process duplicates for auto-clean workflow."""
        try:
            duplicate_groups, ids_to_delete, _ = self.duplicate_manager.identify_duplicates()
            if not duplicate_groups:
                return 0
            deleted_count = self.duplicate_manager.delete_duplicate_jobs_directly(ids_to_delete)
            return deleted_count
        except Exception as exc:  # pylint: disable=broad-except
            console.print(f"Error during auto-clean duplicate processing: {exc}")
            return 0

    def process_duplicates(self) -> None:
        """Process duplicates manually."""
        if self.db.database_type == "production":
            console.print("⚠️  WARNING: Running duplicate processing against PRODUCTION database!")
            console.print("   Consider using --create-working-copy for data cleaning operations.")
            response = console.input("   Continue with production database processing? (y/n): ")
            if response.lower() != "y":
                console.print("Operation cancelled for safety")
                return
        if not self.backup_manager.create_backup_with_prompt("duplicates"):
            return
        console.print("Processing duplicates...")
        duplicate_groups, ids_to_delete, ids_to_keep = self.duplicate_manager.identify_duplicates()
        if not duplicate_groups:
            console.print("No duplicate groups found.")
            return
        self.duplicate_manager.create_delete_ids_file(ids_to_delete)
        console.print("\n=== PROCESSING SUMMARY ===")
        console.print(f"Duplicate groups found: {len(duplicate_groups)}")
        console.print(f"IDs targeted for deletion: {len(ids_to_delete)}")
        console.print(f"IDs to keep (best from each group): {len(ids_to_keep)}")
        console.print(f"Delete IDs file: {os.path.join('configs', 'filters', 'delete_ids.txt')}")

    def clear_jobs(self) -> None:
        """Clear all data from scraped_jobs table."""
        if self.db.database_type == "production":
            console.print("⚠️  WARNING: About to CLEAR ALL DATA from PRODUCTION database!")
            console.print("   This will permanently delete all job records.")
            response = console.input("   Are you absolutely sure? (y/n): ")
            if response.lower() != "y":
                console.print("Operation cancelled for safety")
                return
        if not self.backup_manager.create_backup_with_prompt("clear_all"):
            return
        self.db.clear_all_jobs()

    def delete_jobs_before_date(self, date_str: str) -> None:
        """Delete jobs scraped before a specified date."""
        if self.db.database_type == "production":
            console.print("⚠️  WARNING: Running data deletion against PRODUCTION database!")
            console.print("   Consider using --create-working-copy for data cleaning operations.")
            response = console.input("   Continue with production database deletion? (y/n): ")
            if response.lower() != "y":
                console.print("Operation cancelled for safety")
                return
        if not self.backup_manager.create_backup_with_prompt("delete_by_date"):
            return
        self.db.delete_jobs_before_date(date_str)

    def delete_jobs_by_ids(self, ids_file: Optional[str] = None) -> None:
        """Delete jobs by their IDs from a file."""
        if ids_file is None:
            ids_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "configs", "filters", "delete_ids.txt")
        if self.db.database_type == "production":
            console.print("⚠️  WARNING: Running data deletion against PRODUCTION database!")
            console.print("   Consider using --create-working-copy for data cleaning operations.")
            response = console.input("   Continue with production database deletion? (y/n): ")
            if response.lower() != "y":
                console.print("Operation cancelled for safety")
                return
        if not self.backup_manager.create_backup_with_prompt("delete_by_ids"):
            return
        self.db.delete_jobs_by_ids(ids_file)

    def delete_jobs_by_salary(self, min_threshold: int = 70000, max_threshold: int = 90000) -> None:
        """Delete jobs with salaries below specified thresholds."""
        if self.db.database_type == "production":
            console.print("⚠️  WARNING: Running data cleaning against PRODUCTION database!")
            console.print("   Consider using --create-working-copy for data cleaning operations.")
            response = console.input("   Continue with production database cleaning? (y/n): ")
            if response.lower() != "y":
                console.print("Operation cancelled for safety")
                return
        if not self.backup_manager.create_backup_with_prompt("delete_by_salary"):
            return
        self.db.delete_jobs_by_salary(min_threshold, max_threshold)

    def delete_jobs_by_company(self, companies_file: Optional[str] = None) -> None:
        """Delete jobs by company names from a file."""
        if companies_file is None:
            companies_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "configs", "filters", "delete_companies.txt")
        if self.db.database_type == "production":
            console.print("⚠️  WARNING: Running data cleaning against PRODUCTION database!")
            console.print("   Consider using --create-working-copy for data cleaning operations.")
            response = console.input("   Continue with production database cleaning? (y/n): ")
            if response.lower() != "y":
                console.print("Operation cancelled for safety")
                return
        if not self.backup_manager.create_backup_with_prompt("delete_by_company"):
            return
        self.db.delete_jobs_by_field("company", companies_file)

    def delete_jobs_by_title(self, titles_file: Optional[str] = None) -> None:
        """Delete jobs by titles from a file."""
        if titles_file is None:
            titles_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "configs", "filters", "delete_titles.txt")
        if self.db.database_type == "production":
            console.print("⚠️  WARNING: Running data cleaning against PRODUCTION database!")
            console.print("   Consider using --create-working-copy for data cleaning operations.")
            response = console.input("   Continue with production database cleaning? (y/n): ")
            if response.lower() != "y":
                console.print("Operation cancelled for safety")
                return
        if not self.backup_manager.create_backup_with_prompt("delete_by_title"):
            return
        self.db.delete_jobs_by_field("title", titles_file)


__all__ = ["DataCleaner"]
