from __future__ import annotations

import os
from typing import Optional

from .database import JobDatabase
from .duplicate_manager import DuplicateManager
from .console_interface import console
from .backup_manager import BackupManager
from .base_manager import BaseManager


class DataCleaner(BaseManager):
    """Data deletion and duplicate processing utilities."""

    def __init__(self, db: JobDatabase, duplicate_manager: DuplicateManager, backup_manager: BackupManager) -> None:
        super().__init__(db)
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
        if not self.check_production_safety(
            "⚠️  WARNING: Running duplicate processing against PRODUCTION database!",
            "   Continue with production database processing? (y/n): ",
        ):
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

    # ORIGINAL DELETE METHODS (for backward compatibility)
    def clear_jobs(self) -> None:
        """Clear all data from scraped_jobs table."""
        if not self.check_production_safety(
            "⚠️  WARNING: About to CLEAR ALL DATA from PRODUCTION database!",
            "   Are you absolutely sure? (y/n): ",
            "   This will permanently delete all job records.",
        ):
            return
        if not self.backup_manager.create_backup_with_prompt("clear_all"):
            return
        self.db.clear_all_jobs()

    def delete_jobs_before_date(self, date_str: str) -> None:
        """Delete jobs scraped before a specified date."""
        if not self.check_production_safety(
            "⚠️  WARNING: Running data deletion against PRODUCTION database!",
            "   Continue with production database deletion? (y/n): ",
        ):
            return
        if not self.backup_manager.create_backup_with_prompt("delete_by_date"):
            return
        self.db.delete_jobs_before_date(date_str)

    def delete_jobs_by_ids(self, ids_file: Optional[str] = None) -> None:
        """Delete jobs by their IDs from a file."""
        if ids_file is None:
            ids_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "configs", "filters", "delete_ids.txt")
        if not self.check_production_safety(
            "⚠️  WARNING: Running data deletion against PRODUCTION database!",
            "   Continue with production database deletion? (y/n): ",
        ):
            return
        if not self.backup_manager.create_backup_with_prompt("delete_by_ids"):
            return
        self.db.delete_jobs_by_ids(ids_file)

    def delete_jobs_by_salary(self, min_threshold: int = 70000, max_threshold: int = 90000) -> None:
        """Delete jobs with salaries below specified thresholds."""
        if not self.check_production_safety(
            "⚠️  WARNING: Running data cleaning against PRODUCTION database!",
            "   Continue with production database cleaning? (y/n): ",
        ):
            return
        if not self.backup_manager.create_backup_with_prompt("delete_by_salary"):
            return
        self.db.delete_jobs_by_salary(min_threshold, max_threshold)

    def delete_jobs_by_company(self, companies_file: Optional[str] = None) -> None:
        """Delete jobs by company names from a file."""
        if companies_file is None:
            companies_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "configs", "filters", "delete_companies.txt")
        if not self.check_production_safety(
            "⚠️  WARNING: Running data cleaning against PRODUCTION database!",
            "   Continue with production database cleaning? (y/n): ",
        ):
            return
        if not self.backup_manager.create_backup_with_prompt("delete_by_company"):
            return
        self.db.delete_jobs_by_field("company", companies_file)

    def delete_jobs_by_title(self, titles_file: Optional[str] = None) -> None:
        """Delete jobs by titles from a file."""
        if titles_file is None:
            titles_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "configs", "filters", "delete_titles.txt")
        if not self.check_production_safety(
            "⚠️  WARNING: Running data cleaning against PRODUCTION database!",
            "   Continue with production database cleaning? (y/n): ",
        ):
            return
        if not self.backup_manager.create_backup_with_prompt("delete_by_title"):
            return
        self.db.delete_jobs_by_field("title", titles_file)

    # NEW EXCLUSION METHODS (preserve data, just mark as excluded)
    def mark_excluded_by_ids(self, ids_file: Optional[str] = None, reason: str = "manual") -> None:
        """Mark jobs as excluded using IDs from a file (preserves data)."""
        if ids_file is None:
            ids_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "configs", "filters", "delete_ids.txt")
        
        console.print(f"🏷️  Marking jobs as excluded from file: {ids_file}")
        console.print(f"   Exclusion reason: {reason}")
        
        if not os.path.exists(ids_file):
            console.print(f"❌ File not found: {ids_file}")
            return
        
        try:
            with open(ids_file, 'r', encoding='utf-8') as f:
                job_ids = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            
            if not job_ids:
                console.print("❌ No job IDs found in file")
                return
            
            count = self.db.mark_jobs_excluded_by_ids(job_ids, reason)
            console.print(f"✅ Marked {count} jobs as excluded")
            
        except Exception as e:
            console.print(f"❌ Error marking jobs as excluded: {e}")

    def mark_excluded_by_company(self, companies_file: Optional[str] = None, reason: str = "company_filter") -> None:
        """Mark jobs as excluded by company patterns (preserves data)."""
        if companies_file is None:
            companies_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "configs", "filters", "delete_companies.txt")
        
        console.print(f"🏷️  Marking jobs as excluded by company patterns from: {companies_file}")
        console.print(f"   Exclusion reason: {reason}")
        
        if not os.path.exists(companies_file):
            console.print(f"❌ File not found: {companies_file}")
            return
        
        try:
            with open(companies_file, 'r', encoding='utf-8') as f:
                patterns = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            
            if not patterns:
                console.print("❌ No company patterns found in file")
                return
            
            count = self.db.mark_jobs_excluded_by_field("company", patterns, reason)
            console.print(f"✅ Marked {count} jobs as excluded")
            
        except Exception as e:
            console.print(f"❌ Error marking jobs as excluded: {e}")

    def mark_excluded_by_title(self, titles_file: Optional[str] = None, reason: str = "title_filter") -> None:
        """Mark jobs as excluded by title patterns (preserves data)."""
        if titles_file is None:
            titles_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "configs", "filters", "delete_titles.txt")
        
        console.print(f"🏷️  Marking jobs as excluded by title patterns from: {titles_file}")
        console.print(f"   Exclusion reason: {reason}")
        
        if not os.path.exists(titles_file):
            console.print(f"❌ File not found: {titles_file}")
            return
        
        try:
            with open(titles_file, 'r', encoding='utf-8') as f:
                patterns = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            
            if not patterns:
                console.print("❌ No title patterns found in file")
                return
            
            count = self.db.mark_jobs_excluded_by_field("title", patterns, reason)
            console.print(f"✅ Marked {count} jobs as excluded")
            
        except Exception as e:
            console.print(f"❌ Error marking jobs as excluded: {e}")

    def mark_excluded_by_salary(self, min_threshold: int = 70000, max_threshold: int = 90000, reason: str = "salary_filter") -> None:
        """Mark jobs as excluded with salaries below thresholds (preserves data)."""
        console.print(f"🏷️  Marking jobs as excluded with salary below ${min_threshold} or above ${max_threshold}")
        console.print(f"   Exclusion reason: {reason}")
        
        try:
            count = self.db.mark_jobs_excluded_by_salary(min_threshold, max_threshold, reason)
            console.print(f"✅ Marked {count} jobs as excluded")
            
        except Exception as e:
            console.print(f"❌ Error marking jobs as excluded: {e}")

    def apply_filtering_rules(self) -> None:
        """Apply all filtering rules from config files to mark jobs as excluded."""
        console.print("🎯 Applying all filtering rules...")
        
        base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "configs", "filters")
        
        # Apply company filters
        companies_file = os.path.join(base_path, "delete_companies.txt")
        if os.path.exists(companies_file):
            self.mark_excluded_by_company(companies_file, "company_filter")
        
        # Apply title filters
        titles_file = os.path.join(base_path, "delete_titles.txt")
        if os.path.exists(titles_file):
            self.mark_excluded_by_title(titles_file, "title_filter")
        
        # Apply ID filters
        ids_file = os.path.join(base_path, "delete_ids.txt")
        if os.path.exists(ids_file):
            self.mark_excluded_by_ids(ids_file, "id_filter")
        
        # Apply salary filters (default thresholds)
        self.mark_excluded_by_salary(70000, 90000, "salary_filter")
        
        console.print("✅ All filtering rules applied")

    def unmark_excluded(self, reason: Optional[str] = None) -> None:
        """Remove exclusion marks from jobs (makes them visible again)."""
        if reason:
            console.print(f"🔄 Removing exclusion marks for reason: {reason}")
        else:
            console.print("🔄 Removing all exclusion marks")
        
        try:
            count = self.db.unmark_jobs_excluded(reason)
            console.print(f"✅ Unmarked {count} jobs (they are now visible again)")
            
        except Exception as e:
            console.print(f"❌ Error unmarking jobs: {e}")


__all__ = ["DataCleaner"]