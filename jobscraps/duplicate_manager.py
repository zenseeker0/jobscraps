"""Duplicate job detection and management utilities."""
from __future__ import annotations

import os
import logging
from typing import List, Dict, Tuple

from jobscraps.database import JobDatabase
from .console_interface import console

SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = logging.getLogger(__name__)


class DuplicateManager:
    """Handle duplicate job detection and cleanup."""

    def __init__(self, db: JobDatabase) -> None:
        """Initialize the manager with a database connection."""
        self.db = db
        self.site_preference = ['linkedin', 'indeed', 'google']

    def identify_duplicates(self) -> Tuple[List[List[Dict]], List[str], List[str]]:
        """Identify duplicate jobs and determine which ones to keep/delete."""
        duplicate_groups = self.db.get_duplicate_groups()
        ids_to_delete: List[str] = []
        ids_to_keep: List[str] = []

        for group in duplicate_groups:
            best_job = self._select_best_job(group)
            ids_to_keep.append(best_job['id'])
            for job in group:
                if job['id'] != best_job['id']:
                    ids_to_delete.append(job['id'])
        return duplicate_groups, ids_to_delete, ids_to_keep

    def _select_best_job(self, jobs: List[Dict]) -> Dict:
        """Select the best job from a group of duplicates using a ranking algorithm."""
        candidates = jobs.copy()

        # Step 1: Filter by description (keep jobs with descriptions)
        jobs_with_desc = [job for job in candidates if job.get('description') and job['description'].strip()]
        if jobs_with_desc and len(jobs_with_desc) < len(candidates):
            candidates = jobs_with_desc
        if len(candidates) == 1:
            return candidates[0]

        # Step 2: Filter by Colorado location preference
        colorado_jobs = [
            job for job in candidates
            if job.get('location') and (
                ', CO' in job['location'] or
                'Colorado' in job['location'] or
                ', co' in job['location'].lower() or
                'colorado' in job['location'].lower()
            )
        ]
        if colorado_jobs and len(colorado_jobs) < len(candidates):
            candidates = colorado_jobs
        if len(candidates) == 1:
            return candidates[0]

        # Step 3: Filter by salary (keep jobs with min_amount > 0)
        jobs_with_salary = [job for job in candidates if job.get('min_amount') and job['min_amount'] > 0]
        if jobs_with_salary and len(jobs_with_salary) < len(candidates):
            candidates = jobs_with_salary
        if len(candidates) == 1:
            return candidates[0]

        # Step 4: Select by highest min_amount
        if len(candidates) > 1:
            candidates_with_salary = [job for job in candidates if job.get('min_amount') and job['min_amount'] > 0]
            if candidates_with_salary:
                max_salary = max(job['min_amount'] for job in candidates_with_salary)
                highest_salary_jobs = [job for job in candidates_with_salary if job['min_amount'] == max_salary]
                if len(highest_salary_jobs) < len(candidates):
                    candidates = highest_salary_jobs
        if len(candidates) == 1:
            return candidates[0]

        # Step 5: Filter by remote status (if mixed, prefer remote=1)
        remote_values = {job.get('is_remote', False) for job in candidates}
        if False in remote_values and True in remote_values:
            remote_jobs = [job for job in candidates if job.get('is_remote') is True]
            if remote_jobs:
                candidates = remote_jobs
        if len(candidates) == 1:
            return candidates[0]

        # Step 6: Filter out United States search queries (prefer others)
        non_us_jobs = [
            job for job in candidates
            if not (job.get('search_query') and 'united states' in job['search_query'].lower())
        ]
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
            jobs_with_dates = [job for job in candidates if job.get('date_posted') and job['date_posted'].strip()]
            if jobs_with_dates:
                try:
                    jobs_with_dates.sort(key=lambda x: x['date_posted'], reverse=True)
                    return jobs_with_dates[0]
                except Exception:
                    pass
        # Fallback
        return candidates[0]

    def delete_duplicate_jobs_directly(self, ids_to_delete: List[str]) -> int:
        """Delete duplicate jobs directly from database without creating files."""
        if not ids_to_delete:
            return 0
        try:
            with self.db.conn.cursor() as cursor:
                cursor.execute("DELETE FROM scraped_jobs WHERE id = ANY(%s)", (ids_to_delete,))
                rows_deleted = cursor.rowcount
                self.db.conn.commit()
            logger.info("Deleted %d duplicate jobs directly", rows_deleted)
            return rows_deleted
        except Exception as e:
            logger.error("Error deleting duplicate jobs directly: %s", e)
            return 0

    def create_delete_ids_file(self, ids_to_delete: List[str], filename: str | None = None) -> None:
        """Create/overwrite file with IDs to delete."""
        if filename is None:
            filename = os.path.join(SCRIPT_DIR, "configs", "filters", "delete_ids.txt")
        try:
            config_dir = os.path.dirname(filename)
            if config_dir and not os.path.exists(config_dir):
                os.makedirs(config_dir)
            with open(filename, 'w') as f:
                for job_id in ids_to_delete:
                    f.write(f"{job_id}\n")
            logger.info("Created %s with %d IDs", filename, len(ids_to_delete))
            console.info(f"Created {filename} with {len(ids_to_delete)} IDs to delete")
        except Exception as e:
            logger.error("Error creating delete IDs file: %s", e)
            console.info(f"Error creating delete IDs file: {e}")

__all__ = ["DuplicateManager"]
