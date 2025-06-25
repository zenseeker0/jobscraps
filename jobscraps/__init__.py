"""JobScraps package."""

from .scraper import JobScraper
from .duplicate_manager import DuplicateManager
from .database import DatabaseConfig, JobDatabase, BackupMixin
from .config import JobSearchConfig

__all__ = [
    "JobScraper",
    "DuplicateManager",
    "DatabaseConfig",
    "JobDatabase",
    "BackupMixin",
    "JobSearchConfig",
]
