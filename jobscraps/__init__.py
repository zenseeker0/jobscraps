"""JobScraps package."""

from .scraper import JobScraper
from .duplicate_manager import DuplicateManager
from .backup_manager import BackupManager
from .data_cleaner import DataCleaner
from .scraping_orchestrator import ScrapingOrchestrator
from .console_interface import console
from .database import DatabaseConfig, JobDatabase, BackupMixin
from .config import JobSearchConfig
from .session_manager import SessionManager

__all__ = [
    "JobScraper",
    "BackupManager",
    "DataCleaner",
    "ScrapingOrchestrator",
    "console",
    "DuplicateManager",
    "DatabaseConfig",
    "JobDatabase",
    "BackupMixin",
    "JobSearchConfig",
    "SessionManager",
]
