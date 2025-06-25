from __future__ import annotations

from typing import Optional

from .scraper import JobScraper
from .backup_manager import BackupManager
from .data_cleaner import DataCleaner
from .duplicate_manager import DuplicateManager


class ScrapingOrchestrator:
    """Coordinate scraping and cleaning operations."""

    def __init__(self, config_path: Optional[str] = None, db_config_path: Optional[str] = None, database_type: str = "production") -> None:
        self.scraper = JobScraper(config_path=config_path, db_config_path=db_config_path, database_type=database_type)
        self.backup_manager = BackupManager(self.scraper.db)
        self.data_cleaner = DataCleaner(self.scraper.db, self.scraper.duplicate_manager, self.backup_manager)

    def run_scrape(self) -> None:
        self.scraper.run()

    def create_working_copy(self, auto_clean: bool = True) -> None:
        self.scraper.create_working_copy(auto_clean=auto_clean)

    def close(self) -> None:
        self.scraper.close()


__all__ = ["ScrapingOrchestrator"]
