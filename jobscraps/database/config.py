import os
import json
from typing import Dict

SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class DatabaseConfig:
    """Class to handle database configuration loading."""

    def __init__(self, config_path: str | None = None, database_type: str = "production"):
        """Initialize with configuration file path and database type."""
        if config_path is None:
            config_path = os.path.join(SCRIPT_DIR, "configs", "db", "db_config.json")
        self.config_path = config_path
        self.database_type = database_type
        self.config = self._load_config()

    def _load_config(self) -> Dict:
        """Load database configuration from file."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Database config file not found: {self.config_path}")
        with open(self.config_path, "r") as f:
            return json.load(f)

    def get_connection_params(self) -> Dict:
        """Get database connection parameters."""
        if self.database_type == "working" and "working_database" in self.config:
            db_config = self.config["working_database"]
        elif self.database_type == "production" and "production_database" in self.config:
            db_config = self.config["production_database"]
        elif "database" in self.config:
            db_config = self.config["database"]
        else:
            raise KeyError(f"No database configuration found for type: {self.database_type}")

        conn_config = self.config.get("connection", {})
        return {
            "host": db_config["host"],
            "port": db_config["port"],
            "database": db_config["database"],
            "user": db_config["username"],
            "password": db_config["password"],
            "connect_timeout": conn_config.get("connect_timeout", 30),
            "application_name": "jobscraps_scraper",
        }

    def get_retry_config(self) -> Dict:
        """Get retry configuration."""
        return self.config.get("connection", {})

