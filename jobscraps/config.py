"""Job search configuration utilities."""
from __future__ import annotations

import os
import json
import logging
from typing import Dict, List

from jsonschema import validate
from jsonschema.exceptions import ValidationError

SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class ConfigurationError(Exception):
    """Raised when a configuration file is invalid."""


CONFIG_SCHEMA: Dict = {
    "type": "object",
    "properties": {
        "jobs": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "enabled": {"type": "boolean"},
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "site_name": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "search_term": {"type": "string"},
                            "location": {"type": "string"},
                            "results_wanted": {"type": "integer"},
                            "hours_old": {"type": "integer"},
                            "country_indeed": {"type": "string"},
                        },
                        "required": ["site_name", "search_term", "location"],
                        "additionalProperties": True,
                    },
                },
                "required": ["name", "parameters"],
                "additionalProperties": True,
            },
        },
        "global": {
            "type": "object",
            "properties": {
                "description_format": {"type": "string"},
                "enforce_annual_salary": {"type": "boolean"},
                "verbose": {"type": "integer"},
            },
            "additionalProperties": True,
        },
    },
    "required": ["jobs", "global"],
}
logger = logging.getLogger(__name__)


class JobSearchConfig:
    """Handle job search configuration from a JSON file."""

    def __init__(self, config_path: str | None = None) -> None:
        """Initialize with path to configuration."""
        if config_path is None:
            config_path = os.path.join(SCRIPT_DIR, "configs", "search", "job_search_config.json")
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict:
        """Load configuration from file, creating defaults if needed."""
        config_dir = os.path.dirname(self.config_path)
        if config_dir and not os.path.exists(config_dir):
            os.makedirs(config_dir)
        if not os.path.exists(self.config_path):
            logger.warning(
                "Config file %s not found. Creating default configuration.",
                self.config_path,
            )
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
                    "verbose": 1,
                    "duplicate_preferences": {
                        "locations": ["Colorado"]
                    }
                }
            }
            with open(self.config_path, "w") as f:
                json.dump(default_config, f, indent=4)
            config_data = default_config
        else:
            with open(self.config_path, "r") as f:
                config_data = json.load(f)
        try:
            validate(instance=config_data, schema=CONFIG_SCHEMA)
        except ValidationError as exc:
            raise ConfigurationError(f"Invalid configuration: {exc.message}") from exc
        return config_data

    def get_job_configs(self) -> List[Dict]:
        """Return all enabled job configurations."""
        return [job for job in self.config.get("jobs", []) if job.get("enabled", True)]

    def get_global_params(self) -> Dict:
        """Return global parameters that apply to all searches."""
        return self.config.get("global", {})

__all__ = ["JobSearchConfig", "ConfigurationError"]
