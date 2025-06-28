from __future__ import annotations

from typing import Optional

from .database import JobDatabase
from .console_interface import console


class BaseManager:
    """Base class providing shared helpers for managers."""

    def __init__(self, db: JobDatabase) -> None:
        self.db = db

    def check_production_safety(
        self, warning: str, prompt: str, extra: Optional[str] = None
    ) -> bool:
        """Display warnings and confirm before running on production."""
        if self.db.database_type != "production":
            return True
        console.print(warning)
        if extra:
            console.print(extra)
        console.print(
            "   Consider using --create-working-copy for data cleaning operations."
        )
        response = console.input(prompt)
        if response.lower() != "y":
            console.print("Operation cancelled for safety")
            return False
        return True


__all__ = ["BaseManager"]
