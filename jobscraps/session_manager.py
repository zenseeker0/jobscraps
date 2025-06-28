from __future__ import annotations

import logging
from typing import Optional

from .database.core import JobDatabase

logger = logging.getLogger(__name__)


class SessionManager:
    """Context manager to finalize scraping sessions."""

    def __init__(self, db: JobDatabase, session_id: int) -> None:
        self.db = db
        self.session_id = session_id

    def __enter__(self) -> int:
        return self.session_id

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[object],
    ) -> None:
        status = "error" if exc_type else "completed"
        try:
            self.db.end_session(self.session_id, status)
        except Exception as err:  # pylint: disable=broad-except
            logger.error("Failed to end session %s: %s", self.session_id, err)
        return False

