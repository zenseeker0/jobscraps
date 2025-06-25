from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from .database import JobDatabase
from .console_interface import console


class BackupManager:
    """Handle backup operations for JobScraps."""

    def __init__(self, db: JobDatabase) -> None:
        self.db = db

    def create_backup_with_prompt(self, reason: str) -> bool:
        """Create a backup with a prompt on failure."""
        if self.db.database_type != "production":
            console.print(f"Skipping backup (operating on {self.db.database_type} database)")
            return True
        try:
            console.print("Creating backup before operation on production database...")
            backup_info = self.db.create_backup("auto", reason)
            console.print(
                f"✓ Backup created: {backup_info['filename']} ({backup_info['size_mb']} MB in {backup_info['duration_seconds']}s)"
            )
            retention_result = self.db.manage_backup_retention()
            if retention_result["action"] == "cleanup_performed":
                console.print(
                    f"Backup retention: {retention_result['remaining_backups']} backups, {retention_result['total_size_gb']} GB"
                )
            return True
        except Exception as exc:  # pylint: disable=broad-except
            console.print(f"⚠️  Backup failed: {exc}")
            response = console.input("Continue with operation without backup? (y/n): ")
            if response.lower() != "y":
                console.print("Operation aborted for safety")
                return False
            console.print("Proceeding without backup...")
            return True

    def manual_backup(self) -> None:
        """Create a manual backup."""
        try:
            console.print("Creating manual backup...")
            backup_info = self.db.create_backup("manual", "manual")
            console.print(
                f"✓ Manual backup created: {backup_info['filename']} ({backup_info['size_mb']} MB)"
            )
            retention_result = self.db.manage_backup_retention()
            if retention_result["action"] == "cleanup_performed":
                console.print(
                    f"Backup retention: {retention_result['remaining_backups']} backups, {retention_result['total_size_gb']} GB"
                )
        except Exception as exc:  # pylint: disable=broad-except
            console.print(f"✗ Manual backup failed: {exc}")

    def list_backups(self) -> None:
        """List available backups."""
        backups = self.db.list_backups()
        if not backups:
            console.print("No backups found.")
            return
        console.print("\n=== AVAILABLE BACKUPS ===")
        console.print(f"{'Filename':<50} {'Size (MB)':<10} {'Created':<20} {'Reason'}")
        console.print("-" * 100)
        for backup in backups:
            created = datetime.strptime(backup["timestamp"], "%Y%m%d_%H%M%S").strftime("%Y-%m-%d %H:%M")
            reason = backup.get("reason", "unknown")
            console.print(
                f"{backup['filename']:<50} {backup['size_mb']:<10} {created:<20} {reason}"
            )
        total_size = sum(b["size_mb"] for b in backups)
        console.print(f"\nTotal: {len(backups)} backups, {total_size:.1f} MB")

    def restore_backup(self, backup_filename: str) -> None:
        """Restore from a backup file."""
        console.print(f"Restoring from backup: {backup_filename}")
        response = console.input("This will overwrite all current data. Are you sure? (y/n): ")
        if response.lower() != "y":
            console.print("Restore cancelled.")
            return
        success = self.db.restore_backup(backup_filename)
        if success:
            console.print(f"✓ Database restored successfully from {backup_filename}")
        else:
            console.print(f"✗ Restore failed from {backup_filename}")

    def test_backup(self, backup_filename: str) -> None:
        """Test backup file integrity."""
        console.print(f"Testing backup: {backup_filename}")
        is_valid = self.db.test_backup(backup_filename)
        if is_valid:
            console.print(f"✓ Backup {backup_filename} is valid")
        else:
            console.print(f"✗ Backup {backup_filename} is invalid or corrupted")

    def cleanup_backups(self) -> None:
        """Force cleanup of old backups."""
        console.print("Cleaning up old backups...")
        retention_result = self.db.manage_backup_retention()
        if retention_result["action"] == "cleanup_performed":
            console.print("✓ Cleanup completed:")
            console.print(f"  Removed {retention_result['removed_count']} old backups")
            console.print(
                f"  Remaining: {retention_result['remaining_backups']} backups ({retention_result['total_size_gb']} GB)"
            )
        elif retention_result["action"] == "no_cleanup_needed":
            console.print("✓ No cleanup needed:")
            console.print(
                f"  Current: {retention_result['total_backups']} backups ({retention_result['total_size_gb']} GB)"
            )
        else:
            console.print(f"✗ Cleanup failed: {retention_result.get('message', 'Unknown error')}")


__all__ = ["BackupManager"]
