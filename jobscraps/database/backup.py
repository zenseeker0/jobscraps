import os
import json
import subprocess
import time
import logging
from datetime import datetime
import pandas as pd
from typing import Dict, List
from ..console_interface import console

SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger = logging.getLogger(__name__)


class BackupMixin:
    """Mixin providing PostgreSQL backup utilities."""

    db_config: 'DatabaseConfig'
    conn = None

    def create_backup(self, backup_type: str = "auto", reason: str = "") -> Dict:
        """Create a PostgreSQL backup using pg_dump."""
        backup_dir = os.path.join(SCRIPT_DIR, "backups", "DatabaseBackups")
        os.makedirs(backup_dir, exist_ok=True)
        conn_params = self.db_config.get_connection_params()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        reason_suffix = f"_{reason}" if reason else ""
        filename = f"jobscraps_{timestamp}_{backup_type}{reason_suffix}.sql.gz"
        backup_path = os.path.join(backup_dir, filename)
        cmd = [
            "pg_dump",
            "-h",
            conn_params["host"],
            "-p",
            str(conn_params["port"]),
            "-U",
            conn_params["user"],
            "-d",
            conn_params["database"],
            "--compress=9",
            "--verbose",
            "--file",
            backup_path,
        ]
        env = os.environ.copy()
        env["PGPASSWORD"] = conn_params["password"]
        logger.info("Creating backup: %s", filename)
        start_time = time.time()
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                result = subprocess.run(
                    cmd,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=300,
                )
                if result.returncode == 0:
                    backup_time = time.time() - start_time
                    file_size = os.path.getsize(backup_path)
                    file_size_mb = file_size / (1024 * 1024)
                    backup_info = {
                        "filename": filename,
                        "path": backup_path,
                        "size_bytes": file_size,
                        "size_mb": round(file_size_mb, 1),
                        "duration_seconds": round(backup_time, 1),
                        "timestamp": timestamp,
                        "reason": reason,
                        "backup_type": backup_type,
                    }
                    self._update_backup_manifest(backup_info)
                    logger.info(
                        "Backup created successfully: %s (%.1f MB in %.1fs)",
                        filename,
                        file_size_mb,
                        backup_time,
                    )
                    return backup_info
                error_msg = result.stderr or result.stdout
                logger.warning("pg_dump attempt %s failed: %s", attempt + 1, error_msg)
                if attempt < max_attempts - 1:
                    time.sleep(5)
            except subprocess.TimeoutExpired:
                logger.warning("pg_dump attempt %s timed out", attempt + 1)
                if attempt < max_attempts - 1:
                    time.sleep(5)
            except Exception as e:  # pylint: disable=broad-except
                logger.warning("pg_dump attempt %s error: %s", attempt + 1, e)
                if attempt < max_attempts - 1:
                    time.sleep(5)
        if os.path.exists(backup_path):
            os.remove(backup_path)
        raise Exception(f"pg_dump failed after {max_attempts} attempts")

    def _update_backup_manifest(self, backup_info: Dict) -> None:
        """Update the backup manifest file."""
        manifest_path = os.path.join(SCRIPT_DIR, "backups", "DatabaseBackups", "backup_manifest.json")
        try:
            if os.path.exists(manifest_path):
                with open(manifest_path, "r") as f:
                    manifest = json.load(f)
            else:
                manifest = {"backups": []}
            manifest["backups"].append(backup_info)
            manifest["total_backups"] = len(manifest["backups"])
            total_size = sum(b["size_bytes"] for b in manifest["backups"])
            manifest["total_size_gb"] = round(total_size / (1024 ** 3), 2)
            if manifest["backups"]:
                manifest["oldest_backup"] = min(b["timestamp"] for b in manifest["backups"])
                manifest["newest_backup"] = max(b["timestamp"] for b in manifest["backups"])
                manifest["last_updated"] = datetime.now().strftime("%Y%m%d_%H%M%S")
            with open(manifest_path, "w") as f:
                json.dump(manifest, f, indent=2)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("Failed to update backup manifest: %s", e)

    def manage_backup_retention(self) -> Dict:
        """Manage backup retention policy."""
        backup_dir = os.path.join(SCRIPT_DIR, "backups", "DatabaseBackups")
        manifest_path = os.path.join(backup_dir, "backup_manifest.json")
        try:
            if not os.path.exists(manifest_path):
                return {"action": "no_manifest", "message": "No backup manifest found"}
            with open(manifest_path, "r") as f:
                manifest = json.load(f)
            backups = manifest.get("backups", [])
            total_size_gb = manifest.get("total_size_gb", 0)
            needs_cleanup = len(backups) > 48 or total_size_gb > 4.8
            if not needs_cleanup:
                return {
                    "action": "no_cleanup_needed",
                    "total_backups": len(backups),
                    "total_size_gb": total_size_gb,
                }
            backups.sort(key=lambda x: x["timestamp"])
            target_count = 40
            to_remove = max(0, len(backups) - target_count)
            if total_size_gb > 4.8:
                while len(backups) > to_remove and total_size_gb > 4.5:
                    oldest_backup = backups[to_remove]
                    total_size_gb -= oldest_backup["size_bytes"] / (1024 ** 3)
                    to_remove += 1
            removed_backups = []
            for i in range(to_remove):
                backup = backups[i]
                backup_path = backup["path"]
                if os.path.exists(backup_path):
                    os.remove(backup_path)
                    removed_backups.append(backup["filename"])
                    logger.info("Removed old backup: %s", backup["filename"])
            manifest["backups"] = backups[to_remove:]
            manifest["total_backups"] = len(manifest["backups"])
            if manifest["backups"]:
                total_size = sum(b["size_bytes"] for b in manifest["backups"])
                manifest["total_size_gb"] = round(total_size / (1024 ** 3), 2)
                manifest["oldest_backup"] = min(b["timestamp"] for b in manifest["backups"])
                manifest["last_cleanup"] = datetime.now().strftime("%Y%m%d_%H%M%S")
            else:
                manifest["total_size_gb"] = 0
            with open(manifest_path, "w") as f:
                json.dump(manifest, f, indent=2)
            return {
                "action": "cleanup_performed",
                "removed_count": len(removed_backups),
                "removed_files": removed_backups,
                "remaining_backups": len(manifest["backups"]),
                "total_size_gb": manifest["total_size_gb"],
            }
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Backup retention management failed: %s", e)
            return {"action": "error", "message": str(e)}

    def list_backups(self) -> List[Dict]:
        """List available backups."""
        manifest_path = os.path.join(SCRIPT_DIR, "backups", "DatabaseBackups", "backup_manifest.json")
        try:
            if not os.path.exists(manifest_path):
                return []
            with open(manifest_path, "r") as f:
                manifest = json.load(f)
            backups = manifest.get("backups", [])
            backups.sort(key=lambda x: x["timestamp"], reverse=True)
            return backups
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to list backups: %s", e)
            return []

    def restore_backup(self, backup_filename: str) -> bool:
        """Restore database from backup file."""
        backup_path = os.path.join(SCRIPT_DIR, "backups", "DatabaseBackups", backup_filename)
        if not os.path.exists(backup_path):
            logger.error("Backup file not found: %s", backup_path)
            return False
        try:
            conn_params = self.db_config.get_connection_params()
            if self.conn and not self.conn.closed:
                self.conn.close()
            cmd = [
                "psql",
                "-h",
                conn_params["host"],
                "-p",
                str(conn_params["port"]),
                "-U",
                conn_params["user"],
                "-d",
                conn_params["database"],
                "-f",
                backup_path,
                "--quiet",
            ]
            env = os.environ.copy()
            env["PGPASSWORD"] = conn_params["password"]
            logger.info("Restoring from backup: %s", backup_filename)
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=600)
            if result.returncode == 0:
                logger.info("Database restored successfully from %s", backup_filename)
                self._connect_with_retry()
                return True
            logger.error("Restore failed: %s", result.stderr)
            return False
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Restore operation failed: %s", e)
            return False

    def test_backup(self, backup_filename: str) -> bool:
        """Test backup file integrity."""
        backup_path = os.path.join(SCRIPT_DIR, "backups", "DatabaseBackups", backup_filename)
        if not os.path.exists(backup_path):
            logger.error("Backup file not found: %s", backup_path)
            return False
        try:
            if backup_filename.endswith(".gz"):
                import gzip

                with gzip.open(backup_path, "rt") as f:
                    lines = [f.readline() for _ in range(10)]
                content = "".join(lines)
                if "PostgreSQL database dump" in content or "CREATE TABLE" in content:
                    logger.info("Backup file %s appears to be valid", backup_filename)
                    return True
                logger.error("Backup file %s does not appear to be a valid PostgreSQL dump", backup_filename)
                return False
            logger.error("Backup file %s is not a compressed file", backup_filename)
            return False
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Backup test failed: %s", e)
            return False

    def backup_and_reset(self) -> bool:
        """Create a backup of the database and clear all data."""
        try:
            backup_info = self.create_backup("manual", "backup_and_reset")
            console.print(f"✓ Database backup created: {backup_info['filename']} ({backup_info['size_mb']} MB)")
            backup_dir = os.path.join(SCRIPT_DIR, "backups")
            os.makedirs(backup_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            jobs_df = self.get_all_jobs()
            jobs_backup_path = os.path.join(backup_dir, f"scraped_jobs_{timestamp}.csv")
            jobs_df.to_csv(jobs_backup_path, index=False)
            logger.info("Jobs CSV backup created at %s", jobs_backup_path)
            search_df = pd.read_sql("SELECT * FROM search_history", self.conn)
            search_backup_path = os.path.join(backup_dir, f"search_history_{timestamp}.csv")
            search_df.to_csv(search_backup_path, index=False)
            logger.info("Search history CSV backup created at %s", search_backup_path)
            rows_deleted = self.clear_all_jobs()
            with self.conn.cursor() as cursor:
                cursor.execute("DELETE FROM search_history")
                search_rows_deleted = cursor.rowcount
                self.conn.commit()
            logger.info(
                "Database reset completed. Jobs: %s, Search history: %s",
                rows_deleted,
                search_rows_deleted,
            )
            return True
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Error during database backup and reset: %s", e)
            return False
