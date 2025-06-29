#!/usr/bin/env python3
"""Execute SQL migration files sequentially using psycopg2."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import psycopg2

from jobscraps.database.config import DatabaseConfig

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = PROJECT_ROOT / "configs" / "db" / "db_config.json"
MIGRATIONS_DIR = PROJECT_ROOT / "data" / "schemas" / "migrations"


def iter_migration_files() -> Iterable[Path]:
    """Yield migration SQL files in sorted order."""
    return sorted(MIGRATIONS_DIR.glob("*.sql"))


def apply_migration(conn: psycopg2.extensions.connection, path: Path) -> None:
    """Execute a single SQL migration file."""
    with conn.cursor() as cur, open(path, "r", encoding="utf-8") as sql_file:
        cur.execute(sql_file.read())
    conn.commit()


def run_migrations(config_path: Path, database_type: str) -> None:
    """Run all migrations against the selected database."""
    db_conf = DatabaseConfig(str(config_path), database_type)
    conn = psycopg2.connect(**db_conf.get_connection_params())

    try:
        for sql_file in iter_migration_files():
            print(f"\u25ba Applying {sql_file.name}...", flush=True)
            apply_migration(conn, sql_file)
        print("\u2705 All migrations applied")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run JobScraps DB migrations")
    parser.add_argument(
        "--config",
        "-c",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Path to db_config.json",
    )
    parser.add_argument(
        "--database-type",
        "-d",
        choices=["production", "working"],
        default="production",
        help="Which database section to use from config",
    )
    args = parser.parse_args()

    run_migrations(args.config, args.database_type)


if __name__ == "__main__":
    main()
