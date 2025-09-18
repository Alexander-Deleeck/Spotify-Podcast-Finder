"""Database helpers for the Spotify Podcast Finder application."""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

# The SQLite database is stored next to the project root by default. The
# location can be overridden by passing a different path when obtaining a
# connection.
_DEFAULT_DB_FILENAME = "podcast_finder.db"


def resolve_db_path(db_path: Optional[Union[str, Path]] = None) -> Path:
    """Return the resolved path to the SQLite database file."""
    if db_path is None:
        package_root = Path(__file__).resolve().parent
        return (package_root.parent / _DEFAULT_DB_FILENAME).resolve()
    if isinstance(db_path, Path):
        return db_path.expanduser().resolve()
    return Path(db_path).expanduser().resolve()


def get_connection(db_path: Optional[Union[str, Path]] = None) -> sqlite3.Connection:
    """Return a SQLite connection and ensure foreign keys are enabled."""
    resolved_path = resolve_db_path(db_path)
    # Streamlit can execute callbacks on different threads; allow cross-thread use.
    connection = sqlite3.connect(resolved_path, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    # Enable cascading behaviour for related tables.
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize_db(connection: sqlite3.Connection) -> None:
    """Create the tables used by the application if they do not exist."""
    cursor = connection.cursor()
    cursor.executescript(
        """
        CREATE TABLE IF NOT EXISTS search_queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            term TEXT NOT NULL,
            frequency TEXT NOT NULL,
            exclude_shows TEXT NOT NULL DEFAULT '[]',
            exclude_title_keywords TEXT NOT NULL DEFAULT '[]',
            exclude_description_keywords TEXT NOT NULL DEFAULT '[]',
            include_shows TEXT NOT NULL DEFAULT '[]',
            include_title_keywords TEXT NOT NULL DEFAULT '[]',
            include_description_keywords TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_run TEXT
        );

        CREATE TABLE IF NOT EXISTS episodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_id INTEGER NOT NULL,
            episode_id TEXT NOT NULL,
            name TEXT NOT NULL,
            show_name TEXT NOT NULL,
            release_date TEXT,
            description TEXT,
            external_url TEXT,
            uri TEXT,
            duration_ms INTEGER,
            raw_data TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            UNIQUE(query_id, episode_id),
            FOREIGN KEY(query_id) REFERENCES search_queries(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS search_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_id INTEGER NOT NULL,
            run_at TEXT NOT NULL,
            new_count INTEGER NOT NULL,
            total_results INTEGER NOT NULL,
            FOREIGN KEY(query_id) REFERENCES search_queries(id) ON DELETE CASCADE
        );
        """
    )
    connection.commit()

    # Backfill columns for existing installations (SQLite prior to new columns)
    # We use ALTER TABLE ADD COLUMN guarded by a simple existence check.
    def _has_column(table: str, column: str) -> bool:
        info = connection.execute(f"PRAGMA table_info({table})").fetchall()
        return any(row[1] == column for row in info)

    alter_statements = []
    if not _has_column("search_queries", "exclude_description_keywords"):
        alter_statements.append(
            "ALTER TABLE search_queries ADD COLUMN exclude_description_keywords TEXT NOT NULL DEFAULT '[]'"
        )
    if not _has_column("search_queries", "include_shows"):
        alter_statements.append(
            "ALTER TABLE search_queries ADD COLUMN include_shows TEXT NOT NULL DEFAULT '[]'"
        )
    if not _has_column("search_queries", "include_title_keywords"):
        alter_statements.append(
            "ALTER TABLE search_queries ADD COLUMN include_title_keywords TEXT NOT NULL DEFAULT '[]'"
        )
    if not _has_column("search_queries", "include_description_keywords"):
        alter_statements.append(
            "ALTER TABLE search_queries ADD COLUMN include_description_keywords TEXT NOT NULL DEFAULT '[]'"
        )

    for stmt in alter_statements:
        try:
            connection.execute(stmt)
        except sqlite3.OperationalError:
            # Ignore if the column already exists due to race or prior migration
            pass
    if alter_statements:
        connection.commit()


def utcnow_iso() -> str:
    """Return the current UTC timestamp formatted as ISO-8601 string."""
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
