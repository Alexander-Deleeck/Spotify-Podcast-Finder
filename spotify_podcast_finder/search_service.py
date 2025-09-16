"""Business logic for managing search queries and episode indexing."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Iterable, List, Optional, Sequence

from .db import initialize_db, utcnow_iso
from .models import Episode, SearchQuery, ensure_list
from .spotify_api import SpotifyClient, extract_episode_metadata


# ---------------------------------------------------------------------------
# Helpers to serialise and deserialise database fields
# ---------------------------------------------------------------------------

def _deserialize_datetime(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1]
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _serialize_list(values: Optional[Iterable[str]]) -> str:
    if not values:
        return "[]"
    cleaned = [str(item).strip() for item in values if str(item).strip()]
    return json.dumps(cleaned)


def _deserialize_list(value: Optional[str]) -> List[str]:
    if value is None:
        return []
    text = value.strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return ensure_list(value)
    if isinstance(parsed, list):
        return [str(item).strip() for item in parsed if str(item).strip()]
    if isinstance(parsed, str):
        parsed_text = parsed.strip()
        return [parsed_text] if parsed_text else []
    return []


def _row_to_search_query(row: sqlite3.Row) -> SearchQuery:
    return SearchQuery(
        id=row["id"],
        term=row["term"],
        frequency=row["frequency"],
        exclude_shows=_deserialize_list(row["exclude_shows"]),
        exclude_title_keywords=_deserialize_list(row["exclude_title_keywords"]),
        created_at=_deserialize_datetime(row["created_at"]),
        updated_at=_deserialize_datetime(row["updated_at"]),
        last_run=_deserialize_datetime(row["last_run"]),
    )


# ---------------------------------------------------------------------------
# Search query management
# ---------------------------------------------------------------------------

def create_search_query(
    connection: sqlite3.Connection,
    *,
    term: str,
    frequency: str = "weekly",
    exclude_shows: Optional[Sequence[str]] = None,
    exclude_title_keywords: Optional[Sequence[str]] = None,
) -> SearchQuery:
    """Insert a search query into the database and return it."""
    if not term or not term.strip():
        raise ValueError("term must be a non-empty string")

    initialize_db(connection)
    timestamp = utcnow_iso()
    payload = {
        "term": term.strip(),
        "frequency": frequency.strip() if frequency else "weekly",
        "exclude_shows": _serialize_list(exclude_shows),
        "exclude_title_keywords": _serialize_list(exclude_title_keywords),
        "created_at": timestamp,
        "updated_at": timestamp,
        "last_run": None,
    }
    cursor = connection.execute(
        """
        INSERT INTO search_queries (term, frequency, exclude_shows, exclude_title_keywords, created_at, updated_at, last_run)
        VALUES (:term, :frequency, :exclude_shows, :exclude_title_keywords, :created_at, :updated_at, :last_run)
        """,
        payload,
    )
    connection.commit()
    query_id = cursor.lastrowid
    return get_search_query(connection, query_id)


def get_search_query(connection: sqlite3.Connection, query_id: int) -> SearchQuery:
    cursor = connection.execute(
        "SELECT * FROM search_queries WHERE id = ?",
        (query_id,),
    )
    row = cursor.fetchone()
    if row is None:
        raise LookupError(f"No search query found with id {query_id}")
    return _row_to_search_query(row)


def find_query_by_term(connection: sqlite3.Connection, term: str) -> Optional[SearchQuery]:
    cursor = connection.execute(
        "SELECT * FROM search_queries WHERE term = ? COLLATE NOCASE",
        (term,),
    )
    row = cursor.fetchone()
    return _row_to_search_query(row) if row else None


def list_search_queries(connection: sqlite3.Connection) -> List[SearchQuery]:
    initialize_db(connection)
    cursor = connection.execute(
        "SELECT * FROM search_queries ORDER BY id ASC"
    )
    return [_row_to_search_query(row) for row in cursor.fetchall()]


def update_search_query(
    connection: sqlite3.Connection,
    query_id: int,
    *,
    term: Optional[str] = None,
    frequency: Optional[str] = None,
    exclude_shows: Optional[Sequence[str]] = None,
    exclude_title_keywords: Optional[Sequence[str]] = None,
) -> SearchQuery:
    initialize_db(connection)
    query = get_search_query(connection, query_id)

    new_term = term.strip() if term else query.term
    new_frequency = frequency.strip() if frequency else query.frequency
    new_exclude_shows = _serialize_list(exclude_shows) if exclude_shows is not None else _serialize_list(query.exclude_shows)
    if exclude_title_keywords is not None:
        new_exclude_titles = _serialize_list(exclude_title_keywords)
    else:
        new_exclude_titles = _serialize_list(query.exclude_title_keywords)

    payload = {
        "term": new_term,
        "frequency": new_frequency,
        "exclude_shows": new_exclude_shows,
        "exclude_title_keywords": new_exclude_titles,
        "updated_at": utcnow_iso(),
        "id": query_id,
    }
    connection.execute(
        """
        UPDATE search_queries
        SET term = :term,
            frequency = :frequency,
            exclude_shows = :exclude_shows,
            exclude_title_keywords = :exclude_title_keywords,
            updated_at = :updated_at
        WHERE id = :id
        """,
        payload,
    )
    connection.commit()
    return get_search_query(connection, query_id)


def delete_search_query(connection: sqlite3.Connection, query_id: int) -> None:
    initialize_db(connection)
    connection.execute("DELETE FROM search_queries WHERE id = ?", (query_id,))
    connection.commit()


# ---------------------------------------------------------------------------
# Episode indexing
# ---------------------------------------------------------------------------

def _count_episodes_for_query(connection: sqlite3.Connection, query_id: int) -> int:
    cursor = connection.execute(
        "SELECT COUNT(*) FROM episodes WHERE query_id = ?",
        (query_id,),
    )
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def run_search(
    connection: sqlite3.Connection,
    query: SearchQuery,
    spotify_client: SpotifyClient,
    *,
    market: Optional[str] = None,
    limit: int = 50,
    max_pages: Optional[int] = None,
) -> dict:
    """Execute the Spotify search for the provided query and persist results."""
    initialize_db(connection)
    previous_count = _count_episodes_for_query(connection, query.id)
    now_iso = utcnow_iso()

    exclude_shows_lower = {text.lower() for text in query.exclude_shows}
    exclude_title_keywords_lower = [text.lower() for text in query.exclude_title_keywords]

    processed = 0
    skipped = 0
    new_episodes: List[Episode] = []

    # First pass: collect episode IDs from the search results
    candidate_ids: List[str] = []
    simplified_items: List[dict] = []
    for raw_episode in spotify_client.search_episodes(
        query.term,
        market=market,
        limit=limit,
        max_pages=max_pages,
    ):
        simplified_items.append(raw_episode)
        episode_id = raw_episode.get("id")
        if episode_id:
            candidate_ids.append(episode_id)

    # Fetch full episode details in batch to ensure show info is populated
    full_items_by_id = {item.get("id"): item for item in spotify_client.get_episodes(candidate_ids, market=market)}

    for raw_episode in simplified_items:
        # Prefer full item when available, fallback to simplified
        episode_id = raw_episode.get("id")
        full_item = full_items_by_id.get(episode_id) if episode_id else None
        source = full_item or raw_episode
        metadata = extract_episode_metadata(source)
        episode_id = metadata.get("episode_id")
        if not episode_id:
            continue

        # Ensure we never insert NULL into NOT NULL columns (name, show_name)
        show_name = metadata.get("show_name") or ""
        episode_title = metadata.get("name") or ""
        show_lower = show_name.lower()
        title_lower = episode_title.lower()

        if exclude_shows_lower and show_lower in exclude_shows_lower:
            skipped += 1
            continue
        if exclude_title_keywords_lower and any(keyword in title_lower for keyword in exclude_title_keywords_lower):
            skipped += 1
            continue

        processed += 1
        existing_row = connection.execute(
            "SELECT id FROM episodes WHERE query_id = ? AND episode_id = ?",
            (query.id, episode_id),
        ).fetchone()

        if existing_row:
            connection.execute(
                """
                UPDATE episodes
                SET name = ?,
                    show_name = ?,
                    release_date = ?,
                    description = ?,
                    external_url = ?,
                    uri = ?,
                    duration_ms = ?,
                    raw_data = ?,
                    last_seen_at = ?
                WHERE id = ?
                """,
                (
                    episode_title,
                    show_name,
                    metadata.get("release_date"),
                    metadata.get("description"),
                    metadata.get("external_url"),
                    metadata.get("uri"),
                    metadata.get("duration_ms"),
                    json.dumps(metadata.get("raw", {})),
                    now_iso,
                    existing_row["id"],
                ),
            )
        else:
            connection.execute(
                """
                INSERT INTO episodes (
                    query_id,
                    episode_id,
                    name,
                    show_name,
                    release_date,
                    description,
                    external_url,
                    uri,
                    duration_ms,
                    raw_data,
                    first_seen_at,
                    last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    query.id,
                    metadata.get("episode_id"),
                    episode_title,
                    show_name,
                    metadata.get("release_date"),
                    metadata.get("description"),
                    metadata.get("external_url"),
                    metadata.get("uri"),
                    metadata.get("duration_ms"),
                    json.dumps(metadata.get("raw", {})),
                    now_iso,
                    now_iso,
                ),
            )
            new_episode = Episode(
                episode_id=metadata.get("episode_id"),
                name=episode_title,
                show_name=show_name,
                release_date=metadata.get("release_date"),
                description=metadata.get("description"),
                external_url=metadata.get("external_url"),
                uri=metadata.get("uri"),
                duration_ms=metadata.get("duration_ms"),
                raw=metadata.get("raw", {}),
            )
            new_episodes.append(new_episode)

    connection.execute(
        """
        INSERT INTO search_runs (query_id, run_at, new_count, total_results)
        VALUES (?, ?, ?, ?)
        """,
        (
            query.id,
            now_iso,
            len(new_episodes),
            processed,
        ),
    )
    connection.execute(
        "UPDATE search_queries SET last_run = ?, updated_at = ? WHERE id = ?",
        (now_iso, now_iso, query.id),
    )
    connection.commit()

    current_total = _count_episodes_for_query(connection, query.id)
    return {
        "previous_count": previous_count,
        "current_count": current_total,
        "new_episodes": new_episodes,
        "processed": processed,
        "skipped": skipped,
        "run_timestamp": now_iso,
    }


def list_recent_runs(connection: sqlite3.Connection, limit: int = 20) -> List[sqlite3.Row]:
    initialize_db(connection)
    cursor = connection.execute(
        """
        SELECT r.id, r.query_id, q.term, r.run_at, r.new_count, r.total_results
        FROM search_runs r
        JOIN search_queries q ON q.id = r.query_id
        ORDER BY r.run_at DESC
        LIMIT ?
        """,
        (limit,),
    )
    return cursor.fetchall()
