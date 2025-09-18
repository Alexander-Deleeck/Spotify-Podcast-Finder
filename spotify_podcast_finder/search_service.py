"""Business logic for managing search queries and episode indexing."""
from __future__ import annotations

import json
import re
import fnmatch
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
    row_keys = set(row.keys())

    def _opt(name: str) -> Optional[str]:
        return row[name] if name in row_keys else None

    return SearchQuery(
        id=row["id"],
        term=row["term"],
        frequency=row["frequency"],
        exclude_shows=_deserialize_list(row["exclude_shows"]),
        exclude_title_keywords=_deserialize_list(row["exclude_title_keywords"]),
        exclude_description_keywords=_deserialize_list(_opt("exclude_description_keywords")),
        include_shows=_deserialize_list(_opt("include_shows")),
        include_title_keywords=_deserialize_list(_opt("include_title_keywords")),
        include_description_keywords=_deserialize_list(_opt("include_description_keywords")),
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
    exclude_description_keywords: Optional[Sequence[str]] = None,
    include_shows: Optional[Sequence[str]] = None,
    include_title_keywords: Optional[Sequence[str]] = None,
    include_description_keywords: Optional[Sequence[str]] = None,
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
        "exclude_description_keywords": _serialize_list(exclude_description_keywords),
        "include_shows": _serialize_list(include_shows),
        "include_title_keywords": _serialize_list(include_title_keywords),
        "include_description_keywords": _serialize_list(include_description_keywords),
        "created_at": timestamp,
        "updated_at": timestamp,
        "last_run": None,
    }
    cursor = connection.execute(
        """
        INSERT INTO search_queries (
            term,
            frequency,
            exclude_shows,
            exclude_title_keywords,
            exclude_description_keywords,
            include_shows,
            include_title_keywords,
            include_description_keywords,
            created_at,
            updated_at,
            last_run
        )
        VALUES (
            :term,
            :frequency,
            :exclude_shows,
            :exclude_title_keywords,
            :exclude_description_keywords,
            :include_shows,
            :include_title_keywords,
            :include_description_keywords,
            :created_at,
            :updated_at,
            :last_run
        )
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
    exclude_description_keywords: Optional[Sequence[str]] = None,
    include_shows: Optional[Sequence[str]] = None,
    include_title_keywords: Optional[Sequence[str]] = None,
    include_description_keywords: Optional[Sequence[str]] = None,
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
    if exclude_description_keywords is not None:
        new_exclude_desc = _serialize_list(exclude_description_keywords)
    else:
        new_exclude_desc = _serialize_list(query.exclude_description_keywords)
    new_include_shows = _serialize_list(include_shows) if include_shows is not None else _serialize_list(query.include_shows)
    if include_title_keywords is not None:
        new_include_titles = _serialize_list(include_title_keywords)
    else:
        new_include_titles = _serialize_list(query.include_title_keywords)
    if include_description_keywords is not None:
        new_include_desc = _serialize_list(include_description_keywords)
    else:
        new_include_desc = _serialize_list(query.include_description_keywords)

    payload = {
        "term": new_term,
        "frequency": new_frequency,
        "exclude_shows": new_exclude_shows,
        "exclude_title_keywords": new_exclude_titles,
        "exclude_description_keywords": new_exclude_desc,
        "include_shows": new_include_shows,
        "include_title_keywords": new_include_titles,
        "include_description_keywords": new_include_desc,
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
            exclude_description_keywords = :exclude_description_keywords,
            include_shows = :include_shows,
            include_title_keywords = :include_title_keywords,
            include_description_keywords = :include_description_keywords,
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

    # Compile inclusion and exclusion patterns (supports glob wildcards and /regex/)
    def _has_wildcards(pattern: str) -> bool:
        return any(ch in pattern for ch in ("*", "?", "["))

    def _compile_regex(pattern: str) -> Optional[re.Pattern]:
        if len(pattern) >= 2 and pattern.startswith("/") and pattern.endswith("/"):
            try:
                return re.compile(pattern[1:-1], flags=re.IGNORECASE)
            except re.error:
                return None
        return None

    show_exact_matches = set()
    show_globs: List[str] = []
    show_regexes: List[re.Pattern] = []
    for raw in query.exclude_shows:
        pat = (raw or "").strip()
        if not pat:
            continue
        rx = _compile_regex(pat)
        if rx is not None:
            show_regexes.append(rx)
        elif _has_wildcards(pat):
            show_globs.append(pat.lower())
        else:
            show_exact_matches.add(pat.lower())

    title_substrings: List[str] = []
    title_globs: List[str] = []
    title_regexes: List[re.Pattern] = []
    for raw in query.exclude_title_keywords:
        pat = (raw or "").strip()
        if not pat:
            continue
        rx = _compile_regex(pat)
        if rx is not None:
            title_regexes.append(rx)
        elif _has_wildcards(pat):
            title_globs.append(pat.lower())
        else:
            # Backwards compatible: plain text acts as a substring keyword
            title_substrings.append(pat.lower())

    # Exclude description patterns
    desc_substrings_ex: List[str] = []
    desc_globs_ex: List[str] = []
    desc_regexes_ex: List[re.Pattern] = []
    for raw in getattr(query, "exclude_description_keywords", []) or []:
        pat = (raw or "").strip()
        if not pat:
            continue
        rx = _compile_regex(pat)
        if rx is not None:
            desc_regexes_ex.append(rx)
        elif _has_wildcards(pat):
            desc_globs_ex.append(pat.lower())
        else:
            desc_substrings_ex.append(pat.lower())

    # Include show patterns
    include_show_exact = set()
    include_show_globs: List[str] = []
    include_show_regexes: List[re.Pattern] = []
    for raw in getattr(query, "include_shows", []) or []:
        pat = (raw or "").strip()
        if not pat:
            continue
        rx = _compile_regex(pat)
        if rx is not None:
            include_show_regexes.append(rx)
        elif _has_wildcards(pat):
            include_show_globs.append(pat.lower())
        else:
            include_show_exact.add(pat.lower())

    # Include title patterns
    include_title_substrings: List[str] = []
    include_title_globs: List[str] = []
    include_title_regexes: List[re.Pattern] = []
    for raw in getattr(query, "include_title_keywords", []) or []:
        pat = (raw or "").strip()
        if not pat:
            continue
        rx = _compile_regex(pat)
        if rx is not None:
            include_title_regexes.append(rx)
        elif _has_wildcards(pat):
            include_title_globs.append(pat.lower())
        else:
            include_title_substrings.append(pat.lower())

    # Include description patterns
    include_desc_substrings: List[str] = []
    include_desc_globs: List[str] = []
    include_desc_regexes: List[re.Pattern] = []
    for raw in getattr(query, "include_description_keywords", []) or []:
        pat = (raw or "").strip()
        if not pat:
            continue
        rx = _compile_regex(pat)
        if rx is not None:
            include_desc_regexes.append(rx)
        elif _has_wildcards(pat):
            include_desc_globs.append(pat.lower())
        else:
            include_desc_substrings.append(pat.lower())

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
        description_text = metadata.get("description") or ""
        show_lower = show_name.lower()
        title_lower = episode_title.lower()
        desc_lower = description_text.lower()

        # Show exclusion:
        # - Exact match if provided without wildcard/regex
        # - Glob wildcards (* ? []) supported
        # - Regex supported via /pattern/
        show_skip = False
        if show_exact_matches and show_lower in show_exact_matches:
            show_skip = True
        if not show_skip and show_globs and any(fnmatch.fnmatch(show_lower, glob) for glob in show_globs):
            show_skip = True
        if not show_skip and show_regexes and any(rx.search(show_name) for rx in show_regexes):
            show_skip = True
        if show_skip:
            skipped += 1
            continue

        # Title exclusion:
        # - Plain keywords act as substrings (back-compat)
        # - Glob wildcards and /regex/ supported
        title_skip = False
        if title_substrings and any(substr in title_lower for substr in title_substrings):
            title_skip = True
        if not title_skip and title_globs and any(fnmatch.fnmatch(title_lower, glob) for glob in title_globs):
            title_skip = True
        if not title_skip and title_regexes and any(rx.search(episode_title) for rx in title_regexes):
            title_skip = True
        if title_skip:
            skipped += 1
            continue

        # Description exclusion:
        desc_skip = False
        if desc_substrings_ex and any(substr in desc_lower for substr in desc_substrings_ex):
            desc_skip = True
        if not desc_skip and desc_globs_ex and any(fnmatch.fnmatch(desc_lower, glob) for glob in desc_globs_ex):
            desc_skip = True
        if not desc_skip and desc_regexes_ex and any(rx.search(description_text) for rx in desc_regexes_ex):
            desc_skip = True
        if desc_skip:
            skipped += 1
            continue

        # Include filters: if any include list is provided, it must match.
        # Show include
        if include_show_exact or include_show_globs or include_show_regexes:
            match = False
            if include_show_exact and show_lower in include_show_exact:
                match = True
            if not match and include_show_globs and any(fnmatch.fnmatch(show_lower, glob) for glob in include_show_globs):
                match = True
            if not match and include_show_regexes and any(rx.search(show_name) for rx in include_show_regexes):
                match = True
            if not match:
                skipped += 1
                continue

        # Title include
        if include_title_substrings or include_title_globs or include_title_regexes:
            match = False
            if include_title_substrings and any(substr in title_lower for substr in include_title_substrings):
                match = True
            if not match and include_title_globs and any(fnmatch.fnmatch(title_lower, glob) for glob in include_title_globs):
                match = True
            if not match and include_title_regexes and any(rx.search(episode_title) for rx in include_title_regexes):
                match = True
            if not match:
                skipped += 1
                continue

        # Description include
        if include_desc_substrings or include_desc_globs or include_desc_regexes:
            match = False
            if include_desc_substrings and any(substr in desc_lower for substr in include_desc_substrings):
                match = True
            if not match and include_desc_globs and any(fnmatch.fnmatch(desc_lower, glob) for glob in include_desc_globs):
                match = True
            if not match and include_desc_regexes and any(rx.search(description_text) for rx in include_desc_regexes):
                match = True
            if not match:
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
