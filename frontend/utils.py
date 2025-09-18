from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Optional, Sequence, Tuple

import sqlite3
import streamlit as st

from spotify_podcast_finder.models import Episode, SearchQuery, frequency_to_timedelta


def parse_list_input(text: str) -> List[str]:
    """Convert multiline or comma-separated text into a list of strings."""

    if not text:
        return []
    tokens: List[str] = []
    for raw_line in text.replace("\r", "\n").split("\n"):
        for part in raw_line.split(","):
            item = part.strip()
            if item:
                tokens.append(item)
    return tokens


def format_list_input(values: Sequence[str]) -> str:
    """Return a newline separated representation for editing list inputs."""

    if not values:
        return ""
    return "\n".join(values)


def format_datetime_value(dt: Optional[datetime]) -> str:
    """Return a human-friendly representation of a datetime value."""

    if not dt:
        return "Never"
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def describe_next_run(query: SearchQuery) -> str:
    """Return a description for when a query is next due to run."""

    if query.last_run is None:
        return "Not run yet"
    delta = frequency_to_timedelta(query.frequency)
    if delta is None:
        return "Manual schedule"
    next_due = query.last_run + delta
    return format_datetime_value(next_due)


def markdown_escape(text: str) -> str:
    """Escape characters that have special meaning in Markdown."""

    return (
        text.replace("\\", "\\\\")
        .replace("`", "\\`")
        .replace("*", "\\*")
        .replace("_", "\\_")
        .replace("[", "\\[")
        .replace("]", "\\]")
        .replace("(", "\\(")
        .replace(")", "\\)")
    )


def format_query_option(query: SearchQuery) -> str:
    return f"#{query.id} – {query.term}"


def get_due_queries(queries: Iterable[SearchQuery]) -> List[SearchQuery]:
    """Return a list of queries whose schedule is currently due."""

    now = datetime.utcnow()
    due: List[SearchQuery] = []
    for query in queries:
        delta = frequency_to_timedelta(query.frequency)
        if delta is None or query.last_run is None:
            due.append(query)
            continue
        if query.last_run + delta <= now:
            due.append(query)
    return due


def episodes_to_table_rows(episodes: Sequence[Episode]) -> List[dict]:
    """Convert Episode instances into rows suitable for a table."""

    rows: List[dict] = []
    for episode in episodes:
        rows.append(
            {
                "Episode": episode.name or "Unknown episode",
                "Show": episode.show_name or "Unknown show",
                "Release date": episode.formatted_release_date(),
                "Description": (episode.description or "")[:300],
                "Link": episode.external_url or episode.uri or "",
            }
        )
    return rows


def fetch_episode_rows(
    connection: sqlite3.Connection,
    query_id: int,
    *,
    limit: int,
    order_column: str,
    descending: bool,
) -> Tuple[List[dict], int]:
    """Return stored episode metadata for display in the frontend."""

    valid_columns = {"release_date", "first_seen_at", "last_seen_at"}
    column = order_column if order_column in valid_columns else "release_date"
    direction = "DESC" if descending else "ASC"
    cursor = connection.execute(
        f"""
        SELECT name, show_name, release_date, description, external_url, uri, first_seen_at, last_seen_at
        FROM episodes
        WHERE query_id = ?
        ORDER BY {column} {direction}
        LIMIT ?
        """,
        (query_id, limit),
    )
    rows = cursor.fetchall()
    count_cursor = connection.execute(
        "SELECT COUNT(*) FROM episodes WHERE query_id = ?",
        (query_id,),
    )
    total = int(count_cursor.fetchone()[0]) if rows is not None else 0
    table_rows: List[dict] = []
    for row in rows:
        table_rows.append(
            {
                "Episode": row["name"] or "Unknown episode",
                "Show": row["show_name"] or "Unknown show",
                "Release date": row["release_date"] or "Unknown",
                "First seen": row["first_seen_at"],
                "Last seen": row["last_seen_at"],
                "Description": (row["description"] or "")[:300],
                "Link": row["external_url"] or row["uri"] or "",
            }
        )
    return table_rows, total


def display_run_summary(query: SearchQuery, summary: dict) -> None:
    """Render a summary of a Spotify search run."""

    previous = summary.get("previous_count", 0)
    processed = summary.get("processed", 0)
    skipped = summary.get("skipped", 0)
    new_episodes: Sequence[Episode] = summary.get("new_episodes", [])
    total_count = summary.get("current_count", 0)
    run_timestamp = summary.get("run_timestamp")

    if previous == 0:
        st.info(
            f"Indexed {processed} baseline episodes for **{markdown_escape(query.term)}**. "
            "Future runs will highlight newly released episodes."
        )
    elif new_episodes:
        st.success(
            f"Found {len(new_episodes)} new episodes for **{markdown_escape(query.term)}**."
        )
        rows = episodes_to_table_rows(new_episodes)
        st.dataframe(rows, hide_index=True, width="stretch")
    else:
        st.info(
            f"No new episodes found for **{markdown_escape(query.term)}**."
        )

    caption_parts = [f"Processed {processed} episodes", f"skipped {skipped} via filters"]
    caption_parts.append(f"{total_count} episodes stored in total")
    if run_timestamp:
        caption_parts.append(f"run at {run_timestamp}")
    st.caption("; ".join(caption_parts))


