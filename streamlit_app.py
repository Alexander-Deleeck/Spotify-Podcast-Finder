"""Streamlit frontend for the Spotify Podcast Finder."""
from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Optional, Sequence, Tuple

import sqlite3

import streamlit as st

from spotify_podcast_finder.db import get_connection, initialize_db, resolve_db_path
from spotify_podcast_finder.models import Episode, SearchQuery, frequency_to_timedelta
from spotify_podcast_finder.search_service import (
    create_search_query,
    delete_search_query,
    list_recent_runs,
    list_search_queries,
    run_search,
    update_search_query,
)
from spotify_podcast_finder.spotify_api import SpotifyAuthError, SpotifyAPIError, SpotifyClient


# ---------------------------------------------------------------------------
# Cached resources and shared helpers
# ---------------------------------------------------------------------------


@st.cache_resource(show_spinner=False)
def _get_cached_connection(db_path: Optional[str]) -> sqlite3.Connection:
    connection = get_connection(db_path)
    initialize_db(connection)
    return connection


def get_connection_for_app(db_path: Optional[str]) -> sqlite3.Connection:
    """Return a cached SQLite connection for the provided database path."""

    normalized = db_path.strip() if isinstance(db_path, str) else None
    if normalized == "":
        normalized = None
    return _get_cached_connection(normalized)


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
    """Convert :class:`Episode` instances into rows suitable for a table."""

    rows: List[dict] = []
    for episode in episodes:
        rows.append(
            {
                "Episode": episode.name or "Unknown episode",
                "Show": episode.show_name or "Unknown show",
                "Release date": episode.formatted_release_date(),
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
        SELECT name, show_name, release_date, external_url, uri, first_seen_at, last_seen_at
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


def display_flash_message() -> None:
    """Show and clear any queued flash message from the session state."""

    flash = st.session_state.pop("flash", None)
    if not flash:
        return
    level, message = flash
    if level == "success":
        st.success(message)
    elif level == "error":
        st.error(message)
    else:
        st.info(message)


# ---------------------------------------------------------------------------
# Page sections
# ---------------------------------------------------------------------------


def render_manage_queries(connection: sqlite3.Connection) -> None:
    """Render the management interface for search queries."""

    queries = list_search_queries(connection)

    if queries:
        table_rows = []
        for query in queries:
            table_rows.append(
                {
                    "ID": query.id,
                    "Search term": query.term,
                    "Frequency": query.frequency,
                    "Last run": format_datetime_value(query.last_run),
                    "Next run": describe_next_run(query),
                    "Exclude shows": ", ".join(query.exclude_shows) or "—",
                    "Exclude title keywords": ", ".join(query.exclude_title_keywords) or "—",
                }
            )
        st.dataframe(table_rows, hide_index=True, width="stretch")
    else:
        st.info("No search queries stored yet. Use the form below to create one.")

    st.markdown("### Create a new search query")
    with st.form("create_query_form"):
        term = st.text_input("Search term", placeholder="Michael Levin")
        frequency = st.text_input(
            "Frequency",
            value="weekly",
            help="Examples: weekly, 14d, monthly. Leave empty to keep weekly.",
        )
        exclude_shows = st.text_area(
            "Shows to exclude",
            placeholder="Show names, one per line",
        )
        exclude_titles = st.text_area(
            "Exclude keywords in episode title",
            placeholder="Keywords, one per line",
        )
        submitted = st.form_submit_button("Create search query")
        if submitted:
            try:
                query = create_search_query(
                    connection,
                    term=term,
                    frequency=frequency or "weekly",
                    exclude_shows=parse_list_input(exclude_shows),
                    exclude_title_keywords=parse_list_input(exclude_titles),
                )
            except Exception as exc:  # pragma: no cover - surfacing errors in UI
                st.error(f"Unable to create search query: {exc}")
            else:
                st.session_state["flash"] = (
                    "success",
                    f"Created search query #{query.id} for '{query.term}'.",
                )
                st.experimental_rerun()

    if not queries:
        return

    st.markdown("### Edit existing queries")
    for query in queries:
        with st.expander(f"Query #{query.id}: {query.term}"):
            st.caption(
                f"Frequency: {query.frequency} · Last run: {format_datetime_value(query.last_run)} · "
                f"Next run: {describe_next_run(query)}"
            )
            with st.form(f"update_query_{query.id}"):
                term_value = st.text_input(
                    "Search term",
                    value=query.term,
                    key=f"term_{query.id}",
                )
                frequency_value = st.text_input(
                    "Frequency",
                    value=query.frequency,
                    key=f"frequency_{query.id}",
                )
                exclude_shows_value = st.text_area(
                    "Shows to exclude",
                    value=format_list_input(query.exclude_shows),
                    key=f"exclude_shows_{query.id}",
                )
                exclude_titles_value = st.text_area(
                    "Exclude keywords in title",
                    value=format_list_input(query.exclude_title_keywords),
                    key=f"exclude_titles_{query.id}",
                )
                update_submitted = st.form_submit_button("Save changes")
                if update_submitted:
                    try:
                        update_search_query(
                            connection,
                            query.id,
                            term=term_value,
                            frequency=frequency_value,
                            exclude_shows=parse_list_input(exclude_shows_value),
                            exclude_title_keywords=parse_list_input(exclude_titles_value),
                        )
                    except Exception as exc:  # pragma: no cover - surface in UI
                        st.error(f"Unable to update search query: {exc}")
                    else:
                        st.session_state["flash"] = (
                            "success",
                            f"Updated search query #{query.id}.",
                        )
                        st.experimental_rerun()

            delete_clicked = st.button(
                f"Delete query #{query.id}",
                key=f"delete_query_{query.id}",
                help="Removing a query also deletes its indexed episodes.",
            )
            if delete_clicked:
                try:
                    delete_search_query(connection, query.id)
                except Exception as exc:  # pragma: no cover - surface in UI
                    st.error(f"Unable to delete search query: {exc}")
                else:
                    st.session_state["flash"] = (
                        "success",
                        f"Deleted search query #{query.id}.",
                    )
                    st.experimental_rerun()


def render_run_searches(connection: sqlite3.Connection) -> None:
    """Render controls to execute Spotify searches."""

    queries = list_search_queries(connection)
    if not queries:
        st.info("Create a search query first in the *Manage queries* tab.")
        return

    due_queries = get_due_queries(queries)

    st.markdown("### Run all due queries")
    with st.form("run_due_form"):
        st.write(
            f"{len(due_queries)} search queries are currently due based on their stored frequency."
        )
        due_market = st.text_input("Market (optional)", key="due_market", placeholder="US")
        due_limit = st.number_input(
            "Items per API request",
            min_value=1,
            max_value=50,
            value=50,
            step=1,
            format="%d",
            key="due_limit",
        )
        due_max_pages_value = st.number_input(
            "Maximum pages to fetch (0 = no limit)",
            min_value=0,
            max_value=40,
            value=0,
            step=1,
            format="%d",
            key="due_max_pages",
        )
        run_due_submitted = st.form_submit_button("Run due queries")

    if run_due_submitted:
        if not due_queries:
            st.warning("No queries are currently due to run.")
        else:
            max_pages = int(due_max_pages_value) or None
            try:
                client = SpotifyClient()
            except SpotifyAuthError as exc:
                st.error(str(exc))
            else:
                try:
                    with st.spinner("Fetching results from Spotify..."):
                        for query in due_queries:
                            try:
                                summary = run_search(
                                    connection,
                                    query,
                                    client,
                                    market=due_market or None,
                                    limit=int(due_limit),
                                    max_pages=max_pages,
                                )
                            except SpotifyAPIError as exc:
                                st.error(f"Spotify API error while running '{query.term}': {exc}")
                            else:
                                display_run_summary(query, summary)
                finally:
                    client.close()
                queries = list_search_queries(connection)
                due_queries = get_due_queries(queries)

    st.divider()

    st.markdown("### Run a single query")
    with st.form("run_single_form"):
        selected_query = st.selectbox(
            "Search query",
            options=queries,
            format_func=format_query_option,
            key="single_query_select",
        )
        market = st.text_input("Market (optional)", key="single_market", placeholder="US")
        limit = st.number_input(
            "Items per API request",
            min_value=1,
            max_value=50,
            value=50,
            step=1,
            format="%d",
            key="single_limit",
        )
        max_pages_value = st.number_input(
            "Maximum pages to fetch (0 = no limit)",
            min_value=0,
            max_value=40,
            value=0,
            step=1,
            format="%d",
            key="single_max_pages",
        )
        run_single_submitted = st.form_submit_button("Run selected query")

    if run_single_submitted and selected_query:
        summary = None
        try:
            client = SpotifyClient()
        except SpotifyAuthError as exc:
            st.error(str(exc))
        else:
            try:
                with st.spinner(f"Searching Spotify for '{selected_query.term}'..."):
                    summary = run_search(
                        connection,
                        selected_query,
                        client,
                        market=market or None,
                        limit=int(limit),
                        max_pages=(int(max_pages_value) or None),
                    )
            except SpotifyAPIError as exc:
                st.error(f"Spotify API error: {exc}")
            finally:
                client.close()
        if summary:
            display_run_summary(selected_query, summary)


def render_episode_library(connection: sqlite3.Connection) -> None:
    """Render stored episode metadata for a selected query."""

    queries = list_search_queries(connection)
    if not queries:
        st.info("No search queries available yet. Create one to start indexing episodes.")
        return

    selected_query = st.selectbox(
        "Search query",
        options=queries,
        format_func=format_query_option,
        key="episodes_query_select",
    )
    limit = st.number_input(
        "Number of episodes to display",
        min_value=5,
        max_value=200,
        value=50,
        step=5,
        format="%d",
        key="episodes_limit",
    )
    order_options = {
        "Release date (newest first)": ("release_date", True),
        "Release date (oldest first)": ("release_date", False),
        "First seen (newest first)": ("first_seen_at", True),
        "First seen (oldest first)": ("first_seen_at", False),
        "Last seen (newest first)": ("last_seen_at", True),
        "Last seen (oldest first)": ("last_seen_at", False),
    }
    order_label = st.selectbox(
        "Order results by",
        options=list(order_options.keys()),
        key="episodes_order",
    )
    order_column, descending = order_options[order_label]
    rows, total = fetch_episode_rows(
        connection,
        selected_query.id,
        limit=int(limit),
        order_column=order_column,
        descending=descending,
    )
    if rows:
        st.caption(
            f"Showing {len(rows)} of {total} stored episodes for '{selected_query.term}'."
        )
        st.dataframe(rows, hide_index=True, width="stretch")
    else:
        st.info(
            "No episodes indexed yet for this query. Run the search to populate results."
        )


def render_run_history(connection: sqlite3.Connection) -> None:
    """Render a table with recent Spotify search runs."""

    limit = st.number_input(
        "Number of runs to display",
        min_value=5,
        max_value=100,
        value=20,
        step=5,
        format="%d",
        key="run_history_limit",
    )
    rows = list_recent_runs(connection, limit=int(limit))
    if not rows:
        st.info("No search runs recorded yet.")
        return
    table_rows = []
    for row in rows:
        table_rows.append(
            {
                "Run ID": row["id"],
                "Query": f"#{row['query_id']} – {row['term']}",
                "Run at": row["run_at"],
                "New episodes": row["new_count"],
                "Processed": row["total_results"],
            }
        )
    st.dataframe(table_rows, hide_index=True, width="stretch")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    st.set_page_config(page_title="Spotify Podcast Finder", layout="wide")

    st.title("Spotify Podcast Finder")
    st.caption("Monitor guest appearances across Spotify podcasts with scheduled searches.")

    display_flash_message()

    st.sidebar.markdown("### Database configuration")
    db_path_input = st.sidebar.text_input(
        "Database path",
        value=st.session_state.get("db_path_input", ""),
        help="Leave empty to use the default 'podcast_finder.db' file in the project root.",
    )
    st.session_state["db_path_input"] = db_path_input
    db_path = db_path_input.strip() or None
    connection = get_connection_for_app(db_path)
    resolved_db_path = resolve_db_path(db_path)
    st.sidebar.caption(f"Using database: `{resolved_db_path}`")

    st.sidebar.markdown("### Spotify credentials")
    st.sidebar.markdown(
        "Set the `SPOTIFY_CLIENT_ID` and `SPOTIFY_CLIENT_SECRET` environment variables "
        "before running Spotify searches."
    )

    tabs = st.tabs(["Run searches", "Manage queries", "Episodes", "Run history"])
    with tabs[0]:
        render_run_searches(connection)
    with tabs[1]:
        render_manage_queries(connection)
    with tabs[2]:
        render_episode_library(connection)
    with tabs[3]:
        render_run_history(connection)


if __name__ == "__main__":
    main()

