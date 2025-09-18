from __future__ import annotations

import sqlite3
import streamlit as st

from spotify_podcast_finder.search_service import list_search_queries
from frontend.utils import (
    format_query_option,
    fetch_episode_rows,
)


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


