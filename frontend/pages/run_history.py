from __future__ import annotations

import sqlite3
import streamlit as st

from spotify_podcast_finder.search_service import list_recent_runs


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


