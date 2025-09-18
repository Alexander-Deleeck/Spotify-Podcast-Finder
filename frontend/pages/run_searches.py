from __future__ import annotations

import sqlite3
import streamlit as st

from spotify_podcast_finder.spotify_api import SpotifyAuthError, SpotifyAPIError, SpotifyClient
from spotify_podcast_finder.search_service import list_search_queries, run_search

from frontend.utils import (
    get_due_queries,
    format_query_option,
    display_run_summary,
)


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
                # Refresh due state
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


