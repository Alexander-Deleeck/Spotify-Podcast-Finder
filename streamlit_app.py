"""Streamlit app launcher delegating to modular frontend package."""
from __future__ import annotations

import streamlit as st

from spotify_podcast_finder.db import resolve_db_path
from frontend import (
    get_connection_for_app,
    display_flash_message,
    render_manage_queries,
    render_run_searches,
    render_episode_library,
    render_run_history,
)


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
    db_path = (db_path_input or "").strip() or None
    connection = get_connection_for_app(db_path)
    resolved_db_path = resolve_db_path(db_path)
    st.sidebar.caption(f"Using database: `{resolved_db_path}`")

    st.sidebar.markdown("### Spotify credentials")
    st.sidebar.markdown(
        "Set the `SPOTIFY_CLIENT_ID` and `SPOTIFY_CLIENT_SECRET` environment variables before running Spotify searches."
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


