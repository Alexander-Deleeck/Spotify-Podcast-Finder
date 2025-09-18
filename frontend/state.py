from __future__ import annotations

import sqlite3
import streamlit as st

from typing import Optional
from spotify_podcast_finder.db import get_connection, initialize_db


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


