from __future__ import annotations

import sqlite3
import streamlit as st

from spotify_podcast_finder.search_service import (
    create_search_query,
    delete_search_query,
    list_search_queries,
    update_search_query,
)
from frontend.utils import (
    parse_list_input,
    format_list_input,
    format_datetime_value,
    describe_next_run,
)


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
                    "Exclude title": ", ".join(query.exclude_title_keywords) or "—",
                    "Exclude description": ", ".join(getattr(query, "exclude_description_keywords", []) or []) or "—",
                    "Include shows": ", ".join(getattr(query, "include_shows", []) or []) or "—",
                    "Include title": ", ".join(getattr(query, "include_title_keywords", []) or []) or "—",
                    "Include description": ", ".join(getattr(query, "include_description_keywords", []) or []) or "—",
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
            "Exclude shows",
            placeholder="Show names or patterns, one per line (supports * ? [] or /regex/)",
            help="Examples: 'The * Show', '/^Joe Rogan.*$/'",
        )
        exclude_titles = st.text_area(
            "Exclude title patterns",
            placeholder="Plain keywords (substring), glob (* ? []), or /regex/; one per line",
            help="Examples: '*bonus*', '/\\bRecap\\b/i'",
        )
        exclude_desc = st.text_area(
            "Exclude description patterns (optional)",
            placeholder="Plain keywords, glob (* ? []), or /regex/; one per line",
        )
        st.markdown("Optional include patterns: at least one must match if provided.")
        include_shows = st.text_area(
            "Include shows",
            placeholder="Only include shows matching these patterns",
        )
        include_titles = st.text_area(
            "Include title patterns",
            placeholder="Only include titles matching these patterns",
        )
        include_desc = st.text_area(
            "Include description patterns",
            placeholder="Only include descriptions matching these patterns",
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
                    exclude_description_keywords=parse_list_input(exclude_desc),
                    include_shows=parse_list_input(include_shows),
                    include_title_keywords=parse_list_input(include_titles),
                    include_description_keywords=parse_list_input(include_desc),
                )
            except Exception as exc:  # pragma: no cover - surfacing errors in UI
                st.error(f"Unable to create search query: {exc}")
            else:
                st.session_state["flash"] = (
                    "success",
                    f"Created search query #{query.id} for '{query.term}'.",
                )
                st.rerun()

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
                    "Exclude shows",
                    value=format_list_input(query.exclude_shows),
                    key=f"exclude_shows_{query.id}",
                    help="Supports glob wildcards (* ? []) and regex via /.../",
                )
                exclude_titles_value = st.text_area(
                    "Exclude title patterns",
                    value=format_list_input(query.exclude_title_keywords),
                    key=f"exclude_titles_{query.id}",
                    help="Plain keywords (substring), glob wildcards, or /regex/",
                )
                exclude_desc_value = st.text_area(
                    "Exclude description patterns",
                    value=format_list_input(getattr(query, "exclude_description_keywords", []) or []),
                    key=f"exclude_desc_{query.id}",
                )
                include_shows_value = st.text_area(
                    "Include shows",
                    value=format_list_input(getattr(query, "include_shows", []) or []),
                    key=f"include_shows_{query.id}",
                )
                include_titles_value = st.text_area(
                    "Include title patterns",
                    value=format_list_input(getattr(query, "include_title_keywords", []) or []),
                    key=f"include_titles_{query.id}",
                )
                include_desc_value = st.text_area(
                    "Include description patterns",
                    value=format_list_input(getattr(query, "include_description_keywords", []) or []),
                    key=f"include_desc_{query.id}",
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
                            exclude_description_keywords=parse_list_input(exclude_desc_value),
                            include_shows=parse_list_input(include_shows_value),
                            include_title_keywords=parse_list_input(include_titles_value),
                            include_description_keywords=parse_list_input(include_desc_value),
                        )
                    except Exception as exc:  # pragma: no cover - surface in UI
                        st.error(f"Unable to update search query: {exc}")
                    else:
                        st.session_state["flash"] = (
                            "success",
                            f"Updated search query #{query.id}.",
                        )
                        st.rerun()

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
                    st.rerun()


