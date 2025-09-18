"""Command line interface for the Spotify Podcast Finder."""
from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime
from typing import Iterable, List, Optional

from .db import get_connection, initialize_db
from .models import Episode, SearchQuery, frequency_to_timedelta
from .search_service import (
    create_search_query,
    delete_search_query,
    get_search_query,
    list_recent_runs,
    list_search_queries,
    run_search,
    update_search_query,
)
from .spotify_api import SpotifyAuthError, SpotifyClient


def _format_datetime(dt: Optional[datetime]) -> str:
    if not dt:
        return "never"
    return dt.strftime("%Y-%m-%d %H:%M")


def _format_episode(episode: Episode) -> str:
    release = episode.formatted_release_date()
    show = episode.show_name or "Unknown show"
    link = episode.external_url or episode.uri or ""
    parts = [f"{episode.name} [{show}]", f"Release date: {release}"]
    if link:
        parts.append(f"Link: {link}")
    return " | ".join(parts)


def _print_query_table(queries: List[SearchQuery]) -> None:
    if not queries:
        print("No search queries stored. Use 'add-query' to create one.")
        return

    header = f"{'ID':<4} {'Search Term':<35} {'Frequency':<12} {'Last Run':<17} {'Next Run Due'}"
    print(header)
    print("-" * len(header))
    for query in queries:
        next_due = query.next_run_due()
        next_due_str = _format_datetime(next_due)
        print(
            f"{query.id:<4} {query.term[:33]:<35} {query.frequency:<12} {_format_datetime(query.last_run):<17} {next_due_str}"
        )


def _list_episodes(connection: sqlite3.Connection, query_id: int, limit: int, order_by: str, descending: bool) -> None:
    valid_columns = {
        "release": "release_date",
        "first": "first_seen_at",
        "last": "last_seen_at",
    }
    column = valid_columns[order_by]
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
    if not rows:
        print("No episodes stored for this query yet.")
        return

    for row in rows:
        release = row["release_date"] or "Unknown"
        show = row["show_name"] or "Unknown show"
        link = row["external_url"] or row["uri"] or ""
        seen = row["first_seen_at"]
        print(f"- {row['name']} [{show}] | Release: {release} | Indexed: {seen} | {link}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Spotify Podcast Episode Finder")
    parser.add_argument(
        "--db",
        dest="db_path",
        default=None,
        help="Path to the SQLite database file (defaults to podcast_finder.db in the project root).",
    )
    subparsers = parser.add_subparsers(dest="command")

    add_parser = subparsers.add_parser("add-query", help="Create a new Spotify search query")
    add_parser.add_argument("term", help="Search term to look for, e.g. the guest name")
    add_parser.add_argument("--frequency", default="weekly", help="How often the search should run (e.g. weekly, 14d)")
    add_parser.add_argument(
        "--exclude-show",
        action="append",
        dest="exclude_shows",
        default=[],
        help=(
            "Exclude shows by name. Supports glob wildcards (* ? []) and regex when wrapped in /.../. "
            "Examples: 'The * Show', '/^Joe Rogan.*$/'"
        ),
    )
    add_parser.add_argument(
        "--exclude-title",
        action="append",
        dest="exclude_titles",
        default=[],
        help=(
            "Exclude episodes by title. Plain text is substring match; supports glob wildcards (* ? []) "
            "and regex when wrapped in /.../. Examples: '*bonus*', '/\\bRecap\\b/i'"
        ),
    )

    subparsers.add_parser("list-queries", help="List all stored search queries")

    update_parser = subparsers.add_parser("update-query", help="Update an existing search query")
    update_parser.add_argument("query_id", type=int)
    update_parser.add_argument("--term")
    update_parser.add_argument("--frequency")
    update_parser.add_argument(
        "--exclude-show",
        action="append",
        dest="exclude_shows",
        help="Same pattern rules as for --exclude-show on add-query",
    )
    update_parser.add_argument(
        "--exclude-title",
        action="append",
        dest="exclude_titles",
        help="Same pattern rules as for --exclude-title on add-query",
    )

    delete_parser = subparsers.add_parser("delete-query", help="Remove a search query")
    delete_parser.add_argument("query_id", type=int)

    run_parser = subparsers.add_parser("run-query", help="Run a search query against Spotify")
    run_parser.add_argument("query_id", type=int)
    run_parser.add_argument("--market", help="Spotify market to use (e.g. US)")
    run_parser.add_argument("--limit", type=int, default=50, help="Number of episodes per API request")
    run_parser.add_argument("--max-pages", type=int, help="Maximum number of API pages to retrieve")

    run_all_parser = subparsers.add_parser("run-due", help="Run all queries whose schedule is due")
    run_all_parser.add_argument("--market")
    run_all_parser.add_argument("--limit", type=int, default=50)
    run_all_parser.add_argument("--max-pages", type=int)

    episodes_parser = subparsers.add_parser("list-episodes", help="Show stored episodes for a query")
    episodes_parser.add_argument("query_id", type=int)
    episodes_parser.add_argument("--limit", type=int, default=20)
    episodes_parser.add_argument(
        "--order",
        choices=["release", "first", "last"],
        default="release",
        help="Order results by release date (default), first seen or last seen",
    )
    episodes_parser.add_argument("--asc", action="store_true", help="Sort in ascending order")

    runs_parser = subparsers.add_parser("recent-runs", help="Show the history of recent search runs")
    runs_parser.add_argument("--limit", type=int, default=10)

    return parser


def _open_connection(db_path: Optional[str]) -> sqlite3.Connection:
    connection = get_connection(db_path)
    initialize_db(connection)
    return connection


def cmd_add_query(args: argparse.Namespace) -> None:
    connection = _open_connection(args.db_path)
    try:
        query = create_search_query(
            connection,
            term=args.term,
            frequency=args.frequency,
            exclude_shows=args.exclude_shows,
            exclude_title_keywords=args.exclude_titles,
        )
        print(f"Created search query #{query.id} for term '{query.term}' with frequency '{query.frequency}'.")
    finally:
        connection.close()


def cmd_list_queries(args: argparse.Namespace) -> None:
    connection = _open_connection(args.db_path)
    try:
        queries = list_search_queries(connection)
        _print_query_table(queries)
    finally:
        connection.close()


def cmd_update_query(args: argparse.Namespace) -> None:
    connection = _open_connection(args.db_path)
    try:
        query = update_search_query(
            connection,
            args.query_id,
            term=args.term,
            frequency=args.frequency,
            exclude_shows=args.exclude_shows,
            exclude_title_keywords=args.exclude_titles,
        )
        print(f"Updated query #{query.id}. Term: '{query.term}', frequency: '{query.frequency}'.")
    finally:
        connection.close()


def cmd_delete_query(args: argparse.Namespace) -> None:
    connection = _open_connection(args.db_path)
    try:
        delete_search_query(connection, args.query_id)
        print(f"Deleted search query #{args.query_id} and its episodes.")
    finally:
        connection.close()


def _run_query_once(
    connection: sqlite3.Connection,
    query: SearchQuery,
    *,
    market: Optional[str],
    limit: int,
    max_pages: Optional[int],
) -> dict:
    try:
        client = SpotifyClient()
    except SpotifyAuthError as exc:
        raise SystemExit(str(exc)) from exc

    try:
        summary = run_search(
            connection,
            query,
            client,
            market=market,
            limit=limit,
            max_pages=max_pages,
        )
    finally:
        client.close()
    return summary


def _print_run_summary(query: SearchQuery, summary: dict) -> None:
    previous = summary["previous_count"]
    processed = summary["processed"]
    skipped = summary["skipped"]
    new_episodes: List[Episode] = summary["new_episodes"]

    if previous == 0:
        print(f"Found and indexed {processed} base episodes for your search query: {query.term}")
        return

    if new_episodes:
        print(f"Found {len(new_episodes)} new episodes for '{query.term}':")
        for episode in new_episodes:
            print(f"  - {_format_episode(episode)}")
        print(f"Processed {processed} episodes (skipped {skipped} based on filters).")
    else:
        print(f"No new episodes found for '{query.term}'. Processed {processed} episodes (skipped {skipped}).")


def cmd_run_query(args: argparse.Namespace) -> None:
    connection = _open_connection(args.db_path)
    try:
        query = get_search_query(connection, args.query_id)
        summary = _run_query_once(
            connection,
            query,
            market=args.market,
            limit=args.limit,
            max_pages=args.max_pages,
        )
        _print_run_summary(query, summary)
    finally:
        connection.close()


def cmd_run_due(args: argparse.Namespace) -> None:
    connection = _open_connection(args.db_path)
    try:
        queries = list_search_queries(connection)
        due_queries: List[SearchQuery] = []
        now = datetime.utcnow()
        for query in queries:
            delta = frequency_to_timedelta(query.frequency)
            if delta is None:
                due_queries.append(query)
                continue
            if query.last_run is None or (query.last_run + delta) <= now:
                due_queries.append(query)
        if not due_queries:
            print("No queries are currently due to run.")
            return
        for query in due_queries:
            print(f"Running query #{query.id} ({query.term})...")
            summary = _run_query_once(
                connection,
                query,
                market=args.market,
                limit=args.limit,
                max_pages=args.max_pages,
            )
            _print_run_summary(query, summary)
    finally:
        connection.close()


def cmd_list_episodes(args: argparse.Namespace) -> None:
    connection = _open_connection(args.db_path)
    try:
        get_search_query(connection, args.query_id)  # ensure exists
        _list_episodes(connection, args.query_id, args.limit, args.order, not args.asc)
    finally:
        connection.close()


def cmd_recent_runs(args: argparse.Namespace) -> None:
    connection = _open_connection(args.db_path)
    try:
        rows = list_recent_runs(connection, limit=args.limit)
        if not rows:
            print("No runs have been recorded yet.")
            return
        header = f"{'ID':<4} {'Query':<35} {'Run At':<20} {'New Episodes':<13} {'Processed'}"
        print(header)
        print("-" * len(header))
        for row in rows:
            run_at = row["run_at"]
            print(
                f"{row['id']:<4} {row['term'][:33]:<35} {run_at:<20} {row['new_count']:<13} {row['total_results']}"
            )
    finally:
        connection.close()


def dispatch_command(args: argparse.Namespace) -> None:
    command = args.command
    if command == "add-query":
        cmd_add_query(args)
    elif command == "list-queries":
        cmd_list_queries(args)
    elif command == "update-query":
        cmd_update_query(args)
    elif command == "delete-query":
        cmd_delete_query(args)
    elif command == "run-query":
        cmd_run_query(args)
    elif command == "run-due":
        cmd_run_due(args)
    elif command == "list-episodes":
        cmd_list_episodes(args)
    elif command == "recent-runs":
        cmd_recent_runs(args)
    else:
        raise SystemExit("No command specified. Use --help for usage information.")


def main(argv: Optional[Iterable[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return
    dispatch_command(args)


if __name__ == "__main__":
    main()
