# Spotify Podcast Finder

The Spotify Podcast Finder is a small command line tool that keeps track of
newly released podcast episodes in which a specific guest or topic appears. It
uses the Spotify Web API to perform saved search queries on a schedule (for
example weekly) and stores the resulting episodes in a local SQLite database.
When a query finds new results that were not present during the previous run
they are highlighted so they can be reviewed quickly.

## Features

- Save Spotify search queries together with a preferred refresh frequency.
- Run a query manually or execute all queries that are due based on the stored
  frequency.
- Store all episode metadata in a local SQLite database so that new episodes
  can be detected.
- Apply filters to exclude specific podcast shows or keywords in the episode
  title to reduce noise in the results.
- Inspect the stored episodes and review the history of search runs directly
  from the command line.

## Prerequisites

The tool requires **Python 3.9 or later** and Spotify API credentials. Create a
Spotify developer application and note the **Client ID** and **Client Secret**.
Expose them to the tool via environment variables before running any commands:

```bash
export SPOTIFY_CLIENT_ID="your-client-id"
export SPOTIFY_CLIENT_SECRET="your-client-secret"
```

Install the Python dependency with:

```bash
pip install -r requirements.txt
```

## Usage

Run the command line interface with:

```bash
python main.py --help
```

The most common actions are:

### 1. Create a Search Query

```bash
python main.py add-query "Michael Levin" --frequency weekly \
  --exclude-show "Big Think" \
  --exclude-title "Levine"
  --exclude-title "Levy"
  --exclude-title "Mark Levin"
  --exclude-title "John Levi"
  --exclude-title "Levinson"
  --exclude-title "Janna Levin"
  --exclude-title "Michael Lewis"
```

### 2. List Saved Queries

```bash
python main.py list-queries
```

### 3. Run a Query

Fetch the latest results for a single query:

```bash
python main.py run-query 1 --market US
```

Run all queries that are due based on their stored frequency:

```bash
python main.py run-due --market US
```

When a query is run for the first time the tool simply indexes the baseline set
of episodes. On subsequent runs only new, previously unseen episodes are
reported.

### 4. Review Stored Episodes

```bash
python main.py list-episodes 1 --limit 10 --order release
```

### 5. Inspect the Run History

```bash
python main.py recent-runs --limit 5
```

## Streamlit Frontend

A lightweight Streamlit frontend is included for users who prefer a visual
interface. It exposes the same functionality as the CLI:

- run all saved queries that are due or trigger an individual search on demand,
- create, edit, and delete search queries together with exclusion filters,
- review newly indexed episodes and browse all stored results, and
- inspect the history of previous search runs.

Start the app with:

```bash
streamlit run streamlit_app.py
```

The sidebar allows you to choose a custom SQLite database file (or fall back to
the default `podcast_finder.db`) and reminds you to configure
`SPOTIFY_CLIENT_ID` and `SPOTIFY_CLIENT_SECRET` environment variables before
running any searches.

## Database

All data is stored inside `podcast_finder.db` in the project root by default.
Pass `--db /path/to/file.db` to use a different location. The database contains
three tables:

- `search_queries` – saved searches and their configuration.
- `episodes` – the indexed Spotify episode metadata.
- `search_runs` – a log of each executed search including the number of new
  episodes detected.

## Scheduling

The tool can be executed periodically by an external scheduler such as `cron`.
The `run-due` command automatically skips queries that are not yet scheduled
for another run based on their stored frequency value.

## Extending the Tool

The internal modules provide reusable building blocks (`spotify_api`,
`search_service`, `db`, and `cli`) to simplify further development, such as
adding a web interface or integrating playback progress for a Spotify account.
