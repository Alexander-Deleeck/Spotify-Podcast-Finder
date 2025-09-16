"""Dataclasses representing the application's core domain objects."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, List, Optional


@dataclass
class SearchQuery:
    """A stored Spotify search query."""

    id: int
    term: str
    frequency: str
    exclude_shows: List[str]
    exclude_title_keywords: List[str]
    created_at: datetime
    updated_at: datetime
    last_run: Optional[datetime]

    def next_run_due(self) -> Optional[datetime]:
        """Return when the query is next due to run based on its frequency."""
        if self.last_run is None:
            return None
        delta = frequency_to_timedelta(self.frequency)
        if delta is None:
            return None
        return self.last_run + delta


@dataclass
class Episode:
    """Represents a Spotify podcast episode returned from a search."""

    episode_id: str
    name: str
    show_name: str
    release_date: Optional[str]
    description: Optional[str]
    external_url: Optional[str]
    uri: Optional[str]
    duration_ms: Optional[int]
    raw: dict

    def formatted_release_date(self) -> str:
        if not self.release_date:
            return "Unknown"
        return self.release_date


def frequency_to_timedelta(frequency: str) -> Optional[timedelta]:
    """Translate a stored frequency string into a :class:`timedelta`."""
    if not frequency:
        return None

    normalized = frequency.strip().lower()
    mapping = {
        "daily": timedelta(days=1),
        "weekly": timedelta(weeks=1),
        "biweekly": timedelta(weeks=2),
        "monthly": timedelta(days=30),
        "quarterly": timedelta(days=91),
    }
    if normalized in mapping:
        return mapping[normalized]

    if normalized.endswith("d") and normalized[:-1].isdigit():
        return timedelta(days=int(normalized[:-1]))
    if normalized.endswith("w") and normalized[:-1].isdigit():
        return timedelta(weeks=int(normalized[:-1]))

    return None


def ensure_list(value: Optional[Iterable[str]]) -> List[str]:
    """Safely convert a stored value into a list of strings."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [item.strip() for item in text.split(",") if item.strip()]
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
        if isinstance(parsed, str):
            parsed_text = parsed.strip()
            return [parsed_text] if parsed_text else []
    return []
