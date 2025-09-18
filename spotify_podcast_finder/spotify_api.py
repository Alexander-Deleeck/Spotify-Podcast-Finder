"""Helpers to interact with the Spotify Web API."""
from __future__ import annotations

import os
import time
from typing import Dict, Generator, Iterable, List, Optional
from dotenv import load_dotenv
import requests

load_dotenv()

TOKEN_URL = "https://accounts.spotify.com/api/token"
SEARCH_URL = "https://api.spotify.com/v1/search"
EPISODES_URL = "https://api.spotify.com/v1/episodes"


class SpotifyAuthError(RuntimeError):
    """Raised when Spotify credentials are missing or invalid."""


class SpotifyAPIError(RuntimeError):
    """Raised when Spotify returns an unexpected API response."""


class SpotifyClient:
    """Minimal Spotify client used for podcast episode discovery."""

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.client_id = client_id or os.getenv("SPOTIFY_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("SPOTIFY_CLIENT_SECRET")
        if not self.client_id or not self.client_secret:
            raise SpotifyAuthError(
                "Spotify credentials were not provided. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET."
            )
        self.session = session or requests.Session()
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0

    # ------------------------------------------------------------------
    # Authentication helpers
    # ------------------------------------------------------------------
    def _request_token(self) -> None:
        response = self.session.post(
            TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(self.client_id, self.client_secret),
            timeout=15,
        )
        if response.status_code != 200:
            raise SpotifyAuthError(
                f"Unable to authenticate with Spotify API (status {response.status_code}): {response.text}"
            )
        payload = response.json()
        self._token = payload["access_token"]
        expires_in = payload.get("expires_in", 3600)
        self._token_expires_at = time.time() + expires_in - 30  # refresh slightly early

    def _ensure_token(self) -> str:
        if not self._token or time.time() >= self._token_expires_at:
            self._request_token()
        assert self._token is not None
        return self._token

    # ------------------------------------------------------------------
    # API methods
    # ------------------------------------------------------------------
    def search_episodes(
        self,
        query: str,
        *,
        market: Optional[str] = None,
        limit: int = 50,
        max_pages: Optional[int] = None,
    ) -> Generator[Dict, None, None]:
        """Yield episode objects returned for a search query."""
        if not query:
            raise ValueError("query must be a non-empty string")

        limit = max(1, min(int(limit), 50))
        offset = 0
        pages_retrieved = 0
        while True:
            token = self._ensure_token()
            params = {
                "q": query,
                "type": "episode",
                "limit": limit,
                "offset": offset,
            }
            if market:
                params["market"] = market

            response = self.session.get(
                SEARCH_URL,
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                timeout=20,
            )
            if response.status_code == 401:
                # Token may have expired, refresh and retry once.
                self._request_token()
                token = self._ensure_token()
                response = self.session.get(
                    SEARCH_URL,
                    headers={"Authorization": f"Bearer {token}"},
                    params=params,
                    timeout=20,
                )

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", "1"))
                time.sleep(retry_after)
                continue

            if response.status_code != 200:
                raise SpotifyAPIError(
                    f"Spotify API returned status {response.status_code}: {response.text}"
                )

            payload = response.json()
            episodes = payload.get("episodes")
            if not episodes:
                break

            items = episodes.get("items", [])
            total = episodes.get("total", 0)

            for item in items:
                yield item

            offset += len(items)
            pages_retrieved += 1
            if len(items) == 0:
                break
            if offset >= total:
                break
            if max_pages is not None and pages_retrieved >= max_pages:
                break


    def get_episodes(self, episode_ids: Iterable[str], *, market: Optional[str] = None) -> List[Dict]:
        """Return full episode objects for the provided IDs using the batch endpoint.

        Spotify supports up to 50 IDs per request at /v1/episodes.
        """
        ids = [eid for eid in (str(x).strip() for x in episode_ids) if eid]
        if not ids:
            return []

        results: List[Dict] = []
        token = self._ensure_token()

        for start in range(0, len(ids), 50):
            chunk = ids[start : start + 50]
            params = {"ids": ",".join(chunk)}
            if market:
                params["market"] = market
            response = self.session.get(
                EPISODES_URL,
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                timeout=20,
            )
            if response.status_code == 401:
                self._request_token()
                token = self._ensure_token()
                response = self.session.get(
                    EPISODES_URL,
                    headers={"Authorization": f"Bearer {token}"},
                    params=params,
                    timeout=20,
                )
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", "1"))
                time.sleep(retry_after)
                # retry once after sleeping
                response = self.session.get(
                    EPISODES_URL,
                    headers={"Authorization": f"Bearer {token}"},
                    params=params,
                    timeout=20,
                )

            if response.status_code != 200:
                raise SpotifyAPIError(
                    f"Spotify API returned status {response.status_code} for episodes: {response.text}"
                )

            payload = response.json() or {}
            batch = payload.get("episodes") or []
            # API can include nulls for unavailable episodes
            results.extend([item for item in batch if item])

        return results

    def close(self) -> None:
        self.session.close()


def extract_episode_metadata(raw_episode: Dict) -> Dict:
    """Return a subset of useful metadata from a Spotify episode payload.

    Works with both SimplifiedEpisodeObject (from search) and full EpisodeObject
    (from episodes endpoint). Not all fields are guaranteed in the simplified
    object; in particular, `show` may be omitted.
    """
    show = raw_episode.get("show") or {}
    external_urls = raw_episode.get("external_urls") or {}
    return {
        "episode_id": raw_episode.get("id"),
        "name": raw_episode.get("name"),
        "show_name": show.get("name") or "",
        "release_date": raw_episode.get("release_date"),
        "description": raw_episode.get("description"),
        "external_url": external_urls.get("spotify"),
        "uri": raw_episode.get("uri"),
        "duration_ms": raw_episode.get("duration_ms"),
        "raw": raw_episode,
    }
