"""
twitter_rapid.py
RapidAPI transport for the X (Twitter) platform.

Responsibility: HTTP requests to the RapidAPI x-com2 endpoint — nothing else.
Platform identity, tags, credentials pattern, and fetch_all() all live in
XProvider (x_provider.py). Classification, cleaning, and deduplication live
in SocialMediaProvider (social_base.py).

FUTURE: Replace with XScraper (x_scraper.py) when the Playwright scraper
is stable. Both extend XProvider with the same interface, so the pipeline
swap is one line — no other code changes needed.
"""

import http.client
import json
import logging
import os
import time
from urllib.parse import quote

from x_provider import XProvider

logger = logging.getLogger(__name__)

_HOST = "x-com2.p.rapidapi.com"
_ENDPOINT_TMPL = "/Search/?q={query}&count={count}&tweet_search_mode=live"


class RapidXProvider(XProvider):
    """
    Fetches X posts via the RapidAPI x-com2 Search endpoint.

    Reads RAPIDAPI_KEY from env at init. Raises EnvironmentError immediately
    if the key is missing so the pipeline fails fast rather than mid-run.
    """

    def __init__(
        self,
        max_retries: int = 3,
        retry_backoff: float = 2.0,
        **kwargs,
    ) -> None:
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        # Credential loading and platform init happen in XProvider.__init__
        super().__init__(**kwargs)

    # Credential loading (XProvider hook)
    def _load_credentials(self) -> None:
        self.api_key = os.getenv("X_RAPID_API_KEY", "").strip()
        if not self.api_key:
            raise EnvironmentError(
                "RAPIDAPI_KEY must be set in your .env file. "
                "RapidXProvider cannot authenticate without it."
            )
        logger.debug("RapidXProvider: RAPIDAPI_KEY loaded")

    # fetch — the only method this class owns
    def fetch(self, query: str, count: int = 20) -> list[dict]:
        """Fetch up to *count* deduplicated posts matching *query*."""
        logger.info("fetch() query=%r  count=%d", query, count)
        raw = self._request(query, count)
        posts = self._extract_posts(raw)
        records = self._normalise(posts)
        logger.info("  -> %d new record(s) after dedup", len(records))
        return records

    # HTTP transport
    def _request(self, query: str, count: int) -> dict:
        endpoint = _ENDPOINT_TMPL.format(query=quote(query, safe=""), count=count)
        headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": _HOST,
        }
        backoff = self.retry_backoff

        for attempt in range(1, self.max_retries + 1):
            logger.debug("[HTTP] attempt %d/%d  GET %s", attempt, self.max_retries, endpoint)
            conn = None
            try:
                conn = http.client.HTTPSConnection(_HOST, timeout=30)
                conn.request("GET", endpoint, headers=headers)
                resp = conn.getresponse()
                body = resp.read().decode("utf-8")
                logger.debug("[HTTP] status=%d  body_len=%d", resp.status, len(body))

                if resp.status == 200:
                    return json.loads(body)
                if resp.status == 429:
                    logger.warning("[HTTP] 429 rate-limited — backing off %.1fs", backoff)
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                if 400 <= resp.status < 500:
                    raise RuntimeError(
                        f"RapidAPI {resp.status} for {query!r}: {body[:200]}"
                    )
                logger.warning("[HTTP] %d server error — retrying", resp.status)
                time.sleep(backoff)
                backoff *= 2

            except (http.client.HTTPException, OSError, TimeoutError) as exc:
                logger.warning("[HTTP] connection error attempt %d: %s", attempt, exc)
                if attempt == self.max_retries:
                    raise RuntimeError(
                        f"All {self.max_retries} attempts failed for {query!r}"
                    ) from exc
                time.sleep(backoff)
                backoff *= 2
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

        raise RuntimeError(f"Exhausted {self.max_retries} retries for {query!r}")

    # Response parsing & normalisation
    def _extract_posts(self, data: dict) -> list[dict]:
        """
        Navigate the x-com2 nested response and return a flat list of raw
        tweet result objects (each has 'rest_id' and 'legacy' keys).
 
        Returns [] on any structural deviation — never raises.
        """
        try:
            instructions = (
                data["data"]
                ["search_by_raw_query"]
                ["search_timeline"]
                ["timeline"]
                ["instructions"]
            )
        except (KeyError, TypeError):
            logger.warning("[parse] unexpected top-level shape")
            return []
 
        posts = []
        for instruction in instructions:
            entries = instruction.get("entries", [])
            if not isinstance(entries, list):
                continue
            for entry in entries:
                # Skip cursors, promoted tweets, and other non-tweet entries
                entry_id = entry.get("entryId", "")
                if not entry_id.startswith("tweet-"):
                    continue
                try:
                    result = (
                        entry["content"]
                        ["itemContent"]
                        ["tweet_results"]
                        ["result"]
                    )
                    # Some entries are wrappers (e.g. TweetWithVisibilityResults)
                    # where the real tweet is one level deeper under "tweet"
                    if result.get("__typename") != "Tweet" and "tweet" in result:
                        result = result["tweet"]
                    posts.append(result)
                except (KeyError, TypeError):
                    logger.debug("[parse] skipped malformed entry: %s", entry_id)
                    continue
 
        logger.debug("[parse] extracted %d tweet result objects", len(posts))
        return posts
 
 
    def _normalise(self, posts: list[dict]) -> list[dict]:
        """
        Convert raw tweet result objects (each with 'rest_id' + 'legacy')
        into normalised pipeline records.
 
        Skips posts with no usable text and deduplicates by content hash.
        """
        records: list[dict] = []
        for result in posts:
            try:
                post_id = str(result.get("rest_id") or result.get("legacy", {}).get("id_str", ""))
                legacy  = result.get("legacy", {})
                text    = legacy.get("full_text") or legacy.get("text", "")
                if not post_id or not text:
                    continue
                record = self._make_record(post_id, text)
                if self._is_duplicate(record["id"]):
                    continue
                self._mark_seen(record["id"])
                records.append(record)
            except Exception as exc:
                logger.debug("[normalise] skipped: %s", exc)
        return records
