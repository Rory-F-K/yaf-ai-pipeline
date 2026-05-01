"""
x_provider.py
X (Twitter) platform layer — sits between SocialMediaProvider and the
transport implementations (RapidXProvider, XScraper).

Responsibilities
----------------
- Defines all X-specific platform identity (name, URL format, source ID)
- Loads and validates credentials from env (each subclass declares what it needs)
- Owns AVIATION_TAGS and fetch_all() so both transports share the same tag list
  and inter-query pacing without duplicating code

Does NOT implement fetch() — that belongs to the transport layer:
  RapidXProvider  (twitter_rapid.py)  — HTTP via RapidAPI
  XScraper        (x_scraper.py)      — Playwright browser scraper [future]

Swapping transports in the pipeline is one line:

    # Current
    from twitter_rapid import RapidXProvider
    pipeline = Doc_Process_Pipeline(social_provider=RapidXProvider())

    # Future — when the scraper is ready
    from x_scraper import XScraper
    pipeline = Doc_Process_Pipeline(social_provider=XScraper())
"""

import logging
import os
import random
import time
from abc import abstractmethod

from social_base import AVIATION_RULES, DEFAULT_SECTION, SocialMediaProvider

logger = logging.getLogger(__name__)

# Default search queries for aviation / airline accessibility
AVIATION_TAGS: list[str] = [
    "#WheelchairAccessible",
    "#DisabledTravel",
    "#AccessibleTravel",
    "#AirlineAccessibility",
    "#WheelchairUser",
    "#CripTheVote",
    "#ACAA",
    "#DisabilityRights",
    "airline wheelchair",
    "airport accessibility",
    "disabled flyer",
    "mobility aid airline",
    "accessible boarding",
]

_INTER_QUERY_PAUSE: tuple[float, float] = (1.0, 3.0)


class XProvider(SocialMediaProvider):
    """
    X (Twitter) platform layer.

    Implements the platform-identity abstracts from SocialMediaProvider.
    Loads credentials from env and validates them at init time so failures
    are caught immediately, not mid-run.

    Subclasses must still implement fetch() for their specific transport.
    """

    def __init__(
        self,
        section_rules=AVIATION_RULES,
        default_section=DEFAULT_SECTION,
    ) -> None:
        super().__init__(section_rules=section_rules, default_section=default_section)
        self._load_credentials()

    # Platform identity (SocialMediaProvider abstract impl)
    # These three are identical for every X transport — defined once here.
    @property
    def platform_name(self) -> str:
        return "twitter"

    def platform_url(self, post_id: str) -> str:
        return f"https://x.com/i/web/status/{post_id}"

    def platform_source_id(self, post_id: str) -> str:
        return f"x_tweet_{post_id}"

    # Credential loading — subclasses declare what they need
    def _load_credentials(self) -> None:
        """
        Load and validate credentials from environment variables.

        Each transport subclass overrides this to read the env vars it needs
        and raise EnvironmentError early if any are missing.

        RapidXProvider  → RAPIDAPI_KEY
        XScraper        → X_USERNAME, X_PASSWORD, (optional) X_PHONE
        """

    # fetch_all — shared across all X transports
    def fetch_all(
        self,
        tags: list[str] | None = None,
        count_per_tag: int = 20,
    ) -> list[dict]:
        """
        Fetch posts for every tag in *tags* with cross-query deduplication.

        Uses self.fetch() — whichever transport the concrete subclass provides.
        """
        self.reset_seen()
        active_tags = tags or AVIATION_TAGS
        all_records: list[dict] = []

        for tag in active_tags:
            logger.info("[%s] fetching tag: %s", self.platform_name, tag)
            try:
                records = self.fetch(tag, count=count_per_tag)
                all_records.extend(records)
                logger.info("  ✓ %d new post(s)", len(records))
            except Exception as exc:
                logger.error("  ✗ failed '%s': %s", tag, exc)

            time.sleep(random.uniform(*_INTER_QUERY_PAUSE))

        logger.info(
            "[%s] fetch_all complete — %d total records",
            self.platform_name, len(all_records),
        )
        return all_records

    # fetch() remains abstract — implemented by each transport subclass
    @abstractmethod
    def fetch(self, query: str, count: int = 20) -> list[dict]:
        """Fetch posts matching *query* using this transport's data source."""