"""
social_base.py
Abstract base for all social media data providers.

Platform-agnostic: no knowledge of any specific platform, API, or credentials.
All platform-specific concerns (URL format, env vars, tags) live one level down
in the platform layer (e.g. XProvider, RedditProvider).

Architecture
------------
SocialMediaProvider
    _clean / _hash / _classify / _make_record
   
XProvider
    (x_provider.py)
    platform identity,
    env-var loading,
    AVIATION_TAGS,
    fetch_all()
    
RapidXProvider           XScraper
    (twitter_rapid.py)      (x_scraper.py)
    HTTP transport only     Playwright transport only
    reads RAPIDAPI_KEY      reads X_USERNAME / X_PASSWORD
"""

import hashlib
import re
from abc import ABC, abstractmethod


# Built-in domain classifiers
# Stored as named constants so callers can compose or extend them.

#: Aviation & airline accessibility rules (ordered; first match wins).
AVIATION_RULES: list[tuple[list[str], str]] = [
    (
        ["complaint", "discrimination", "refused", "denied", "ignored"],
        "Discrimination & Complaints",
    ),
    (
        ["regulation", "rule", "law", "acaa", "dot rule", "policy", "cfr"],
        "Regulatory Reference",
    ),
    (
        ["tip", "advice", "recommend", "suggest", "guide", "how to"],
        "Passenger Tips & Advice",
    ),
    (
        ["wheelchair", "mobility aid", "power chair", "rollator"],
        "Passenger Mobility Feedback",
    ),
    (
        ["damage", "broken", "destroyed", "lost", "crack", "bent", "scooter"],
        "Equipment Damage",
    ),
    (
        ["gate", "boarding", "jetbridge", "aisle", "deplaning", "ramp"],
        "Boarding & Deplaning",
    ),
    (
        ["staff", "crew", "agent", "attendant", "helped"],
        "Staff Assistance Feedback",
    ),
    (
        ["delay", "late", "wait", "waiting", "hours", "stranded"],
        "Passenger Assistance Delays",
    ),
]

#: Generic accessibility rules for non-aviation contexts.
GENERAL_ACCESSIBILITY_RULES: list[tuple[list[str], str]] = [
    (["complaint", "discrimination", "refused", "denied"], "Complaints"),
    (["regulation", "law", "policy", "rule"], "Regulatory Reference"),
    (["tip", "advice", "recommend", "guide"], "Tips & Advice"),
    (["wheelchair", "mobility", "disabled"], "Mobility Feedback"),
    (["damage", "broken", "lost"], "Equipment Damage"),
    (["staff", "helped", "assisted"], "Staff Feedback"),
    (["delay", "wait", "stranded"], "Assistance Delays"),
]

#: Fallback label when no rule matches.
DEFAULT_SECTION = "General Accessibility Feedback"


class SocialMediaProvider(ABC):
    """
    Abstract base for all social media data providers.

    Knows nothing about any specific platform, API, or credentials.
    Subclasses are organised in two layers:

      Layer 1 — platform layer (e.g. XProvider, RedditProvider)
        Implements platform_name, platform_url, platform_source_id.
        Loads credentials from env. Defines default tags.
        Implements fetch_all().

      Layer 2 — transport layer (e.g. RapidXProvider, XScraper)
        Implements fetch() using a specific data source (API, scraper, etc.).
        Contains no platform-identity or credential logic.

    The pipeline types its social_provider field as SocialMediaProvider and
    never imports anything from the platform or transport layers directly.
    """

    def __init__(
        self,
        section_rules: list[tuple[list[str], str]] = AVIATION_RULES,
        default_section: str = DEFAULT_SECTION,
    ) -> None:
        self._section_rules = section_rules
        self._default_section = default_section
        self._seen_ids: set[str] = set()

    # Abstract interface - Layer 1 (platform) must implement these
    @abstractmethod
    def fetch(self, query: str, count: int = 20) -> list[dict]:
        """Fetch posts matching *query*; return normalised records."""

    @abstractmethod
    def platform_url(self, post_id: str) -> str:
        """Canonical URL for a single post."""

    @abstractmethod
    def platform_source_id(self, post_id: str) -> str:
        """Namespaced source ID, e.g. 'x_tweet_123'."""

    @property
    @abstractmethod
    def platform_name(self) -> str:
        """Short platform identifier, e.g. 'twitter', 'reddit'."""

    # Deduplication
    def reset_seen(self) -> None:
        """Clear the deduplication set between independent fetch sessions."""
        self._seen_ids.clear()

    def _is_duplicate(self, record_id: str) -> bool:
        return record_id in self._seen_ids

    def _mark_seen(self, record_id: str) -> None:
        self._seen_ids.add(record_id)

    # Record factory — single definition of the output schema
    def _make_record(self, post_id: str, text: str) -> dict:
        """
        Build a normalised output record.

        Schema
        ------
        id          str   SHA-256[:32] of cleaned text
        parent_id   None  reserved for reply threading
        section     str   classification label from section_rules
        text        str   cleaned post body
        source      str   canonical post URL  (via platform_url)
        source_id   str   namespaced post id  (via platform_source_id)
        platform    str   platform name       (via platform_name)
        type        str   always "social"
        sent        bool  always False
        """
        cleaned = self._clean(text)
        return {
            "id": self._hash(cleaned),
            "parent_id": None,
            "section": self._classify(cleaned),
            "text": cleaned,
            "source": self.platform_url(post_id),
            "source_id": self.platform_source_id(post_id),
            "platform": self.platform_name,
            "type": "social",
            "sent": False,
        }

    # Shared text utilities, used by all platforms and transports
    @staticmethod
    def _clean(text: str) -> str:
        """Collapse all whitespace runs to a single space."""
        return " ".join(text.split())

    def _classify(self, text: str) -> str:
        """First-match rule classifier with whole-word boundary matching."""
        lower = text.lower()
        for keywords, section in self._section_rules:
            if any(
                re.search(r"\b" + re.escape(kw) + r"\b", lower)
                for kw in keywords
            ):
                return section
        return self._default_section

    @staticmethod
    def _hash(text: str) -> str:
        """SHA-256 fingerprint (first 32 hex chars) for deduplication."""
        return hashlib.sha256(text.encode()).hexdigest()[:32]