"""
parser/remote/quality_gate.py

Validates extracted text before it enters the pipeline.
Replaces the bare `len(text) < 100` checks in remote_ingest with a
structured gate that catches soft 404s, paywalls, CAPTCHA walls,
maintenance pages, and low-signal content (nav dumps, near-empty pages).

Usage:
    from parser.remote.quality_gate import check_quality

    result = check_quality(text, url=url)
    if not result.passed:
        print(f"[Skip] {url} — {result.reason}")
        continue
"""

from dataclasses import dataclass


# Failure phrase lists
# All checked against a lowercased sample of the first 1 000 characters — failure signals appear early in the page content.

_SOFT_404 = [
    "page not found",
    "404 not found",
    "404 error",
    "doesn't exist",
    "does not exist",
    "no longer available",
    "has been removed",
    "has been deleted",
    "we couldn't find",
    "we could not find",
    "nothing found",
    "oops! that page",
    "oops, that page",
    "the page you requested",
    "the page you are looking for",
    "the requested page",
    "this page is gone",
]

_PAYWALL = [
    "sign in to continue",
    "log in to continue",
    "subscribe to read",
    "subscribe to continue",
    "subscription required",
    "create an account to",
    "create a free account",
    "members only",
    "member only",
    "login required",
    "please log in",
    "please sign in",
    "unlock this article",
    "unlock full access",
    "start your free trial",
    "this content is for subscribers",
    "already a subscriber",
    "get full access",
]

_CAPTCHA = [
    # generic
    "are you a robot",
    "are you human",
    "verify you are human",
    "verify you're human",
    "verifies you are not a bot",
    "not a bot",
    "complete the captcha",
    "please verify",
    "one more step",
    # security service / bot manager pages (Akamai, Cloudflare, etc.)
    "security verification",
    "security service to protect",
    "protect against malicious bots",
    "malicious bots",
    "performing security verification",
    "security check",
    "bot protection",
    "ddos protection",
    "checking your browser",
    "cloudflare ray id",
    "enable javascript and cookies",
    "browser security check",
    "while the website verifies",
]

_MAINTENANCE = [
    "maintenance mode",
    "under maintenance",
    "be right back",
    "temporarily unavailable",
    "down for maintenance",
    "under construction",
    "scheduled maintenance",
    "site is currently unavailable",
    "coming soon",
]

_ACCESS_DENIED = [
    "access denied",
    "403 forbidden",
    "you don't have permission",
    "you do not have permission",
    "not authorized",
    "unauthorised",
    "unauthorized",
    "restricted content",
    "this content is restricted",
]

# Characters that make up structural separator lines (table borders, dividers).
# A line composed entirely of these is formatting noise, not content.
_STRUCTURAL_CHARS = frozenset(r'|/-_=+*•·')


# Result type

@dataclass
class QualityResult:
    passed: bool
    reason: str # "ok" on pass; short descriptor on fail e.g. "soft_404"


# Helpers

def _is_structural_noise(line: str) -> bool:
    """Return True for lines that are pure punctuation/symbol separators.

    Examples that are filtered: "|", "---", "===", "| |", "* * *"
    These are table-border or divider artifacts from HTML→text conversion
    and should not be counted in noise-ratio or repetition checks.
    """
    return bool(line) and all(c in _STRUCTURAL_CHARS or c.isspace() for c in line)


# Main check

def check_quality(text: str, url: str = "") -> QualityResult:
    """
    Run all quality checks on the extracted plain text.

    Parameters
    ----------
    text : Extracted plain text from universal_scrape or a dedicated parser.
    url  : Optional — included in log output only, not used for logic.

    Returns
    -------
    QualityResult(passed=True, reason="ok")  on success
    QualityResult(passed=False, reason=...)  on any failure
    """

    if not text or not text.strip():
        return QualityResult(False, "empty")

    cleaned = text.strip()
    length  = len(cleaned)

    # 1. Minimum length
    if length < 150:
        return QualityResult(False, f"too_short ({length} chars)")

    # 2. Phrase checks on first 1 000 chars
    # Using a short sample keeps the checks fast and avoids false positives from body text that happens to contain one of the phrases.
    sample = cleaned[:1000].lower()

    for phrase in _SOFT_404:
        if phrase in sample:
            return QualityResult(False, f"soft_404 — matched: {phrase!r}")

    for phrase in _PAYWALL:
        if phrase in sample:
            return QualityResult(False, f"paywall — matched: {phrase!r}")

    for phrase in _CAPTCHA:
        if phrase in sample:
            return QualityResult(False, f"captcha — matched: {phrase!r}")

    for phrase in _MAINTENANCE:
        if phrase in sample:
            return QualityResult(False, f"maintenance — matched: {phrase!r}")

    for phrase in _ACCESS_DENIED:
        if phrase in sample:
            return QualityResult(False, f"access_denied — matched: {phrase!r}")

    # 3. Noise ratio
    # Strip structural-noise lines (pure punctuation/symbols, e.g. "|", "---") 
    # before any line-level heuristics so table/column separators produced by HTML→text conversion don't skew the results.
    #
    # If more than 75 % of the *remaining* lines are 1–2 words it is almost
    # certainly a nav dump, a menu, or structured data that extraction failed to clean.
    # #  Only applied when there are enough lines to be meaningful.
    lines = [
        l.strip()
        for l in cleaned.splitlines()
        if l.strip() and not _is_structural_noise(l.strip())
    ]

    if len(lines) > 15:
        short_lines = sum(1 for l in lines if len(l.split()) <= 2)
        ratio = short_lines / len(lines)
        if ratio > 0.75:
            return QualityResult(
                False,
                f"noise_ratio {ratio:.0%} ({short_lines}/{len(lines)} short lines)"
            )

    # 4. Repetition check
    # If the same sentence appears more than 5 times the page is likely a pagination artifact or a redirect loop that produced repeated content.
    # Structural-noise lines are already excluded from `lines` above.
    if len(lines) > 10:
        from collections import Counter
        top = Counter(lines).most_common(1)[0]
        if top[1] > 5:
            return QualityResult(
                False,
                f"repetition — line appears {top[1]}x: {top[0][:60]!r}"
            )

    return QualityResult(True, "ok")