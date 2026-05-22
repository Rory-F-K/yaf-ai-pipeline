"""
parser/remote/remote_ingest.py

Entry point for all remote URL-based ingestion in the pipeline.

    pages = ingest_remote(config)

config keys
───────────
  url             required  starting URL
  allowed_domains optional  list of domain substrings to stay within during BFS
  max_pages       optional  crawl limit (default 5)

Returns
───────
  [{"url": str, "text": str}, ...]   clean plain text, ready for semantic chunking
"""

from __future__ import annotations

import sys
from pathlib import Path

# Path bootstrapping — must run before any project imports.
# Resolves whether the file is run as a script or imported as a module:
#   script : python parser/remote/remote_ingest.py <url>
#   module : from parser.remote.remote_ingest import ingest_remote
_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import os
import re
import tempfile
import requests

from collections import deque
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

from parser.remote.universal_scraper import universal_scrape, ScrapeResult
from parser.remote.quality_gate import check_quality
from parser.remote.html_cleaner import clean_html
from parser.local.local_ingest import ingest_local
from parser.local.pdf_parser import detect_headings


# Existing site-specific scrapers — wrapped so a broken dependency in any
# one scraper cannot prevent the file from loading.
try:
    from parser.site_scrapers import get_scraper
    _SITE_SCRAPERS_OK = True
except Exception as e:
    print(f"[Warning] site_scrapers unavailable: {e}")
    _SITE_SCRAPERS_OK = False
    def get_scraper(_url: str):
        return None


# URL skip patterns
# Applied only to links discovered during BFS, never to the start URL.

_SKIP_PATTERNS = [
    "calendar", "chart", "/cart", "/checkout",
    "/login", "/signin", "/register",
    "javascript:", "mailto:", "tel:",
]


# PDF helpers

def _is_pdf(resp) -> bool:
    ct = resp.headers.get("Content-Type", "").lower()
    return "application/pdf" in ct or resp.content[:4] == b"%PDF"


def _download_pdf(url: str) -> str | None:
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    if _is_pdf(resp):
        tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        tmp.write(resp.content)
        tmp.close()
        return tmp.name
    return None


# Text cleaning

def _clean_text(text: str, already_extracted: bool = False) -> str:
    """
    Normalise plain text for the pipeline.

    already_extracted=True  text came from trafilatura (universal_scrape).
                            Only empty lines dropped; short accordion answers like "Yes." or "No." are preserved.

    already_extracted=False text came from a dedicated parser or raw scrape.
                            Lines shorter than 20 chars are filtered as noise.
    """
    text     = re.sub(r'\r', '\n', text)
    text     = re.sub(r'(?<=[.!?])\s+', '\n\n', text)
    min_len  = 3 if already_extracted else 20
    seen:    set[str] = set()
    lines:   list[str] = []

    for line in text.split("\n"):
        line = line.strip()
        if not line or len(line) < min_len or line in seen:
            continue
        seen.add(line)
        lines.append(line)

    return detect_headings("\n".join(lines))


# Type detection

def _is_html(text: str) -> bool:
    if not isinstance(text, str):
        return False
    t = text.lower()
    return "<html" in t or "<body" in t or "<div" in t or "<a " in t


def _is_json(text: str) -> bool:
    if not isinstance(text, str):
        return False
    s = text.strip()
    return (s.startswith("{") and s.endswith("}")) or \
           (s.startswith("[") and s.endswith("]"))



# Failure diagnosis

def _diagnose_failure(result: ScrapeResult) -> str:
    # Returns a human-readable reason why extraction produced no content.
    html = result.html.lower() if result.html else ""

    if not html:
        return "no response — network error or timeout"

    if any(s in html for s in [
        "are you a robot", "verify you are human", "captcha",
        "cloudflare", "ray id", "security check", "ddos protection",
        "access denied", "unusual traffic", "checking your browser",
    ]):
        return "bot detection — page blocked the scraper"

    if any(s in html for s in [
        "subscribe to read", "sign in to continue", "members only",
        "login required", "subscription required", "unlock this article",
        "create an account",
    ]):
        return "paywall — content requires authentication"

    if any(s in html for s in [
        "page not found", "404 not found", "doesn\'t exist",
        "no longer available", "has been removed",
    ]):
        return "page not found (soft 404)"

    if len(html) < 1000:
        return "near-empty response — likely a redirect or shell page"

    return "no content extracted — page structure not recognised"

# Link extraction

def _extract_links(html: str, base_url: str, allowed_domains: list[str]) -> list[str]:
    if not html or not _is_html(html):
        return []
    links = []
    try:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all("a", href=True):
            full = urljoin(base_url, tag["href"])
            if any(p in full for p in _SKIP_PATTERNS):
                continue
            domain = urlparse(full).netloc
            if allowed_domains and not any(ad in domain for ad in allowed_domains):
                continue
            links.append(full)
    except Exception as e:
        print(f"[Links] {e}")
    return links


# Main ingestion function

def ingest_remote(config: dict) -> list[dict]:
    start_url       = config["url"]
    allowed_domains = config.get("allowed_domains", [])
    max_pages       = config.get("max_pages", 5)

    visited: set[str]   = set()
    queue:   deque[str] = deque([start_url])
    pages:   list[dict] = []

    while queue and len(pages) < max_pages:
        url = queue.popleft()

        if url in visited:
            continue

        # Skip patterns apply to discovered links only, not the start URL
        if url != start_url and any(p in url for p in _SKIP_PATTERNS):
            continue

        visited.add(url)
        print(f"[Remote] {url}")

        # PDF
        try:
            pdf_path = _download_pdf(url)
            if pdf_path:
                print("[PDF] Detected")
                try:
                    text    = _clean_text(ingest_local(pdf_path))
                    quality = check_quality(text, url=url)
                    if quality.passed:
                        pages.append({"url": url, "text": text})
                        print(f"[PDF] Accepted {len(text)} chars")
                    else:
                        print(f"[PDF] Rejected — {quality.reason}")
                finally:
                    os.remove(pdf_path)
                continue
        except Exception as e:
            print(f"[PDF] Error: {e}")

        # Scraping
        raw_text          = ""
        html_for_links    = ""
        already_extracted = False

        try:
            # 1. Site-specific scraper
            scraper = get_scraper(url)
            if scraper:
                print(f"[Scraper] Site-specific")
                try:
                    raw_text = scraper(url)
                except Exception as e:
                    print(f"[Scraper] Site-specific error: {e}")

            # 2. Universal scraper — used when no site-specific scraper exists, or when it returned insufficient content
            if not raw_text or len(raw_text) < 200:
                if scraper:
                    print(f"[Scraper] Site-specific insufficient — falling back to universal")
                print(f"[Scraper] Universal")
                result: ScrapeResult = universal_scrape(url)
                raw_text          = result.text
                html_for_links    = result.html
                already_extracted = result.via != "failed"

                if not raw_text or len(raw_text) < 100:
                    reason = _diagnose_failure(result)
                    print(f"[Skip] {reason}: {url}")
                    continue

            # Clean
            if already_extracted:
                cleaned = _clean_text(raw_text, already_extracted=True)
            elif _is_json(raw_text):
                cleaned = raw_text
            elif _is_html(raw_text):
                html_for_links = raw_text
                cleaned = _clean_text(clean_html(raw_text))
            else:
                cleaned = _clean_text(raw_text)

            # Quality gate
            quality = check_quality(cleaned, url=url)
            if not quality.passed:
                print(f"[Skip] {quality.reason}: {url}")
                continue

            pages.append({"url": url, "text": cleaned})
            print(f"[Ingest] Accepted {len(cleaned)} chars")

        except Exception as e:
            print(f"[Error] {url}: {e}")
            continue

        # BFS link extraction
        for link in _extract_links(html_for_links, url, allowed_domains):
            if link not in visited:
                queue.append(link)

    return pages


# CLI test
# Run a single URL through the scraper without starting the full pipeline.
#
#   python parser/remote/remote_ingest.py <url>
#   python parser/remote/remote_ingest.py <url> --pages 3
#   python parser/remote/remote_ingest.py <url> --scraper-only

if __name__ == "__main__":
    import textwrap

    args = sys.argv[1:]

    if not args or args[0] in ("-h", "--help"):
        print(
            "Usage:\n"
            "  python parser/remote/remote_ingest.py <url>\n"
            "  python parser/remote/remote_ingest.py <url> --pages N\n"
            "  python parser/remote/remote_ingest.py <url> --scraper-only\n"
        )
        sys.exit(0)

    url          = args[0]
    scraper_only = "--scraper-only" in args
    max_pages    = 1

    if "--pages" in args:
        try:
            max_pages = int(args[args.index("--pages") + 1])
        except (IndexError, ValueError):
            print("--pages requires an integer"); sys.exit(1)

    DIV = "─" * 72

    def _show(idx: int, page_url: str, text: str) -> None:
        words = len(text.split())
        print(f"\n{'═' * 72}")
        print(f"  Page {idx}  |  {len(text):,} chars  {words:,} words")
        print(f"  {page_url}")
        print(f"{'═' * 72}")
        print("\n── start " + DIV[8:])
        print(textwrap.fill(text[:600].strip(), width=72))
        if len(text) > 600:
            print("\n── end " + DIV[6:])
            print(textwrap.fill(text[-300:].strip(), width=72))
        print(f"\n{DIV}")

    if scraper_only:
        print(f"\n[Test] scraper-only — {url}\n")
        result = universal_scrape(url)
        print(f"\nvia          : {result.via}")
        print(f"text length  : {len(result.text):,} chars")
        print(f"html length  : {len(result.html):,} chars")
        if result.text:
            _show(1, url, result.text)
        from parser.remote.quality_gate import check_quality
        qr = check_quality(result.text, url=url)
        print(f"\nQuality gate : passed={qr.passed}  reason={qr.reason!r}")
    else:
        print(f"\n[Test] ingest_remote — {url}  max_pages={max_pages}\n")
        results = ingest_remote({"url": url, "max_pages": max_pages})
        print(f"\n\n{'═' * 72}")
        print(f"  {len(results)} page(s) accepted")
        print(f"{'═' * 72}")
        if not results:
            print("\n  Nothing passed the quality gate.\n")
        for i, page in enumerate(results, 1):
            _show(i, page["url"], page["text"])