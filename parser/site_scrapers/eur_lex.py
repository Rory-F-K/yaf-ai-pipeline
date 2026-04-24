# Scraper for EUR-Lex Legal Acts
# Target: https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32006R1107

import asyncio
import json
import logging
import random
import re
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

from bs4 import BeautifulSoup, Tag
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

try:
    from . import register
except ImportError:
    def register(domain):
        def decorator(func):
            return func
        return decorator

log = logging.getLogger(__name__)

BASE_URL  = "https://eur-lex.europa.eu"
SEED_PATH = "/legal-content/EN/TXT/"
SEED_QUERY = "uri=CELEX:32006R1107"

# EUR-Lex frequently serves via Akamai — these phrases indicate a challenge page
_BOT_SIGNALS = [
    "access denied",
    "unusual behaviour",
    "security check",
    "please enable cookies",
    "cf-error",
    "ray id",
    "verify you are human",
    "captcha",
    "403 forbidden",
]


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------

async def _pause(lo: float = 1.2, hi: float = 3.0) -> None:
    await asyncio.sleep(random.uniform(lo, hi))


# ---------------------------------------------------------------------------
# Browser + context setup
# ---------------------------------------------------------------------------

async def _make_context(playwright):
    browser = await playwright.chromium.launch(
        headless=True,
        channel="chrome",
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
        ],
    )
    context = await browser.new_context(
        viewport={"width": 1440, "height": 900},
        device_scale_factor=1,
        locale="en-GB",               # EUR-Lex serves EN-GB content
        timezone_id="Europe/Brussels",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        extra_http_headers={
            "Accept-Language": "en-GB,en;q=0.9",
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            "sec-ch-ua": (
                '"Chromium";v="124", "Google Chrome";v="124", '
                '"Not-A.Brand";v="99"'
            ),
            "sec-ch-ua-mobile":   "?0",
            "sec-ch-ua-platform": '"Windows"',
        },
    )
    await Stealth().apply_stealth_async(context)
    return browser, context


# Human-like interaction helpers: waiting for content, mouse movement, scrolling, cookie banner dismissal
async def _wait_for_content(page, timeout: float = 20.0) -> None:
    """Wait for networkidle then confirm a meaningful EUR-Lex content element."""
    try:
        await page.wait_for_load_state("networkidle", timeout=timeout * 1000)
    except Exception:
        pass

    # EUR-Lex content lives inside #document1 or .eli-container; fall back to
    # the generic text panel id or any h1.
    content_selectors = [
        "#document1",
        ".eli-container",
        "#textTabContent",
        ".oj-doc-ti",
        "h1",
        "article",
        "main",
    ]
    try:
        await page.wait_for_selector(
            ", ".join(content_selectors),
            timeout=10_000,
            state="visible",
        )
    except Exception:
        pass


async def _human_mouse_wander(page) -> None:
    w = (page.viewport_size or {}).get("width",  1440)
    h = (page.viewport_size or {}).get("height",  900)
    for _ in range(random.randint(3, 6)):
        x = random.randint(100, w - 100)
        y = random.randint(100, h - 100)
        await page.mouse.move(x, y, steps=random.randint(8, 20))
        await _pause(0.15, 0.5)


async def _scroll_page(page) -> None:
    for pct in [0.0, 0.25, 0.5, 0.75, 1.0, 0.0]:
        await page.evaluate(
            f"window.scrollTo({{top: document.body.scrollHeight * {pct},"
            f" behavior: 'smooth'}});"
        )
        await _pause(0.5, 1.5)


async def _dismiss_cookie_banner(page) -> None:
    """Click the EUR-Lex cookie-consent 'Accept' button if it appears."""
    selectors = [
        "button#cookie-consent-button",
        "button[data-testid='cookie-accept']",
        "button.wt-cck-btn-refuse",   # "Refuse non-essential" — also clears banner
        "#cookie-consent-accept",
        ".cck-actions-button",
    ]
    for sel in selectors:
        try:
            btn = await page.query_selector(sel)
            if btn and await btn.is_visible():
                await btn.click()
                log.info("  Cookie banner dismissed.")
                await _pause(0.5, 1.0)
                return
        except Exception:
            continue


# Bot-wall detection: look for common phrases in the rendered text content
def _is_bot_wall(html: str) -> bool:
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True).lower()
    return any(sig in text for sig in _BOT_SIGNALS)


# EUR-Lex URLs contain the CELEX identifier in a query parameter (usually 'uri' or 'from').
def _celex_from_url(url: str) -> str:
    """Extract the CELEX identifier from a EUR-Lex URL query string."""
    qs = parse_qs(urlparse(url).query)
    for key in ("uri", "from"):
        val = qs.get(key, [None])[0]
        if val and val.upper().startswith("CELEX:"):
            return val.split(":", 1)[1]
    return ""


def _doc_title(soup: BeautifulSoup) -> str:
    """
    EUR-Lex titles live in .oj-doc-ti paragraphs or the <title> tag.
    We concatenate the first two non-empty .oj-doc-ti elements (type + date)
    separated by ' — '.
    """
    parts = [
        p.get_text(" ", strip=True)
        for p in soup.select(".oj-doc-ti")
        if p.get_text(strip=True)
    ]
    if parts:
        return " — ".join(parts[:2])
    # Fallback: browser <title>, trim the site name suffix
    title_tag = soup.find("title")
    if title_tag:
        return title_tag.get_text(strip=True).split(" - EUR-Lex")[0].strip()
    h1 = soup.find("h1")
    return h1.get_text(strip=True) if h1 else ""


def _doc_date(soup: BeautifulSoup) -> str:
    """Pull the OJ publication date from the table header row (.oj-hd-date)."""
    date_cell = soup.select_one(".oj-hd-date")
    if date_cell:
        return date_cell.get_text(strip=True)
    time_tag = soup.select_one("time[datetime]")
    if time_tag:
        return time_tag.get("datetime", "")
    return ""


def _oj_reference(soup: BeautifulSoup) -> str:
    """Official Journal reference, e.g. 'L 204/1'."""
    oj_cell = soup.select_one(".oj-hd-oj")
    return oj_cell.get_text(strip=True) if oj_cell else ""


def _url_to_section(url: str) -> str:
    celex = _celex_from_url(url)
    if celex:
        return f"CELEX:{celex}"
    path = urlparse(url).path.rstrip("/")
    return path.split("/")[-1].replace("-", " ").title()


# Content selectors: where to look for the main legislative text in the rendered HTML

# EUR-Lex renders the legislative text inside #textTabContent > #document1.
# The .eli-container wraps the recitals + articles.
# We prefer the most-specific selector and fall back gracefully.
_CONTENT_SELECTORS = [
    # ── Primary: the rendered text panel ─────────────────────────────────────
    "#textTabContent",          # full text tab (articles + annexes)
    "#document1",               # single-document view
    ".eli-container",           # ELI-structured legislative body
    # ── Fallback ─────────────────────────────────────────────────────────────
    "#text",                    # outer text panel div
    "main",
]

# Tags to strip from the content node before text extraction
_NOISE_TAGS = [
    "script", "style", "nav", "aside", "header", "footer",
    "noscript", "form", "button", "figure",
]

# Class / id fragments that flag non-article elements
_NOISE_CLASSES = re.compile(
    r"(breadcrumb|language.?switcher|language.?bar|document.?tabs|"
    r"tabs.?bar|quicklinks|share|social|cookie|banner|alert|skip|"
    r"pagination|pager|utility|search|toolbar|metadata.?block)",
    re.I,
)


def _is_noise_element(tag) -> bool:
    if not isinstance(tag, Tag):
        return False
    if not tag.attrs:
        return False
    classes = " ".join(tag.get("class", []))
    tag_id  = tag.get("id", "")
    return bool(_NOISE_CLASSES.search(classes) or _NOISE_CLASSES.search(tag_id))


def _find_content_node(soup: BeautifulSoup):
    for selector in _CONTENT_SELECTORS:
        node = soup.select_one(selector)
        if node:
            return node
    return None


def _extract_text(node) -> str:
    """
    Convert the content node to clean plain text.

    EUR-Lex uses heavily nested <table> elements for article numbering
    (the article number is in one <td>, the text in the adjacent <td>).
    We render these as 'Article N — text…' lines so the structure survives
    stripping.
    """
    if node is None:
        return ""

    node = BeautifulSoup(str(node), "html.parser")

    # Remove noise subtrees
    for tag in node.find_all(_NOISE_TAGS):
        tag.decompose()
    for tag in node.find_all(True):
        if _is_noise_element(tag):
            tag.decompose()

    # Unwrap article-number tables: <tr><td>(n)</td><td>text</td></tr> become "(n)  text"  so paragraph numbers are preserved inline.
    for tr in node.find_all("tr"):
        cells = tr.find_all("td", recursive=False)
        if len(cells) == 2:
            num_text  = cells[0].get_text(strip=True)
            body_text = cells[1].get_text(" ", strip=True)
            if num_text and body_text:
                tr.replace_with(
                    BeautifulSoup(
                        f"<p>{num_text}&nbsp;&nbsp;{body_text}</p>",
                        "html.parser",
                    )
                )

    lines = []
    for el in node.descendants:
        if isinstance(el, str):
            text = el.strip()
            if text:
                lines.append(text)
        elif hasattr(el, "name") and el.name in (
            "p", "li", "h1", "h2", "h3", "h4", "h5", "h6",
            "br", "dt", "dd", "hr",
        ):
            lines.append("")

    text = "\n".join(lines)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


# Multiple related pages (e.g. annexes) are linked from the seed page's navigation tabs.
def _discover_related_urls(soup: BeautifulSoup, seed_url: str) -> list[str]:
    """
    EUR-Lex sometimes splits a regulation into separate HTML pages
    (the main body + one page per annex).  These are linked from the
    document-tab navigation bar ('#documentTab' or '.tabsWrap').

    We collect only same-celex-family links that differ solely in a
    'from' or 'tab' query parameter to avoid wandering into case-law etc.
    """
    seed_parsed = urlparse(seed_url)
    seed_celex  = _celex_from_url(seed_url)

    seen, result = set(), []

    for tag in soup.find_all("a", href=True):
        href     = tag["href"].strip()
        absolute = urljoin(seed_url, href)
        parsed   = urlparse(absolute)

        # Same host only
        if parsed.netloc and parsed.netloc != seed_parsed.netloc:
            continue

        # Must share the same /legal-content/EN/ path
        if not parsed.path.startswith("/legal-content/EN/"):
            continue

        # Must reference the same CELEX document
        if seed_celex and _celex_from_url(absolute) != seed_celex:
            continue

        # Skip the seed itself
        clean = parsed._replace(fragment="").geturl()
        if clean == seed_url or clean in seen:
            continue

        seen.add(clean)
        result.append(clean)

    return sorted(result)


# Per page fetch + parse with human-like interaction and bot-wall detection
async def _fetch_and_parse(
    context, url: str, referer: str, celex: str = ""
) -> dict | None:
    """
    Open a fresh tab, load the EUR-Lex page with human-like interaction,
    then parse the rendered HTML into a normalised article dict.
    """
    log.info(f"  -> {url}")
    page = await context.new_page()
    try:
        await page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=25_000,
            referer=referer,
        )
        # Wait for content to load, dismiss cookie banner, then interact with the page
        await _wait_for_content(page)
        await _dismiss_cookie_banner(page)
        await _pause(1.5, 3.5)
        await _human_mouse_wander(page)
        await _scroll_page(page)

        html = await page.content()

        if _is_bot_wall(html):
            log.warning(f"  x Bot-wall — skipping {url}")
            return None

        soup    = BeautifulSoup(html, "html.parser")
        title   = _doc_title(soup)
        content = _find_content_node(soup)
        body    = _extract_text(content)
        pub_date = _doc_date(soup)
        oj_ref   = _oj_reference(soup)

        if not body:
            log.warning(f"  x No body content extracted from {url}")
            return None

        effective_celex = celex or _celex_from_url(url)
        slug = (
            effective_celex.replace(":", "-")
            or urlparse(url).path.strip("/").replace("/", "--")
            or "root"
        )

        return { # The article dict we return for each page
            "id": slug,
            "celex": effective_celex,
            "title": title,
            "url": url,
            "section": _url_to_section(url),
            "oj_reference": oj_ref,
            "published": pub_date,
            "body": body,
        }

    except Exception as exc:
        log.warning(f"  x Failed ({exc}) — {url}")
        return None
    finally:
        await page.close()


# Async for expansion potential
async def _run(entry_url: str) -> str:
    articles: list[dict] = []
    celex = _celex_from_url(entry_url)

    async with async_playwright() as pw:
        browser, context = await _make_context(pw)
        try:
            # Seed page: load with human-like behaviour, extract main content + metadata
            log.info(f"[EUR-Lex] Entry: {entry_url}")
            seed_page = await context.new_page()
            await seed_page.goto(
                entry_url,
                wait_until="domcontentloaded",
                timeout=25_000,
            )
            await _wait_for_content(seed_page)
            await _dismiss_cookie_banner(seed_page)
            await _pause(2.0, 4.0)
            await _human_mouse_wander(seed_page)
            await _scroll_page(seed_page)

            seed_html = await seed_page.content()
            await seed_page.close()

            if _is_bot_wall(seed_html):
                log.error("[EUR-Lex] Bot-wall on entry page — aborting.")
                return json.dumps([], ensure_ascii=False)

            # Parse seed page and extract main article content + metadata
            seed_soup  = BeautifulSoup(seed_html, "html.parser")
            seed_body  = _extract_text(_find_content_node(seed_soup))
            pub_date   = _doc_date(seed_soup)
            oj_ref     = _oj_reference(seed_soup)

            if seed_body:
                seed_slug = celex.replace(":", "-") if celex else "root"
                articles.append({
                    "id":           seed_slug,
                    "celex":        celex,
                    "title":        _doc_title(seed_soup),
                    "url":          entry_url,
                    "section":      _url_to_section(entry_url),
                    "oj_reference": oj_ref,
                    "published":    pub_date,
                    "body":         seed_body,
                })

            # Reveal related pages
            related_urls = _discover_related_urls(seed_soup, entry_url)
            # print(f"Found {1 + len(related_urls)} page(s) to scrape "f"({len(related_urls)} related page(s))\n")

            # Scrape related pages
            for i, rel_url in enumerate(related_urls, 1):
                # print(f"  [{i}/{len(related_urls)}] {rel_url}")
                await _pause(3.0, 6.0)   # be gentle with EUR-Lex servers
                article = await _fetch_and_parse(
                    context, rel_url, referer=entry_url, celex=celex
                )
                if article:
                    articles.append(article)

        finally:
            await browser.close()

    print(f"\nDone. {len(articles)} article(s) extracted.")
    return json.dumps(articles, ensure_ascii=False, indent=2)


# Public scraper function
@register("eur-lex.europa.eu")
def scrape(url: str) -> str:
    """
    Scrape a EUR-Lex legal act from its HTML text view.

    Accepts any EUR-Lex TXT URL, e.g.:
        https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32006R1107

    Returns a JSON string (array of objects):
        [
          {
            "id":           str,  # CELEX-derived slug, e.g. "32006R1107"
            "celex":        str,  # raw CELEX id, e.g. "32006R1107"
            "title":        str,  # full legislative title
            "url":          str,  # canonical page URL
            "section":      str,  # "CELEX:32006R1107" or path-derived label
            "oj_reference": str,  # OJ reference, e.g. "L 204/1"
            "published":    str,  # publication date (DD.M.YYYY or ISO)
            "body":         str,  # plain text, HTML stripped
          },
          ...                     # one entry per document page / annex
        ]
    """
    return asyncio.run(_run(url))


# Standalone test
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    test_url = f"{BASE_URL}{SEED_PATH}?{SEED_QUERY}"
    print(f"Scraping: {test_url}\n")
    results = json.loads(scrape(test_url))

    print(f"\n{'='*60}")
    print(f"Total documents returned: {len(results)}")
    print(f"{'='*60}\n")

    for doc in results:
        preview = {
            k: (v[:300] + "..." if k == "body" and len(v) > 300 else v)
            for k, v in doc.items()
        }
        print(json.dumps(preview, indent=2))
        print("---")