# Scraper for DOT Air Consumer Passengers with Disabilities
# Target: https://www.transportation.gov/airconsumer/passengers-disabilities

import asyncio
import json
import logging
import random
import re
from urllib.parse import urljoin, urlparse

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

BASE_URL  = "https://www.transportation.gov"
SEED_PATH = "/airconsumer/passengers-disabilities"

_BOT_SIGNALS = [
    "access denied",
    "unusual behaviour",
    "security check",
    "please enable cookies",
    "cf-error",
    "ray id",
]


# Timing helpers
async def _pause(lo: float = 1.2, hi: float = 3.0) -> None:
    await asyncio.sleep(random.uniform(lo, hi))


# Browser + context setup
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
        locale="en-US",
        timezone_id="America/New_York",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
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
    context.on("page", lambda page: asyncio.ensure_future(Stealth().apply_stealth_async(page)))
    return browser, context


# Human-behaviour helpers to reduce the risk of bot detection
async def _wait_for_content(page, timeout: float = 15.0) -> None:
    # Wait for networkidle first (JS rendering complete), then confirm a meaningful content element is actually present in the DOM.
    try:
        # networkidle means no network requests for 500 ms — JS has finished
        await page.wait_for_load_state("networkidle", timeout=timeout * 1000)
    except Exception:
        pass  # cap reached — carry on

    # Secondary check: wait for at least one real content element
    content_selectors = [
        "article",
        "main",
        ".field--name-body",
        ".node__content",
        "[class*='paragraph']",
        "h1",
    ]
    try:
        await page.wait_for_selector(
            ", ".join(content_selectors),
            timeout=8_000,
            state="visible",   # must be visible, not just in DOM
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
    for pct in [0.0, 0.3, 0.6, 1.0, 0.0]:
        await page.evaluate(
            f"window.scrollTo({{top: document.body.scrollHeight * {pct},"
            f" behavior: 'smooth'}});"
        )
        await _pause(0.6, 1.8)


async def _expand_accordions(page) -> None:
    # Click every collapsed accordion / details trigger so hidden content is added to the DOM before we snapshot the HTML.
    # Targets the common Drupal / USWDS patterns used on transportation.gov.
    selectors = [
        "details:not([open]) summary",       # native <details>
        "[aria-expanded='false']",            # ARIA accordion buttons
        ".accordion__button[aria-expanded='false']",  # USWDS accordion
        "button.usa-accordion__button",       # USWDS alt class
    ]
    clicked = 0
    for sel in selectors:
        els = await page.query_selector_all(sel)
        for el in els:
            try:
                await el.scroll_into_view_if_needed()
                await _pause(0.1, 0.3)
                await el.click(force=True)
                clicked += 1
                await _pause(0.2, 0.5)
            except Exception:
                continue
    if clicked:
        log.info(f"  Expanded {clicked} accordion(s)")
        await _pause(0.5, 1.0)   # let content animate in


# Bot-detection heuristic
def _is_bot_wall(html: str) -> bool:
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True).lower()
    return any(sig in text for sig in _BOT_SIGNALS)


# Content extraction helpers

# Ordered from most-specific to most-general.
# The search widget and navigation both live outside these selectors so they won't pollute the extracted body text.
_CONTENT_SELECTORS = [
    # ── Confirmed from live DOT HTML ──────────────────────────────────────────
    # The article body sits inside div.mb-4.clearfix directly under article.node--type-article > div.node__content.
    # Anchor to the article element and avoid the sidebar entirely.
    "article.node--type-article div.node__content",
    "article.node--view-mode-full div.node__content",
    # Any node article on the page (covers non-article node types too)
    "article[data-history-node-id] div.node__content",
    # Drupal field fallbacks
    ".field--name-body",
    ".field--name-field-body",
    ".node__content",
    # USWDS / DOT-specific layout regions
    ".usa-prose",
    ".l-content",
    ".region-content",
    # Generic article / main — used only as last resort because they include sidebars on some pages; noise is filtered by _extract_text.
    "article.node--type-page",
    "article.node",
    # Absolute fallback
    "main",
]


def _find_content_node(soup: BeautifulSoup):
    for selector in _CONTENT_SELECTORS:
        node = soup.select_one(selector)
        if node:
            return node
    return None


# Tags whose text we always discard even when inside the content node
_NOISE_TAGS = ["script", "style", "nav", "aside", "header", "footer",
               "noscript", "form", "button", "figure"]

# Class / id fragments that indicate non-article elements
_NOISE_CLASSES = re.compile(
    r"(search|breadcrumb|social|share|related|sidebar|widget|menu|pager|"
    r"pagination|cookie|banner|alert|skip|utility|tool)", re.I
)


def _is_noise_element(tag) -> bool:
    # Must be a real Tag — NavigableString/Comment/etc. have no .attrs.
    if not isinstance(tag, Tag):
        return False
    # Some malformed-HTML tags are parsed with attrs=None by html.parser.
    if not tag.attrs:
        return False
    classes = " ".join(tag.get("class", []))
    tag_id  = tag.get("id", "")
    return bool(_NOISE_CLASSES.search(classes) or _NOISE_CLASSES.search(tag_id))


def _extract_text(node) -> str:
    if node is None:
        return ""

    # Work on a copy so we don't mutate the shared soup tree
    node = BeautifulSoup(str(node), "html.parser")

    # Remove known-noise subtrees
    for tag in node.find_all(_NOISE_TAGS):
        tag.decompose()
    for tag in node.find_all(True):
        if _is_noise_element(tag):
            tag.decompose()

    lines = []
    for el in node.descendants:
        if isinstance(el, str):
            text = el.strip()
            if text:
                lines.append(text)
        elif hasattr(el, "name") and el.name in (
            "p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "br", "tr", "dt", "dd"
        ):
            lines.append("")

    text = "\n".join(lines)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _page_title(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(strip=True)
    title_tag = soup.find("title")
    if title_tag:
        return title_tag.get_text(strip=True).split("|")[0].strip()
    return ""


def _last_updated(soup: BeautifulSoup) -> str:
    time_tag = soup.select_one("time[datetime]")
    if time_tag:
        return time_tag.get("datetime", "")
    # DOT uses "Last updated: <date>" as a plain text span
    for tag in soup.find_all(string=re.compile(r"Last updated", re.I)):
        parent_text = tag.parent.get_text(strip=True) if tag.parent else ""
        if parent_text:
            return parent_text
    date_div = soup.select_one("[class*='date']")
    if date_div:
        return date_div.get_text(strip=True)
    return ""


def _url_to_section(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    return path.split("/")[-1].replace("-", " ").title()


# Link discovery
def _discover_child_urls(soup: BeautifulSoup, seed_url: str) -> list[str]:
    """
    Collect transportation.gov links that belong to the same /airconsumer/
    subtree as the seed URL, de-duplicated and sorted.

    We match on /airconsumer/ rather than the full seed path so that sibling
    pages (e.g. /airconsumer/filing-complaint) are NOT included, but all
    pages directly under /airconsumer/passengers-disabilities/ are.
    """
    seed_path = urlparse(seed_url).path.rstrip("/")
    seen, result = set(), []

    for tag in soup.find_all("a", href=True):
        href     = tag["href"].strip()
        absolute = urljoin(seed_url, href)
        parsed   = urlparse(absolute)

        # Same domain only
        if parsed.netloc and parsed.netloc != "www.transportation.gov":
            continue

        # Must be a child of the seed path (not the seed itself, not siblings)
        if not parsed.path.startswith(seed_path + "/"):
            continue

        clean = parsed._replace(fragment="", query="").geturl().rstrip("/")
        if clean not in seen:
            seen.add(clean)
            result.append(clean)

    return sorted(result)


# Per-page fetch + parse
async def _fetch_and_parse(context, url: str, referer: str) -> dict | None:
    """
    Open a fresh tab, load the page, simulate human interaction, expand
    accordions, then parse the rendered HTML into a normalised article dict.
    """
    log.info(f"  -> {url}")
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded",
                        timeout=20_000, referer=referer)
        await _wait_for_content(page)
        await _pause(1.5, 3.0)
        await _human_mouse_wander(page)
        await _scroll_page(page)
        await _expand_accordions(page)
        # One final scroll after expansion to catch any newly-revealed content
        await _scroll_page(page)

        html = await page.content()

        if _is_bot_wall(html):
            log.warning(f"  x Bot-wall — skipping {url}")
            return None

        soup    = BeautifulSoup(html, "html.parser")
        title   = _page_title(soup)
        content = _find_content_node(soup)
        body    = _extract_text(content)
        updated = _last_updated(soup)

        if not body:
            log.warning(f"  x No body content at {url}")
            return None

        slug = urlparse(url).path.strip("/").replace("/", "--") or "root"
        return {
            "id":         slug,
            "title":      title,
            "url":        url,
            "section":    _url_to_section(url),
            "body":       body,
            "updated_at": updated,
        }

    except Exception as exc:
        log.warning(f"  x Failed ({exc}) — {url}")
        return None
    finally:
        await page.close()


# Async pipeline

async def _run(entry_url: str) -> str:
    articles: list[dict] = []

    async with async_playwright() as pw:
        browser, context = await _make_context(pw)
        try:
            # Step 1: seed page 
            log.info(f"[DOT] Entry: {entry_url}")
            seed_page = await context.new_page()
            await seed_page.goto(entry_url, wait_until="domcontentloaded",
                                 timeout=20_000)
            await _wait_for_content(seed_page)
            await _pause(2.0, 4.0)
            await _human_mouse_wander(seed_page)
            await _scroll_page(seed_page)
            await _expand_accordions(seed_page)
            await _scroll_page(seed_page)

            seed_html = await seed_page.content()
            await seed_page.close()

            if _is_bot_wall(seed_html):
                log.error("[DOT] Bot-wall on entry page — aborting.")
                return json.dumps([], ensure_ascii=False)

            # Parse seed inline 
            seed_soup = BeautifulSoup(seed_html, "html.parser")
            seed_body = _extract_text(_find_content_node(seed_soup))
            if seed_body:
                seed_slug = urlparse(entry_url).path.strip("/").replace("/", "--")
                articles.append({
                    "id":         seed_slug,
                    "title":      _page_title(seed_soup),
                    "url":        entry_url,
                    "section":    _url_to_section(entry_url),
                    "body":       seed_body,
                    "updated_at": _last_updated(seed_soup),
                })

            # Step 2: discover child pages
            child_urls = _discover_child_urls(seed_soup, entry_url)
            print(f"Found {1 + len(child_urls)} page(s) to scrape "
                  f"({len(child_urls)} child page(s))\n")

            # Step 3: scrape children
            for i, child_url in enumerate(child_urls, 1):
                print(f"  [{i}/{len(child_urls)}] {child_url}")
                await _pause(2.5, 5.5)
                article = await _fetch_and_parse(context, child_url,
                                                 referer=entry_url)
                if article:
                    articles.append(article)

        finally:
            await browser.close()

    print(f"\nDone. {len(articles)} article(s) extracted.")
    return json.dumps(articles, ensure_ascii=False, indent=2)


# Public scraper entry point
@register("www.transportation.gov")
def scrape(url: str) -> str:
    """
    Scrape articles from a transportation.gov Passengers-with-Disabilities URL.

    Returns a JSON string (array of objects):
        [
          {
            "id":         str, # slug derived from URL path
            "title":      str,
            "url":        str,
            "section":    str, # last path segment, title-cased
            "body":       str, # plain text, HTML stripped
            "updated_at": str, # ISO date when available, else ""
          },
          ...
        ]
    """
    return asyncio.run(_run(url))


# Standalone test
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    test_url = f"{BASE_URL}{SEED_PATH}"
    print(f"Scraping: {test_url}\n")
    results = json.loads(scrape(test_url))

    print(f"\n{'='*60}")
    print(f"Total articles returned: {len(results)}")
    print(f"{'='*60}\n")

    for article in results:
        preview = {
            k: (v[:3000] + "..." if k == "body" and len(v) > 300 else v)
            for k, v in article.items()
        }
        print(json.dumps(preview, indent=2))
        print("---")