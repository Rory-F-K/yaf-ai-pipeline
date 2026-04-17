"""
Swiss Airlines (swiss.com) site-specific scraper.

swiss.com sits behind Akamai/Cloudflare bot protection that rejects plain
requests regardless of headers.  This scraper uses Playwright in stealth mode
to render the page as a real browser would.

Dependencies:
    pip install playwright playwright-stealth beautifulsoup4
    playwright install chromium

URL pattern handled:
    https://www.swiss.com/{market}/{locale}/...any content page...
"""

import json
import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup

try:
    from . import register
except ImportError:
    def register(domain):
        def decorator(func):
            return func
        return decorator


# Common tags that contain non-essential information.
NOISE_TAGS = {
    "header", "footer", "nav", "aside",
    "script", "style", "noscript", "svg", "form",
}

MAIN_CONTENT_SELECTORS = [
    "main",
    "[role='main']",
    ".page-content",
    ".content-wrapper",
    ".lx-page-content",
    ".lh-content",
    "article",
    "#main-content",
]


# Fetch with Playwright in stealth mode to bypass bot protection and get fully-rendered HTML
def _fetch_html(url: str) -> str:
    """
    Launch a headless Chromium browser with stealth patches applied, navigate to `url`, wait for the main content to settle, and return the fully-rendered page HTML.

    Supports both playwright-stealth API versions:
      - v2.x: Stealth class  (pip install playwright-stealth)
      - v1.x: stealth_sync   (pip install tf-playwright-stealth)
    """
    from playwright.sync_api import sync_playwright

    # -- stealth import: try new v2 API first, fall back to v1 --
    try:
        from playwright_stealth import Stealth as _Stealth
        _stealth_v2 = True
    except ImportError:
        from playwright_stealth import stealth_sync as _stealth_sync
        _stealth_v2 = False

    launch_args = [
        "--no-sandbox",
        "--disable-blink-features=AutomationControlled",
    ]
    context_kwargs = dict(
        viewport={"width": 1440, "height": 900},
        locale="en-US",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    )

    def _navigate_and_get_html(page, browser) -> str:
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        try:
            page.wait_for_selector(
                "main, [role='main'], article",
                timeout=10_000,
            )
        except Exception:
            page.wait_for_load_state("networkidle", timeout=15_000)
        html = page.content()
        browser.close()
        return html

    if _stealth_v2:
        # v2: Stealth wraps the playwright context manager itself
        with _Stealth().use_sync(sync_playwright()) as p:
            browser = p.chromium.launch(headless=True, args=launch_args)
            context = browser.new_context(**context_kwargs)
            page = context.new_page()
            return _navigate_and_get_html(page, browser)
    else:
        # v1: stealth_sync is applied per-page after creation
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=launch_args)
            context = browser.new_context(**context_kwargs)
            page = context.new_page()
            _stealth_sync(page)
            return _navigate_and_get_html(page, browser)


# Helpers for Page Parsing and Section Extraction
def _remove_noise(soup: BeautifulSoup) -> None:
    for tag in NOISE_TAGS:
        for el in soup.find_all(tag):
            el.decompose()
    for el in soup.find_all(attrs={"aria-hidden": "true"}):
        el.decompose()
    for el in soup.find_all(class_=re.compile(r"sr-only|visually-hidden|skip")):
        el.decompose()


def _find_main(soup: BeautifulSoup):
    for selector in MAIN_CONTENT_SELECTORS:
        el = soup.select_one(selector)
        if el:
            return el
    return soup.body or soup


# Tags used to identify table structures
_ROW_TAGS  = ["tr",  "maui-table-row"]
_CELL_TAGS = ["th",  "td", "maui-table-cell"]
_TABLE_TAGS = ["table", "maui-table"]

# maui-table-head rows are treated as header rows (get the --- separator)
_HEAD_TAGS = ["thead", "maui-table-head"]


def _table_to_text(table) -> str:
    """
    Example output:
        | Aircraft type | Cargo door |   |
        |---|---|---|
        |   | Width in cm/in | Height in cm/in |
        | A220-100 | 118 / 46,5 | 83 / 32,6 |
    """
    # Identify which rows belong to the header section so we know where to place the Markdown separator.
    head_rows = set()
    for head_section in table.find_all(_HEAD_TAGS):
        for tr in head_section.find_all(_ROW_TAGS):
            head_rows.add(id(tr))

    rows_data = []   # list of (is_header_row, [cell_text, ...])
    for tr in table.find_all(_ROW_TAGS):
        cells = []
        for td in tr.find_all(_CELL_TAGS):
            text = td.get_text(separator=" ", strip=True)
            cells.append(text)
        if any(c for c in cells):   # skip rows that are entirely empty
            rows_data.append((id(tr) in head_rows, cells))

    if not rows_data:
        return ""

    col_count = max(len(r) for _, r in rows_data)
    rows_data = [(hdr, r + [""] * (col_count - len(r))) for hdr, r in rows_data]

    lines = []
    separator = "|" + "|".join(["---"] * col_count) + "|"
    prev_was_header = False

    for is_header, row in rows_data:
        lines.append("| " + " | ".join(row) + " |")
        # Insert separator after the last consecutive header row
        if prev_was_header and not is_header:
            lines.insert(-1, separator)   # insert before this data row
        prev_was_header = is_header

    # If every row was a header (edge case), append separator at the end
    if prev_was_header:
        lines.append(separator)

    return "\n".join(lines)


def _extract_sections(container) -> list[dict]:
    from bs4 import NavigableString

    # Replace every table (HTML or maui) with its rendered text so the descendant walk below never sees raw cell tags.
    for table in container.find_all(_TABLE_TAGS):
        text = _table_to_text(table)
        if text:
            table.replace_with(NavigableString("\n\n" + text + "\n\n"))
        else:
            table.decompose()

    sections: list[dict] = []
    current_heading = ""
    current_body: list[str] = []

    HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}

    def _flush():
        body = " ".join(current_body).strip()
        if body:
            sections.append({"heading": current_heading, "body": body})

    for el in container.descendants:
        # NavigableString (and bare Python strings) both land here.
        # BeautifulSoup NavigableStrings have .name == None, treat "name is None" as text — not skip it — otherwise the pipe-delimited table strings injected by replace_with() are lost.
        if not hasattr(el, "name") or el.name is None:
            text = str(el).strip()
            if text and len(text) > 15:
                current_body.append(text)
            continue

        if el.name in HEADING_TAGS:
            text = el.get_text(separator=" ", strip=True)
            if text:
                _flush()
                current_heading = text
                current_body = []

        elif el.name in {"p", "li", "dd", "figcaption"}:
            # Only grab leaf-level nodes to avoid double-counting nested lists
            if not any(
                child.name in {"p", "li"}
                for child in el.children
                if hasattr(child, "name")
            ):
                text = el.get_text(separator=" ", strip=True)
                if text and len(text) > 15:
                    current_body.append(text)

    _flush()
    return sections


def _page_title(soup: BeautifulSoup) -> str:
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return og["content"].strip()
    title_tag = soup.find("title")
    if title_tag:
        return title_tag.get_text(strip=True).split("|")[0].strip()
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(strip=True)
    return ""


def _market_locale(url: str) -> tuple[str, str]:
    parts = urlparse(url).path.strip("/").split("/")
    return (parts[0] if len(parts) > 0 else ""), (parts[1] if len(parts) > 1 else "")


# Public Scrape Function with Domain Registration
@register("swiss.com")
def scrape(url: str) -> str:
    """
    Returns a JSON string — array with one object per page:
        [
          {
            "url":      str,
            "market":   str,        # e.g. "ch"
            "locale":   str,        # e.g. "en"
            "title":    str,
            "sections": [
              {"heading": str, "body": str},
              ...
            ]
          }
        ]
    """
    html = _fetch_html(url)

    soup = BeautifulSoup(html, "html.parser")
    _remove_noise(soup)

    title = _page_title(soup)
    market, locale = _market_locale(url)
    sections = _extract_sections(_find_main(soup))

    return json.dumps(
        [{"url": url, "market": market, "locale": locale,
          "title": title, "sections": sections}],
        ensure_ascii=False,
    )


# Standalone test
if __name__ == "__main__":
    test_url = (
        "https://www.swiss.com/ch/en/prepare/special-care/accessible-travel.html"
    )
    print(f"Scraping: {test_url}\n")
    raw = scrape(test_url)
    data = json.loads(raw)
    page = data[0]
    print(f"Title:    {page['title']}")
    print(f"Market:   {page['market']}  Locale: {page['locale']}")
    print(f"Sections: {len(page['sections'])}\n")
    for s in page["sections"]:
        heading = s["heading"] or "(intro)"
        preview = s["body"][:10000].replace("\n", " ")
        print(f"  [{heading}]\n  {preview}...\n")