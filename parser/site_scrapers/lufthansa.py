# Scraper for Lufthansa's accessible travel page and certain linked sub-pages
# Takes longer than other scrapers due to the need for slow, human-like interactions to bypass Lufthansa's aggressive bot-detection.

import asyncio
import logging
import random
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from parser.site_scrapers import register

log = logging.getLogger(__name__)

BASE_URL  = "https://www.lufthansa.com"
ENTRY_URL = "https://www.lufthansa.com/be/en/accessible-travel"

# If any of these strings appear in the rendered page we've hit the WAF wall.
_BOT_SIGNALS = [
    "unusual behaviour from your browser",
    "resembles that of a bot",
    "Sicherheitscheck",
    "Contrôle de sécurité",
]


# Timing helpers to mimic human reading / interaction patterns and avoid rate-limiting or bot-detection flags. Critical for success.
async def _pause(lo: float = 1.5, hi: float = 3.5) -> None:
    await asyncio.sleep(random.uniform(lo, hi))


# DOM readiness helper
async def _wait_for_content(page, timeout: float = 5.0) -> None:
    content_selectors = [
        "main",
        "[class*='content']",
        "h1",
        "article",
    ]
    try:
        await page.wait_for_selector(
            ", ".join(content_selectors),
            timeout=timeout * 1000,
            state="attached",
        )
    except Exception:
        pass   # cap reached — carry on anyway



# Browser context setup
async def _make_context(playwright):
    """
    Key anti-detection measures:
    • channel="chrome" uses the real Chrome binary when available, whose fingerprint is far harder to detect than Playwright's bundled Chromium.
    • --disable-blink-features=AutomationControlled removes the 'webdriver' flag that every WAF checks for.
    • Full locale + timezone matching the target region (Belgium / EN).
    • Stealth patches applied at context level so every tab inherits them.
    """
    browser = await playwright.chromium.launch(
        headless=True,
        channel="chrome", # real Chrome binary; falls back to Chromium
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
        locale="en-BE",
        timezone_id="Europe/Brussels",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        extra_http_headers={
            "Accept-Language": "en-GB,en;q=0.9,nl;q=0.8",
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

    # Apply stealth to the whole context — every page opened from it is covered
    context.on("page", lambda page: asyncio.ensure_future(Stealth().apply_stealth_async(page)))

    return browser, context


# Human-like interaction helpers
async def _human_mouse_wander(page) -> None:
    # Random mouse moves to signal a human is present.
    w = (page.viewport_size or {}).get("width",  1440)
    h = (page.viewport_size or {}).get("height",  900)
    for _ in range(random.randint(3, 6)):
        x = random.randint(100, w - 100)
        y = random.randint(100, h - 100)
        await page.mouse.move(x, y, steps=random.randint(8, 20))
        await _pause(0.2, 0.6)


async def _force_render(page) -> None:
    # Smooth, non-uniform scroll to trigger lazy-loading / hydration.
    for pct in [0.0, 0.25, 0.5, 0.75, 1.0]:
        await page.evaluate(
            f"window.scrollTo({{top: document.body.scrollHeight * {pct},"
            f" behavior: 'smooth'}});"
        )
        await _pause(0.8, 2.0)


async def _expand_accordions(page) -> None:
    # Click every accordion/collapsible trigger so its content is in the DOM.
    candidates = await page.query_selector_all(
        "maui-collapsible-item button, details summary"
    )
    clicked = 0
    for el in candidates:
        try:
            await el.scroll_into_view_if_needed()
            await _pause(0.15, 0.4)
            await el.click(force=True)
            clicked += 1
            await _pause(0.2, 0.5)
        except Exception:
            continue
    log.info(f"Expanded {clicked} accordion(s)")


# Bot-detection heuristics
def _is_bot_wall(html: str) -> bool:
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    return any(sig.lower() in text.lower() for sig in _BOT_SIGNALS)


# Content parsing heuristics
def _parse_html(html: str, url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    content = []
    for h in soup.find_all(["h1", "h2", "h3"]):
        block = []
        for sib in h.find_all_next():
            if sib.name in ["h1", "h2", "h3"]:
                break
            txt = sib.get_text(" ", strip=True)
            if txt and len(txt) > 20:
                block.append(txt)
        if block:
            content.append({
                "section": h.get_text(strip=True),
                "type":    "text",
                "content": " ".join(block)[:2000],
                "url":     url,
            })

    # Fallback: full page text
    if not content:
        content.append({
            "section": "page",
            "type":    "text",
            "content": soup.get_text(" ", strip=True)[:2000],
            "url":     url,
        })

    return content


def _format_sections(sections: list[dict], page_url: str) -> str:
    header = f"## Source: {page_url}\n"
    body   = "\n\n".join(f"### {s['section']}\n{s['content']}" for s in sections)
    return header + body


# Sub-URL discovery heuristics
def _extract_teaser_urls(html: str, base: str = BASE_URL) -> list[str]:
    # Return absolute Lufthansa URLs from `a.teaser.partner` anchors.
    soup = BeautifulSoup(html, "html.parser")
    urls = []
    for a in soup.select("a.teaser.partner[href]"):
        href = a["href"].strip()
        if not href or href.startswith("#"):
            continue
        absolute = href if href.startswith("http") else urljoin(base, href)
        if urlparse(absolute).netloc.endswith("lufthansa.com"):
            urls.append(absolute)
    return list(dict.fromkeys(urls))  # deduplicate, preserve order


# Page scraping workflow for sub-URLs
async def _scrape_page(context, url: str) -> str:
    # A new page per URL avoids the suspicious 'page navigated immediately after load' pattern that WAFs flag, and gives each request a clean referrer chain.
    log.info(f"  -> Scraping: {url}")
    page = await context.new_page()
    try:
        await page.goto(
            url,
            wait_until="load",
            timeout=15_000,
            referer=ENTRY_URL, # looks like natural internal navigation
        )
        await _wait_for_content(page)
        await _pause(2.0, 4.0)
        await _human_mouse_wander(page)
        await _force_render(page)
        await _expand_accordions(page)
        await _force_render(page)

        html = await page.content()

        if _is_bot_wall(html):
            log.warning(f"  x Bot-wall on {url} — skipping")
            return (
                f"## Source: {url}\n"
                "[BLOCKED] Bot-detection page returned; content unavailable."
            )

        return _format_sections(_parse_html(html, url), url)

    except Exception as exc:
        log.warning(f"  x Failed to scrape {url}: {exc}")
        return f"## Source: {url}\n[ERROR] {exc}"

    finally:
        await page.close()


# Public scraper function — synchronous entry point
@register("lufthansa.com")
def lufthansa_accessible_travel(url: str = ENTRY_URL) -> str:
    # Synchronous pipeline entry point — runs the async Playwright workflow.
    return asyncio.run(_run(url))


async def _run(entry_url: str) -> str:
    results: list[str] = []

    async with async_playwright() as pw:
        browser, context = await _make_context(pw)

        try:
            # Step 1: entry page
            log.info(f"[Lufthansa] Entry page: {entry_url}")
            entry_page = await context.new_page()

            await entry_page.goto(
                entry_url,
                wait_until="load",
                timeout=15_000,
            )
            await _wait_for_content(entry_page)
            await _pause(2.5, 5.0)
            await _human_mouse_wander(entry_page)
            await _force_render(entry_page)
            await _expand_accordions(entry_page)
            await _force_render(entry_page)

            entry_html = await entry_page.content()
            await entry_page.close()

            if _is_bot_wall(entry_html):
                log.error("[Lufthansa] Bot-wall on entry page — aborting.")
                return "[BLOCKED] Bot-detection triggered on the entry page."

            results.append(_format_sections(_parse_html(entry_html, entry_url), entry_url))

            # Step 2: discover sub-URLs from the teaser grid
            sub_urls = _extract_teaser_urls(entry_html)
            log.info(f"[Lufthansa] Found {len(sub_urls)} teaser URL(s): {sub_urls}")

            # Step 3: scrape each sub-page with a human-paced inter-page gap
            for sub_url in sub_urls:
                await _pause(3.0, 7.0)   # inter-page delay — critical
                result = await _scrape_page(context, sub_url)
                results.append(result)

        finally:
            await browser.close()

    return "\n\n" + ("─" * 80 + "\n\n").join(results)