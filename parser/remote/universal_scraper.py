"""
parser/remote/universal_scraper.py

Extracts clean plain text from any URL.

Cascade:
  1. httpx + trafilatura        fast static path (~200 ms)
  2. Playwright + trafilatura   JS-heavy pages   (~3-8 s)
       Phase 1 — JS DOM injection : reveals all hidden accordion/tab content
                                    without clicking (single-use, multi-use,
                                    and nested accordions all handled)
       Phase 2 — lazy-click loop  : clicks remaining triggers whose content
                                    is not yet in the DOM (network-loaded)

Bot walls (Akamai, Cloudflare, etc.) are detected in raw HTML before
trafilatura runs.  When a challenge page is found after Playwright renders,
the scraper waits up to 8 s for the challenge JS to auto-resolve before
giving up, covering the most common managed-challenge patterns.

Returns ScrapeResult(text, html, via) so remote_ingest can use the raw HTML
for BFS link extraction without an extra network request.
"""

from __future__ import annotations
from dataclasses import dataclass, field

import httpx
import trafilatura
from playwright.sync_api import sync_playwright, Browser, BrowserContext, Playwright

try:
    from playwright_stealth import Stealth as _Stealth
    _stealth = _Stealth()
    _STEALTH_AVAILABLE = True
except ImportError:
    _stealth = None
    _STEALTH_AVAILABLE = False
    print("[Warning] playwright-stealth not installed — bot evasion disabled.")
    print("          pip install git+https://github.com/Mattwmaster58/playwright_stealth.git")


# Result type

@dataclass
class ScrapeResult:
    text: str
    html: str = field(default="")
    via:  str = field(default="failed")   # "static" | "playwright" | "failed"


# Constants

MIN_CONTENT_LENGTH   = 200
BLOCKED_RESOURCES    = {"image", "font", "media"}
CHALLENGE_WAIT_MS    = 4000   # initial wait for challenge JS to run
CHALLENGE_RETRY_MS   = 3000   # second wait if still blocked after scroll
CHALLENGE_MAX_TRIES  = 2      # how many times to wait before giving up

# Signals that the page needs a real browser (JS rendering required)
JS_SIGNALS = [
    'aria-expanded="false"',
    'data-state="closed"',
    'data-state="inactive"',
    "<details",
    "window.__NEXT_DATA__",
    "window.__NUXT__",
    "__vue_app__",
    "ng-version",
    "data-reactroot",
    "svelte-announcer",
]

# Signals that indicate a bot-wall / security challenge page.
# Checked against a lowercased slice of the raw HTML — never against
# trafilatura output — so challenge text never reaches the pipeline.
_BOT_WALL_SIGNALS = [
    "verifies you are not a bot",
    "not a bot",
    "are you a robot",
    "performing security verification",
    "security verification",
    "security service to protect",
    "protect against malicious bots",
    "malicious bots",
    "while the website verifies",
    "checking your browser",
    "enable javascript and cookies",
    "browser security check",
    "ddos protection",
    "cloudflare ray id",
    "cf-browser-verification",
    "one more step",
    "_cf_chl", # Cloudflare challenge token in HTML
    "ak_bmsc", # Akamai bot manager cookie name leaks into some pages
]

# Realistic browser context settings — used for every Playwright page.
# Matching a common Windows + Chrome profile reduces bot-manager fingerprint hits.
_CONTEXT_SETTINGS = dict(
    viewport           = {"width": 1440, "height": 900},
    device_scale_factor= 1,
    locale             = "en-GB",
    timezone_id        = "Europe/London",
    user_agent         = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    extra_http_headers = {
        "Accept-Language": "en-GB,en;q=0.9",
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "sec-ch-ua": (
            '"Chromium";v="124","Google Chrome";v="124","Not-A.Brand";v="99"'
        ),
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
    },
)


# DOM injection script
# Injected after page load. Directly reveals all content that is in the DOM
# but hidden by CSS / JS state — without clicking anything.
# Covers: native <details>, aria-controls, aria-expanded, data-state
# (Radix/shadcn/Ark/Kobalte), Headless UI, Bootstrap 4+5, ARIA tab panels,
# custom data-expanded/open/collapsed attributes, scoped aria-hidden panels,
# CSS override layer.

_DOM_REVEAL_SCRIPT = """
(function () {
    'use strict';

    function show(el) {
        if (!el) return;
        el.removeAttribute('hidden');
        el.setAttribute('aria-hidden', 'false');
        const s = el.style;
        s.setProperty('display',        'block',   'important');
        s.setProperty('visibility',     'visible', 'important');
        s.setProperty('height',         'auto',    'important');
        s.setProperty('max-height',     'none',    'important');
        s.setProperty('overflow',       'visible', 'important');
        s.setProperty('opacity',        '1',       'important');
        s.setProperty('clip-path',      'none',    'important');
        s.setProperty('transform',      'none',    'important');
        s.setProperty('pointer-events', 'auto',    'important');
    }

    // 1. Native <details>
    document.querySelectorAll('details:not([open])').forEach(d => {
        d.setAttribute('open', '');
    });

    // 2. aria-controls — value may be space-separated list of IDs
    document.querySelectorAll('[aria-controls]').forEach(trigger => {
        trigger.getAttribute('aria-controls')
            .trim().split(/\\s+/)
            .forEach(id => show(document.getElementById(id)));
        trigger.setAttribute('aria-expanded', 'true');
        trigger.setAttribute('aria-selected', 'true');
    });

    // 3. aria-expanded="false" — update state and locate panel
    document.querySelectorAll('[aria-expanded="false"]').forEach(trigger => {
        trigger.setAttribute('aria-expanded', 'true');
        const ns = trigger.nextElementSibling;
        if (ns) show(ns);
        const ps = trigger.parentElement && trigger.parentElement.nextElementSibling;
        if (ps && ps !== ns) show(ps);
        const container = trigger.closest(
            '[class*="accordion-item"],[class*="faq-item"],' +
            '[class*="expandable-item"],[class*="disclosure-item"],' +
            '[class*="collapse-item"],[class*="panel-item"]'
        );
        if (container) {
            container.querySelectorAll(
                '[class*="panel"],[class*="content"],[class*="body"],' +
                '[class*="answer"],[class*="collapse"],[class*="detail"]'
            ).forEach(show);
        }
    });

    // 4. data-state (Radix UI, shadcn/ui, Ark UI, Kobalte)
    document.querySelectorAll('[data-state="closed"],[data-state="inactive"]').forEach(el => {
        el.setAttribute('data-state', 'open');
        show(el);
        const ns = el.nextElementSibling;
        if (ns) show(ns);
    });

    // 5. Headless UI v1
    document.querySelectorAll('[data-headlessui-state~="closed"]').forEach(el => {
        el.setAttribute('data-headlessui-state', 'open');
        show(el);
        const ns = el.nextElementSibling;
        if (ns) show(ns);
    });

    // 6. Bootstrap 4 + 5
    document.querySelectorAll('.collapse:not(.show)').forEach(el => {
        el.classList.add('show');
        show(el);
    });
    document.querySelectorAll('.accordion-button.collapsed').forEach(btn => {
        btn.classList.remove('collapsed');
        btn.setAttribute('aria-expanded', 'true');
    });

    // 7. ARIA tab panels — reveal all tabs, not just the active one
    document.querySelectorAll('[role="tabpanel"]').forEach(show);
    document.querySelectorAll('[role="tab"]').forEach(tab => {
        tab.setAttribute('aria-selected', 'true');
        tab.setAttribute('aria-expanded', 'true');
    });

    // 8. Custom data-attribute patterns
    document.querySelectorAll(
        '[data-expanded="false"],[data-open="false"],[data-collapsed="true"]'
    ).forEach(el => {
        el.setAttribute('data-expanded', 'true');
        el.setAttribute('data-open',     'true');
        el.setAttribute('data-collapsed','false');
        show(el);
        const ns = el.nextElementSibling;
        if (ns) show(ns);
    });

    // 9. aria-hidden panels scoped to accordion containers
    const SCOPES = [
        '[class*="accordion"]','[class*="collapse"]','[class*="faq"]',
        '[class*="expandable"]','[class*="disclosure"]','[role="tablist"]',
    ].join(',');
    document.querySelectorAll(SCOPES).forEach(scope => {
        scope.querySelectorAll('[aria-hidden="true"]').forEach(show);
    });
    document.querySelectorAll('[role="tabpanel"][aria-hidden="true"]').forEach(show);
    document.querySelectorAll('details > *:not(summary)').forEach(show);

    // 10. CSS override layer
    const style = document.createElement('style');
    style.setAttribute('data-injected', 'universal-scraper');
    style.textContent = `
        details > *:not(summary)        { display:block !important; }
        .accordion-collapse             { display:block !important; height:auto !important; }
        .tab-pane                       { display:block !important; opacity:1 !important; }
        [data-state="open"]             { display:block !important; }
        [data-radix-accordion-content]  { display:block !important; height:auto !important; }
        [data-radix-tabs-content]       { display:block !important; }
        [data-headlessui-state~="open"] { display:block !important; }
        .is-hidden, .is-collapsed       { display:block !important; visibility:visible !important; }
    `;
    document.head.appendChild(style);
})();
"""

_LAZY_CLICK_SELECTORS = [
    "button[aria-expanded='false']",
    "a[aria-expanded='false']",
    "[role='button'][aria-expanded='false']",
    "[data-toggle='collapse'][aria-expanded='false']",
    "[data-bs-toggle='collapse']:not([aria-expanded='true'])",
    "[data-accordion-trigger]:not([aria-expanded='true'])",
]


# Bot wall detection

def _is_bot_wall(html: str) -> bool:
    sample = html[:5000].lower()
    return any(sig in sample for sig in _BOT_WALL_SIGNALS)


# Browser singleton

class _BrowserPool:
    """
    Single browser instance for the process lifetime.
    Tries Google Chrome first (better bot evasion than bundled Chromium),
    falls back to standard Chromium if Chrome is not installed.
    """
    _pw:      Playwright | None = None
    _browser: Browser    | None = None

    _LAUNCH_ARGS = [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
        "--disable-infobars",
        "--disable-extensions",
        "--disable-plugins-discovery",
    ]

    @classmethod
    def get(cls) -> Browser:
        if cls._browser is None or not cls._browser.is_connected():
            if cls._pw:
                try:
                    cls._pw.stop()
                except Exception:
                    pass
            cls._pw = sync_playwright().start()

            # Prefer the installed Google Chrome binary — it has a more complete and realistic fingerprint than bundled Chromium.
            for channel, label in [("chrome", "Google Chrome"), (None, "Chromium")]:
                try:
                    kwargs = dict(headless=True, args=cls._LAUNCH_ARGS)
                    if channel:
                        kwargs["channel"] = channel
                    cls._browser = cls._pw.chromium.launch(**kwargs)
                    print(f"[Browser] {label} launched")
                    break
                except Exception:
                    if channel is None:
                        raise # Chromium fallback also failed — re-raise

        return cls._browser

    @classmethod
    def close(cls) -> None:
        try:
            if cls._browser:
                cls._browser.close()
            if cls._pw:
                cls._pw.stop()
        except Exception:
            pass
        finally:
            cls._browser = None
            cls._pw      = None


# Internal helpers

def _fetch_raw(url: str) -> str | None:
    try:
        r = httpx.get(
            url, timeout=10, follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        return r.text if r.status_code == 200 else None
    except Exception as e:
        print(f"[HTTP] {url}: {e}")
        return None


def _needs_headless(html: str) -> bool:
    return len(html) < 5000 or any(sig in html for sig in JS_SIGNALS)


def _run_lazy_clicks(page) -> int:
    total = 0
    for pass_num in range(5):
        clicked = 0
        for selector in _LAZY_CLICK_SELECTORS:
            for el in page.query_selector_all(selector):
                try:
                    if el.is_visible():
                        el.click()
                        try:
                            page.wait_for_load_state("networkidle", timeout=2000)
                        except Exception:
                            pass
                        clicked += 1
                except Exception:
                    pass
        total += clicked
        if clicked == 0:
            break
        print(f"[Phase 2] Pass {pass_num + 1}: {clicked} lazy trigger(s) clicked")
    return total


def _make_context(browser: Browser) -> BrowserContext:
    """Create a fresh browser context with realistic fingerprint settings."""
    return browser.new_context(**_CONTEXT_SETTINGS)


def _wait_for_challenge(page, url: str) -> bool:
    """
    After detecting a bot-wall, give the challenge JS time to auto-resolve.
    Some managed-challenge services (Cloudflare Managed Challenge, Akamai
    lower-tier) complete automatically when the browser passes fingerprint
    checks — they just need a few seconds and minimal interaction.

    Returns True if the challenge resolved (page is no longer a bot wall).
    Returns False if the page is still blocked after all retries.
    """
    for attempt in range(1, CHALLENGE_MAX_TRIES + 1):
        wait_ms = CHALLENGE_WAIT_MS if attempt == 1 else CHALLENGE_RETRY_MS
        print(f"[Challenge] Waiting {wait_ms // 1000}s for auto-resolution "
              f"(attempt {attempt}/{CHALLENGE_MAX_TRIES})")

        page.wait_for_timeout(wait_ms)

        # Gentle scroll — provides a minimal human-like signal
        try:
            page.evaluate(
                "window.scrollTo({top: document.body.scrollHeight * 0.3,"
                " behavior: 'smooth'})"
            )
            page.wait_for_timeout(800)
        except Exception:
            pass

        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass

        if not _is_bot_wall(page.content()):
            print(f"[Challenge] Resolved after attempt {attempt}")
            return True

    return False


def _fetch_with_playwright(url: str) -> str | None:
    context = None
    try:
        browser = _BrowserPool.get()
        context = _make_context(browser)
        page    = context.new_page()

        if _STEALTH_AVAILABLE:
            _stealth.apply_stealth_sync(page)

        page.route(
            "**/*",
            lambda route: (
                route.abort()
                if route.request.resource_type in BLOCKED_RESOURCES
                else route.continue_()
            ),
        )

        page.goto(url, wait_until="networkidle", timeout=20000)

        # If we landed on a challenge page, attempt to wait it out
        if _is_bot_wall(page.content()):
            print(f"[Playwright] Challenge page detected — attempting auto-resolution")
            resolved = _wait_for_challenge(page, url)
            if not resolved:
                print(f"[Playwright] Challenge did not resolve: {url}")
                html = page.content()
                return html   # caller checks _is_bot_wall on the returned HTML

        # Phase 1: DOM reveal
        page.evaluate(_DOM_REVEAL_SCRIPT)
        page.wait_for_timeout(400)

        # Phase 2: lazy-click
        lazy = _run_lazy_clicks(page)
        if lazy:
            print(f"[Playwright] {lazy} lazy click(s): {url}")

        return page.content()

    except Exception as e:
        print(f"[Playwright] Failed {url}: {e}")
        return None

    finally:
        if context:
            try:
                context.close()
            except Exception:
                pass


def _extract(html: str) -> str:
    return trafilatura.extract(
        html,
        include_tables=True,
        include_links=False,
        no_fallback=False,
        deduplicate=True,
        favor_recall=True,
    ) or ""


# Public interface

def universal_scrape(url: str) -> ScrapeResult:
    """
    Scrape any URL. Returns ScrapeResult(text, html, via).

    .text  — clean plain text for the pipeline
    .html  — raw HTML for BFS link extraction in remote_ingest
    .via   — "static" | "playwright" | "failed"
    """
    raw_html = _fetch_raw(url)

    # Static path — only for pages with no JS signals and no bot wall
    if raw_html:
        if _is_bot_wall(raw_html):
            print(f"[Universal] Bot wall on static fetch — escalating: {url}")
        elif not _needs_headless(raw_html):
            text = _extract(raw_html)
            if len(text) >= MIN_CONTENT_LENGTH:
                print(f"[Universal] Static → {len(text)} chars  {url}")
                return ScrapeResult(text=text, html=raw_html, via="static")
            print(f"[Universal] Static thin ({len(text)} chars) — escalating: {url}")

    # Playwright path
    expanded_html = _fetch_with_playwright(url)
    if expanded_html:
        if _is_bot_wall(expanded_html):
            print(f"[Universal] Bot wall after Playwright render: {url}")
            return ScrapeResult(text="", html=expanded_html, via="failed")

        text = _extract(expanded_html)
        if len(text) >= MIN_CONTENT_LENGTH:
            print(f"[Universal] Playwright → {len(text)} chars  {url}")
            return ScrapeResult(text=text, html=expanded_html, via="playwright")

    print(f"[Universal] All paths failed: {url}")
    return ScrapeResult(text="", html=raw_html or "", via="failed")