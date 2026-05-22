"""
Handles static HTML sites, JS-heavy sites (React, Vue etc.), and bot-protected
sites via ScraperAPI fallback.

Fetch strategy per URL (in order):
  1. Fast static fetch (requests) - free, very fast
  2. If thin content or 403 → Playwright headless browser - free, handles JS
  3. If still blocked → ScraperAPI - paid, bypasses bot protection

On completion writes 3 files:
  - scraped_output.json   - full results for every URL
  - failed_urls.txt       - clean list of URLs that need manual handling
  - scrape_report.txt     - summary of what happened and why

Usage:
    python scraper.py                          # uses URL_LIST below
    python scraper.py urls.txt                 # one URL per line
    python scraper.py urls.txt output.json     # custom output file
"""

# TODO:
# - Check if formatting is consistent with other scrapers
# - Test if output is consistent with hardcoded scrapers
# - Potentially relocate URL list to seperate file


from __future__ import annotations
import json, time, sys, re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout


# ── Configuration ─────────────────────────────────────────────────────────────

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

SCRAPER_API_KEY = "585fd135f09d164143582f1f7d58dd6d" # Not following good practises by leaving this here however its a free tier for testing purposes. Would be updated if used going forward.

DELAY           = 1.5       # Seconds between requests
TIMEOUT_HTTP    = 15        # Seconds for plain requests
TIMEOUT_PW      = 25_000   # Milliseconds for Playwright
TIMEOUT_API     = 60        # Seconds for ScraperAPI

JS_RENDER_THRESHOLD = 50    # Word count below this value: assume JS rendering needed

OUTPUT_FILE = "scraped_output.json"

URL_LIST = [
    # URL's to scrape

    # YAF Sources
    "https://www.swiss.com/ch/en/prepare/special-care/accessible-travel.html",
    "https://help.ryanair.com/hc/en-lv/categories/12489466690833",
    "https://help.vueling.com/hc/en-gb/articles/30891224305425-Wheelchair-Check-in",

    # European carriers
    "https://www.easyjet.com/en/help/boarding-and-flying/special-assistance",
    "https://www.lufthansa.com/us/en/accessible-travel",
    "https://www.klm.com/information/assistance-health/mobility-assistance",
    "https://www.britishairways.com/content/information/disability-assistance",

    # Middle East / Asia-Pacific
    "https://www.emirates.com/us/english/before-you-fly/health/accessible-travel/",
    "https://www.singaporeair.com/en_UK/us/travel-info/special-assistance/disability-assistance/",
    "https://www.qantas.com/us/en/travel-info/specific-needs.html",

    # North American
    "https://www.aa.com/i18n/travel-info/special-assistance/special-assistance.jsp",
    "https://www.jetblue.com/at-the-airport/accessibility-assistance/mobility-assistance",

    # Zendesk help centre (like Ryanair/Vueling)
    "https://help.virginatlantic.com/gb/en/special-assistance.html",
]

# ── Failure reasons (used in report) ─────────────────────────────────────────

FAIL_ROBOTS   = "robots_blocked"       # robots.txt explicitly disallows
FAIL_AKAMAI   = "enterprise_protection" # 403 even with Playwright + ScraperAPI
FAIL_API      = "scraperapi_failed"    # ScraperAPI configured but returned error
FAIL_NO_API   = "scraperapi_not_configured"  # Would need API key but none set
FAIL_TIMEOUT  = "timeout"
FAIL_NETWORK  = "network_error"
FAIL_UNKNOWN  = "unknown_error"


# ── robots.txt ────────────────────────────────────────────────────────────────

ROBOTS_ALLOWED = "allowed"
ROBOTS_BLOCKED = "blocked"
ROBOTS_UNKNOWN = "unknown"

def check_robots(base_url: str, url: str) -> str:
    robots_url = urljoin(base_url, "/robots.txt")
    try:
        r = requests.get(robots_url, headers={"User-Agent": USER_AGENT}, timeout=10)
        if r.status_code != 200:
            return ROBOTS_UNKNOWN
        parser = RobotFileParser()
        parser.set_url(robots_url)
        parser.parse(r.text.splitlines())
        return ROBOTS_ALLOWED if parser.can_fetch(USER_AGENT, url) else ROBOTS_BLOCKED
    except Exception:
        return ROBOTS_UNKNOWN


# ── Content extraction ────────────────────────────────────────────────────────

def extract_content(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "head",
                     "nav", "footer", "aside", "form", "button", "iframe"]):
        tag.decompose()

    title     = (soup.title.string or "").strip() if soup.title else ""
    meta_tag  = soup.find("meta", attrs={"name": "description"})
    meta_desc = (meta_tag.get("content") or "").strip() if meta_tag else ""
    raw_text  = soup.get_text(separator=" ", strip=True)
    clean_text = re.sub(r"\s+", " ", raw_text).strip()

    base = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(url))
    links = list(dict.fromkeys(
        href if href.startswith("http") else urljoin(base, href)
        for a in soup.find_all("a", href=True)
        if (href := a["href"].strip()) and (href.startswith("http") or href.startswith("/"))
    ))

    return {
        "title":            title,
        "meta_description": meta_desc,
        "text":             clean_text,
        "links_found":      links,
        "word_count":       len(clean_text.split()),
    }


# ── Fetch strategies ──────────────────────────────────────────────────────────

def fetch_static(url: str) -> tuple[str | None, int | None, str | None]:
    try:
        r = requests.get(url, headers={
            "User-Agent":      USER_AGENT,
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }, timeout=TIMEOUT_HTTP)
        return r.text, r.status_code, None
    except requests.exceptions.Timeout:
        return None, None, "timeout"
    except requests.exceptions.ConnectionError as e:
        return None, None, f"network_error: {e}"
    except Exception as e:
        return None, None, str(e)


def fetch_with_playwright(url: str, pw_instance) -> tuple[str | None, int | None, str | None]:
    browser = None
    try:
        browser = pw_instance.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page    = context.new_page()
        page.goto(url, wait_until="networkidle", timeout=TIMEOUT_PW)
        page.wait_for_timeout(1500)
        return page.content(), 200, None
    except PWTimeout:
        return None, None, "timeout"
    except Exception as e:
        return None, None, f"playwright_error: {e}"
    finally:
        if browser:
            browser.close()


def fetch_with_scraperapi(url: str) -> tuple[str | None, int | None, str | None]:
    if not SCRAPER_API_KEY or SCRAPER_API_KEY == "YOUR_API_KEY_HERE":
        return None, None, "not_configured"
    try:
        r = requests.get("http://api.scraperapi.com", params={
            "api_key": SCRAPER_API_KEY,
            "url":     url,
            "render":  "true",
            "premium": "true",
        }, timeout=TIMEOUT_API)
        if r.status_code == 200:
            return r.text, 200, None
        return None, r.status_code, f"http_{r.status_code}"
    except Exception as e:
        return None, None, str(e)


# ── Per-URL orchestration ─────────────────────────────────────────────────────

def scrape_url(url: str, robots_cache: dict, pw_instance) -> dict:
    parsed   = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    result = {
        "url":              url,
        "scraped_at":       datetime.now(timezone.utc).isoformat(),
        "status":           None,
        "http_status_code": None,
        "robots_allowed":   None,
        "render_method":    None,
        "failure_reason":   None,   # populated when status == "error" or "blocked"
        "title":            "",
        "meta_description": "",
        "text":             "",
        "word_count":       0,
        "links_found":      [],
        "error":            None,
    }

    # ── Step 1: robots.txt ────────────────────────────────────────────────────
    if base_url not in robots_cache:
        print(f"  [robots] Checking {base_url}/robots.txt")
        robots_cache[base_url] = check_robots(base_url, url)

    robots_status = robots_cache[base_url]
    if robots_status == ROBOTS_BLOCKED:
        result["robots_allowed"]  = False
        result["status"]          = "blocked"
        result["failure_reason"]  = FAIL_ROBOTS
        print(f"  [blocked] robots.txt explicitly disallows this URL")
        return result
    result["robots_allowed"] = None if robots_status == ROBOTS_UNKNOWN else True
    if robots_status == ROBOTS_UNKNOWN:
        print(f"  [robots] Could not fetch robots.txt — proceeding anyway")

    # ── Step 2: Static fetch ──────────────────────────────────────────────────
    html, status, error = fetch_static(url)
    result["http_status_code"] = status

    if not error and status == 200:
        content = extract_content(html, url)
        if content["word_count"] >= JS_RENDER_THRESHOLD:
            result.update(content)
            result["status"]        = "success"
            result["render_method"] = "static"
            print(f"  [ok] {content['word_count']} words via static — \"{content['title'][:55]}\"")
            return result
        print(f"  [js] Only {content['word_count']} words in static HTML — trying Playwright...")
    else:
        print(f"  [warn] Static failed ({error or f'HTTP {status}'}) — trying Playwright...")

    # ── Step 3: Playwright ────────────────────────────────────────────────────
    html, _, pw_error = fetch_with_playwright(url, pw_instance)
    if not pw_error and html:
        content = extract_content(html, url)
        if content["word_count"] >= JS_RENDER_THRESHOLD:
            result.update(content)
            result["status"]        = "success"
            result["render_method"] = "playwright"
            print(f"  [ok] {content['word_count']} words via Playwright — \"{content['title'][:55]}\"")
            return result
        print(f"  [warn] Playwright got {content['word_count']} words — trying ScraperAPI...")
    else:
        print(f"  [warn] Playwright failed ({pw_error}) — trying ScraperAPI...")

    # ── Step 4: ScraperAPI ────────────────────────────────────────────────────
    api_configured = SCRAPER_API_KEY and SCRAPER_API_KEY != "YOUR_API_KEY_HERE"
    if not api_configured:
        print(f"  [skip] ScraperAPI not configured")
    else:
        print(f"  [api] Routing through ScraperAPI...")
        html, api_status, api_error = fetch_with_scraperapi(url)
        if not api_error and html:
            content = extract_content(html, url)
            if content["word_count"] >= JS_RENDER_THRESHOLD:
                result.update(content)
                result["status"]           = "success"
                result["render_method"]    = "scraperapi"
                result["http_status_code"] = api_status
                print(f"  [ok] {content['word_count']} words via ScraperAPI — \"{content['title'][:55]}\"")
                return result
        else:
            print(f"  [warn] ScraperAPI failed ({api_error})")

    # ── All methods failed — classify why ────────────────────────────────────
    result["status"] = "error"

    if not api_configured:
        result["failure_reason"] = FAIL_NO_API
        result["error"] = "Blocked by bot protection. Configure ScraperAPI key to retry."
    elif pw_error and "timeout" in pw_error:
        result["failure_reason"] = FAIL_TIMEOUT
        result["error"] = "Page timed out across all methods."
    elif error and "network" in (error or ""):
        result["failure_reason"] = FAIL_NETWORK
        result["error"] = f"Network error: {error}"
    else:
        result["failure_reason"] = FAIL_AKAMAI
        result["error"] = (
            "Site uses enterprise bot protection (e.g. Akamai) that defeats all "
            "automated methods. Requires manual copy-paste."
        )

    print(f"  [error] {result['error']}")
    return result


# ── Report writers ────────────────────────────────────────────────────────────

def write_failed_urls(results: list[dict], output_path: str):
    """Write a plain list of failed URLs — easy to hand to the client."""
    failed = [r for r in results if r["status"] in ("error", "blocked")]
    if not failed:
        return

    path = Path(output_path).with_name("failed_urls.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# Failed URLs — {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"# These {len(failed)} URL(s) could not be scraped automatically.\n")
        f.write(f"# Reason codes: robots_blocked | enterprise_protection | scraperapi_failed | timeout\n\n")
        for r in failed:
            f.write(f"{r['url']}\n")
            f.write(f"  reason: {r['failure_reason']}\n")
            f.write(f"  detail: {r['error']}\n\n")
    print(f"  Failed URLs → {path}")


def write_report(results: list[dict], summary: dict, output_path: str):
    """Write a human-readable plain text report."""
    path = Path(output_path).with_name("scrape_report.txt")
    failed  = [r for r in results if r["status"] in ("error", "blocked")]
    success = [r for r in results if r["status"] == "success"]

    with open(path, "w", encoding="utf-8") as f:
        f.write("=" * 62 + "\n")
        f.write("  SCRAPE REPORT\n")
        f.write(f"  {summary['finished_at']}\n")
        f.write("=" * 62 + "\n\n")

        f.write(f"Total URLs:  {summary['total_urls']}\n")
        f.write(f"Successful:  {summary['successful']}\n")
        f.write(f"Failed:      {len(failed)}\n\n")

        f.write("Method breakdown (successful URLs):\n")
        f.write(f"  Static fetch:  {summary['method_breakdown']['static']}\n")
        f.write(f"  Playwright:    {summary['method_breakdown']['playwright']}\n")
        f.write(f"  ScraperAPI:    {summary['method_breakdown']['scraperapi']}\n\n")

        if success:
            f.write("-" * 62 + "\n")
            f.write("SUCCESSFULLY SCRAPED\n")
            f.write("-" * 62 + "\n")
            for r in success:
                f.write(f"\n  {r['url']}\n")
                f.write(f"  Method: {r['render_method']} | Words: {r['word_count']}\n")
                f.write(f"  Title:  {r['title'] or '(no title)'}\n")

        if failed:
            f.write("\n" + "─" * 62 + "\n")
            f.write("FAILED — MANUAL ACTION REQUIRED\n")
            f.write("─" * 62 + "\n")
            for r in failed:
                f.write(f"\n  {r['url']}\n")
                f.write(f"  Reason: {r['failure_reason']}\n")
                f.write(f"  Detail: {r['error']}\n")
                if r["failure_reason"] == FAIL_AKAMAI:
                    f.write(f"  Action: Visit this URL in a browser, select all text (Ctrl+A),\n")
                    f.write(f"          copy and paste into the JSON as the 'text' field.\n")
                elif r["failure_reason"] == FAIL_ROBOTS:
                    f.write(f"  Action: robots.txt disallows scraping: check ToS before\n")
                    f.write(f"          manually copying content from this site.\n")

    print(f"  Report       → {path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(urls: list[str], output_path: str):
    api_configured = SCRAPER_API_KEY and SCRAPER_API_KEY != "YOUR_API_KEY_HERE"

    print(f"\n{'='*62}")
    print(f"  Web Scraper:  {len(urls)} URL(s)")
    print(f"  ScraperAPI:  {'✓ configured' if api_configured else '✗ not set'}")
    print(f"  Output    →  {output_path}")
    print(f"{'='*62}\n")

    results      = []
    robots_cache = {}

    with sync_playwright() as pw:
        for i, url in enumerate(urls, 1):
            print(f"[{i}/{len(urls)}] {url}")
            result = scrape_url(url, robots_cache, pw)
            results.append(result)
            if i < len(urls):
                time.sleep(DELAY)

    summary = {
        "started_at":       results[0]["scraped_at"] if results else None,
        "finished_at":      datetime.now(timezone.utc).isoformat(),
        "total_urls":       len(results),
        "successful":       sum(1 for r in results if r["status"] == "success"),
        "blocked":          sum(1 for r in results if r["status"] == "blocked"),
        "errors":           sum(1 for r in results if r["status"] == "error"),
        "method_breakdown": {
            "static":     sum(1 for r in results if r.get("render_method") == "static"),
            "playwright": sum(1 for r in results if r.get("render_method") == "playwright"),
            "scraperapi": sum(1 for r in results if r.get("render_method") == "scraperapi"),
        }
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"scrape_run": summary, "results": results}, f, indent=2, ensure_ascii=False)

    failed_count = summary["blocked"] + summary["errors"]
    print(f"\n{'='*62}")
    print(f"  ✓ Scraped:   {summary['successful']} URL(s)")
    print(f"  ✗ Failed:    {failed_count} URL(s) need manual handling")
    print(f"\n  Output files:")
    print(f"  JSON data    → {output_path}")
    write_failed_urls(results, output_path)
    write_report(results, summary, output_path)
    print(f"{'='*62}\n")

    if failed_count:
        print(f"  ⚠  {failed_count} URL(s) could not be scraped automatically.")
        print(f"     See failed_urls.txt and scrape_report.txt for instructions.\n")


if __name__ == "__main__":
    urls = URL_LIST
    out  = OUTPUT_FILE

    if len(sys.argv) >= 2:
        with open(sys.argv[1]) as f:
            urls = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    if len(sys.argv) >= 3:
        out = sys.argv[2]

    run(urls, out)
