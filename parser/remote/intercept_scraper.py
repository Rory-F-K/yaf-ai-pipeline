# parser/remote/intercept_scraper.py
import random
import time
import os

from playwright.sync_api import sync_playwright

STATE_FILE = "browser_state.json"

# A more advanced scraper that attempts to bypass WAFs by mim

# Stealth techniques, human-like behavior, and capturing responses in-flight.
STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });

window.chrome = { runtime: {} };
"""

# This function checks for common WAF block indicators in the page content after loading.
def _wait_for_waf(page) -> bool:
    try:
        page.wait_for_load_state("domcontentloaded", timeout=15000)
        page.wait_for_timeout(random.randint(4000, 8000))

        content = page.content().lower()

        if any(x in content for x in [
            "javascript is disabled",
            "awswafintegration",
            "access denied"
        ]):
            return False

        return True
    except:
        return False

# The main function that performs the intercept scraping. It captures responses, handles downloads, and attempts to bypass WAFs.
def intercept_scrape(url: str, proxy: dict | None = None) -> tuple[str, dict]:
    collected = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            proxy=proxy,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage"
            ]
        )

        context = browser.new_context(
            storage_state=STATE_FILE if os.path.exists(STATE_FILE) else None,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="Australia/Melbourne",
        )

        context.add_init_script(STEALTH_SCRIPT)

        page = context.new_page()

        # warmup
        page.goto("about:blank")
        page.mouse.move(200, 300)
        page.wait_for_timeout(random.randint(500, 1200))

        MAX_CAPTURE = 25

        def handle_response(response):
            if len(collected) >= MAX_CAPTURE:
                return

            try:
                ct = response.headers.get("content-type", "")

                if "application/json" in ct or "text/html" in ct:
                    body = response.text()

                    if body and len(body) > 800:
                        collected.append(body)

            except:
                pass

        page.on("response", handle_response)

        ## download handling (for PDFs or other direct file responses)
        try:
            with page.expect_download(timeout=5000) as dl:
                page.goto(url, timeout=60000)
        except:
            page.goto(url, timeout=60000)
            dl = None

        if dl:
            download = dl.value
            path = download.path()

            with open(path, "rb") as f:
                return f.read().decode(errors="ignore"), {}

        # WAF detection and fallback
        if not _wait_for_waf(page):
            print("[WAF] Blocked")
            browser.close()
            return "", {}

        # behavior
        try:
            page.mouse.move(100, 200)
            page.keyboard.press("PageDown")
            page.wait_for_timeout(random.randint(1500, 2500))
        except:
            pass

        time.sleep(random.uniform(2, 4))

        # DOM capture as a last resort if we haven't collected enough from responses. This can sometimes bypass WAFs that block API responses but still render content.
        if len(collected) < 3:
            try:
                dom = page.evaluate("document.body.innerText || ''")
                if len(dom) > 500:
                    collected.append(dom)
            except:
                pass

        cookies = context.cookies()
        context.storage_state(path=STATE_FILE)

        browser.close()

    return "\n\n".join(collected), {"cookies": cookies}