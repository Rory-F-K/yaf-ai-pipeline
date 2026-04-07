# parser/remote/generic_scraper.py

import time
import random
from playwright.sync_api import sync_playwright

# A more generic scraper that doesn't attempt to bypass WAFs, but can still handle JS rendering and basic anti-bot measures.
def generic_scrape(url: str) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        )

        page = context.new_page()

        page.goto(url, timeout=60000)
        time.sleep(random.uniform(3, 5))

        html = page.content().lower()

        if any(x in html for x in [
            "captcha",
            "javascript is disabled",
            "awswafintegration"
        ]):
            browser.close()
            return ""

        text = page.evaluate("document.body.innerText || ''")

        browser.close()

        return text