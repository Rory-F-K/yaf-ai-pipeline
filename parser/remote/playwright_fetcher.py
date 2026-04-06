# parser/remote/playwright_fetcher.py
from playwright.sync_api import sync_playwright

def fetch_html(url, timeout=30000, headless=True):
    """
    Fetch HTML content from a URL using Playwright.
    Only use for HTML pages, not PDFs.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        print(f"[Playwright] Loading: {url}")
        page.goto(url, timeout=timeout, wait_until="networkidle")
        html = page.content()
        browser.close()
        return html