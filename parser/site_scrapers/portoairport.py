from playwright.sync_api import sync_playwright
from parser.remote.section_extractor import extract_sections
from parser.site_scrapers import register
import time


def extract_page(page):
    sections = []

    buttons = page.query_selector_all(".accordion-button")
    for btn in buttons:
        try:
            btn.scroll_into_view_if_needed(timeout=50)
            btn.click(timeout=5)
            time.sleep(0.3)

            content = btn.evaluate_handle(
                "el => el.nextElementSibling || el.parentElement"
            )

            text = content.evaluate("el => el.innerText").strip()

            if text:
                sections.extend(extract_sections(text))

        except:
            continue

    if not sections:
        body = page.query_selector("body")
        if body:
            sections.extend(extract_sections(body.inner_text()))

    return sections


@register("portoairport.pt")
def porto_scraper(url: str) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.goto(url, timeout=60000)
        page.wait_for_timeout(3000)

        sections = extract_page(page)

        browser.close()

    return "\n\n".join(s["text"] for s in sections if "text" in s)