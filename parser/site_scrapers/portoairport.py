import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from playwright.sync_api import sync_playwright
from parser.remote.section_extractor import extract_sections
from parser.site_scrapers import register
import time

def extract_page(page):
    sections = []

    # Page title / breadcrumb heading
    h2 = page.query_selector(".content-header h2")
    h1 = page.query_selector(".content-header h1")
    page_title = (h1 or h2)
    if page_title:
        title_text = page_title.inner_text().strip()
        print(f"[debug] Page title: {title_text!r}")

    # Intro body text (excludes the empty top_body_tabs div)
    for intro_el in page.query_selector_all(".content-header .accordion-content"):
        if "top_body_tabs" in (intro_el.get_attribute("class") or ""):
            continue
        intro_text = intro_el.inner_text().strip()
        if intro_text:
            sections.append({"title": page_title and page_title.inner_text().strip() or "Introduction", "content": intro_text})

    # All accordion items — read directly from DOM, no clicking needed
    items = page.query_selector_all("li.accordion-box")
    print(f"[debug] Found {len(items)} accordion items")

    for i, item in enumerate(items):
        try:
            title_el = item.query_selector(".accordion-title")
            content_el = item.query_selector(".accordion-content")

            title = title_el.inner_text().strip() if title_el else ""
            # inner_text() returns empty for hidden elements — use innerHTML + evaluate instead
            content = item.evaluate(
                "el => el.querySelector('.accordion-content')?.innerText"
            ) or ""
            content = content.strip()

            print(f"[debug] Item {i}: {title!r} -> {content[:60]!r}")

            if content:
                sections.append({"title": title, "content": content})

        except Exception as e:
            # print(f"[debug] Item {i} error: {e}")
            continue

    print(f"[debug] Total sections: {len(sections)}")
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

    return "\n\n".join(
        f"{s['title']}\n{s['content']}" for s in sections if s.get("content")
    )


# Standalone test
if __name__ == "__main__":
    import json

    test_url = (
        "https://www.portoairport.pt/en/opo/services-shopping/essential-services/reduced-mobility"
    )
    raw = porto_scraper(test_url)
    parts = [p for p in raw.split("\n\n") if p.strip()]
    print(f"Sections: {len(parts)}\n")
    for i, section in enumerate(parts, 1):
        preview = section[:10000].replace("\n", " ")
        print(f"  [Section {i}]\n  {preview}...\n")