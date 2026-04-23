import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from playwright.sync_api import sync_playwright
from parser.site_scrapers import register


def extract_page(page):
    """
    IATA pages use flat .rich-text divs with h2/h3 headings as section markers,
    plus .collapsible-content blocks. We read all of these directly from the DOM
    without clicking, since content is present in the HTML regardless of visibility.
    """
    sections = []

    # Grab all rich-text blocks scoped to main content (excludes right-nav sidebar)
    blocks = page.query_selector_all("main .rich-text")
    # print(f"[debug] Found {len(blocks)} rich-text blocks")

    for i, block in enumerate(blocks):
        text = block.evaluate("el => el.innerText").strip()
        if not text:
            continue

        # Split the block on h2/h3 boundaries so each heading becomes its own section
        chunk_data = block.evaluate("""el => {
            const results = [];
            let currentTitle = null;
            let currentLines = [];

            for (const node of el.childNodes) {
                const tag = node.nodeName.toLowerCase();
                if (tag === 'h2' || tag === 'h3') {
                    if (currentLines.join('').trim()) {
                        results.push({ title: currentTitle, content: currentLines.join('\\n').trim() });
                    }
                    currentTitle = node.innerText?.trim() || null;
                    currentLines = [];
                } else {
                    const t = node.innerText ?? node.textContent ?? '';
                    if (t.trim()) currentLines.push(t.trim());
                }
            }
            if (currentLines.join('').trim()) {
                results.push({ title: currentTitle, content: currentLines.join('\\n').trim() });
            }
            return results;
        }""")

        for chunk in chunk_data:
            if chunk.get("content"):
                sections.append(chunk)
                # print(f"[debug] Block {i} section: {chunk.get('title')!r} -> {chunk['content'][:60]!r}")

    # Also grab collapsible sections (data-toggle="collapse") — content is in DOM already
    collapsibles = page.query_selector_all("main .collapsible-content")
    # print(f"[debug] Found {len(collapsibles)} collapsible blocks")

    for i, col in enumerate(collapsibles):
        title_el = col.query_selector(".is-collapsible")
        title = title_el.evaluate("el => el.innerText").strip() if title_el else ""

        content_el = col.query_selector(".rich-text")
        content = content_el.evaluate("el => el.innerText").strip() if content_el else ""

        # print(f"[debug] Collapsible {i}: {title!r} -> {content[:60]!r}")
        if content:
            sections.append({"title": title, "content": content})

    # print(f"[debug] Total sections: {len(sections)}")
    return sections


@register("iata.org")
def iata_scraper(url: str) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.goto(url, timeout=60000)
        page.wait_for_timeout(3000)

        sections = extract_page(page)
        browser.close()

    return "\n\n".join(
        f"{s['title']}\n{s['content']}" if s.get("title") else s["content"]
        for s in sections
        if s.get("content")
    )


if __name__ == "__main__":
    test_url = "https://www.iata.org/en/programs/passenger/accessibility/"
    print(f"Scraping: {test_url}\n")

    raw = iata_scraper(test_url)
    parts = [p for p in raw.split("\n\n") if p.strip()]

    print(f"Sections: {len(parts)}\n")
    for i, section in enumerate(parts, 1):
        lines = section.split("\n")
        heading = lines[0] if lines else "(intro)"
        preview = " ".join(lines[1:])[:200].replace("\n", " ")
        print(f"  [{heading}]\n  {preview}...\n")