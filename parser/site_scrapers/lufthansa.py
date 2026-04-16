"""
Lufthansa Accessibility Scraper
==========================================================
Returns:
    str → chunk-ready output for pipeline ingestion

Register:
    @register("lufthansa_accessible_travel")
"""

import time
import json
import logging
from typing import List, Dict

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from parser.site_scrapers import register


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


URL = "https://www.lufthansa.com/be/en/accessible-travel"


# Cookie handling
def accept_cookies(driver):
    try:
        time.sleep(2)

        buttons = driver.find_elements(By.XPATH, "//button")
        for b in buttons:
            txt = (b.text or "").lower()
            if "accept" in txt or "agree" in txt:
                b.click()
                log.info("Cookie banner accepted")
                time.sleep(2)
                return

        log.warning("No cookie button found (likely pre-accepted or blocked)")
    except Exception as e:
        log.warning(f"Cookie handling failed: {e}")


# Accordions are used extensively on the Lufthansa page, and must be expanded to access content.
def expand_accordions(driver):
    time.sleep(2)

    buttons = driver.find_elements(By.CSS_SELECTOR, "button")

    clicked = 0
    for b in buttons:
        try:
            aria = b.get_attribute("aria-expanded")
            if aria == "false":
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", b)
                time.sleep(0.2)
                b.click()
                clicked += 1
                time.sleep(0.3)
        except:
            continue

    log.info(f"Expanded {clicked} accordions")


# Parse the final HTML with BeautifulSoup to extract clean sections
def parse_sections(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")

    sections = []

    # Lufthansa uses multiple container patterns
    blocks = soup.select("maui-collapsible-item")

    for b in blocks:
        title = b.get("headline") or "Unknown section"

        # tables
        table = b.select_one("maui-table")
        if table:
            rows = []
            for r in table.select("maui-table-row"):
                cols = [c.get_text(strip=True) for c in r.select("maui-table-cell")]
                if cols:
                    rows.append(cols)

            sections.append({
                "section": title,
                "type": "table",
                "content": rows
            })
        else:
            text = b.get_text(" ", strip=True)
            sections.append({
                "section": title,
                "type": "text",
                "content": text
            })

    return sections

def create_driver():
    opts = Options()

    # HARD RESET STATE (this is what fixes repeat failure)
    opts.add_argument("--headless=new")
    opts.add_argument("--incognito")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")

    # IMPORTANT: prevent shared cache/session reuse
    opts.add_argument("--disable-application-cache")
    opts.add_argument("--disable-cache")
    opts.add_argument("--disk-cache-size=0")

    # fingerprint variation (light but effective)
    opts.add_argument("--lang=en-GB")
    opts.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=opts)

    # extra hard reset at runtime
    driver.delete_all_cookies()

    return driver

# Pipeline entrypoint
@register("lufthansa_accessible_travel")
def lufthansa_accessible_travel(url: str = URL) -> str:
    """
    Pipeline entrypoint:
    MUST return string (NOT file, NOT driver, NOT dict)
    """

    driver = create_driver()

    try:
        log.info(f"Loading: {url}")
        driver.get(url)

        accept_cookies(driver)
        expand_accordions(driver)

        html = driver.page_source
        sections = parse_sections(html)

        # IMPORTANT: pipeline-safe output
        output = []

        for s in sections:
            if s["type"] == "text":
                output.append(f"### {s['section']}\n{s['content']}")
            else:
                output.append(f"### {s['section']}\nTABLE:\n{json.dumps(s['content'], indent=2)}")

        return "\n\n".join(output)

    except Exception as e:
        log.error(f"Scraper failed: {e}", exc_info=True)
        return f"[ERROR] Lufthansa scraper failed: {str(e)}"

    finally:
        try:
            driver.quit()
        except:
            pass