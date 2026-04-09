"""
Lufthansa Accessibility Scraper
================================
Scrapes wheelchair/accessibility info from Lufthansa's website and outputs:
  - output_accessibility.json   accordion sections (text + tables)
  - output_door_dimensions.csv aircraft cargo door dimensions table
"""

import json
import time
import random
import logging
import numpy as np
import pandas as pd
import undetected_chromedriver as uc

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
URL = "https://www.lufthansa.com/be/en/passengers-using-wheelchairs.html"
OUTPUT_JSON = "output_accessibility.json"
OUTPUT_CSV  = "output_door_dimensions.csv"
CHROME_VERSION = 146


# ── Driver ────────────────────────────────────────────────────────────────────
def init_driver(version: int) -> uc.Chrome:
    log.info("Launching Chrome...")
    driver = uc.Chrome(version_main=version)
    return driver


# ── Human-like click ──────────────────────────────────────────────────────────
def human_click(driver, element) -> None:
    """Move to element and click with a random delay to mimic human behaviour."""
    ActionChains(driver).move_to_element(element).perform()
    time.sleep(random.uniform(0.5, 1.5))
    ActionChains(driver).click().perform()


# ── Shadow DOM helpers ────────────────────────────────────────────────────────
SHADOW_WALK_JS = """
    function findInShadow(root, selector, found = []) {
        root.querySelectorAll('*').forEach(el => {
            if (el.matches && el.matches(selector)) found.push(el);
            if (el.shadowRoot) findInShadow(el.shadowRoot, selector, found);
        });
        return found;
    }
    window.__accordionButtons = findInShadow(document, 'button.header');
"""

def expand_all_accordions(driver) -> None:
    """Walk the full shadow DOM tree and click every collapsed accordion button."""
    driver.execute_script(SHADOW_WALK_JS)
    count = driver.execute_script("return window.__accordionButtons.length;")
    log.info(f"Found {count} accordion buttons")

    for i in range(count):
        expanded, label = driver.execute_script("""
            const btn = window.__accordionButtons[arguments[0]];
            return [btn.getAttribute('aria-expanded'), btn.textContent.trim()];
        """, i)

        log.info(f"  [{i}] '{label[:60]}' | expanded: {expanded}")

        if expanded == "false":
            driver.execute_script(
                "window.__accordionButtons[arguments[0]].scrollIntoView({block: 'center'});", i
            )
            time.sleep(0.4)
            driver.execute_script("window.__accordionButtons[arguments[0]].click();", i)
            time.sleep(random.uniform(0.8, 1.5))

    time.sleep(2)  # let final renders settle


# ── Accordion extraction ──────────────────────────────────────────────────────
def extract_table_from_item(item) -> list[dict]:
    """Extract rows from a maui-table inside a collapsible item."""
    rows = item.find_elements(By.TAG_NAME, "maui-table-row")[2:]  # skip header rows
    table_data = []
    for row in rows:
        cells = row.find_elements(By.TAG_NAME, "maui-table-cell")
        if len(cells) >= 3:
            table_data.append({
                "aircraft": cells[0].text.strip(),
                "height":   cells[1].text.strip(),
                "width":    cells[2].text.strip(),
            })
    return table_data


def extract_accordion_data(driver) -> list[dict]:
    """Return a list of {section, content, type} dicts from all accordion items."""
    items = driver.find_elements(By.CSS_SELECTOR, "maui-collapsible-item")
    log.info(f"Extracting {len(items)} accordion sections...")
    data = []

    for item in items:
        headline = item.get_attribute("headline")
        tables   = item.find_elements(By.TAG_NAME, "maui-table")

        if tables:
            content      = extract_table_from_item(item)
            content_type = "table"
        else:
            raw_text   = item.text
            clean_text = raw_text.replace(headline, "", 1).strip()
            clean_text = clean_text.replace("The link will be opened in a new browser tab", "")
            content      = " ".join(clean_text.split())
            content_type = "text"

        data.append({
            "section": headline,
            "content": content,
            "type":    content_type,
        })

    return data


# ── Door dimensions table ─────────────────────────────────────────────────────
def extract_door_dimensions(html: str) -> pd.DataFrame:
    """Parse the aircraft door dimensions table from saved page HTML."""
    soup       = BeautifulSoup(html, "html.parser")
    table_divs = soup.find_all("div", class_="table event")

    if not table_divs:
        log.warning("No door dimensions table found on page.")
        return pd.DataFrame()

    table_div  = table_divs[0]
    header_row = table_div.find("maui-table-head").find("maui-table-row")
    headers    = [c.get_text(strip=True) for c in header_row.find_all("maui-table-cell")]

    rows = []
    for row in table_div.find("maui-table-body").find_all("maui-table-row"):
        cells = row.find_all("maui-table-cell")
        rows.append([c.get_text(strip=True) for c in cells])

    df = pd.DataFrame(rows, columns=headers)
    df = df.replace("", np.nan).dropna(axis=1, how="all").iloc[1:].reset_index(drop=True)
    df.columns = ["Aircraft type", "Cargo Door Height", "Cargo Door Width"]
    df.insert(0, "Airline", "Lufthansa")
    return df


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    driver = init_driver(CHROME_VERSION)

    try:
        # 1. Load page
        log.info(f"Loading {URL}")
        driver.get(URL)
        time.sleep(2)

        # 2. Decline cookies
        cookie_btn = driver.find_element(By.ID, "cm-acceptNone")
        human_click(driver, cookie_btn)
        log.info("Declined cookies")

        # 3. Navigate to Aircraft door dimensions tab
        tab = driver.find_element(By.LINK_TEXT, "Aircraft door dimensions")
        human_click(driver, tab)
        log.info("Clicked 'Aircraft door dimensions' tab")
        time.sleep(2)

        # 4. Expand all accordions
        expand_all_accordions(driver)

        # 5. Extract accordion sections (JSON)
        accordion_data = extract_accordion_data(driver)
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(accordion_data, f, indent=4, ensure_ascii=False)
        log.info(f"Saved {len(accordion_data)} sections - {OUTPUT_JSON}")

        # 6. Grab full page HTML parse door dimensions table CSV
        html = driver.execute_script("return document.documentElement.outerHTML")
        df   = extract_door_dimensions(html)

        # if not df.empty:
        #     df.to_csv(OUTPUT_CSV, index=False)
        #     log.info(f"Saved door dimensions ({len(df)} rows) - {OUTPUT_CSV}")
        #     print(df.to_string(index=False))
        # else:
        #     log.warning("Door dimensions table was empty — CSV not saved.")

    except Exception as e:
        log.error(f"Scraper failed: {e}", exc_info=True)



if __name__ == "__main__":
    main()