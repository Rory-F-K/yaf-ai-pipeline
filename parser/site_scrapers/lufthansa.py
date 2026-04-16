import time
import json
import logging

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import undetected_chromedriver as uc
from parser.site_scrapers import register

log = logging.getLogger(__name__)


URL = "https://www.lufthansa.com/be/en/accessible-travel"

def create_driver():
    log.info("Launching Chrome...")

    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    driver = uc.Chrome(options=options)
    return driver

# Force render by scrolling through the page to trigger lazy loading and any scroll-based hydration
def force_render(driver):
    try:
        driver.execute_script("window.scrollTo(0, 300);")
        time.sleep(1)

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
        time.sleep(1)

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
    except:
        pass


# Generic clicker for likely accordion triggers
def expand_all_clickables(driver):
    clicked = 0

    # ONLY likely accordion triggers
    candidates = driver.find_elements(
        By.CSS_SELECTOR,
        "maui-collapsible-item button, details summary"
    )

    for el in candidates:
        try:
            driver.execute_script(
                "arguments[0].scrollIntoView({block:'center'});", el
            )
            time.sleep(0.2)

            driver.execute_script("arguments[0].click();", el)
            clicked += 1
            time.sleep(0.2)
        except:
            continue

    log.info(f"Expanded {clicked} real accordions")


# Safe parsing of any text content
def parse_any_content(html: str):
    soup = BeautifulSoup(html, "html.parser")

    content = []

    # remove junk
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    # try structured blocks first
    headers = soup.find_all(["h1", "h2", "h3"])

    for h in headers:
        text_block = []

        for sib in h.find_all_next():
            if sib.name in ["h1", "h2", "h3"]:
                break

            txt = sib.get_text(" ", strip=True)
            if txt and len(txt) > 20:
                text_block.append(txt)

        if text_block:
            content.append({
                "section": h.get_text(strip=True),
                "type": "text",
                "content": " ".join(text_block)[:2000]
            })

    # fallback: full page text
    if not content:
        text = soup.get_text(" ", strip=True)
        content.append({
            "section": "page",
            "type": "text",
            "content": text
        })

    return content


# Pipeline entry point
@register("lufthansa.com")
def lufthansa_accessible_travel(url: str = URL) -> str:
    try:
        log.info(f"[Lufthansa DOM Scraper] {url}")

        driver = create_driver()
        driver.get(url)

        time.sleep(3)  # allow initial render

        force_render(driver)
        expand_all_clickables(driver)
        force_render(driver)

        html = driver.page_source

        sections = parse_any_content(html)

        output = []

        for s in sections:
            output.append(f"### {s['section']}\n{s['content']}")

        driver.quit()

        return "\n\n".join(output)

    except Exception as e:
        log.error(f"Lufthansa DOM scraper failed: {e}", exc_info=True)
        return f"[ERROR] {str(e)}"