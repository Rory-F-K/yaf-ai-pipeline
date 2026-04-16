import re
import requests
import tempfile
import os
from collections import deque
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup

from parser.site_scrapers import get_scraper
from parser.remote.generic_scraper import generic_scrape
from parser.remote.intercept_scraper import intercept_scrape
from parser.remote.html_cleaner import clean_html

from parser.local.local_ingest import ingest_local
from parser.local.pdf_parser import detect_headings


# PDF handling heuristics
def is_pdf_response(resp):
    content_type = resp.headers.get("Content-Type", "").lower()
    return "application/pdf" in content_type or resp.content[:4] == b"%PDF"


def download_pdf(url):
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    if is_pdf_response(resp):
        tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        tmp.write(resp.content)
        tmp.close()
        return tmp.name
    return None


# Text cleaning heuristics
def clean_text(text: str) -> str:
    text = re.sub(r'\r', '\n', text)
    text = re.sub(r'(?<=[.!?])\s+', '\n\n', text)

    lines = []
    seen = set()

    for l in text.split("\n"):
        l = l.strip()
        if len(l) < 20 or l in seen:
            continue
        seen.add(l)
        lines.append(l)

    text = "\n".join(lines)
    return detect_headings(text)


# Type Detection Heuristics
def is_html(text: str) -> bool:
    if not isinstance(text, str):
        return False
    t = text.lower()
    return "<html" in t or "<body" in t or "<a " in t or "<div" in t

def is_json(text: str) -> bool:
    if not isinstance(text, str):
        return False
    stripped = text.strip()
    return (stripped.startswith("{") and stripped.endswith("}")) or \
           (stripped.startswith("[") and stripped.endswith("]"))

# Pipeline entry point for remote ingestion
def ingest_remote(config):
    start_url = config["url"]
    allowed_domains = config.get("allowed_domains", [])
    max_pages = config.get("max_pages", 5)

    visited = set()
    queue = deque([start_url])
    pages = []

    while queue and len(pages) < max_pages:
        url = queue.popleft()

        if url in visited:
            continue

        if any(x in url for x in ["ref_=", "#", "calendar", "chart"]):
            continue

        visited.add(url)
        print(f"[Remote] Processing: {url}")

        # PDF detection and handling
        try:
            pdf_path = download_pdf(url)
            if pdf_path:
                print("[PDF] Detected")
                try:
                    raw_text = ingest_local(pdf_path)
                    text = clean_text(raw_text)

                    pages.append({"url": url, "text": text})
                    print(f"[Ingest] {url} → PDF page")
                finally:
                    os.remove(pdf_path)
                continue
        except Exception as e:
            print(f"[PDF] Failed: {e}")

        # Scraping
        scraper = get_scraper(url)
        text = ""
        raw_output = ""

        try:
            # site-specific scraper
            if scraper:
                print("try site specific scraper")
                text = scraper(url)

            # intercept scraper
            if not text or len(text) < 200:
                print("try intercept scraper")
                result = intercept_scrape(url)

                if isinstance(result, tuple):
                    text = result[0]
                else:
                    text = result

            # generic scraper
            if not text or len(text) < 200:
                print("try generic scraper")
                text = generic_scrape(url)

            if not text or len(text) < 100:
                print("[Skip] Low-value")
                continue

            raw_output = text

            # Type detection and cleaning
            if is_json(raw_output):
                cleaned_text = raw_output   
            elif is_html(raw_output):
                cleaned_html = clean_html(raw_output)
                cleaned_text = clean_text(cleaned_html)
            else:
                cleaned_text = clean_text(raw_output)

            if not cleaned_text or len(cleaned_text) < 100:
                print("[Skip] After cleaning → Low-value")
                continue

            pages.append({
                "url": url,
                "text": cleaned_text
            })

        except Exception as e:
            print(f"[Error] {e}")
            continue

        # Link extraction and queuing
        try:
            if is_html(raw_output):
                soup = BeautifulSoup(raw_output, "html.parser")

                for link in soup.find_all("a", href=True):
                    full_url = urljoin(url, link["href"])
                    domain = urlparse(full_url).netloc

                    if allowed_domains and not any(ad in domain for ad in allowed_domains):
                        continue

                    if full_url not in visited:
                        queue.append(full_url)

        except Exception as e:
            print(f"[Link Parsing Error] {e}")

    return pages