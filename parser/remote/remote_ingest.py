# parser/remote/remote_ingest.py
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


def is_pdf_response(resp):
    """Detect PDF by content type header or magic bytes."""
    content_type = resp.headers.get("Content-Type", "").lower()
    return "application/pdf" in content_type or resp.content[:4] == b"%PDF"


def download_pdf(url):
    """Download a PDF to a temporary file and return the path."""
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    if is_pdf_response(resp):
        tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        tmp.write(resp.content)
        tmp.close()
        return tmp.name
    return None


def clean_text(text: str) -> str:
    """Unified cleaning for HTML and PDF text."""
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
    # Detect headings (capitalized short lines)
    text = detect_headings(text)
    return text


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

        # PDF handling 
        try:
            pdf_path = download_pdf(url)
            if pdf_path:
                print("[PDF] Detected")
                try:
                    # Robust ingestion via ingest_local, which triggers OCR if needed
                    raw_text = ingest_local(pdf_path)
                    text = clean_text(raw_text)
                    pages.append({"url": url, "text": text})
                    print(f"[Ingest] {url} → 1 pages")
                finally:
                    os.remove(pdf_path)
                continue
        except Exception as e:
            print(f"[PDF] Failed: {e}")

        # HTML scraping
        scraper = get_scraper(url)
        text = ""
        raw_html = ""

        try:
            # site-specific scraper
            try:
                if scraper:
                    text = scraper(url)
            except Exception as e:
                print(f"[Scraper Error] {e}")

            # intercept scraper
            try:
                if not text or len(text) < 200:
                    text, _ = intercept_scrape(url)
            except Exception as e:
                print(f"[Intercept Error] {e}")

            # generic scraper
            try:
                if not text or len(text) < 200:
                    text = generic_scrape(url)
            except Exception as e:
                print(f"[Generic Scraper Error] {e}")

            if not text or len(text) < 100:
                print("[Skip] Low-value")
                continue

            # Preserve raw HTML separately
            raw_html = text

            # Step 1: Clean HTML to extract visible text and remove noise before further processing
            cleaned_html = clean_html(raw_html)

            # Skip if nothing meaningful remains
            if not cleaned_html or len(cleaned_html) < 100:
                print("[Skip] After HTML cleaning → Low-value")
                continue

            # Step 2: Clean text to fix encoding issues, normalize spacing, and detect headings for better chunking later on
            text = clean_text(cleaned_html)

            pages.append({
                "url": url,
                "text": text
            })

        except Exception as e:
            print(f"[Error] {e}")
            continue

        # enqueue new links
        try:
            # Only parse links if we have raw HTML content, to avoid false positives from text that looks like HTML but isn't.
            if raw_html and "<a" in raw_html:  # only parse if real HTML
                soup = BeautifulSoup(raw_html, "html.parser")

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