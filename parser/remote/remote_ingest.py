# parser/remote/remote_ingest.py
import tempfile
import requests
from pathlib import Path
from parser.remote.html_parser import extract_clean_html
from parser.local.pdf_parser import extract_clean_pdf
from parser.remote.playwright_fetcher import fetch_html
from playwright.sync_api import Error as PlaywrightError

def ingest_remote(source):
    # Extract URL from dict or string
    url = source["url"] if isinstance(source, dict) else source
    print(f"[Remote] Processing URL: {url}")

    suffix = Path(url).suffix.lower()

    # Case 1: URL explicitly ends with .pdf - try direct PDF fetch first
    if suffix == ".pdf":
        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            if "pdf" in resp.headers.get("Content-Type", "").lower() or resp.content[:4] == b"%PDF":
                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                    tmp.write(resp.content)
                    tmp.flush()
                    text = extract_clean_pdf(tmp.name)
                return [{"text": text, "path": url}]
            else:
                print(f"[Error] URL ends with .pdf but content not PDF: {url}")
                return []
        except Exception as e:
            print(f"[Error] PDF download failed: {e}")
            return []

    # Case 2: Try Playwright fetch first for HTML content
    try:
        html = fetch_html(url)
        if html:
            text = extract_clean_html(html)
            return [{"text": text, "path": url}]
        else:
            print(f"[Warning] Playwright returned empty HTML: {url}")
    except PlaywrightError as e:
        # Handle Playwright "Download is starting" fallback
        if "Download is starting" in str(e):
            print(f"[Playwright] Detected download, falling back to direct PDF fetch: {url}")
            try:
                resp = requests.get(url, timeout=20)
                if "pdf" in resp.headers.get("Content-Type", "").lower() or resp.content[:4] == b"%PDF":
                    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                        tmp.write(resp.content)
                        tmp.flush()
                        text = extract_clean_pdf(tmp.name)
                    return [{"text": text, "path": url}]
                else:
                    print(f"[Error] Content not PDF: {url}")
                    return []
            except Exception as pdf_err:
                print(f"[Error] PDF download failed: {pdf_err}")
                return []
        else:
            print(f"[Error] Playwright failed: {e}")
            return []

    # Case 3: Final fallback - try direct fetch and determine content type
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "").lower()
        if "pdf" in content_type or resp.content[:4] == b"%PDF":
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                tmp.write(resp.content)
                tmp.flush()
                text = extract_clean_pdf(tmp.name)
            return [{"text": text, "path": url}]
        else:
            text = extract_clean_html(resp.text)
            return [{"text": text, "path": url}]
    except Exception as e:
        print(f"[Error] Final fallback failed: {e}")
        return []