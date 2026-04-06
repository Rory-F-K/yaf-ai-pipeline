# parser/remote/pdf_parser.py
import requests
import tempfile
from parser.local.pdf_parser import extract_clean_pdf

# Fetches a PDF from a URL, saves it temporarily, and extracts clean text using local PDF parsing logic. Returns the extracted text.
def fetch_pdf(url: str) -> str:
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Failed to download PDF: {url}")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(response.content)
        tmp_path = tmp.name

    return extract_clean_pdf(tmp_path)