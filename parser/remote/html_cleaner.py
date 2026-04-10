# parser/remote/html_cleaner.py
from bs4 import BeautifulSoup
import re


# Remove non-content tags that often contain navigation, ads, or other non-essential information. 
REMOVE_TAGS = [
    "script",
    "style",
    "noscript",
    "header",
    "footer",
    "nav",
    "aside",
    "form",
    "svg"
]


# Patterns that indicate UI elements or navigation prompts
NOISE_PATTERNS = [
    r"Go to navigation",
    r"Go to main content",
    r"Go to footer",
    r"Go to sidebar",
    r"Go to search",
]


# Main HTML cleaning function that removes unwanted tags, extracts visible text, and cleans up noise and artifacts.
def clean_html(raw_html: str) -> str:
    soup = BeautifulSoup(raw_html, "html.parser")

    # Remove unwanted tags completely
    for tag in REMOVE_TAGS:
        for element in soup.find_all(tag):
            element.decompose()

    # Remove meta/link/title tags explicitly
    for element in soup.find_all(["meta", "link", "title"]):
        element.decompose()

    # Extract visible text
    text = soup.get_text(separator="\n")

    # Normalize whitespace
    text = re.sub(r'\n+', '\n\n', text)

    # Remove UI/navigation junk
    for pattern in NOISE_PATTERNS:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)

    # Remove leftover HTML artifacts like <div class=...>
    text = re.sub(r'<[^>]+>', '', text)

    # Clean excessive spacing
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()