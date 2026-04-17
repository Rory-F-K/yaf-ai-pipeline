# parser/remote/html_cleaner.py
from bs4 import BeautifulSoup, NavigableString, Tag
import re


REMOVE_TAGS = [
    "script", "style", "noscript", "header", "footer",
    "nav", "aside", "form", "svg", "meta", "link", "title",
]

# Block-level tags that should produce a paragraph break
BLOCK_TAGS = {
    "p", "div", "section", "article", "main",
    "h1", "h2", "h3", "h4", "h5", "h6",
    "li", "dt", "dd",
    "tr", "td", "th",
    "blockquote", "pre",
    "br", "hr",
}

NOISE_PATTERNS = [
    r"Go to navigation",
    r"Go to main content",
    r"Go to footer",
    r"Go to sidebar",
    r"Go to search",
]


def _extract_text(element, parts: list):
    # Recursively walk the DOM tree, emitting text with appropriate spacing.
    if isinstance(element, NavigableString):
        text = str(element)
        # Collapse all internal whitespace to a single space; caller decides
        # where newlines go.
        text = re.sub(r"[\r\n\t ]+", " ", text)
        if text.strip():
            parts.append(text)
        return

    if not isinstance(element, Tag):
        return

    tag = element.name.lower() if element.name else ""

    if tag in BLOCK_TAGS:
        parts.append("\n\n")

    for child in element.children:
        _extract_text(child, parts)

    if tag in BLOCK_TAGS:
        parts.append("\n\n")


def clean_html(raw_html: str) -> str:
    soup = BeautifulSoup(raw_html, "html.parser")

    # Remove non-content tags
    for tag_name in REMOVE_TAGS:
        for el in soup.find_all(tag_name):
            el.decompose()

    # Remove boilerplate class/id patterns
    for el in soup.find_all(True):
        classes = " ".join(el.get("class", [])).lower()
        el_id = (el.get("id") or "").lower()
        if any(x in classes or x in el_id
               for x in ["nav", "menu", "footer", "header", "cookie", "banner"]):
            el.decompose()

    # Walk the tree, emitting text with correct block boundaries
    parts: list = []
    _extract_text(soup, parts)
    text = "".join(parts)

    # Collapse runs of spaces (never collapse newlines here)
    text = re.sub(r" {2,}", " ", text)

    # Normalise line breaks: 3+ → 2
    text = re.sub(r"\n{3,}", "\n\n", text)

    # Strip UI/navigation noise
    for pattern in NOISE_PATTERNS:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)

    # Remove any stray HTML that somehow survived
    text = re.sub(r"<[^>]+>", "", text)

    # Final normalisation
    text = re.sub(r"\n{3,}", "\n\n", text)

    # Strip box-drawing characters used as visual dividers on airline/travel sites
    text = re.sub(r'[\u2500-\u259F]+', '', text)
    text = re.sub(r'(?m)^[\s\-–—_=~*#|]+$', '', text)
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()