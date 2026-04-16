# parser/remote/structured_cleaner.py
import re
from bs4 import BeautifulSoup
import html

# detect language with simple heuristics to help guide cleaning and chunking
def detect_language(text: str) -> str:
    if not text:
        return "unknown"

    text_lower = text.lower()

    # very simple heuristics (fast, no deps)
    if any(w in text_lower for w in [" el ", " la ", " los ", " una ", " servicios "]):
        return "es"
    if any(w in text_lower for w in [" o ", " de ", " para ", " serviços "]):
        return "pt"
    return "en"


def decode_weird_encoding(text: str) -> str:
    if not text:
        return ""

    # step 1: unescape unicode HTML sequences
    text = text.replace("\\u003c", "<")
    text = text.replace("\\u003e", ">")
    text = text.replace("\\u0026", "&")

    # step 2: normal HTML entity decode
    text = html.unescape(text)

    return text


# remove boilerplate content based on tag types and common class/id patterns
def remove_boilerplate(soup: BeautifulSoup):
    # remove obvious non-content tags
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside"]):
        tag.decompose()

    # remove elements with typical boilerplate classes/ids
    for tag in soup.find_all(True):
        classes = " ".join(tag.get("class", [])).lower()
        tid = (tag.get("id") or "").lower()

        if any(x in classes for x in ["nav", "menu", "footer", "header", "cookie", "banner"]):
            tag.decompose()
            continue

        if any(x in tid for x in ["nav", "menu", "footer", "header", "cookie", "banner"]):
            tag.decompose()

    return soup


# deduplicate blocks of text based on normalized content to remove repeated sections like disclaimers, repeated instructions, or boilerplate paragraphs
def deduplicate_blocks(blocks):
    seen = set()
    result = []

    for b in blocks:
        if not isinstance(b, dict):
            continue

        text = b.get("text", "")
        if not text:
            continue

        key = re.sub(r"\s+", " ", text.lower())

        if key in seen:
            continue

        seen.add(key)
        result.append(b)

    return result


# extract structured content based on heading hierarchy and text blocks, creating a more organized representation of the page
def extract_structured_content(soup: BeautifulSoup):
    content = []
    current_heading = None

    for el in soup.find_all(["h1", "h2", "h3", "h4", "p", "li"]):
        if not el:
            continue

        text = el.get_text(" ", strip=True) if hasattr(el, "get_text") else None

        if not text:
            continue

        if el.name and el.name.startswith("h"):
            current_heading = text
        else:
            content.append({
                "heading": current_heading,
                "text": text
            })

    return content


# flatten the structured content into a single text blob while preserving some of the heading hierarchy for better chunking and embedding quality
def flatten_content(blocks):
    parts = []

    for b in blocks:
        if not isinstance(b, dict):
            continue

        heading = b.get("heading")
        text = b.get("text", "")

        if not text:
            continue

        if heading:
            parts.append(f"{heading}\n{text}")
        else:
            parts.append(text)

    return "\n\n".join(parts)

# decode text with multiple steps to handle common encoding issues found in web content, especially from APIs that escape HTML in weird ways
def decode_text(text: str) -> str:
    if not text:
        return ""

    # fix escaped unicode HTML (very common in Zendesk / APIs)
    text = text.replace("\\u003c", "<")
    text = text.replace("\\u003e", ">")
    text = text.replace("\\u0026", "&")

    # HTML entity decode
    text = html.unescape(text)

    return text

# main cleaning function that combines all steps
def clean_records(html_str: str):
    if not html_str:
        return ""

    # STEP 1: decode BEFORE parsing (IMPORTANT FIX)
    html_str = decode_text(html_str)

    soup = BeautifulSoup(html_str, "html.parser")
    soup = remove_boilerplate(soup)

    # STEP 2: structured extraction
    blocks = extract_structured_content(soup)

    # STEP 3: dedupe
    blocks = deduplicate_blocks(blocks)

    # STEP 4: flatten
    return flatten_content(blocks)