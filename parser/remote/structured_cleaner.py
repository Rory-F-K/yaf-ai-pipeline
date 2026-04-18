# parser/remote/structured_cleaner.py
import re
from bs4 import BeautifulSoup
import html as html_lib
import json

# Heuristic language detection
def detect_language(text: str) -> str:
    if not text:
        return "unknown"
    text_lower = text.lower()
    if any(w in text_lower for w in [" el ", " la ", " los ", " una ", " servicios "]):
        return "es"
    if any(w in text_lower for w in [" o ", " de ", " para ", " serviços "]):
        return "pt"
    return "en"


def decode_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\\u003c", "<")
    text = text.replace("\\u003e", ">")
    text = text.replace("\\u0026", "&")
    text = html_lib.unescape(text)
    return text

# Remove common boilerplate elements and noise patterns that are unlikely to be relevant content.
def remove_boilerplate(soup: BeautifulSoup) -> BeautifulSoup:
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside"]):
        tag.decompose()

    for tag in soup.find_all(True):
        classes = " ".join(tag.get("class", [])).lower()
        tid = (tag.get("id") or "").lower()
        if any(x in classes or x in tid
               for x in ["nav", "menu", "footer", "header", "cookie", "banner"]):
            tag.decompose()

    return soup

# Deduplicate text blocks
def deduplicate_blocks(blocks: list) -> list:
    seen = set()
    result = []
    for b in blocks:
        if not isinstance(b, dict):
            continue
        text = b.get("text", "")
        if not text:
            continue
        key = re.sub(r"\s+", " ", text.lower().strip())
        if key in seen:
            continue
        seen.add(key)
        result.append(b)
    return result


# Heading level → markdown prefix so the chunker's is_heading() reliably fires.
_HEADING_PREFIX = {
    "h1": "# ",
    "h2": "## ",
    "h3": "### ",
    "h4": "#### ",
}


def extract_structured_content(soup: BeautifulSoup) -> list:
    # Walk the document in order, emitting text blocks with their nearest preceding heading context.
    content = []
    current_heading = None
    current_heading_level = 0

    # Traverse in document order; skip elements that are inside a heading (avoids duplicating heading text as both a heading and a body block).
    seen_ids = set()

    for el in soup.find_all(
        ["h1", "h2", "h3", "h4", "p", "li", "div", "section", "blockquote"]
    ):
        # Skip if this element is a descendant of another element already processed (prevents double-emitting nested structures).
        el_id = id(el)
        if el_id in seen_ids:
            continue

        # Mark all descendants so we don't re-emit them.
        for desc in el.find_all(True):
            seen_ids.add(id(desc))
        seen_ids.add(el_id)

        tag = el.name.lower()

        # Use separator=" " so inline elements don't create mid-word breaks.
        text = el.get_text(" ", strip=True)
        if not text:
            continue

        if tag in _HEADING_PREFIX:
            # Store the markdown-prefixed heading text.
            current_heading = _HEADING_PREFIX[tag] + text
            current_heading_level = int(tag[1])
        else:
            # Skip very short noise fragments (single words, stray punctuation)
            if len(text) < 15:
                continue

            content.append({
                "heading": current_heading,
                "heading_level": current_heading_level,
                "text": text,
            })

    return content


def flatten_content(blocks: list) -> str:
    # Emit headings as standalone lines so chunker's is_heading() can split on them correctly.
    parts = []
    last_heading = None

    for b in blocks:
        if not isinstance(b, dict):
            continue

        heading = b.get("heading")
        text = b.get("text", "").strip()

        if not text:
            continue

        # Emit the heading only when it changes, as a standalone line.
        if heading and heading != last_heading:
            parts.append(heading)
            last_heading = heading

        parts.append(text)

    return "\n\n".join(parts)

# Regex to detect lines that look like raw JSON key-value pairs, which are common artifacts in extracted text from HTML/JSON sources.
_JSON_FIELD_RE = re.compile(
    r'^"[a-z_]+":\s*' # "some_key":
    r'(?:"[^"]*"|[\d\-]+)' # "string value" or number
    r',?\s*$' # optional trailing comma
)

# If the input is a JSON API response, extract the HTML body field and return it for normal HTML processing.
def extract_body_if_json(raw: str) -> str:
    raw = raw.strip()
    if not raw.startswith(('{', '[')):
        return raw

    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        lines = raw.splitlines()
        cleaned = [l for l in lines if not _JSON_FIELD_RE.match(l.strip())]
        return "\n".join(cleaned)

    # Unwrap common envelope keys
    if isinstance(data, dict):
        for key in ("article", "result", "data", "results"):
            if key in data and isinstance(data[key], (dict, list)):
                data = data[key]
                break

    if isinstance(data, dict):
        for key in ("body", "html", "content", "description"):
            if key in data and isinstance(data[key], str):
                value = data[key]
                if value.strip().startswith(('{', '[')):
                    try:
                        inner = json.loads(value)
                        if isinstance(inner, dict):
                            for inner_key in ("body", "html", "content", "description"):
                                if inner_key in inner and isinstance(inner[inner_key], str):
                                    return inner[inner_key]
                    except (json.JSONDecodeError, ValueError):
                        pass
                return value

        # Skip metadata fields that are common in API responses and unlikely to contain relevant content. This prevents emitting raw JSON key-value pairs as text blocks.
        METADATA_KEYS = {
            "id", "locale", "url", "html_url", "author_id", "source_locale",
            "position", "vote_sum", "vote_count", "created_at", "updated_at",
            "edited_at", "outdated", "outdated_locales", "draft",
            "comments_disabled", "label_names", "section_id", "promoted",
        }
        content_parts = []
        for key, value in data.items():
            if key in METADATA_KEYS or not isinstance(value, str):
                continue
            if key == "title":
                # Emit title as a markdown heading so the chunker picks it up
                content_parts.insert(0, f"## {value}")
            elif key in ("body", "html", "content", "description"):
                content_parts.append(value)
            # Skip all other string fields (url, locale, etc.)
        if content_parts:
            return "\n\n".join(content_parts)

    if isinstance(data, list):
        bodies = []
        for item in data:
            if not isinstance(item, dict):
                continue
            # Recurse on each list item using the same logic
            result = extract_body_if_json(json.dumps(item))
            if result:
                bodies.append(result)
        if bodies:
            return "\n\n".join(bodies)

    return ""

def clean_records(html_str: str) -> str:
    if not html_str:
        return ""

    # Decode JSON escapes BEFORE anything else
    html_str = decode_text(html_str)

    # If is a JSON API payload, unwrap it to raw HTML first
    html_str = extract_body_if_json(html_str)

    soup = BeautifulSoup(html_str, "html.parser")
    soup = remove_boilerplate(soup)
    blocks = extract_structured_content(soup)
    blocks = deduplicate_blocks(blocks)
    return flatten_content(blocks)
