# chunker/semantic.py

from typing import List, Dict
import re
import hashlib


def generate_id(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def clean_text(text: str) -> str:
    if not text:
        return ""

    # First, decode common escape sequences that may be present in raw extracted text
    text = text.replace('\\n', ' ').replace('\\t', ' ')
    text = text.replace('\\r', '')

    # Strip markdown heading prefixes
    text = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)

    # Strip "Source: URL" artifacts
    text = re.sub(r'^Source:\s*https?://\S+', '', text, flags=re.MULTILINE)

    # Strip box-drawing characters (common in PDF tables) and similar noise
    text = re.sub(r'[\u2500-\u259F]+', '', text)
    # If the line is now just trailing punctuation or whitespace, drop it entirely
    text = re.sub(r'(?m)^[\s\-–—_=~*#|]+$', '', text)
    # ── END NEW

    text = text.replace("\ufffd", "")  # replacement char (was "")
    text = text.replace("\xa0", " ")
    text = text.replace("–", "-").replace("—", "-")
    text = text.replace("\u201c", '"').replace("\u201d", '"')
    text = text.replace("\u2018", "'")
    text = text.replace("\t", " ")
    text = re.sub(r"[ ]{2,}", " ", text)

    return text.strip()



def clean_section(section: str) -> str:
    # Normalize section headings: strip markdown, source prefixes, and URLs

    # Strip markdown heading markers
    section = re.sub(r'^#{1,6}\s+', '', section.strip())
    # Strip "Source: https://..." pattern
    section = re.sub(r'^Source:\s*https?://\S+', '', section).strip()
    # If only a URL remains, reduce to its path for readability
    if re.match(r'^https?://', section):
        from urllib.parse import urlparse
        parsed = urlparse(section)
        section = (parsed.path.rstrip('/').split('/')[-1] or parsed.netloc).replace('-', ' ')
    return section or "General"


def is_heading(line: str) -> bool:
    line = line.strip()

    if not line:
        return False

    # Explicit markdown headings (## or ###) — always a heading
    if re.match(r'^#{1,6}\s+', line):
        return True

    # Question-style headings (FAQs, docs)
    if line.endswith("?") and len(line) < 120:
        return True

    # Short lines likely to be headings
    if len(line) < 100:
        if (
            line.isupper()
            or line.endswith(":")
            or re.match(r'^\d+(\.\d+)*\s', line)
        ):
            return True

    # Title Case heuristic
    if (
        len(line.split()) <= 10
        and all(word[0].isupper() for word in line.split() if word.isalpha())
    ):
        return True

    return False


def is_fragment(line: str) -> bool:
    # Detect lines that are clearly mid-sentence orphans from PDF/HTML extraction.
    line = line.strip()
    if not line:
        return False

    # Starts with a closing bracket — orphaned end of a parenthetical
    if line[0] in ')]}':
        return True

    # Starts lowercase — continuation of a sentence that was broken mid-line
    if line[0].islower():
        return True

    return False


def is_truncated(text: str) -> bool:
    # Detect text that ends mid-word or mid-sentence without proper punctuation.
    text = text.strip()
    if not text:
        return True

    last_char = text[-1]

    # Proper endings: sentence punctuation, closing quotes/brackets, digits
    if last_char in '.!?:,;)"\'-–0123456789':
        return False

    # Ends in a letter — could be a truncated word
    if last_char.isalpha():
        last_word = text.split()[-1]
        # Very short final "word" that looks like a cut-off
        if len(last_word) <= 4 and last_word.islower():
            return True
        # Last word with no vowels looks malformed (e.g. "safet" → borderline)
        # Use a simple heuristic: if it ends in a consonant cluster unlikely to end a word
        if re.search(r'[^aeiouAEIOU\s]{3}$', last_word):
            return True

    return False


def is_bullet(line: str) -> bool:
    return bool(
        re.match(r"^[-•*]\s+", line)
        or re.match(r"^\d+\.\s+", line)
        or re.match(r"^[a-zA-Z]\)\s+", line)
    )


def normalize_text(text: str) -> str:
    text = clean_text(text)

    # Break inline headings
    text = re.sub(
        r'(?<=[.!?])\s+(?=[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,6}\s)',
        '\n\n',
        text
    )

    text = re.sub(r'\n+', '\n\n', text)
    text = re.sub(r'(?<=[.!?])\s+', '\n\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()


def split_sentences(text: str) -> List[str]:
    sentences = re.split(r'(?<=[.!?])\s+', text)
    return [s.strip() for s in sentences if s.strip()]


def finalise_text(text: str) -> str:
    # Clean up text before finalizing a chunk: collapse newlines and trim spaces

    # Collapse paragraph breaks into a single space
    text = re.sub(r'\n{2,}', ' ', text)
    # Collapse any remaining single newlines
    text = text.replace('\n', ' ')
    # Clean up any double spaces that result
    text = re.sub(r' {2,}', ' ', text)
    return text.strip()


def flush_chunk(chunks, section, current_text):
    chunk_text = finalise_text(current_text)


    if not chunk_text:
        return

    # Drop chunks that are clearly truncated extraction artifacts
    if is_truncated(chunk_text):
        return

    chunks.append({
        "id": generate_id(chunk_text),
        "section": section,
        "text": chunk_text,
        "sent": False
    })


def semantic_chunk(
    text: str,
    chunk_size: int = 1200
) -> List[Dict]:

    text = normalize_text(text)
    paragraphs = text.split("\n\n")

    chunks = []
    current_text = ""
    current_section = "General"

    for p in paragraphs:
        p = p.strip()
        if not p:
            continue

        # Discard orphaned fragments before any other processing
        if is_fragment(p):
            continue

        if is_heading(p):
            if current_text:
                chunk_text = current_text.strip()
                if not is_truncated(chunk_text):
                    chunks.append({
                        "id": generate_id(chunk_text),
                        "section": current_section,
                        "text": chunk_text,
                        "sent": False
                    })
                current_text = ""

            # Store a cleaned version of the section heading
            current_section = clean_section(p)
            continue

        if is_bullet(p):
            candidate = current_text + p + "\n"
            if len(candidate) > chunk_size:
                flush_chunk(chunks, current_section, current_text)
                current_text = p + "\n"
            else:
                current_text += p + "\n"
            continue

        if len(p) > chunk_size:
            sentences = split_sentences(p)
            temp = ""
            for s in sentences:
                if len(temp) + len(s) + 2 > chunk_size:
                    if temp and not is_truncated(temp.strip()):
                        chunks.append({
                            "id": generate_id(temp.strip()),
                            "section": current_section,
                            "text": temp.strip(),
                            "sent": False
                        })
                    temp = s + " "
                else:
                    temp += s + " "
            if temp and not is_truncated(temp.strip()):
                chunks.append({
                    "id": generate_id(temp.strip()),
                    "section": current_section,
                    "text": temp.strip(),
                    "sent": False
                })
            continue

        if len(current_text) + len(p) + 2 > chunk_size:
            if current_text:
                flush_chunk(chunks, current_section, current_text)
            current_text = p + "\n\n"
        else:
            current_text += p + "\n\n"

    if current_text:
        flush_chunk(chunks, current_section, current_text)

    print(f"[Semantic] Generated {len(chunks)} chunks")
    return chunks