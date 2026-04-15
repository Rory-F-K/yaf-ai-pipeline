# chunker/semantic.py

from typing import List, Dict
import re
import hashlib


# ID generation using MD5 hash of the text content, which is approach for deduplication and consistent referencing.
def generate_id(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


# Heading detection using multiple heuristics:
def is_heading(line: str) -> bool:
    line = line.strip()

    if not line:
        return False

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

    # Title Case heuristic (e.g., "Air Travel Accessibility Background")
    if (
        len(line.split()) <= 10
        and all(word[0].isupper() for word in line.split() if word.isalpha())
    ):
        return True

    return False


# Text cleaning to fix common PDF encoding issues and normalize spacing, which is crucial for accurate chunking and heading detection.
def clean_text(text: str) -> str:
    if not text:
        return ""

    # bad chars
    text = text.replace("�", "")
    text = text.replace("\xa0", " ")

    # normalize dashes / quotes
    text = text.replace("–", "-").replace("—", "-")
    text = text.replace("“", '"').replace("”", '"')
    text = text.replace("’", "'")

    # tabs
    text = text.replace("\t", " ")

    # collapse spaces
    text = re.sub(r"[ ]{2,}", " ", text)

    return text.strip()

# Bullet detection for lists
def is_bullet(line: str) -> bool:
    return bool(
        re.match(r"^[-•*]\s+", line)
        or re.match(r"^\d+\.\s+", line)
        or re.match(r"^[a-zA-Z]\)\s+", line)
    )

# Normalize text by cleaning, breaking inline headings, and ensuring proper paragraphing
def normalize_text(text: str) -> str:
    text = clean_text(text)

    # Break inline headings (VERY IMPORTANT for your data)
    # e.g. "... Thank you. Abstract The aviation industry..."
    text = re.sub(
        r'(?<=[.!?])\s+(?=[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,6}\s)',
        '\n\n',
        text
    )

    # Convert newlines properly
    text = re.sub(r'\n+', '\n\n', text)

    # Split sentences into cleaner blocks
    text = re.sub(r'(?<=[.!?])\s+', '\n\n', text)

    # Remove excessive spacing
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()


# Sentence splitting for large paragraphs that exceed chunk size, to ensure we don't break in the middle of sentences.
def split_sentences(text: str) -> List[str]:
    sentences = re.split(r'(?<=[.!?])\s+', text)
    return [s.strip() for s in sentences if s.strip()]

# Flush the current accumulated text into a chunk when we hit a new section or exceed size limits, ensuring we don't lose any content.
def flush_chunk(chunks, section, current_text):
    chunk_text = current_text.strip()

    if not chunk_text:
        return

    chunks.append({
        "id": generate_id(chunk_text),
        "section": section,
        "text": chunk_text,
        "sent": False
    })

# Semantic chunking that respects headings and logical sections.
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

        # Heading detection: If we hit a heading, we flush the current chunk and start a new section
        if is_heading(p):
            if current_text:
                chunk_text = current_text.strip()

                chunks.append({
                    "id": generate_id(chunk_text),
                    "section": current_section,
                    "text": chunk_text
                })

                current_text = ""

            current_section = p
            continue

        if is_bullet(p):
            candidate = current_text + p + "\n"

            if len(candidate) > chunk_size:
                flush_chunk(chunks, current_section, current_text)
                current_text = p + "\n"
            else:
                current_text += p + "\n"

            continue
        # Size control: If a single paragraph is too big, split it into sentences
        if len(p) > chunk_size:
            sentences = split_sentences(p)

            temp = ""
            for s in sentences:
                if len(temp) + len(s) + 2 > chunk_size:
                    if temp:
                        chunks.append({
                            "id": generate_id(temp.strip()),
                            "section": current_section,
                            "text": temp.strip(),
                            "sent": False
                        })
                    temp = s + " "
                else:
                    temp += s + " "

            if temp:
                chunks.append({
                    "id": generate_id(temp.strip()),
                    "section": current_section,
                    "text": temp.strip(),
                    "sent": False
                })

            continue

        # Normal accumulation of paragraphs into chunks
        if len(current_text) + len(p) + 2 > chunk_size:
            if current_text:
                chunk_text = current_text.strip()

                chunks.append({
                    "id": generate_id(chunk_text),
                    "section": current_section,
                    "text": chunk_text,
                    "sent": False
                })

            current_text = p + "\n\n"
        else:
            current_text += p + "\n\n"

    # Final flush of remaining text
    if current_text:
        chunk_text = current_text.strip()

        chunks.append({
            "id": generate_id(chunk_text),
            "section": current_section,
            "text": chunk_text,
            "sent": False
        })

    print(f"[Semantic] Generated {len(chunks)} chunks")

    return chunks