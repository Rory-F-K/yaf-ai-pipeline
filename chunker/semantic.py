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
    # Fix common PDF encoding issues
    text = text.replace("�", "")
    text = text.replace("  ", " ")

    # Remove weird spacing in words (e.g., "Disserta o")
    text = re.sub(r'(\w)\s+(\w)', r'\1 \2', text)

    # Normalize unicode quotes/dashes if needed
    text = text.replace("–", "-").replace("—", "-")

    return text


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
                            "text": temp.strip()
                        })
                    temp = s + " "
                else:
                    temp += s + " "

            if temp:
                chunks.append({
                    "id": generate_id(temp.strip()),
                    "section": current_section,
                    "text": temp.strip()
                })

            continue

        # Normal accumulation of paragraphs into chunks
        if len(current_text) + len(p) + 2 > chunk_size:
            if current_text:
                chunk_text = current_text.strip()

                chunks.append({
                    "id": generate_id(chunk_text),
                    "section": current_section,
                    "text": chunk_text
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
            "text": chunk_text
        })

    print(f"[Semantic] Generated {len(chunks)} chunks")

    return chunks