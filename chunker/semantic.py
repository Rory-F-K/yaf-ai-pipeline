# chunker/semantic.py
from typing import List, Dict
import re
import hashlib


# ID generation based on text content for deduplication and reference
def generate_id(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


# Heading detection with enhanced heuristics, including question-style headings which are common in FAQs and help centers.
def is_heading(line: str) -> bool:
    line = line.strip()

    if not line:
        return False

    # NEW: question-style headings (VERY IMPORTANT)
    if line.endswith("?") and len(line) < 120:
        return True

    # Existing heuristics
    if len(line) < 80 and (
        line.isupper() or
        line.endswith(":") or
        re.match(r'^\d+(\.\d+)*\s', line)
    ):
        return True

    return False


# Text normalization to ensure consistent chunking, including converting single newlines to paragraph breaks and adding spacing after sentence endings.
def normalize_text(text: str) -> str:
    # Convert single newlines into paragraph breaks
    text = re.sub(r'\n+', '\n\n', text)

    # Add spacing after sentence endings
    text = re.sub(r'(?<=[.!?])\s+', '\n\n', text)

    # Clean excessive spacing
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()


# Semantic chunking that respects both heading boundaries and a maximum chunk size, ensuring that chunks are coherent and appropriately sized for embedding and retrieval.
def semantic_chunk(text: str, chunk_size: int = 1200) -> List[Dict]:

    # CRITICAL: normalize first
    text = normalize_text(text)

    paragraphs = text.split("\n\n")

    chunks = []
    current_text = ""
    current_section = "General"

    for p in paragraphs:
        p = p.strip()

        if not p:
            continue

        # Headings always start a new chunk, even if the previous chunk is not full.
        if is_heading(p):
            # Flush previous chunk
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

        # Chunking logic: if adding the next paragraph exceeds the chunk size, flush the current chunk and start a new one.
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

    # Final chunk
    if current_text:
        chunk_text = current_text.strip()

        chunks.append({
            "id": generate_id(chunk_text),
            "section": current_section,
            "text": chunk_text
        })

    print(f"[Semantic] Generated {len(chunks)} chunks")

    return chunks