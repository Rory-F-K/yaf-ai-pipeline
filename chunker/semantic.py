# chunker/semantic.py
from typing import List, Dict
import re
import hashlib

# Generates a unique ID for a given text using MD5 hashing
def generate_id(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()

# Simple heuristic to detect if a line is a heading based on formatting cues
def is_heading(line: str) -> bool:
    line = line.strip()

    # Heuristics:
    if len(line) < 80 and (
        line.isupper() or
        line.endswith(":") or
        re.match(r'^\d+(\.\d+)*\s', line)  # 1. 1.1 2.3 etc
    ):
        return True

    return False

# Basic semantic chunking based on paragraphs and headings
def semantic_chunk(text: str, chunk_size: int = 2000) -> List[Dict]:

    paragraphs = text.split("\n\n")

    chunks = []
    current_text = ""
    current_section = "General"

    for p in paragraphs:
        p = p.strip()

        if not p:
            continue

        # Detect heading
        if is_heading(p):
            current_section = p
            continue

        # If adding this paragraph exceeds chunk size → flush
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

    return chunks

# TODO: Implement more advanced semantic chunking using NLP techniques, such as sentence tokenization and topic modeling, to create more coherent chunks that preserve context better than simple paragraph breaks.
# TODO: Save semantic chunks to a local JSON file with metadata hash of the original text chunk.
# TODO: After creation of new semantic chunks compare to previous semantic chunks and only send new/changed chunks to agentic chunking.
# Stops unchanged chunks from being reprocessed, and allows us to compare the original semantic chunks with the final agentic chunks.