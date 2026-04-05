# chunker/semantic.py
from typing import List

# Simple semantic chunking based on paragraph breaks.
def semantic_chunk(text: str, chunk_size: int = 2000) -> List[str]:
    paragraphs = text.split("\n\n")
    chunks = []
    current = ""

    for p in paragraphs:
        if len(current) + len(p) + 2 <= chunk_size:
            current += p + "\n\n"
        else:
            if current:
                chunks.append(current.strip())
            current = p + "\n\n"

    if current:
        chunks.append(current.strip())

    return chunks

# TODO: Save semantic chunks to a local JSON file with metadata hash of the original text chunk.
# TODO: After creation of new semantic chunks compare to previous semantic chunks and only send new/changed chunks to agentic chunking.
# Stops unchanged chunks from being reprocessed, and allows us to compare the original semantic chunks with the final agentic chunks.