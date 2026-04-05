import re
from pathlib import Path

# Primary TXT extraction
def extract_txt(path: str) -> str:
    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(f"File not found: {path}")
    
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    
    return text

# Cleaning function (similar to PDF)
def clean_text(text: str) -> str:
    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text)

    # Restore paragraph breaks after periods
    text = re.sub(r'\.\s+', '.\n\n', text)

    # Remove non-ASCII artifacts
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)

    # Fix bullet points if present
    text = text.replace("•", "\n- ")

    return text.strip()

# Heuristic to detect headings
def detect_headings(text: str) -> str:
    lines = text.split("\n")
    processed = []

    for line in lines:
        line = line.strip()
        # Headings: short lines that are uppercased
        if len(line) < 80 and line.isupper():
            processed.append(f"\n## {line}\n")
        else:
            processed.append(line)
    
    return "\n".join(processed)

# Main function to extract and clean TXT text
def extract_clean_txt(path: str) -> str:
    text = extract_txt(path)
    text = clean_text(text)
    text = detect_headings(text)
    return text