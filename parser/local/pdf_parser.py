# parser/local/pdf_parser.py
import pdfplumber # for PDF parsing
import fitz  # PyMuPDF for fallback PDF parsing
import re # for cleaning text

# Primary PDF extraction method using pdfplumber
def extract_pdf_primary(path: str) -> str:
    text_blocks = []

    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            words = page.extract_words()

            if not words:
                continue

            # Group words into lines based on vertical position
            lines = {}
            for w in words:
                y = round(w["top"], 1)
                lines.setdefault(y, []).append(w["text"])

            for y in sorted(lines.keys()):
                line = " ".join(lines[y])
                text_blocks.append(line)

    return "\n".join(text_blocks)

# Fallback PDF extraction method using PyMuPDF (fitz) for cases where pdfplumber fails
def extract_pdf_fallback(path):
    doc = fitz.open(path)
    text = ""

    for page in doc:
        text += page.get_text("text")

    return text

# Main function to extract PDF text, trying primary method first and falling back if necessary
def extract_pdf(path):
    text = extract_pdf_primary(path)

    if len(text.strip()) < 500:
        text = extract_pdf_fallback(path)

    return text

# Cleaning function to fix common PDF extraction issues
def clean_text(text: str) -> str:

    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text)

    # Restore paragraph breaks
    text = re.sub(r'\.\s+', '.\n\n', text)

    # Remove weird artifacts
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)

    # Fix broken bullet points
    text = text.replace("•", "\n- ")

    return text.strip()

# Heuristic to detect headings based on capitalization and length
def detect_headings(text: str) -> str:
    lines = text.split("\n")
    processed = []

    for line in lines:
        line = line.strip()

        # Heuristic: headings are short + capitalized
        if len(line) < 80 and line.isupper():
            processed.append(f"\n## {line}\n")
        else:
            processed.append(line)

    return "\n".join(processed)

# Main function to extract and clean PDF text
def extract_clean_pdf(path: str) -> str:

    text = extract_pdf(path)

    text = clean_text(text)

    text = detect_headings(text)

    return text
