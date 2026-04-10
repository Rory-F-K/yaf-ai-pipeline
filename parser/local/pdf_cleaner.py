from calendar import c
import re


# low-value block detection to filter out common non-informative sections of PDFs, such as TOC, page numbers, and generic headers/footers.
def is_low_value_block(text: str) -> bool:
    if not text:
        return True

    t = text.strip().lower()

    noise_markers = [
        "table of contents",
        "list of tables",
        "list of acronyms",
        "acknowledgements",
        "contents"
    ]

    if any(m in t for m in noise_markers):
        return True

    if re.fullmatch(r"\d+", t):
        return True

    if re.fullmatch(r"[.\-\s]{10,}", t):
        return True

    if len(t) < 40 and re.search(r"\b(page|chapter|table|figure)\b", t):
        return True

    return False


# Table of contents line detection
def is_toc_line(text: str) -> bool:
    t = text.strip().lower()

    if re.search(r"\.{3,}", t):
        return True

    if re.match(r"^\d+\s+\d+(\.\d+)*\.\s+.*", t):
        return True

    return False


# Figure/table artifact detection
def is_chart_artifact(text: str) -> bool:
    t = text.lower()

    if re.search(r"\b(0%|10%|20%|30%|40%|50%|60%|70%|80%|90%|100%)\b", t):
        return True

    if "%" in t and len(re.findall(r"\d+%", t)) > 3:
        return True

    return False

# Numeric block detection
def is_numeric_block(text: str) -> bool:
    t = text.strip()
    tokens = t.split()

    if not tokens:
        return True

    numbers = re.findall(r"\d+[,\.]?\d*%?", t)

    if len(numbers) / max(len(tokens), 1) > 0.4:
        return True

    return False


# Header/footer repetition detection
def is_repeated_header_footer(line: str) -> bool:
    if len(line) < 5:
        return True

    if re.search(r"page\s*\d+", line.lower()):
        return True

    if len(set(line)) < 5 and len(line) > 20:
        return True

    return False

# reference line detection
def is_reference_line(line: str) -> bool:
    t = line.strip()

    # Year-based citations (VERY STRONG SIGNAL)
    if re.search(r"\b(19|20)\d{2}[a-z]?\b", t):
        if len(t.split()) < 12:
            return True

    # Author format: "Surname, Initial., Surname, Initial."
    if re.match(r"^[A-Z][a-z]+,\s[A-Z]\.", t):
        return True

    # Multiple commas + year-like structure
    if t.count(",") >= 2 and re.search(r"\d{4}", t):
        return True

    return False

# OCR corruption detection (e.g., broken words, excessive single-letter splits, and nonsense spacing)
def is_ocr_corrupt(line: str) -> bool:
    t = line.strip()

    # broken words like "T xis", "Serra D'Arga Lda"
    if re.search(r"\b\w\s\w\s\w\b", t):
        return True

    # too many single-letter splits
    if len(re.findall(r"\b[a-zA-Z]\b", t)) > 3:
        return True

    # nonsense spacing ratio
    if len(t) > 20:
        space_ratio = t.count(" ") / len(t)
        if space_ratio > 0.25:
            return True

    return False

# Form template detection
def is_form_template(line: str) -> bool:
    t = line.lower()

    if "observations and/or suggestions" in t:
        return True

    if "thank you" in t:
        return True

    if re.search(r"_{3,}", t):
        return True

    if len(re.findall(r"[a-zA-Z]", t)) < 5 and "_" in t:
        return True

    return False

# Cleaning function
def clean_text(text: str) -> str:
    if not text:
        return ""

    text = text.replace("�", "")
    text = text.replace("\u00a0", " ")

    text = text.replace("–", "-").replace("—", "-")

    text = re.sub(r"(\w)\s{2,}(\w)", r"\1 \2", text)
    text = re.sub(r" +", " ", text)

    return text.strip()


# Main function to clean PDF
def clean_pdf_text(text: str) -> str:
    if not text:
        return ""

    text = clean_text(text)

    lines = text.split("\n")
    cleaned = []

    for line in lines:
        line = line.strip()

        if not line:
            continue

        if is_low_value_block(line):
            continue

        if is_toc_line(line):
            continue

        if is_chart_artifact(line):
            continue

        if is_numeric_block(line):
            continue

        if is_repeated_header_footer(line):
            continue

        if is_reference_line(line):
            continue

        if is_ocr_corrupt(line):
            continue

        if is_form_template(line):
            continue

        cleaned.append(line)

    return re.sub(r"\n{3,}", "\n\n", "\n".join(cleaned)).strip()