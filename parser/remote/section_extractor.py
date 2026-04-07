# parser/remote/section_extractor.py
def extract_sections(text: str):
    # Normalize text to ensure consistent section extraction
    sections = []

    lines = [l.strip() for l in text.split("\n") if l.strip()]

    current_title = None
    current_content = []

    for line in lines:
        # Detect headings (questions / titles)
        if is_heading(line):
            # Save previous section
            if current_title:
                sections.append({
                    "title": current_title,
                    "content": " ".join(current_content).strip()
                })

            current_title = line
            current_content = []

        else:
            current_content.append(line)

    # Save last
    if current_title:
        sections.append({
            "title": current_title,
            "content": " ".join(current_content).strip()
        })

    return sections

# Heuristic heading detection that identifies both question-style headings (common in FAQs) and traditional title-style headings, improving the accuracy of section extraction from unstructured text.
def is_heading(line: str) -> bool:
    # Questions
    if line.endswith("?"):
        return True

    # Short titles
    if len(line) < 80 and line.istitle():
        return True

    # ALL CAPS headings
    if line.isupper() and len(line) < 60:
        return True

    return False