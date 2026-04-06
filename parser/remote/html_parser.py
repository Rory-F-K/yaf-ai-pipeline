# parser/remote/html_parser.py
from bs4 import BeautifulSoup
import re

def remove_noise(soup: BeautifulSoup):
    for tag in soup([
        "script", "style", "noscript",
        "header", "footer", "nav",
        "aside", "form", "iframe"
    ]):
        tag.decompose()

def extract_structured_text(soup: BeautifulSoup) -> str:
    lines = []
    for element in soup.find_all(["h1","h2","h3","h4","p","li"]):
        text = element.get_text(strip=True)
        if not text:
            continue
        if element.name.startswith("h"):
            level = int(element.name[1])
            lines.append(f"\n{'#'*level} {text}\n")
        elif element.name == "li":
            lines.append(f"- {text}")
        else:
            lines.append(text)
    return "\n".join(lines)

def clean_text(text: str) -> str:
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\.\s+', '.\n\n', text)
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)
    return text.strip()

def extract_clean_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    remove_noise(soup)
    text = extract_structured_text(soup)
    return clean_text(text)