# parser/local/local_ingest.py
from pathlib import Path

from .txt_parser import extract_clean_txt
from .pdf_cleaner import clean_pdf_text
from .pdf_parser import extract_clean_pdf
# future imports:
# from .json_parser import extract_clean_json

def ingest_local(file_path: str) -> str:
    suffix = Path(file_path).suffix.lower()
    if suffix == ".pdf":
        text = extract_clean_pdf(file_path)
        text = clean_pdf_text(text)
        return text
    elif suffix == ".txt":
         return extract_clean_txt(file_path)
    # elif suffix == ".json":
    #     with open(path, "r", encoding="utf-8") as f:
    #        return f.read()

    else:
        raise ValueError(f"Unsupported file type: {suffix}")

