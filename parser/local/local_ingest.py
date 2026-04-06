# parser/local/local_ingest.py
from pathlib import Path

from parser.local.txt_parser import extract_clean_txt
from .pdf_parser import extract_clean_pdf
# future imports:
# from .json_parser import extract_clean_json

def ingest_local(file_path: str) -> str:
    suffix = Path(file_path).suffix.lower()
    if suffix == ".pdf":
        return extract_clean_pdf(file_path)
    elif suffix == ".txt":
         return extract_clean_txt(file_path)
    # elif suffix == ".json":
    #     with open(path, "r", encoding="utf-8") as f:
    #        return f.read()

    else:
        raise ValueError(f"Unsupported file type: {suffix}")

