from pathlib import Path

from parser.txt_parser import extract_clean_txt
from .pdf_parser import extract_clean_pdf
# future imports:
# from .html_parser import parse_html
# from .txt_parser import parse_txt
# from .json_parser import parse_json

def ingest_file(file_path: str) -> str:
    suffix = Path(file_path).suffix.lower()
    if suffix == ".pdf":
        return extract_clean_pdf(file_path)
    # elif suffix == ".html":
    #     return extract_clean_html(file_path)
    elif suffix == ".txt":
         return extract_clean_txt(file_path)
    # elif suffix == ".json":
    #     return extract_clean_json(file_path)
    else:
        raise ValueError(f"Unsupported file type: {suffix}")

