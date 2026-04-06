# List of remote sources to process
# Each source is a dict with:
#   - id: unique identifier for output files
#   - url: the page to scrape/download
#   - max_pages: optional, max pages to fetch (if multi-page)
#   - dedup: optional, remove duplicates within content
#   - allowed_domains: optional, list of domains to restrict crawling

SOURCES = [
    {
        "id": "eur_lex_32006R1107_pdf",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/PDF/?uri=CELEX:32006R1107",
        "max_pages": 5,
        "dedup": True,
        "allowed_domains": ["eur-lex.europa.eu"]
    },
    {
        "id": "iata_accessibility_fact_sheet",
        "url": "https://www.iata.org/en/iata-repository/pressroom/fact-sheets/fact-sheet-accessibility/",
        "max_pages": 5,
        "dedup": True,
        "allowed_domains": ["iata.org"]
    },
    {
        "id": "iata_mobility_aid_guidance",
        "url": "https://www.iata.org/contentassets/6fea26dd84d24b26a7a1fd5788561d6e/mobility-aid-guidance-document.pdf",
        "max_pages": 5,
        "dedup": True,
        "allowed_domains": ["iata.org"]
    },
    {
        "id": "eur_lex_32006R1107_html",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32006R1107",
        "max_pages": 1,
        "dedup": True,
        "allowed_domains": ["eur-lex.europa.eu"]
    },
    {
        "id": "porto_airport_prm",
        "url": "https://www.portoairport.pt/en/opo/services-shopping/essential-services/reduced-mobility",
        "max_pages": 5,
        "dedup": True,
        "allowed_domains": ["portoairport.pt"]
    }
]