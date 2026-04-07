# Variables and settings for the web scraping and processing pipeline

# List of remote sources to process
# Each source is a dict with:
#   - id: unique identifier for output files
#   - url: the page to scrape/download
#   - max_pages: optional, max pages to fetch (if multi-page)
#   - dedup: optional, remove duplicates within content
#   - allowed_domains: optional, list of domains to restrict crawling

class Config:
    ENABLE_AGENT_CHUNKS=False
    SOURCES = [   
        {
            "id": "iata_accessibility_fact_sheet",
            "url": "https://www.iata.org/en/iata-repository/pressroom/fact-sheets/fact-sheet-accessibility/",
            "max_pages": 5,
            "dedup": True,
            "allowed_domains": ["iata.org"]
        },
        {
            "id": "eur_lex_32006R1107_html",
            "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32006R1107",
            "max_pages": 2,
            "dedup": True,
            "allowed_domains": ["eur-lex.europa.eu"]
        },  
        {
            "id": "porto_airport_prm",
            "url": "https://www.portoairport.pt/en/opo/services-shopping/essential-services/reduced-mobility",
            "max_pages": 2,
            "dedup": True,
            "allowed_domains": ["portoairport.pt"]
        }
    ]

