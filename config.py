# Variables and settings for the web scraping and processing pipeline

# List of remote sources to process
# Each source is a dict with:
#   - id: unique identifier for output files
#   - url: the page to scrape/download
#   - max_pages: optional, max pages to fetch (if multi-page)
#   - dedup: optional, remove duplicates within content
#   - allowed_domains: optional, list of domains to restrict crawling

class Config:
    ENABLE_AGENT_CHUNKS=True
    SOURCES = [   
        {
            "id": "iata_accessibility_fact_sheet",
            "url": "https://www.iata.org/en/iata-repository/pressroom/fact-sheets/fact-sheet-accessibility/",
            "max_pages": 5,
            "dedup": True,
            "allowed_domains": ["iata.org"]
        },  
        # {
        #     "id": "iata_mobility-aid-guidance-document",
        #     "url": "https://www.iata.org/contentassets/6fea26dd84d24b26a7a1fd5788561d6e/mobility-aid-guidance-document.pdf",
        #     "max_pages": 3,
        #     "dedup": True,
        #     "allowed_domains": ["iata.org"]
        # },  
        # {
        #     "id": "iata_accessibility_program",
        #     "url": "https://www.iata.org/en/programs/passenger/accessibility/",
        #     "max_pages": 3,
        #     "dedup": True,
        #     "allowed_domains": ["iata.org"]
        # },  
        {
            "id": "eur_lex_32006R1107_html",
            "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32006R1107",
            "max_pages": 3,
            "dedup": True,
            "allowed_domains": ["eur-lex.europa.eu"]
        },  
        {
            "id": "porto_airport_prm",
            "url": "https://www.portoairport.pt/en/opo/services-shopping/essential-services/reduced-mobility",
            "max_pages": 3,
            "dedup": True,
            "allowed_domains": ["portoairport.pt"]
        },  
        {
            "id": "lufthansa_accessible_travel",
            "url": "https://www.lufthansa.com/be/en/accessible-travel",
            "max_pages": 3,
            "dedup": True,
            "allowed_domains": ["lufthansa.com"]
        # },
        # {
        #     "id": "swiss_accessible_travel",
        #     "url": "https://www.swiss.com/ch/en/prepare/special-care/accessible-travel.html",
        #     "max_pages": 3,
        #     "dedup": True,
        #     "allowed_domains": ["swiss.com"]
        # },  
        # {
        #     "id": "ryanair_accessible_travel",
        #     "url": "https://help.ryanair.com/hc/en-lv/categories/12489466690833",
        #     "max_pages": 3,
        #     "dedup": True,
        #     "allowed_domains": ["ryanair.com"]
        # },  
        # {
        #     "id": "vueling_wheelchair_checkin",
        #     "url": "https://help.vueling.com/hc/en-gb/articles/30891224305425-Wheelchair-Check-in",
        #     "max_pages": 3,
        #     "dedup": True,
        #     "allowed_domains": ["vueling.com"]
        # },  
        # {
        #     "id": "us_transportation_passengers_disabilities",
        #     "url": "https://www.transportation.gov/airconsumer/passengers-disabilities",
        #     "max_pages": 3,
        #     "dedup": True,
        #     "allowed_domains": ["transportation.gov"]
        }
    ]