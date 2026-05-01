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
        "max_pages": 1,
        "dedup": True,
        "allowed_domains": ["iata.org"],
        "entity": "IATA",
        "entity_type": "industry_body",
    },
    {
        "id": "iata_mobility_aid_guidance_2026",
        "url": "https://www.iata.org/contentassets/6fea26dd84d24b26a7a1fd5788561d6e/mobility-aid-guidance-document.pdf",
        "max_pages": 1,
        "dedup": True,
        "allowed_domains": ["iata.org"],
        "entity": "IATA",
        "entity_type": "industry_body",
    },
    {
        "id": "iata_mobility_aid_guidance_feb2023",
        "url": "https://www.iata.org/contentassets/7b3762815ac44a10b83ccf5560c1b308/iata-guidance-on-the-transport-of-mobility-aids-final-feb2023.pdf",
        "max_pages": 1,
        "dedup": True,
        "allowed_domains": ["iata.org"],
        "entity": "IATA",
        "entity_type": "industry_body",
    },
    {
        "id": "iata_accessibility_program",
        "url": "https://www.iata.org/en/programs/passenger/accessibility/",
        "max_pages": 1,
        "dedup": True,
        "allowed_domains": ["iata.org"],
        "entity": "IATA",
        "entity_type": "industry_body",
    },
    {
        "id": "eur_lex_32006R1107_html",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32006R1107",
        "max_pages": 1,
        "dedup": True,
        "allowed_domains": ["eur-lex.europa.eu"],
        "entity": "European Union",
        "entity_type": "regulator",
    },
    {
        "id": "porto_airport_prm",
        "url": "https://www.portoairport.pt/en/opo/services-shopping/essential-services/reduced-mobility",
        "max_pages": 1,
        "dedup": True,
        "allowed_domains": ["portoairport.pt"],
        "entity": "Porto Airport",
        "entity_type": "airport",
    },
    {
        "id": "lufthansa_accessible_travel",
        "url": "https://www.lufthansa.com/be/en/accessible-travel",
        "max_pages": 1,
        "dedup": True,
        "allowed_domains": ["lufthansa.com"],
        "entity": "Lufthansa",
        "entity_type": "airline",
    },
    {
        "id": "swiss_accessible_travel",
        "url": "https://www.swiss.com/ch/en/prepare/special-care/accessible-travel.html",
        "max_pages": 1,
        "dedup": True,
        "allowed_domains": ["swiss.com"],
        "entity": "Swiss International Air Lines",
        "entity_type": "airline",
    },
    {
        "id": "ryanair_accessible_travel",
        "url": "https://help.ryanair.com/hc/en-lv/categories/12489466690833",
        "max_pages": 1,
        "dedup": True,
        "allowed_domains": ["ryanair.com"],
        "entity": "Ryanair",
        "entity_type": "airline",
    },
    {
        "id": "vueling_special_assistance",
        "url": "https://help.vueling.com/hc/en-gb/categories/19798714411665-Special-Assistance",
        "max_pages": 1,
        "dedup": True,
        "allowed_domains": ["vueling.com"],
        "entity": "Vueling",
        "entity_type": "airline",
    },
    {
        "id": "us_transportation_passengers_disabilities",
        "url": "https://www.transportation.gov/airconsumer/passengers-disabilities",
        "max_pages": 3,
        "dedup": True,
        "allowed_domains": ["transportation.gov"],
        "entity": "US Department of Transportation",
        "entity_type": "regulator",
    },
]