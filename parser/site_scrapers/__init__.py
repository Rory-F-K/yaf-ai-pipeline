SCRAPER_REGISTRY = {}
# Each scraper is designed to get the most out of the specific domain. Will be flattened and filtered for relevance in the next steps of the pipeline

# Decorator to register a scraper function for a specific domain
def register(domain):
    def decorator(func):
        SCRAPER_REGISTRY[domain] = func
        return func
    return decorator

def get_scraper(url: str):
    from urllib.parse import urlparse

    domain = urlparse(url).netloc

    for key in SCRAPER_REGISTRY:
        if key in domain:
            return SCRAPER_REGISTRY[key]

    return None

from . import eur_lex
from . import iata
from . import lufthansa
from . import portoairport
from . import ryanair
from . import swiss
from . import us_gov_transportation
from . import vueling
