SCRAPER_REGISTRY = {}

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

from . import lufthansa
from . import portoairport
from . import ryanair
from . import swiss

