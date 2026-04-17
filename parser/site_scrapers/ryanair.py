# Scraper for Ryanair Help Center (Zendesk)

import json
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup

try:
    from . import register # relative import when used as part of a package
except ImportError:
    def register(domain): # fallback for standalone testing
        def decorator(func):
            return func
        return decorator

BASE_URL = "https://help.ryanair.com"
API_BASE = f"{BASE_URL}/api/v2/help_center"

SESSION = requests.Session()
SESSION.headers.update({
    "Accept": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (compatible; HelpCenterScraper/1.0)"
    ),
})


# Zendesk API helpers

def _get_json(url: str, params: dict = None) -> dict:
    # GET a Zendesk API endpoint and return parsed JSON.
    response = SESSION.get(url, params=params, timeout=15)
    response.raise_for_status()
    return response.json()


def _paginate(url: str, key: str, params: dict = None) -> list:
    # Follow Zendesk's next_page cursor until exhausted and collect all items
    # under `key` (e.g. 'sections', 'articles').
    params = dict(params or {})
    params.setdefault("per_page", 100)

    items = []
    while url:
        data = _get_json(url, params)
        items.extend(data.get(key, []))
        url = data.get("next_page")   # None when there are no more pages
        params = {}                   # next_page already includes all params
    return items


# Helpers to extract resource IDs and locale from URLs

def _locale_from_url(parsed) -> str:
    # Extract locale from path, e.g. /hc/en-lv/categories/… → 'en-lv'.
    parts = parsed.path.strip("/").split("/")
    # parts[0] == 'hc', parts[1] == locale
    if len(parts) >= 2:
        return parts[1]
    return "en-us"


def _get_sections_for_category(category_id: str, locale: str) -> list[dict]:
    url = f"{API_BASE}/{locale}/categories/{category_id}/sections.json"
    return _paginate(url, "sections")


def _get_articles_for_section(section_id: str, locale: str) -> list[dict]:
    url = f"{API_BASE}/{locale}/sections/{section_id}/articles.json"
    return _paginate(url, "articles")


def _get_article(article_id: str, locale: str) -> dict:
    url = f"{API_BASE}/{locale}/articles/{article_id}.json"
    data = _get_json(url)
    return data.get("article", {})


def _html_to_text(html: str) -> str:
    # Strip HTML tags and return clean plain text.
    if not html:
        return ""
    return BeautifulSoup(html, "html.parser").get_text(separator="\n", strip=True)


# public scrape function registered for help.ryanair.com
@register("help.ryanair.com")
def scrape(url: str) -> list[dict]:
    """
    Scrape articles from a Ryanair Help Center URL.

    Returns a JSON string (array of objects), each object representing one article:
        [
          {
            "id":         int,
            "title":      str,
            "url":        str,
            "section_id": int,
            "locale":     str,
            "body":       str,   # plain text, HTML stripped
            "created_at": str,
            "updated_at": str,
          },
          ...
        ]
    """
    parsed = urlparse(url)
    locale = _locale_from_url(parsed)
    path_parts = parsed.path.strip("/").split("/")

    # Determine resource type from path: /hc/{locale}/{resource_type}/{id}
    # path_parts: ['hc', locale, resource_type, id, ...]
    if len(path_parts) < 4:
        raise ValueError(f"Cannot determine resource type from URL: {url}")

    resource_type = path_parts[2]   # 'categories', 'sections', or 'articles'
    resource_id   = path_parts[3]

    articles_raw: list[dict] = []

    if resource_type == "categories":
        sections = _get_sections_for_category(resource_id, locale)
        for section in sections:
            articles_raw.extend(
                _get_articles_for_section(str(section["id"]), locale)
            )

    elif resource_type == "sections":
        articles_raw = _get_articles_for_section(resource_id, locale)

    elif resource_type == "articles":
        article = _get_article(resource_id, locale)
        articles_raw = [article] if article else []

    else:
        raise ValueError(f"Unsupported resource type in URL: {resource_type}")

    return json.dumps([_normalise(a) for a in articles_raw if a], ensure_ascii=False)


def _normalise(article: dict) -> dict:
    # Convert a raw Zendesk article dict into standard schema.
    return {
        "id":         article.get("id"),
        "title":      article.get("title", ""),
        "url":        article.get("html_url", ""),
        "section_id": article.get("section_id"),
        "locale":     article.get("locale", ""),
        "body":       _html_to_text(article.get("body", "")),
        "created_at": article.get("created_at", ""),
        "updated_at": article.get("updated_at", ""),
    }


# test code for standalone execution
if __name__ == "__main__":
    import json

    test_url = "https://help.ryanair.com/hc/en-lv/categories/12489466690833"
    print(f"Scraping: {test_url}\n")
    results = json.loads(scrape(test_url))
    print(f"Found {len(results)} articles\n")
    for r in results[:3]:
        print(json.dumps({k: v if k != "body" else v[:200] for k, v in r.items()}, indent=2))
        print("---")