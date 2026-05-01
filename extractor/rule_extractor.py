# extractor/rule_extractor.py
#
# Purpose:
#   Extracts structured PRM services from airline and airport sources using Gemini AI.
#   Each source is classified as an airline or airport via URL domain lookup.
#   Output is a single entity document (airline or airport) with a services array.
#
# Key class:
#   RuleExtractor — main class with the following public methods:
#     - run(source, output_path)        auto-detect source type and extract entity
#     - extract_from_pdf(path)          parse and extract from a local PDF file
#     - extract_from_url(url)           scrape a website or remote PDF and extract
#     - extract_from_text(text, source) extract services from any raw string
#     - extract_from_chunks(chunks)     extract from pre-processed pipeline chunks
#     - save(entities, output_path)     save extracted entities to a JSON file
#
# Output format (each entity):
#   Airline: { airline_id, name, source, services: [{type, description, is_presented}] }
#   Airport: { airport_id, name, source, services: [{type, description, is_presented}] }
#
# Source classification:
#   Domains are matched against DOMAIN_MAP to determine entity type and identity.
#   Sources not in DOMAIN_MAP (e.g. eur-lex, iata, transportation.gov) are skipped.
#
# Dependencies:
#   - GEMINI_API_KEY and GEMINI_MODEL_NAME must be set in .env
#   - parser/local/pdf_parser.py, parser/remote/generic_scraper.py,
#     parser/remote/pdf_fetcher.py

import json
import re
import os
import time
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional

from dotenv import load_dotenv
from google import genai
from google.genai.types import Content, Part

from parser.local.pdf_parser import extract_clean_pdf
from parser.remote.generic_scraper import generic_scrape
from parser.remote.pdf_fetcher import fetch_pdf

load_dotenv()

GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL_NAME")

# Maps registrable domain → entity metadata.
# Add new airlines/airports here as sources are added to config.py.
DOMAIN_MAP: dict[str, dict] = {
    "lufthansa.com":     {"entity_type": "airline", "entity_id": "lufthansa", "name": "Lufthansa"},
    "swiss.com":         {"entity_type": "airline", "entity_id": "swiss",     "name": "Swiss International Air Lines"},
    "ryanair.com":       {"entity_type": "airline", "entity_id": "ryanair",   "name": "Ryanair"},
    "vueling.com":       {"entity_type": "airline", "entity_id": "vueling",   "name": "Vueling"},
    "portoairport.pt":   {"entity_type": "airport", "entity_id": "porto",     "name": "Porto Airport"},
    "madeiraairport.pt": {"entity_type": "airport", "entity_id": "madeira",   "name": "Madeira Airport"},
}

SERVICES_EXTRACTION_PROMPT = """
You are an expert in aviation accessibility for passengers with reduced mobility (PRM).

Extract ALL services, assistance options, procedures, and policies relevant to PRM passengers
from the provided text.

For each item output:
- type: a short lowercase snake_case label describing the service
        (e.g. "wheelchair_assistance", "mobility_aid_transport", "pre_notification",
              "special_seating", "escort_service", "battery_limit", "documentation_required")
- description: a clear, complete statement of what is offered, required, or allowed

STRICT RULES:
- Only extract what is explicitly stated — do not infer or hallucinate
- Each item must be directly useful to a PRM traveller
- Skip navigation text, advertisements, menus, and generic marketing copy
- Merge closely related details into one description rather than splitting artificially
- The description must be a full sentence, minimum 20 characters

OUTPUT FORMAT (strict JSON array only):
[
  {
    "type": "snake_case_label",
    "description": "Clear statement of the service, policy, or procedure"
  }
]

ONLY return valid JSON. No markdown, no explanation.
"""


class RuleExtractor:
    def __init__(self):
        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY not set in .env")
        self.client = genai.Client(api_key=GEMINI_API_KEY)

    # ── domain / source classification ────────────────────────────────────────

    def _extract_domain(self, url: str) -> str:
        """Return the registrable domain (e.g. 'lufthansa.com') from a URL."""
        host = urlparse(url).hostname or ""
        if host.startswith("www."):
            host = host[4:]
        parts = host.split(".")
        # collapse subdomains: help.ryanair.com → ryanair.com
        if len(parts) > 2:
            host = ".".join(parts[-2:])
        return host

    def _classify_source(self, source: str) -> Optional[dict]:
        """Return entity metadata dict for a source URL, or None if unrecognised."""
        domain = self._extract_domain(source)
        return DOMAIN_MAP.get(domain)

    # ── Gemini interaction ─────────────────────────────────────────────────────

    def _extract_json(self, text: str) -> list:
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                return []
        return []

    def _validate_services(self, raw: list) -> list:
        """Filter and normalise raw service dicts from Gemini."""
        valid = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            svc_type = str(item.get("type", "")).strip().lower().replace(" ", "_")
            desc = str(item.get("description", "")).strip()
            if not svc_type or not desc or len(desc) < 20:
                continue
            valid.append({
                "type": svc_type,
                "description": desc,
                "is_presented": True,
            })
        return valid

    def _call_gemini(self, text: str, retries: int = 5) -> list:
        contents = [
            Content(parts=[Part(text=SERVICES_EXTRACTION_PROMPT)]),
            Content(parts=[Part(text=text)]),
        ]
        for attempt in range(retries):
            try:
                response = self.client.models.generate_content(
                    model=GEMINI_MODEL_NAME,
                    contents=contents,
                    config={"temperature": 0, "max_output_tokens": 16384},
                )
                raw = response.text
                try:
                    services = json.loads(raw)
                except json.JSONDecodeError:
                    services = self._extract_json(raw)
                return self._validate_services(services)
            except Exception as e:
                wait = 2 ** attempt
                print(f"[RuleExtractor] Gemini error (attempt {attempt+1}/{retries}): {e} — retrying in {wait}s")
                time.sleep(wait)
        print("[RuleExtractor] All retries failed for this batch, skipping.")
        return []

    def _split_text(self, text: str, chunk_size: int = 50000) -> list:
        """Split text into batches at paragraph boundaries."""
        if len(text) <= chunk_size:
            return [text]
        batches, current, current_len = [], [], 0
        for para in text.split("\n\n"):
            if current_len + len(para) > chunk_size and current:
                batches.append("\n\n".join(current))
                current, current_len = [], 0
            current.append(para)
            current_len += len(para)
        if current:
            batches.append("\n\n".join(current))
        return batches

    def _dedupe_services(self, services: list) -> list:
        """Remove services with duplicate types, keeping the first occurrence."""
        seen, unique = set(), []
        for s in services:
            key = s.get("type", "").lower().strip()
            if key not in seen:
                seen.add(key)
                unique.append(s)
        return unique

    # ── public extraction methods ──────────────────────────────────────────────

    def extract_from_text(self, text: str) -> list:
        """
        Extract a list of service dicts from raw text, batching if necessary.
        Returns a deduplicated list of {type, description, is_presented} dicts.
        """
        if not text or not text.strip():
            return []

        text = text.strip()
        batches = self._split_text(text)
        print(f"[RuleExtractor] {len(text)} chars → {len(batches)} batch(es)")

        all_services = []
        for i, batch in enumerate(batches, start=1):
            print(f"[RuleExtractor] Processing batch {i}/{len(batches)}...")
            services = self._call_gemini(batch)
            print(f"[RuleExtractor] Batch {i} → {len(services)} services")
            all_services.extend(services)

        all_services = self._dedupe_services(all_services)
        return all_services

    def extract_entity(self, text: str, source: str) -> Optional[dict]:
        """
        Classify source and extract one entity (airline or airport).
        Returns an entity dict or None if the source is not in DOMAIN_MAP.
        """
        entity_info = self._classify_source(source)
        if not entity_info:
            print(f"[RuleExtractor] Skipping unclassified source: {source}")
            return None

        services = self.extract_from_text(text)
        if not services:
            print(f"[RuleExtractor] No services extracted from {source}")
            return None

        print(f"[RuleExtractor] {entity_info['entity_type'].capitalize()} '{entity_info['name']}' → {len(services)} services")

        if entity_info["entity_type"] == "airline":
            return {
                "airline_id": entity_info["entity_id"],
                "name":       entity_info["name"],
                "source":     source,
                "services":   services,
            }
        else:
            return {
                "airport_id": entity_info["entity_id"],
                "name":       entity_info["name"],
                "source":     source,
                "services":   services,
            }

    def extract_from_pdf(self, path: str) -> list:
        """Extract entity from a local PDF file. Returns [entity] or []."""
        print(f"[RuleExtractor] Reading PDF: {path}")
        text = extract_clean_pdf(path)
        entity = self.extract_entity(text, source=str(path))
        return [entity] if entity else []

    def extract_from_url(self, url: str) -> list:
        """Extract entity from a website URL or remote PDF. Returns [entity] or []."""
        print(f"[RuleExtractor] Fetching URL: {url}")
        text = fetch_pdf(url) if url.lower().endswith(".pdf") else generic_scrape(url)
        entity = self.extract_entity(text, source=url)
        return [entity] if entity else []

    def extract_from_chunks(self, chunks: list) -> list:
        """Extract entity from pre-processed pipeline chunks. Returns [entity] or []."""
        if not chunks:
            return []
        text = "\n\n".join(
            f"{c.get('section', '')}\n{c.get('text', '')}".strip()
            for c in chunks
            if c.get("text")
        )
        source = chunks[0].get("source", "unknown")
        print(f"[RuleExtractor] Extracting from {len(chunks)} chunks (source: {source})")
        entity = self.extract_entity(text, source)
        return [entity] if entity else []

    # ── output ─────────────────────────────────────────────────────────────────

    def save(self, entities: list, output_path: str):
        """Save a list of entity dicts to a JSON file."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(entities, f, indent=2)
        print(f"[RuleExtractor] Saved {len(entities)} entities → {output_path}")

    # ── main entry point ───────────────────────────────────────────────────────

    def run(self, source, output_path: str = None) -> list:
        """
        Auto-detect source type and extract entity.

        Args:
            source: one of —
                - str starting with 'http'  → scrape website or remote PDF
                - str ending with '.pdf'    → local PDF file
                - str (other)               → treat as raw text
                - list of dicts             → pre-processed pipeline chunks
            output_path: optional path to save extracted entities as JSON

        Returns:
            list containing one entity dict, or empty list if source unclassified
        """
        if isinstance(source, list):
            entities = self.extract_from_chunks(source)
        elif isinstance(source, str) and source.startswith("http"):
            entities = self.extract_from_url(source)
        elif isinstance(source, str) and source.lower().endswith(".pdf"):
            entities = self.extract_from_pdf(source)
        elif isinstance(source, str):
            entity = self.extract_entity(source, source="unknown")
            entities = [entity] if entity else []
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")

        print(f"[RuleExtractor] Entities extracted: {len(entities)}")

        if output_path and entities:
            self.save(entities, output_path)

        return entities
