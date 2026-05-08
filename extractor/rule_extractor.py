# extractor/rule_extractor.py
#
# Purpose:
#   Extracts PRM services (airports) and equipment rules (airlines) from scraped
#   source text using Gemini AI, producing output in the exact Firestore schema.
#
# Static-field preservation:
#   All non-dynamic fields (images, IDs, contacts, sub_rules) are preserved in
#   this priority order:
#     1. Existing Firestore doc  — highest priority (live truth)
#     2. entity_metadata.json   — fallback for new entities not yet in Firestore
#     3. Minimal auto-generated skeleton — last resort for truly unknown entities
#
# Adding a new airline/airport:
#   1. Add its domain to DOMAIN_MAP (choose a doc_id — any unique string)
#   2. Add its source URL to Config.SOURCES in config.py
#   3. Run:  python scripts/refresh_metadata.py   (re-syncs entity_metadata.json)
#   If the entity has no Firestore doc yet, the pipeline creates one using the
#   entity_metadata.json entry as the static template.
#
# Output format mirrors exact Firestore schema:
#   Airport entity: static fields + services: [{type, description:{en,ro}, is_presented}]
#   Airline entity: static fields + rules: {sub_rule_N: {crutches, walker, ...}}
#   Both carry _doc_id and _entity_type (stripped before Firestore push).

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

GEMINI_API_KEY    = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL_NAME")

# Path to the static metadata file (generated from Firestore; refresh with scripts/refresh_metadata.py)
_METADATA_FILE = Path(__file__).parent.parent / "entity_metadata.json"

def _load_entity_metadata() -> dict:
    """Load entity_metadata.json → {doc_id: {entity_type, domain, static}}."""
    if _METADATA_FILE.exists():
        with open(_METADATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

ENTITY_METADATA: dict = _load_entity_metadata()

# Domains that are regulatory / reference sources — never airline or airport entities.
# Sources from these domains are silently skipped during entity extraction.
# Add any future reference-only domains here so they never trigger warnings.
REFERENCE_DOMAINS: set[str] = {
    "iata.org",       # IATA guidance documents
    "europa.eu",      # EUR-Lex regulations
    "transportation.gov",  # US DOT passenger rights
}

# Maps registrable domain → {entity_type, doc_id}.
# doc_id must match the Firestore document ID (or be a new unique string for new entities).
# To add a new airline/airport: add its domain here, then run scripts/refresh_metadata.py.
DOMAIN_MAP: dict[str, dict] = {
    # Airlines
    "lufthansa.com":        {"entity_type": "airline", "doc_id": "wNEldqMTgEhadVENnwX4"},
    "swiss.com":            {"entity_type": "airline", "doc_id": "ozY6JntsXs8xaX5recMq"},
    "austrian.com":         {"entity_type": "airline", "doc_id": "2ikWd2vBkcfYC8pl5ol0"},
    "transavia.com":        {"entity_type": "airline", "doc_id": "3SiVsDcp48a5m5EqqKha"},
    "airdolomiti.eu":       {"entity_type": "airline", "doc_id": "BnTa7i6dnYUYbSR58ee9"},
    "brusselsairlines.com": {"entity_type": "airline", "doc_id": "G54BwnVTjLZUfvwHN9x2"},
    "klm.com":              {"entity_type": "airline", "doc_id": "LCQwUE5zXgp2nEhXCouz"},
    "britishairways.com":   {"entity_type": "airline", "doc_id": "m5039tGWrtuhTJavDShl"},
    # Airports
    "portoairport.pt":      {"entity_type": "airport", "doc_id": "rALEi1r9nmneBaupDrgQ"},
    "madeiraairport.pt":    {"entity_type": "airport", "doc_id": "BFHXjIKvD5vV7bgTjj9h"},
    "aena.es":              {"entity_type": "airport", "doc_id": "BSYBocSUduqgvkNnfFKj"},
    "ana.pt":               {"entity_type": "airport", "doc_id": "IovTojuhO54xSxLnDv5D"},
    "dubaiairports.ae":     {"entity_type": "airport", "doc_id": "PXAlYYpQkQgv2wF1RWxm"},
    "airport.md":           {"entity_type": "airport", "doc_id": "mNufBUXCPQmA6eB7NAI2"},
    "lisbon-airport.com":   {"entity_type": "airport", "doc_id": "mIm85XEjY7gkFQnz7d3G"},
}

# ── Default static skeletons ───────────────────────────────────────────────────
# Used as last-resort when an entity has no Firestore doc AND no entity_metadata entry.

def _airline_skeleton(name: str) -> dict:
    return {
        "name":                 {"en": name, "ro": name},
        "id":                   "",
        "icon":                 "",
        "background_image":     "",
        "accessibility_info":   [],
        "accessibility_contacts": {
            "phone": "", "email": "", "url": "", "whatsapp": "",
            "availability": {"en": "", "ro": ""},
        },
        "sub_rules": [{"id": "sub_rule_0", "icon": "", "name": {"en": "", "ro": ""}, "subtitle": {"en": "", "ro": ""}}],
    }

def _airport_skeleton(name: str) -> dict:
    return {
        "name":             name,
        "full_name":        name,
        "id":               "",
        "code":             "",
        "phone":            "",
        "email":            "",
        "whatsapp":         "",
        "airport_url":      "",
        "accessibility_url": "",
        "background_image": "",
        "image":            "",
    }

# ── Gemini prompts ─────────────────────────────────────────────────────────────

AIRPORT_SERVICES_PROMPT = """
You are an expert in aviation accessibility for passengers with reduced mobility (PRM).

Extract service information from the provided airport text.

Use ONLY the following service types — do not invent new ones:
  assistance_service              → PRM personal/baggage assistance service
  lounge_area                     → Lounge or dedicated waiting area
  toilets                         → Accessible toilets/bathrooms
  lifts                           → Elevators or lifts between floors
  meeting_points                  → Designated PRM meeting/arrival points
  ambulift                        → Ambulift for aircraft boarding or deplaning
  customs_declaration_assistance  → Customs/duty assistance for PRM
  immigration_assistance          → Passport/border control assistance for PRM
  transport_to_airport            → Ground transport options (bus, taxi, shuttle)
  parking                         → Accessible parking information

For each service type that has relevant information in the text, output one entry.
For services with no information (e.g. ambulift where only presence is confirmed),
use is_presented: true and omit the description field entirely.

OUTPUT — strict JSON array only, no markdown, no explanation:
[
  {
    "type": "<service_type>",
    "description": {
      "en": "<Full English description, complete sentences, min 30 chars>",
      "ro": "<Accurate Romanian translation>"
    },
    "is_presented": true
  }
]

STRICT RULES:
- Only include service types explicitly described in the text
- Descriptions must be specific and informative, not generic
- All text must be in proper sentences
- Return ONLY valid JSON
"""

AIRLINE_RULES_PROMPT = """
You are an expert in aviation PRM (Passengers with Reduced Mobility) equipment transport rules.

Extract equipment transport rules from the provided airline text and output a JSON object
that follows this EXACT schema. Fill in all values from the source text.

NUMERIC RULES:
- All dimension values (height, length, width) are integers in centimetres
- ONLY set a numeric limit if the source text EXPLICITLY states it with clear units
- If a dimension limit is not mentioned in the text, use 99999
- All battery capacity values are integers in Watt-hours (Wh)
- If a capacity is not stated, use 300 for Wh limits and 2 for maxCount

TEXT RULES:
- All text fields require BOTH "en" (English from source) and "ro" (accurate Romanian translation)
- Texts must be complete professional sentences (min 30 chars)
- positive → equipment/battery IS accepted with this message
- negative → equipment/battery is NOT accepted with this message
- attention → special handling instructions

OUTPUT — strict JSON object only (this is the content of ONE sub_rule), no markdown:

{
  "crutches": {
    "base": {
      "positive": {"en": "...", "ro": "..."}
    },
    "positive": {"en": "...", "ro": "..."}
  },
  "walker": {
    "base": {
      "height": 99999, "length": 99999, "width": 99999,
      "positive": {"en": "...", "ro": "..."},
      "negative": {"en": "...", "ro": "..."}
    },
    "positive": {"en": "...", "ro": "..."}
  },
  "wheelchairManual": {
    "base": {
      "height": 99999, "length": 99999, "width": 99999,
      "positive": {"en": "...", "ro": "..."},
      "negative": {"en": "...", "ro": "..."}
    },
    "positive": {"en": "...", "ro": "..."}
  },
  "wheelchairElectric": {
    "base": {
      "height": <int>, "length": <int>, "width": <int>,
      "positive": {"en": "...", "ro": "..."},
      "negative": {"en": "...", "ro": "..."}
    },
    "battery": {
      "lithiumIon": {
        "capacity": {
          "capacityOfOne": <int>, "capacityPerOne": <int>,
          "positive": {"en": "...", "ro": "..."},
          "negative": {"en": "...", "ro": "..."}
        },
        "nonRemovable": {"attention": {"en": "...", "ro": "..."}},
        "removable":    {"attention": {"en": "...", "ro": "..."}},
        "spareBatteries": {
          "capacityOfOne": <int>, "capacityPerOne": <int>, "maxCount": <int>,
          "positive": {"en": "...", "ro": "..."},
          "negative": {"en": "...", "ro": "..."}
        }
      },
      "nonSpillableNikelDry": {
        "nonRemovable": {"attention": {"en": "...", "ro": "..."}},
        "removable":    {"attention": {"en": "...", "ro": "..."}},
        "spareBatteries": {
          "maxCount": <int>,
          "positive": {"en": "...", "ro": "..."},
          "negative": {"en": "...", "ro": "..."}
        }
      },
      "nonSpillableWet": {
        "nonRemovable": {"attention": {"en": "...", "ro": "..."}},
        "removable":    {"attention": {"en": "...", "ro": "..."}},
        "spareBatteries": {
          "maxCount": <int>,
          "positive": {"en": "...", "ro": "..."},
          "negative": {"en": "...", "ro": "..."}
        }
      },
      "spillable": {
        "nonRemovable": {"attention": {"en": "...", "ro": "..."}},
        "removable":    {"attention": {"en": "...", "ro": "..."}},
        "spareBatteries": {
          "maxCount": <int>,
          "negative": {"en": "...", "ro": "..."}
        }
      }
    },
    "positive": {"en": "...", "ro": "..."}
  },
  "electricScooter": {
    "base": {
      "height": <int>, "length": <int>, "width": <int>,
      "positive": {"en": "...", "ro": "..."},
      "negative": {"en": "...", "ro": "..."}
    },
    "battery": {
      "lithiumIon": {
        "capacity": {
          "capacityOfOne": <int>, "capacityPerOne": <int>,
          "positive": {"en": "...", "ro": "..."},
          "negative": {"en": "...", "ro": "..."}
        },
        "nonRemovable": {"attention": {"en": "...", "ro": "..."}},
        "removable":    {"attention": {"en": "...", "ro": "..."}},
        "spareBatteries": {
          "capacityOfOne": <int>, "capacityPerOne": <int>, "maxCount": <int>,
          "positive": {"en": "...", "ro": "..."},
          "negative": {"en": "...", "ro": "..."}
        }
      },
      "nonSpillableNikelDry": {
        "nonRemovable": {"attention": {"en": "...", "ro": "..."}},
        "removable":    {"attention": {"en": "...", "ro": "..."}},
        "spareBatteries": {
          "maxCount": <int>,
          "positive": {"en": "...", "ro": "..."},
          "negative": {"en": "...", "ro": "..."}
        }
      },
      "nonSpillableWet": {
        "nonRemovable": {"attention": {"en": "...", "ro": "..."}},
        "removable":    {"attention": {"en": "...", "ro": "..."}},
        "spareBatteries": {
          "maxCount": <int>,
          "positive": {"en": "...", "ro": "..."},
          "negative": {"en": "...", "ro": "..."}
        }
      },
      "spillable": {
        "nonRemovable": {"attention": {"en": "...", "ro": "..."}},
        "removable":    {"attention": {"en": "...", "ro": "..."}},
        "spareBatteries": {
          "maxCount": <int>,
          "negative": {"en": "...", "ro": "..."}
        }
      }
    },
    "positive": {"en": "...", "ro": "..."}
  },
  "powerAttachment": {
    "base": {
      "height": <int>, "length": <int>, "width": <int>, "weight": <int>,
      "positive": {"en": "...", "ro": "..."},
      "negative": {"en": "...", "ro": "..."}
    },
    "battery": {
      "lithiumIon": {
        "capacity": {
          "capacityOfOne": <int>, "capacityPerOne": <int>,
          "positive": {"en": "...", "ro": "..."},
          "negative": {"en": "...", "ro": "..."}
        },
        "nonRemovable": {"attention": {"en": "...", "ro": "..."}},
        "removable":    {"attention": {"en": "...", "ro": "..."}},
        "spareBatteries": {
          "capacityOfOne": <int>, "capacityPerOne": <int>, "maxCount": <int>,
          "positive": {"en": "...", "ro": "..."},
          "negative": {"en": "...", "ro": "..."}
        }
      },
      "nonSpillableNikelDry": {
        "nonRemovable": {"attention": {"en": "...", "ro": "..."}},
        "removable":    {"attention": {"en": "...", "ro": "..."}},
        "spareBatteries": {
          "maxCount": <int>,
          "positive": {"en": "...", "ro": "..."},
          "negative": {"en": "...", "ro": "..."}
        }
      },
      "nonSpillableWet": {
        "nonRemovable": {"attention": {"en": "...", "ro": "..."}},
        "removable":    {"attention": {"en": "...", "ro": "..."}},
        "spareBatteries": {
          "maxCount": <int>,
          "positive": {"en": "...", "ro": "..."},
          "negative": {"en": "...", "ro": "..."}
        }
      },
      "spillable": {
        "nonRemovable": {"attention": {"en": "...", "ro": "..."}},
        "removable":    {"attention": {"en": "...", "ro": "..."}},
        "spareBatteries": {
          "maxCount": <int>,
          "negative": {"en": "...", "ro": "..."}
        }
      }
    },
    "positive": {"en": "...", "ro": "..."}
  }
}

Return ONLY valid JSON matching this exact schema. No markdown, no explanation.
"""

AIRLINE_RULES_UPDATE_PROMPT = """
You are an expert in aviation PRM (Passengers with Reduced Mobility) equipment transport rules.

You will receive:
  1. EXISTING_RULES — the current curated rules JSON for this airline (from Firestore)
  2. SOURCE_TEXT — new content from the airline's accessibility/PRM webpage

Your task: return an UPDATED version of EXISTING_RULES where ONLY the text descriptions
(all "en" and "ro" string values inside positive/negative/attention keys) may change.

STRICT RULES — read carefully:
- NEVER change numeric values (height, length, width, weight, capacity, capacityOfOne,
  capacityPerOne, maxCount) UNLESS the SOURCE_TEXT explicitly states a different number
  with clear units (e.g. "max 150 cm", "300 Wh").  If in doubt, keep the existing value.
- NEVER add or remove keys from the JSON structure — return exactly the same keys.
- Update "en" text only when the source provides clearer, more accurate wording.
- Always set "ro" to an accurate Romanian translation of the final "en" text.
- If the source text has no information about a rule, copy the existing text unchanged.

Return ONLY the updated JSON object (same structure as EXISTING_RULES).
No markdown code fences, no explanation — raw JSON only.
"""


class RuleExtractor:
    _MIN_CALL_INTERVAL = 5.0  # seconds between Gemini calls

    def __init__(self):
        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY not set in .env")
        self.client = genai.Client(api_key=GEMINI_API_KEY)
        self._last_call_time = 0.0

    # ── domain classification ──────────────────────────────────────────────────

    def _extract_domain(self, url: str) -> str:
        host = urlparse(url).hostname or ""
        if host.startswith("www."):
            host = host[4:]
        parts = host.split(".")
        if len(parts) > 2:
            host = ".".join(parts[-2:])
        return host

    def _classify_source(self, source: str) -> Optional[dict]:
        """
        Return classification info for a source URL.

        Checks DOMAIN_MAP first. If not found, scans ENTITY_METADATA by domain field
        so entities registered only in entity_metadata.json (not yet added to DOMAIN_MAP)
        are still recognised.

        Returns dict with entity_type + doc_id, or None if source should be skipped.
        Silently skips REFERENCE_DOMAINS and local files (no domain).
        Prints a one-line warning for unknown airline/airport domains.
        """
        domain = self._extract_domain(source)

        # Silently skip: local files (no URL domain) and known reference-only sources
        if not domain or domain in REFERENCE_DOMAINS:
            return None

        # Primary: DOMAIN_MAP lookup
        if domain in DOMAIN_MAP:
            return DOMAIN_MAP[domain]

        # Secondary: scan entity_metadata.json by domain field
        for doc_id, meta in ENTITY_METADATA.items():
            if meta.get("domain") == domain:
                print(f"[RuleExtractor] Found '{domain}' in entity_metadata.json (doc_id={doc_id})")
                return {"entity_type": meta["entity_type"], "doc_id": doc_id}

        # Unknown airline/airport — one-line warning, no multi-line block
        print(f"[RuleExtractor] Skipped '{domain}' — add to DOMAIN_MAP to enable extraction")
        return None

    # ── Gemini helpers ─────────────────────────────────────────────────────────

    def _extract_json(self, text: str):
        """Extract first JSON object or array from raw Gemini text."""
        for pattern in (r"\{.*\}", r"\[.*\]"):
            match = re.search(pattern, text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group())
                except json.JSONDecodeError:
                    pass
        return None

    def _call_gemini(self, prompt: str, text: str, retries: int = 6):
        # Enforce minimum gap between calls to stay within free-tier RPM limit
        elapsed = time.time() - self._last_call_time
        if elapsed < self._MIN_CALL_INTERVAL:
            time.sleep(self._MIN_CALL_INTERVAL - elapsed)
        self._last_call_time = time.time()

        # Everything in a single user message avoids multi-turn confusion and
        # lets the model see the full context before generating its response.
        full_input = prompt + "\n\n" + text
        contents = [
            Content(role="user", parts=[Part(text=full_input)]),
        ]
        for attempt in range(retries):
            try:
                response = self.client.models.generate_content(
                    model=GEMINI_MODEL_NAME,
                    contents=contents,
                    config={"temperature": 0, "max_output_tokens": 65536},
                )
                self._last_call_time = time.time()
                raw = response.text
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    result = self._extract_json(raw)
                    if result is not None:
                        return result
                    print(f"[RuleExtractor] JSON parse failed on attempt {attempt+1}, raw snippet: {raw[:300]}")
            except Exception as e:
                err_str = str(e).lower()
                is_overload = any(s in err_str for s in (
                    "429", "quota", "rate", "503", "unavailable", "high demand",
                ))
                if is_overload:
                    # Exponential backoff: 5s, 10s, 20s, 40s, 60s
                    wait = min(5 * (2 ** attempt), 60)
                    print(f"[RuleExtractor] Rate-limited (attempt {attempt+1}/{retries}) — waiting {wait}s")
                else:
                    wait = min(2 ** attempt, 60)
                    print(f"[RuleExtractor] Gemini error (attempt {attempt+1}/{retries}): {e} — retrying in {wait}s")
                time.sleep(wait)
                self._last_call_time = time.time()
        print("[RuleExtractor] All retries failed, returning None.")
        return None

    def _split_text(self, text: str, chunk_size: int = 50000) -> list:
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

    # ── airport extraction ─────────────────────────────────────────────────────

    def _extract_airport_services(self, text: str) -> list:
        """Extract airport services array in Firestore format {type, description:{en,ro}, is_presented}."""
        batches = self._split_text(text)
        all_services: dict[str, dict] = {}

        for i, batch in enumerate(batches, 1):
            print(f"[RuleExtractor] Airport batch {i}/{len(batches)}...")
            result = self._call_gemini(AIRPORT_SERVICES_PROMPT, batch)
            if not isinstance(result, list):
                continue
            for svc in result:
                if not isinstance(svc, dict) or not svc.get("type"):
                    continue
                svc_type = svc["type"].strip()
                if svc_type not in all_services:
                    all_services[svc_type] = svc

        return list(all_services.values())

    # ── airline extraction ─────────────────────────────────────────────────────

    # Phrases Gemini writes when it can't find relevant info in the source text.
    # Any updated string containing one of these is treated as "no info found"
    # and the existing Firestore text is restored in its place.
    _CANT_FIND_PHRASES = [
        "provided text does not contain",
        "textul furnizat nu conține",
        "source text does not",
        "text does not mention",
        "not mentioned in",
        "not specified in",
        "not stated in",
        "no information",
        "no specific information",
        "no specific positive",
        "no specific negative",
        "no specific handling",
        "no specific transport",
        "nu conține reguli specifice",
        "nu conține instrucțiuni specifice",
    ]

    @classmethod
    def _restore_from_existing(cls, existing, updated):
        """
        Recursively merge Gemini's updated output with the existing Firestore
        value, enforcing two hard rules:

        1. Numerics (int/float): ALWAYS restore from existing — Gemini cannot
           be trusted to honour dimension / capacity values.
        2. Strings: if Gemini wrote a "can't find" explanation instead of real
           content, restore the existing text verbatim.

        This makes the update prompt-failure-safe: even if Gemini ignores the
        numeric-preservation instruction, the post-processing corrects it.
        """
        # Rule 1 — numeric values are always taken from existing
        if isinstance(existing, (int, float)) and not isinstance(existing, bool):
            return existing

        # Rule 2 — if Gemini admitted it couldn't find the info, keep existing
        if isinstance(existing, str) and isinstance(updated, str):
            lower = updated.lower()
            if any(phrase in lower for phrase in cls._CANT_FIND_PHRASES):
                return existing
            return updated  # genuine update — keep it

        # Recurse into dicts
        if isinstance(existing, dict) and isinstance(updated, dict):
            result = {}
            for k in set(existing) | set(updated):
                if k not in updated:
                    result[k] = existing[k]          # key removed by Gemini — restore
                elif k not in existing:
                    pass                             # new key from Gemini — reject (update prompt: "NEVER add keys")
                else:
                    result[k] = cls._restore_from_existing(existing[k], updated[k])
            return result

        # Everything else (bool, list, None) — use updated if present
        return updated if updated is not None else existing

    def _extract_airline_rules(self, text: str, sub_rule_ids: list,
                               existing_rules: dict = None) -> dict:
        """
        Extract airline equipment rules in Firestore format.

        UPDATE mode (existing_rules provided):
          Sends existing canonical sub_rule + source text to Gemini, asking it
          to refresh only text descriptions.  After the call, _restore_from_existing
          is applied to fix any numerics or "can't find" texts Gemini got wrong.

        FRESH mode (no existing_rules):
          Extracts everything from the source text.

        Returns {sub_rule_id: rule_dict} for every sub_rule ID.
        """
        ids = sub_rule_ids if sub_rule_ids else ["sub_rule_0"]

        if existing_rules:
            first_key = next(iter(existing_rules))
            canonical_existing = existing_rules[first_key]
            print(
                f"[RuleExtractor] UPDATE mode — existing rules found "
                f"({len(existing_rules)} sub_rule(s)), refreshing text only..."
            )
            combined_prompt = (
                AIRLINE_RULES_UPDATE_PROMPT
                + "\n\nEXISTING_RULES:\n"
                + json.dumps(canonical_existing, ensure_ascii=False, indent=2)
                + "\n\nSOURCE_TEXT:\n"
            )
            result = self._call_gemini(combined_prompt, text[:30000])

            if not isinstance(result, dict):
                # Gemini failed entirely — return every sub_rule exactly as it
                # is in Firestore (do NOT copy sub_rule_0 across all sub_rules,
                # since each may have different dimension values).
                print("[RuleExtractor] Gemini returned no valid dict — keeping existing rules unchanged.")
                return existing_rules

            # Post-process per sub_rule: restore numerics and "can't find"
            # texts using each sub_rule's own Firestore values as the base,
            # so sub_rule_3's dimensions aren't overwritten by sub_rule_0's.
            output = {}
            total_restored = 0
            for sr_id in ids:
                base = existing_rules.get(sr_id, canonical_existing)
                restored = self._restore_from_existing(base, result)
                total_restored += self._count_numeric_restorations(base, result)
                output[sr_id] = restored
            if total_restored:
                print(f"[RuleExtractor] Restored {total_restored} numeric value(s) overwritten by Gemini.")
            return output

        else:
            print("[RuleExtractor] FRESH mode — extracting from source text...")
            result = self._call_gemini(AIRLINE_RULES_PROMPT, text[:50000])

            if not isinstance(result, dict):
                print("[RuleExtractor] Airline rules extraction returned no valid dict.")
                return {}

            return {sr_id: result for sr_id in ids}

    @classmethod
    def _count_numeric_restorations(cls, existing, updated, _count=None) -> int:
        """Count how many numeric fields were restored (for logging)."""
        if _count is None:
            _count = [0]
        if isinstance(existing, (int, float)) and not isinstance(existing, bool):
            if existing != updated:
                _count[0] += 1
        elif isinstance(existing, dict) and isinstance(updated, dict):
            for k in existing:
                if k in updated:
                    cls._count_numeric_restorations(existing[k], updated[k], _count)
        return _count[0]

    # ── entity assembly ────────────────────────────────────────────────────────

    def _resolve_static_base(self, doc_id: str, entity_type: str,
                              existing_docs: dict, source_name: str) -> dict:
        """
        Return the static field base for an entity, in priority order:

        1. Existing Firestore doc  — live truth, always preferred
        2. entity_metadata.json   — fallback for new entities not yet in Firestore
        3. Auto-generated skeleton — last resort; warns user to fill in metadata

        The caller then overlays the extracted dynamic field (services/rules) on top.
        """
        # Priority 1: Firestore doc exists
        existing = (existing_docs or {}).get(doc_id)
        if existing:
            return existing

        # Priority 2: entity_metadata.json has a static template
        meta = ENTITY_METADATA.get(doc_id, {})
        if meta.get("static"):
            print(
                f"[RuleExtractor] '{source_name}' is NEW — not in Firestore yet.\n"
                f"  Using entity_metadata.json template as static base.\n"
                f"  A new Firestore document will be created (doc_id={doc_id}).\n"
                f"  Update images, contacts, and sub_rules in Firestore after first push."
            )
            return dict(meta["static"])

        # Priority 3: generate a minimal skeleton, warn loudly
        print(
            f"\n[RuleExtractor] ⚠ '{source_name}' has no Firestore doc AND no entity_metadata.json entry.\n"
            f"  Creating a minimal skeleton — static fields (name, images, contacts) will be EMPTY.\n"
            f"  After the pipeline runs, fill in the missing fields in Firestore manually,\n"
            f"  then run:  python scripts/refresh_metadata.py  to sync entity_metadata.json.\n"
        )
        if entity_type == "airline":
            return _airline_skeleton(source_name)
        return _airport_skeleton(source_name)

    def extract_entity(self, text: str, source: str, existing_docs: dict = None) -> Optional[dict]:
        """
        Classify source, extract dynamic content, and merge with the correct static base.

        Static fields are resolved via _resolve_static_base() (three-tier priority).
        Returns entity dict with _doc_id and _entity_type metadata (stripped before push).
        Returns None if source domain is completely unknown (not in DOMAIN_MAP or metadata).
        """
        info = self._classify_source(source)
        if not info:
            return None  # warning already printed by _classify_source

        doc_id      = info["doc_id"]
        entity_type = info["entity_type"]

        # Resolve display name for logs
        meta_name = ENTITY_METADATA.get(doc_id, {}).get("static", {}).get("name") or doc_id
        if isinstance(meta_name, dict):
            meta_name = meta_name.get("en", doc_id)

        print(f"[RuleExtractor] Processing {entity_type}: {meta_name} (doc_id={doc_id})")

        static_base = self._resolve_static_base(doc_id, entity_type, existing_docs, meta_name)

        if entity_type == "airport":
            services = self._extract_airport_services(text)
            if not services:
                print(f"[RuleExtractor] No services extracted for {meta_name}")
                return None
            print(f"[RuleExtractor] Extracted {len(services)} services")
            entity = {**static_base, "services": services}

        else:  # airline
            sub_rule_ids = [sr["id"] for sr in static_base.get("sub_rules", [])]
            # Pass existing rules so Gemini can preserve numeric values
            existing_rules = static_base.get("rules") or None
            rules = self._extract_airline_rules(text, sub_rule_ids,
                                                existing_rules=existing_rules)
            if not rules:
                print(f"[RuleExtractor] No rules extracted for {meta_name}")
                return None
            print(f"[RuleExtractor] Extracted rules for {len(rules)} sub_rule(s)")
            entity = {**static_base, "rules": rules}

        entity["_doc_id"]      = doc_id
        entity["_entity_type"] = entity_type
        return entity

    # ── source ingestion ───────────────────────────────────────────────────────

    def extract_from_chunks(self, chunks: list, existing_docs: dict = None) -> list:
        """Extract entity from pre-processed pipeline chunks."""
        if not chunks:
            return []
        text = "\n\n".join(
            f"{c.get('section', '')}\n{c.get('text', '')}".strip()
            for c in chunks if c.get("text")
        )
        source = chunks[0].get("source", "unknown")
        print(f"[RuleExtractor] Extracting from {len(chunks)} chunks (source: {source})")
        entity = self.extract_entity(text, source, existing_docs)
        return [entity] if entity else []

    def extract_from_url(self, url: str, existing_docs: dict = None) -> list:
        """Extract entity from a website URL or remote PDF."""
        print(f"[RuleExtractor] Fetching URL: {url}")
        text = fetch_pdf(url) if url.lower().endswith(".pdf") else generic_scrape(url)
        entity = self.extract_entity(text, url, existing_docs)
        return [entity] if entity else []

    def extract_from_pdf(self, path: str, existing_docs: dict = None) -> list:
        """Extract entity from a local PDF file."""
        print(f"[RuleExtractor] Reading PDF: {path}")
        text = extract_clean_pdf(path)
        entity = self.extract_entity(text, str(path), existing_docs)
        return [entity] if entity else []

    # ── output ─────────────────────────────────────────────────────────────────

    def save(self, entities: list, output_path: str):
        """Save all entities to a single JSON file (pipeline-internal metadata included)."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(entities, f, indent=2, ensure_ascii=False)
        print(f"[RuleExtractor] Saved {len(entities)} entities → {output_path}")

    def save_split(self, entities: list, output_dir: str):
        """
        Save each entity to its own file keyed by Firestore doc_id.

        Airlines → <output_dir>/airlines/<doc_id>.json
        Airports → <output_dir>/airports/<doc_id>.json
        """
        airlines_dir = Path(output_dir) / "airlines"
        airports_dir = Path(output_dir) / "airports"
        airlines_dir.mkdir(parents=True, exist_ok=True)
        airports_dir.mkdir(parents=True, exist_ok=True)

        for entity in entities:
            doc_id      = entity.get("_doc_id")
            entity_type = entity.get("_entity_type")
            if not doc_id or not entity_type:
                print(f"[RuleExtractor] Skipping entity missing _doc_id/_entity_type")
                continue
            folder = airlines_dir if entity_type == "airline" else airports_dir
            dest = folder / f"{doc_id}.json"
            with open(dest, "w", encoding="utf-8") as f:
                json.dump(entity, f, indent=2, ensure_ascii=False)
            print(f"[RuleExtractor] Saved → {dest}")

        print(f"[RuleExtractor] Split save complete — {len(entities)} entities")

    # ── main entry point ───────────────────────────────────────────────────────

    def run(self, source, existing_docs: dict = None, output_path: str = None) -> list:
        """
        Auto-detect source type and extract entity.

        Args:
            source:        list of chunk dicts | URL string | local PDF path
            existing_docs: {doc_id: firestore_data} from FirestoreClient.fetch_all()
            output_path:   optional path to save extracted entities as JSON

        Returns:
            list containing one entity dict, or [] if source unclassified
        """
        if isinstance(source, list):
            entities = self.extract_from_chunks(source, existing_docs)
        elif isinstance(source, str) and source.startswith("http"):
            entities = self.extract_from_url(source, existing_docs)
        elif isinstance(source, str) and source.lower().endswith(".pdf"):
            entities = self.extract_from_pdf(source, existing_docs)
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")

        print(f"[RuleExtractor] Entities extracted: {len(entities)}")

        if output_path and entities:
            self.save(entities, output_path)

        return entities
