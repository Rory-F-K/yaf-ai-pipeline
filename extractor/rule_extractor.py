import json
import re
import os
import time
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from google import genai
from google.genai.types import Content, Part

load_dotenv()

GEMINI_API_KEY    = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL_NAME")

EXTRACTION_PROMPT = """
You are an expert in aviation accessibility for passengers with reduced mobility (PRM).

Extract ALL services, assistance options, procedures, and policies relevant to PRM passengers
from the provided text.

For each item output:
- type: short lowercase snake_case label (e.g. "wheelchair_assistance", "pre_notification", "battery_limit")
- description.en: clear, complete English statement (minimum 20 characters)
- description.ro: accurate Romanian translation of description.en

STRICT RULES:
- Only extract what is explicitly stated — do not infer or hallucinate
- Each item must be directly useful to a PRM traveller
- Skip navigation text, ads, menus, and generic marketing copy
- Merge closely related details into one item rather than splitting artificially
- Both en and ro must be full sentences

OUTPUT FORMAT — strict JSON array only, no markdown, no explanation:
[
  {
    "type": "snake_case_label",
    "description": {
      "en": "Clear English statement of the service or policy.",
      "ro": "Traducere română clară a serviciului sau politicii."
    }
  }
]
"""

# entity_type values that represent a real airline or airport entity
_SUPPORTED_TYPES = {"airline", "airport"}


class RuleExtractor:
    def __init__(self):
        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY not set in .env")
        self.client = genai.Client(api_key=GEMINI_API_KEY)

    # ── Gemini ─────────────────────────────────────────────────────────────────

    def _extract_json(self, text: str) -> list:
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                return []
        return []

    def _validate_services(self, raw: list) -> list:
        valid = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            svc_type = str(item.get("type", "")).strip().lower().replace(" ", "_")
            desc = item.get("description", {})
            if not isinstance(desc, dict):
                continue
            en = str(desc.get("en", "")).strip()
            ro = str(desc.get("ro", "")).strip()
            if not svc_type or not en or len(en) < 20:
                continue
            if not ro:
                ro = en  # fallback if Gemini omits ro
            valid.append({
                "type":         svc_type,
                "description":  {"en": en, "ro": ro},
                "is_presented": True,
            })
        return valid

    def _call_gemini(self, text: str, retries: int = 5) -> list:
        contents = [
            Content(parts=[Part(text=EXTRACTION_PROMPT)]),
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
        print("[RuleExtractor] All retries failed — skipping.")
        return []

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

    def _dedupe_services(self, services: list) -> list:
        seen, unique = set(), []
        for s in services:
            key = s.get("type", "").lower().strip()
            if key not in seen:
                seen.add(key)
                unique.append(s)
        return unique

    # ── shared text extraction ─────────────────────────────────────────────────

    def _extract_text_and_call(self, chunks: list, source_id: str, label: str) -> list:
        """Join chunk text, split into batches, call Gemini, return deduped services."""
        text = "\n\n".join(
            f"{c.get('section', '')}\n{c.get('text', '')}".strip()
            for c in chunks
            if c.get("text")
        )
        if not text.strip():
            print(f"[RuleExtractor] Skipping {source_id} — no text in chunks")
            return []

        batches = self._split_text(text)
        print(f"[RuleExtractor] {label} | {len(text)} chars → {len(batches)} batch(es)")

        all_services = []
        for i, batch in enumerate(batches, start=1):
            if i > 1:
                time.sleep(3)
            print(f"[RuleExtractor] Batch {i}/{len(batches)}...")
            svcs = self._call_gemini(batch)
            print(f"[RuleExtractor] Batch {i} → {len(svcs)} services")
            all_services.extend(svcs)

        return self._dedupe_services(all_services)

    # ── public API ─────────────────────────────────────────────────────────────

    def extract_entity_from_chunks(self, chunks: list) -> Optional[dict]:
        """
        Extract PRM services from airline/airport agentic chunks.
        Uses entity_name/entity_type already embedded by the pipeline.
        Returns None if entity_type is not 'airline' or 'airport'.
        """
        if not chunks:
            return None

        first       = chunks[0]
        entity_name = first.get("entity")
        entity_type = first.get("entity_type")
        source_id   = first.get("source_id", "unknown")

        if not entity_name or entity_type not in _SUPPORTED_TYPES:
            print(f"[RuleExtractor] Skipping {source_id} — entity_type={entity_type!r}")
            return None

        services = self._extract_text_and_call(chunks, source_id, f"{entity_name} ({entity_type})")
        if not services:
            print(f"[RuleExtractor] No services extracted for {entity_name}")
            return None

        print(f"[RuleExtractor] {entity_name} → {len(services)} services total")
        return {
            "source_id":   source_id,
            "entity_name": entity_name,
            "entity_type": entity_type,
            "services":    services,
        }

    def extract_reference_from_chunks(self, chunks: list) -> Optional[dict]:
        """
        Extract PRM rules from regulatory/industry reference chunks (IATA, EUR-LEX, etc.).
        No entity_type restriction — all sources are processed.
        Returns None only if the file is empty or yields no services.
        """
        if not chunks:
            return None

        first       = chunks[0]
        source_id   = first.get("source_id", "unknown")
        entity_name = first.get("entity") or source_id
        entity_type = first.get("entity_type", "reference")
        source_url  = first.get("source", "")

        services = self._extract_text_and_call(chunks, source_id, f"{entity_name} ({entity_type})")
        if not services:
            print(f"[RuleExtractor] No rules extracted for {entity_name}")
            return None

        print(f"[RuleExtractor] {entity_name} → {len(services)} rules total")
        return {
            "source_id":   source_id,
            "entity":      entity_name,
            "entity_type": entity_type,
            "source_url":  source_url,
            "services":    services,
        }
