# extractor/rule_extractor.py
#
# Purpose:
#   Extracts structured, actionable rules from multiple source types using Gemini AI.
#   Handles local PDFs, remote URLs (websites and PDFs), raw text, and pre-processed
#   pipeline chunks. Large documents are automatically split into batches to stay
#   within Gemini's token limits, and results are deduplicated and re-sequenced.
#
# Key class:
#   RuleExtractor — main class with the following public methods:
#     - run(source, output_path)       auto-detect source type and extract rules
#     - extract_from_pdf(path)         parse and extract from a local PDF file
#     - extract_from_url(url)          scrape a website or remote PDF and extract
#     - extract_from_text(text)        extract from any raw string
#     - extract_from_chunks(chunks)    extract from pre-processed pipeline chunks
#     - save(rules, output_path)       save extracted rules to a JSON file
#
# Output format (each rule):
#   { rule_id, category, title, description, source }
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

from dotenv import load_dotenv
from google import genai
from google.genai.types import Content, Part

from parser.local.pdf_parser import extract_clean_pdf
from parser.remote.generic_scraper import generic_scrape
from parser.remote.pdf_fetcher import fetch_pdf

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL_NAME")

RULE_EXTRACTION_PROMPT = """
You are an expert in aviation accessibility and services for passengers with reduced mobility (PRM).

Your task is to extract ALL information that would be useful to a person with reduced mobility
travelling by air — including rules, services, procedures, entitlements, tips, and updates.

EXTRACT anything that is:
- A rule, regulation, requirement, or restriction affecting PRM travellers
- A service or assistance offered (wheelchair, escort, lounge access, special seating, etc.)
- A procedure a PRM traveller must follow (how to request help, when to notify, what to bring)
- An entitlement or right the traveller has under law or airline policy
- Practical information (what equipment is allowed, battery limits, booking steps, contacts)
- Important updates or changes to PRM policies or services

STRICT RULES:
- Every item must be self-contained and directly useful to a PRM traveller
- DO NOT include navigation text, advertisements, menus, or generic marketing copy
- DO NOT hallucinate or infer anything not explicitly stated in the text
- SKIP vague filler sentences with no actionable content

CATEGORY — pick the best fit from:
Accessibility, Assistance, Baggage, Boarding, Booking, Check-in, Compensation,
Complaints, Documentation, Facilities, General, Information, Legal Rights, Medical,
Mobility Aid, Notification, Pre-Flight, Safety, Security, Service, Special Equipment,
Training, Travel Policy

OUTPUT FORMAT (STRICT JSON ONLY):
[
  {
    "rule_id": "R001",
    "category": "one category from the list above",
    "title": "short descriptive title (max 10 words)",
    "description": "clear, complete statement of the rule / service / information",
    "source": "source URL or file path provided"
  }
]

ONLY RETURN VALID JSON. No explanation or markdown.
"""


class RuleExtractor:
    def __init__(self):
        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY not set in .env")
        self.client = genai.Client(api_key=GEMINI_API_KEY)
        self._counter = 0

    def _next_id(self):
        self._counter += 1
        return f"R{self._counter:03d}"

    def _extract_json(self, text: str) -> list:
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                return []
        return []

    def _validate(self, rules: list, source: str) -> list:
        valid = []
        for r in rules:
            if not isinstance(r, dict):
                continue
            title = r.get("title", "").strip()
            description = r.get("description", "").strip()
            if not title or not description or len(description) < 20:
                continue
            valid.append({
                "rule_id": self._next_id(),   # always self-generated — never trust Gemini's IDs
                "category": r.get("category", "General").strip(),
                "title": title,
                "description": description,
                "source": source,
            })
        return valid

    def _call_gemini(self, text: str, source: str, retries: int = 5) -> list:
        contents = [
            Content(parts=[Part(text=RULE_EXTRACTION_PROMPT)]),
            Content(parts=[Part(text=text)])
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
                    rules = json.loads(raw)
                except json.JSONDecodeError:
                    rules = self._extract_json(raw)
                return self._validate(rules, source)
            except Exception as e:
                wait = 2 ** attempt
                print(f"[RuleExtractor] Gemini error (attempt {attempt+1}/{retries}): {e} — retrying in {wait}s")
                time.sleep(wait)
        print("[RuleExtractor] All retries failed for this batch, skipping.")
        return []

    def _split_text(self, text: str, chunk_size: int = 50000) -> list:
        """Split text into chunks at paragraph boundaries to stay within token limits."""
        if len(text) <= chunk_size:
            return [text]

        batches = []
        paragraphs = text.split("\n\n")
        current = []
        current_len = 0

        for para in paragraphs:
            if current_len + len(para) > chunk_size and current:
                batches.append("\n\n".join(current))
                current = []
                current_len = 0
            current.append(para)
            current_len += len(para)

        if current:
            batches.append("\n\n".join(current))

        return batches

    def _dedupe_rules(self, rules: list) -> list:
        """Remove duplicate rules based on normalized description."""
        seen = set()
        unique = []
        for r in rules:
            key = re.sub(r"\s+", " ", r.get("description", "").lower().strip())
            if key not in seen:
                seen.add(key)
                unique.append(r)
        return unique

    def _reassign_ids(self, rules: list) -> list:
        """Reassign sequential rule IDs after merging batches."""
        for i, r in enumerate(rules, start=1):
            r["rule_id"] = f"R{i:03d}"
        return rules

    # ── public extraction methods ──────────────────────────────────────────────

    def extract_from_text(self, text: str, source: str = "unknown") -> list:
        """Extract rules from a raw text string, batching if text is large."""
        if not text or not text.strip():
            return []

        text = text.strip()
        batches = self._split_text(text)
        print(f"[RuleExtractor] {len(text)} chars → {len(batches)} batch(es)")

        all_rules = []
        for i, batch in enumerate(batches, start=1):
            print(f"[RuleExtractor] Processing batch {i}/{len(batches)}...")
            rules = self._call_gemini(batch, source)
            print(f"[RuleExtractor] Batch {i} → {len(rules)} rules")
            all_rules.extend(rules)

        all_rules = self._dedupe_rules(all_rules)
        all_rules = self._reassign_ids(all_rules)
        return all_rules

    def extract_from_pdf(self, path: str) -> list:
        """Extract rules from a local PDF file."""
        print(f"[RuleExtractor] Reading PDF: {path}")
        text = extract_clean_pdf(path)
        return self.extract_from_text(text, source=str(path))

    def extract_from_url(self, url: str) -> list:
        """Extract rules from a website URL or remote PDF."""
        print(f"[RuleExtractor] Fetching URL: {url}")
        if url.lower().endswith(".pdf"):
            text = fetch_pdf(url)
        else:
            text = generic_scrape(url)
        return self.extract_from_text(text, source=url)

    def extract_from_chunks(self, chunks: list) -> list:
        """Extract rules from pre-processed pipeline chunk list."""
        if not chunks:
            return []
        text = "\n\n".join(
            f"{c.get('section', '')}\n{c.get('text', '')}".strip()
            for c in chunks
            if c.get("text")
        )
        source = chunks[0].get("source", "unknown")
        print(f"[RuleExtractor] Extracting from {len(chunks)} chunks")
        return self.extract_from_text(text, source=source)

    # ── output ─────────────────────────────────────────────────────────────────

    def save(self, rules: list, output_path: str):
        """Save extracted rules to a JSON file."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(rules, f, indent=2)
        print(f"[RuleExtractor] Saved {len(rules)} rules → {output_path}")

    # ── main entry point ────────────────────────────────────────────────────────

    def run(self, source, output_path: str = None) -> list:
        """
        Auto-detect source type and extract rules.

        Args:
            source: one of —
                - str starting with 'http'  → scrape website or remote PDF
                - str ending with '.pdf'    → local PDF file
                - str (other)               → treat as raw text
                - list of dicts             → pre-processed pipeline chunks
            output_path: optional path to save extracted rules as JSON

        Returns:
            list of rule dicts
        """
        if isinstance(source, list):
            rules = self.extract_from_chunks(source)
        elif isinstance(source, str) and source.startswith("http"):
            rules = self.extract_from_url(source)
        elif isinstance(source, str) and source.lower().endswith(".pdf"):
            rules = self.extract_from_pdf(source)
        elif isinstance(source, str):
            rules = self.extract_from_text(source)
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")

        print(f"[RuleExtractor] Total rules extracted: {len(rules)}")

        if output_path:
            self.save(rules, output_path)

        return rules
