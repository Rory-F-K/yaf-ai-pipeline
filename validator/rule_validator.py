# validator/rule_validator.py
#
# Purpose:
#   Validates a list of extracted rules across five independent checks to ensure
#   quality, consistency, and correctness before rules are stored or used downstream.
#
# Checks performed:
#   1. structure     — required fields present, correct types, rule_id format (R<digits>)
#   2. quality       — description length, vague language, all-caps headings, generic titles
#   3. consistency   — duplicate rule_ids, non-sequential ordering, unknown categories
#   4. duplicates    — near-identical descriptions within the ruleset (≥85% similarity)
#   5. cross_source  — rules from different sources that may conflict (Gemini-confirmed)
#
# Severity levels:
#   error   — rule is excluded from clean_rules output
#   warning — rule is kept but flagged for review
#   info    — informational note, no action required
#
# Key class:
#   RuleValidator — main class with the following public methods:
#     - validate(rules)                    run all checks on a list of rule dicts
#     - validate_file(json_path)           load a JSON file and validate it
#     - save_report(report, output_path)   save full validation report as JSON
#     - save_clean_rules(report, path)     save only error-free rules as JSON
#     - print_summary(report)              print human-readable summary to stdout
#
# Dependencies:
#   - GEMINI_API_KEY and GEMINI_MODEL_NAME must be set in .env (only for cross_source check)
#   - Set use_gemini=False to run fully offline

import json
import re
import os
import time
from difflib import SequenceMatcher
from pathlib import Path
from collections import defaultdict

from dotenv import load_dotenv
from google import genai
from google.genai.types import Content, Part

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL_NAME")

REQUIRED_FIELDS = {"rule_id", "title", "description", "category", "source"}

KNOWN_CATEGORIES = {
    "Accessibility", "Assistance", "Baggage", "Boarding", "Booking",
    "Check-in", "Compensation", "Complaints", "Documentation", "Facilities",
    "General", "Information", "Legal Rights", "Medical", "Mobility Aid",
    "Notification", "Pre-Flight", "Safety", "Security", "Service",
    "Special Equipment", "Training", "Travel Policy",
}

VAGUE_PHRASES = [
    "may or may not", "and so on", "general information",
    "based on circumstances", "click here", "learn more", "read more",
    "for more information", "please visit", "contact us for details",
]

DUPLICATE_THRESHOLD = 0.85    # similarity score to flag as duplicate
CONFLICT_THRESHOLD  = 0.65    # similarity score to flag as cross-source conflict

CONFLICT_PROMPT = """
You are a policy expert. You are given two rules from different sources that appear to be about the same topic.

Determine if they CONFLICT with each other (i.e. they make contradictory statements about the same policy).

Rule A (source: {source_a}):
{desc_a}

Rule B (source: {source_b}):
{desc_b}

Reply with a single JSON object:
{{
  "conflict": true or false,
  "reason": "one sentence explanation"
}}

ONLY return valid JSON.
"""


# ── Issue model ────────────────────────────────────────────────────────────────

def _issue(rule_id, check, severity, message):
    return {
        "rule_id":  rule_id,
        "check":    check,
        "severity": severity,   # "error" | "warning" | "info"
        "message":  message,
    }


# ── Validation checks ──────────────────────────────────────────────────────────

def check_structure(rules: list) -> list:
    """Check required fields, types, and non-empty values."""
    issues = []

    for r in rules:
        rid = r.get("rule_id", "UNKNOWN")

        # missing or empty required fields
        for field in REQUIRED_FIELDS:
            val = r.get(field)
            if not val or not str(val).strip():
                issues.append(_issue(rid, "structure",  "error",
                    f"Missing or empty required field: '{field}'"))

        # rule_id format must be R followed by digits
        if not re.fullmatch(r"R\d+", str(r.get("rule_id", ""))):
            issues.append(_issue(rid, "structure", "error",
                f"rule_id '{r.get('rule_id')}' does not match expected format R<number>"))

        # all fields must be strings
        for field in REQUIRED_FIELDS:
            val = r.get(field)
            if val is not None and not isinstance(val, str):
                issues.append(_issue(rid, "structure", "error",
                    f"Field '{field}' must be a string, got {type(val).__name__}"))

    return issues


def check_quality(rules: list) -> list:
    """Flag rules that are too short, too vague, or look like noise."""
    issues = []

    for r in rules:
        rid = r.get("rule_id", "UNKNOWN")
        desc  = r.get("description", "").strip()
        title = r.get("title", "").strip()

        # description too short
        if len(desc) < 30:
            issues.append(_issue(rid, "quality", "error",
                f"Description too short ({len(desc)} chars) — likely not a real rule"))

        # description is all-caps (looks like a heading, not a rule)
        if desc.isupper() and len(desc) < 100:
            issues.append(_issue(rid, "quality", "warning",
                "Description appears to be a heading, not a rule statement"))

        # vague phrases
        desc_lower = desc.lower()
        for phrase in VAGUE_PHRASES:
            if phrase in desc_lower:
                issues.append(_issue(rid, "quality", "warning",
                    f"Description contains vague phrase: '{phrase}'"))
                break

        # title too generic
        generic_titles = {"general", "overview", "introduction", "note", "other", "misc"}
        if title.lower() in generic_titles:
            issues.append(_issue(rid, "quality", "warning",
                f"Title '{title}' is too generic"))

        # description same as title
        if desc.lower() == title.lower():
            issues.append(_issue(rid, "quality", "warning",
                "Description is identical to title — add more detail"))

    return issues


def check_consistency(rules: list) -> list:
    """Check for duplicate IDs, non-sequential ordering, and unknown categories."""
    issues = []

    id_counts = defaultdict(list)
    for r in rules:
        id_counts[r.get("rule_id", "")].append(r)

    # duplicate rule_ids
    for rid, group in id_counts.items():
        if len(group) > 1:
            issues.append(_issue(rid, "consistency", "error",
                f"rule_id '{rid}' appears {len(group)} times — IDs must be unique"))

    # non-sequential IDs
    ids = [r.get("rule_id", "") for r in rules]
    numeric = []
    for rid in ids:
        m = re.match(r"R(\d+)", rid)
        if m:
            numeric.append(int(m.group(1)))

    for i, n in enumerate(numeric):
        expected = i + 1
        if n != expected:
            issues.append(_issue(ids[i], "consistency", "warning",
                f"rule_id sequence broken: expected R{expected:03d}, found {ids[i]}"))

    # unknown categories
    for r in rules:
        cat = r.get("category", "").strip()
        if cat and cat not in KNOWN_CATEGORIES:
            issues.append(_issue(r.get("rule_id", "UNKNOWN"), "consistency", "info",
                f"Unknown category '{cat}' — consider aligning to a standard category"))

    return issues


def check_duplicates(rules: list) -> list:
    """Flag rules with near-identical descriptions (within same or across sources)."""
    issues = []
    flagged = set()

    for i in range(len(rules)):
        for j in range(i + 1, len(rules)):
            pair = (rules[i].get("rule_id"), rules[j].get("rule_id"))
            if pair in flagged:
                continue

            desc_i = rules[i].get("description", "").lower().strip()
            desc_j = rules[j].get("description", "").lower().strip()

            score = SequenceMatcher(None, desc_i, desc_j).ratio()

            if score >= DUPLICATE_THRESHOLD:
                flagged.add(pair)
                issues.append(_issue(pair[0], "duplicates", "warning",
                    f"Near-duplicate of {pair[1]} (similarity {score:.0%}) — consider merging"))
                issues.append(_issue(pair[1], "duplicates", "warning",
                    f"Near-duplicate of {pair[0]} (similarity {score:.0%}) — consider merging"))

    return issues


def check_cross_source_conflicts(rules: list, use_gemini: bool = True) -> list:
    """
    Find rules from different sources that are similar enough to potentially conflict.
    Uses SequenceMatcher for candidate selection, then Gemini to confirm actual conflicts.
    """
    issues = []

    # only compare rules from different sources
    multi_source = len({r.get("source") for r in rules}) > 1
    if not multi_source:
        return []

    client = None
    if use_gemini and GEMINI_API_KEY:
        client = genai.Client(api_key=GEMINI_API_KEY)

    candidates = []
    for i in range(len(rules)):
        for j in range(i + 1, len(rules)):
            src_i = rules[i].get("source", "")
            src_j = rules[j].get("source", "")

            if src_i == src_j:
                continue

            desc_i = rules[i].get("description", "").lower().strip()
            desc_j = rules[j].get("description", "").lower().strip()

            score = SequenceMatcher(None, desc_i, desc_j).ratio()

            if score >= CONFLICT_THRESHOLD:
                candidates.append((rules[i], rules[j], score))

    for rule_a, rule_b, score in candidates:
        rid_a = rule_a.get("rule_id", "?")
        rid_b = rule_b.get("rule_id", "?")

        if client:
            conflict, reason = _gemini_conflict_check(client, rule_a, rule_b)
        else:
            # fallback: flag as potential without Gemini confirmation
            conflict = True
            reason = f"High textual similarity ({score:.0%}) across sources — manual review needed"

        if conflict:
            issues.append(_issue(rid_a, "cross_source", "warning",
                f"Possible conflict with {rid_b} (source: {rule_b.get('source')}): {reason}"))
            issues.append(_issue(rid_b, "cross_source", "warning",
                f"Possible conflict with {rid_a} (source: {rule_a.get('source')}): {reason}"))

    return issues


def _gemini_conflict_check(client, rule_a: dict, rule_b: dict, retries: int = 3) -> tuple:
    prompt = CONFLICT_PROMPT.format(
        source_a=rule_a.get("source", "unknown"),
        desc_a=rule_a.get("description", ""),
        source_b=rule_b.get("source", "unknown"),
        desc_b=rule_b.get("description", ""),
    )
    contents = [Content(parts=[Part(text=prompt)])]

    for attempt in range(retries):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL_NAME,
                contents=contents,
                config={"temperature": 0, "max_output_tokens": 256},
            )
            raw = response.text.strip()
            # strip markdown code fences if present
            raw = re.sub(r"^```json|```$", "", raw, flags=re.MULTILINE).strip()
            result = json.loads(raw)
            return result.get("conflict", False), result.get("reason", "")
        except Exception as e:
            wait = 2 ** attempt
            print(f"[Validator] Gemini conflict check error (attempt {attempt+1}/{retries}): {e} — retrying in {wait}s")
            time.sleep(wait)

    return False, "Gemini check failed — manual review recommended"


# ── Main validator class ───────────────────────────────────────────────────────

class RuleValidator:
    def __init__(self, use_gemini: bool = True):
        """
        Args:
            use_gemini: if True, uses Gemini to confirm cross-source conflicts.
                        Set to False to run fully offline.
        """
        self.use_gemini = use_gemini

    def validate(self, rules: list) -> dict:
        """
        Run all validation checks on a list of rule dicts.

        Returns a report dict with:
          - total:        total rules checked
          - passed:       rules with no errors
          - issues:       list of all issue dicts
          - summary:      counts by check type and severity
          - clean_rules:  rules that passed all error-level checks
        """
        if not rules:
            return {"total": 0, "passed": 0, "issues": [], "summary": {}, "clean_rules": []}

        print(f"[Validator] Running checks on {len(rules)} rules...")

        all_issues = []
        all_issues += check_structure(rules)
        all_issues += check_quality(rules)
        all_issues += check_consistency(rules)
        all_issues += check_duplicates(rules)
        all_issues += check_cross_source_conflicts(rules, use_gemini=self.use_gemini)

        # summary counts
        summary = defaultdict(lambda: defaultdict(int))
        for issue in all_issues:
            summary[issue["check"]][issue["severity"]] += 1

        # rules with no error-level issues
        error_ids = {i["rule_id"] for i in all_issues if i["severity"] == "error"}
        clean_rules = [r for r in rules if r.get("rule_id") not in error_ids]

        passed = len(clean_rules)

        print(f"[Validator] {passed}/{len(rules)} rules passed | {len(all_issues)} issues found")

        return {
            "total":       len(rules),
            "passed":      passed,
            "issues":      all_issues,
            "summary":     {k: dict(v) for k, v in summary.items()},
            "clean_rules": clean_rules,
        }

    def validate_file(self, json_path: str) -> dict:
        """Load a rules JSON file and validate it."""
        with open(json_path, "r", encoding="utf-8") as f:
            rules = json.load(f)
        return self.validate(rules)

    def save_report(self, report: dict, output_path: str):
        """Save the full validation report to a JSON file."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"[Validator] Report saved → {output_path}")

    def save_clean_rules(self, report: dict, output_path: str):
        """Save only the rules that passed all error-level checks."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report["clean_rules"], f, indent=2)
        print(f"[Validator] Clean rules saved → {output_path}")

    def print_summary(self, report: dict):
        """Print a human-readable summary to stdout."""
        print("\n" + "=" * 50)
        print(f"  VALIDATION REPORT")
        print("=" * 50)
        print(f"  Total rules  : {report['total']}")
        print(f"  Passed       : {report['passed']}")
        print(f"  Issues found : {len(report['issues'])}")
        print()

        for check, severities in report["summary"].items():
            print(f"  [{check}]")
            for sev, count in severities.items():
                print(f"    {sev:<10} : {count}")

        if report["issues"]:
            print("\n  ISSUES:")
            for issue in report["issues"]:
                icon = {"error": "✖", "warning": "⚠", "info": "ℹ"}.get(issue["severity"], "-")
                print(f"  {icon} [{issue['rule_id']}] ({issue['check']}) {issue['message']}")

        print("=" * 50 + "\n")
