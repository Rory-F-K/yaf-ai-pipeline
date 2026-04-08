# CHANGES

This document describes what was added to the pipeline beyond the original ingestion and chunking code.

---

## New Modules

### `extractor/rule_extractor.py`

Extracts structured, actionable rules from documents using Gemini AI.

**Supports:**
- Local PDF files
- Remote URLs (websites and PDFs via Playwright scraper)
- Raw text strings
- Pre-processed pipeline chunk lists

**Key behaviour:**
- Automatically batches large documents (>50,000 chars) into smaller segments to stay within Gemini token limits
- Deduplicates rules across batches by normalised description
- Re-sequences rule IDs (R001, R002, ...) after merging batches
- Retries Gemini calls with exponential backoff on 503 errors (up to 5 attempts)
- Filters noise: navigation text, ads, vague statements, and hallucinated rules are excluded by the prompt

**Output format per rule:**
```json
{
  "rule_id": "R001",
  "category": "Accessibility",
  "title": "Short rule title",
  "description": "Clear and concise rule statement",
  "source": "sources/file.pdf"
}
```

**Usage:**
```python
from extractor.rule_extractor import RuleExtractor

ex = RuleExtractor()
rules = ex.run("sources/file.pdf", output_path="rules/output.json")
rules = ex.run("https://example.com/policy", output_path="rules/output.json")
rules = ex.run(pipeline_chunks, output_path="rules/output.json")
```

---

### `validator/rule_validator.py`

Validates extracted rules across five independent checks before they are stored or used downstream.

**Checks:**

| Check | What it catches | Severity |
|---|---|---|
| `structure` | Missing/empty fields, wrong rule_id format, non-string values | error |
| `quality` | Description too short (<30 chars), all-caps headings, vague phrases, generic titles, description = title | error / warning |
| `consistency` | Duplicate rule_ids, broken sequential order, unknown categories | error / warning / info |
| `duplicates` | Near-identical descriptions within the ruleset (≥85% similarity) | warning |
| `cross_source` | Rules from different sources with similar content that may contradict each other (Gemini-confirmed) | warning |

**Severity levels:**
- `error` — rule is excluded from the clean output
- `warning` — rule is kept but flagged for review
- `info` — informational note only

**Usage:**
```python
from validator.rule_validator import RuleValidator

v = RuleValidator(use_gemini=True)   # use_gemini=False for offline mode
report = v.validate_file("rules/output.json")
v.print_summary(report)
v.save_report(report, "rules/validation_report.json")
v.save_clean_rules(report, "rules/clean_rules.json")
```

---

### `models/rule.py`

Pydantic data model for a rule. Enforces field-level validation and acts as the single source of truth for rule structure across the pipeline.

**Fields:**

| Field | Type | Description |
|---|---|---|
| `rule_id` | str | Unique ID, must match `R<digits>` format |
| `category` | str | Topic label from known categories; falls back to `"General"` |
| `title` | str | Short rule name, must be non-empty |
| `description` | str | Full rule statement, minimum 20 characters |
| `source` | str | Origin URL or file path |
| `version` | int | Starts at 1, increments each time content changes |
| `content_hash` | str | SHA-256 of title + description, auto-set on creation |

**Known categories:**
`Accessibility`, `Aircraft Safety`, `Assistance`, `Baggage`, `Boarding`, `Booking`,
`Check-in`, `Complaints`, `Compensation`, `Documentation`, `General`, `Information`,
`Infrastructure`, `Legal Rights`, `Medical`, `Pre-Flight`, `Safety`, `Security`,
`Service`, `Training`, `Travel Policy`

**Usage:**
```python
from models.rule import Rule

rule = Rule.from_extractor_output(raw_dict)
rule = Rule.from_dict({"rule_id": "R001", ...})
d = rule.to_dict()
```

---

### `utils/hashing.py`

Content hashing, snapshot management, and change detection for the rules pipeline. Enables versioning of rules across runs.

**Functions:**

| Function | Description |
|---|---|
| `hash_rule_content(description, title)` | SHA-256 of a single rule's content — detects edits |
| `hash_ruleset(rules)` | Single hash for the whole ruleset — quick check for any change |
| `save_snapshot(rules, path)` | Persist current hashes to a JSON snapshot file |
| `load_snapshot(path)` | Load a previously saved snapshot |
| `detect_changes(rules, snapshot_path)` | Diff current rules vs last snapshot → added / removed / modified / unchanged |
| `apply_versions(rules, snapshot_path)` | Return rules with version numbers bumped where content changed |

**Usage:**
```python
from utils.hashing import detect_changes, save_snapshot, apply_versions

changes = detect_changes(rules, "rules/snapshots/snapshot.json")
# → { "added": [...], "removed": [...], "modified": [...], "unchanged": [...] }

versioned_rules = apply_versions(rules, "rules/snapshots/snapshot.json")
save_snapshot(versioned_rules, "rules/snapshots/snapshot.json")
```

---

## Output Files

| File | Description |
|---|---|
| `rules/<name>_rules.json` | Raw extracted rules from a source |
| `rules/clean_rules.json` | Rules that passed all error-level validation checks |
| `rules/validation_report.json` | Full validation report including all issues |
| `rules/snapshots/snapshot.json` | Hashed snapshot of the last saved ruleset for change detection |
