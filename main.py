# main.py - Entry point for the full pipeline
#
# Stages (run top-to-bottom when you execute: python main.py):
#
#   1. SCRAPE & CHUNK
#      - Local files (sources/*.pdf / *.txt / *.html / *.json) → chunker
#      - Remote URLs from Config.SOURCES → chunker
#      - All output goes to chunk_store/semantic/ + chunk_store/agentic/
#      - Skips sources whose agentic output already exists (cached)
#
#   2. RULE EXTRACTION  (agentic store only)
#      - Reads every completed *.json in chunk_store/agentic/
#      - Skips *_partial.json files
#      - Calls Gemini to extract structured rules from each chunk file
#      - Per-source results saved to rules/extracted/{source_id}.json
#      - Cached: skips a source if rules/extracted/{source_id}.json exists
#      - After all sources processed: IDs reassigned globally (R001, R002, ...)
#
#   3. RULE VALIDATION
#      - Runs 5-check validation on the merged rule list
#      - Saves full report  → rules/validated/report.json
#      - Saves clean rules  → rules/validated/clean_rules.json
#
#   4. VERSIONING
#      - Diffs against rules/snapshots/snapshot.json
#      - New → version=1, modified → version+1, unchanged → kept
#
#   5. FIRESTORE PUSH
#      - Uses content_hash as Firestore document ID (natural dedup key)
#      - Fetches all existing doc IDs in one call — skips unchanged rules
#      - Batch-writes only new rules (max 500 per commit)

import json
from pathlib import Path

from config import Config
from pipeline_flow_doc_process import Doc_Process_Pipeline
from extractor.rule_extractor import RuleExtractor
from validator.rule_validator import RuleValidator
from utils.hashing import apply_versions, save_snapshot, detect_changes
from firestore.client import FirestoreClient

# ── Paths ──────────────────────────────────────────────────────────────────────
SOURCES_DIR    = Path("sources")
SOCIAL_RAW_DIR = Path("chunk_store") / "social" / "raw"
AGENTIC_DIR    = Path("chunk_store/agentic")
EXTRACTED_DIR  = Path("rules/extracted")
VALIDATED_DIR  = Path("rules/validated")
SNAPSHOT_PATH  = "rules/snapshots/snapshot.json"

CLEAN_RULES_PATH       = str(VALIDATED_DIR / "clean_rules.json")
VALIDATION_REPORT_PATH = str(VALIDATED_DIR / "report.json")

LOCAL_EXTENSIONS = ["*.pdf", "*.txt", "*.html", "*.json"]


# ── Stage 1: Scrape + chunk ALL sources ───────────────────────────────────────

def stage_chunk():
    """
    Run the scrape → semantic → agentic chunker for every source.
    Local files and remote URLs are both processed here.
    Skips any source whose agentic output file already exists.
    """
    print("\n" + "=" * 60)
    print("  STAGE 1: SCRAPE & CHUNK")
    print("=" * 60)

    pipeline = Doc_Process_Pipeline(
        enable_agentic=Config.ENABLE_AGENT_CHUNKS,
        agentic_rpm=3,
        batch_size=3,
        checkpoint_every=10,
    )

    # Local files
    local_files = []
    for pattern in LOCAL_EXTENSIONS:
        local_files.extend(SOURCES_DIR.glob(pattern))

    for file_path in sorted(local_files):
        input_id = file_path.stem
        if (AGENTIC_DIR / f"{input_id}.json").exists():
            print(f"[Skip] Already chunked: {input_id}")
            continue
        if (AGENTIC_DIR / f"{input_id}_partial.json").exists():
            print(f"[Skip] Partial exists: {input_id}")
            continue
        print(f"[Chunk] Local: {file_path.name}")
        pipeline.process(str(file_path))

    print("[Done] Local files processed")

    # Remote sources
    for src in Config.SOURCES:
        input_id = src.get("id", "remote")
        if (AGENTIC_DIR / f"{input_id}.json").exists():
            print(f"[Skip] Already chunked: {input_id}")
            continue
        if (AGENTIC_DIR / f"{input_id}_partial.json").exists():
            print(f"[Skip] Partial exists: {input_id}")
            continue
        print(f"[Chunk] Remote: {input_id}")
        pipeline.process(src)

    # Social media sources
    social_run_id = "x_social"
    if (SOCIAL_RAW_DIR / f"{social_run_id}.json").exists():
        print(f"[Skip] Already scraped: {social_run_id}")
    else:
        print(f"[Chunk] Social: {social_run_id}")
        pipeline.process_social(run_id=social_run_id)

    print("[Done] All sources processed")


# ── Stage 2: Rule extraction from both semantic + agentic stores ──────────────

# Similarity threshold: rules with description similarity >= this are duplicates.
# Keep the longer one, drop the shorter. Below this threshold → different rules, keep both.
DEDUP_SIMILARITY = 0.92

# Single output file for ALL extracted rules (pre-validation)
ALL_RULES_PATH = EXTRACTED_DIR / "all_rules.json"


def _extract_from_file(extractor: RuleExtractor, chunk_file: Path, label: str) -> list:
    """Extract rules from one chunk file. Returns rules without rule_id."""
    with open(chunk_file, "r", encoding="utf-8") as f:
        chunks = json.load(f)

    if not chunks:
        print(f"[Extractor] {label}/{chunk_file.stem}: empty — skipping")
        return []

    print(f"[Extractor] {label}/{chunk_file.stem}: {len(chunks)} chunks...")
    rules = extractor.run(chunks)   # no per-source output_path — single file only
    print(f"[Extractor] {label}/{chunk_file.stem}: {len(rules)} rules")

    for rule in rules:
        rule.pop("rule_id", None)   # reassigned globally after merge
    return rules


def _dedup_across_stores(rules: list) -> list:
    """
    Deduplicate rules from both stores.
    - similarity >= DEDUP_SIMILARITY → same rule; keep the longer description, drop shorter
    - similarity <  DEDUP_SIMILARITY → different rules; keep both
    """
    from difflib import SequenceMatcher

    drop = set()

    for i in range(len(rules)):
        if i in drop:
            continue
        for j in range(i + 1, len(rules)):
            if j in drop:
                continue
            desc_i = rules[i].get("description", "").lower().strip()
            desc_j = rules[j].get("description", "").lower().strip()
            if SequenceMatcher(None, desc_i, desc_j).ratio() >= DEDUP_SIMILARITY:
                # drop the one with less content
                if len(desc_i) >= len(desc_j):
                    drop.add(j)
                else:
                    drop.add(i)
                    break

    return [r for idx, r in enumerate(rules) if idx not in drop]


def stage_extract() -> list:
    """
    Extract rules from BOTH chunk_store/semantic/ and chunk_store/agentic/,
    deduplicate across both stores, and save everything to ONE file:
        rules/extracted/all_rules.json

    If all_rules.json already exists it is loaded directly (cache).
    Delete it to force a full re-extraction.
    """
    print("\n" + "=" * 60)
    print("  STAGE 2: RULE EXTRACTION  (semantic + agentic → single file)")
    print("=" * 60)

    EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)

    # ── Load from single cache file if available ──
    if ALL_RULES_PATH.exists():
        print(f"[Extractor] Loading cached rules from {ALL_RULES_PATH}")
        with open(ALL_RULES_PATH, "r", encoding="utf-8") as f:
            rules = json.load(f)
        print(f"[Extractor] {len(rules)} rules loaded from cache")
        return rules

    # ── Discover chunk files ──
    semantic_files = sorted(Path("chunk_store/semantic").glob("*.json"))
    agentic_files  = sorted(
        p for p in AGENTIC_DIR.glob("*.json")
        if not p.name.endswith("_partial.json")
    )

    if not semantic_files and not agentic_files:
        print("[Extractor] No chunk files found in either store.")
        return []

    print(f"[Extractor] Semantic files : {len(semantic_files)}")
    print(f"[Extractor] Agentic files  : {len(agentic_files)}")

    extractor      = RuleExtractor()
    semantic_rules = []
    agentic_rules  = []

    for f in semantic_files:
        semantic_rules.extend(_extract_from_file(extractor, f, "semantic"))

    for f in agentic_files:
        agentic_rules.extend(_extract_from_file(extractor, f, "agentic"))

    print(f"\n[Extractor] Before dedup — Semantic: {len(semantic_rules)} | Agentic: {len(agentic_rules)}")

    # ── Deduplicate across both stores ──
    merged = semantic_rules + agentic_rules
    unique = _dedup_across_stores(merged)
    print(f"[Extractor] Dedup removed {len(merged) - len(unique)} duplicate(s) — {len(unique)} unique rules kept")

    # ── Assign sequential IDs ──
    for i, rule in enumerate(unique, start=1):
        rule["rule_id"] = f"R{i:03d}"

    # ── Save to single file ──
    with open(ALL_RULES_PATH, "w", encoding="utf-8") as f:
        json.dump(unique, f, indent=2)
    print(f"[Extractor] All rules saved → {ALL_RULES_PATH}")

    return unique


# ── Stage 3: Validation ────────────────────────────────────────────────────────

def stage_validate(rules: list) -> list:
    """Validate rules — return only error-free ones."""
    print("\n" + "=" * 60)
    print("  STAGE 3: RULE VALIDATION")
    print("=" * 60)

    VALIDATED_DIR.mkdir(parents=True, exist_ok=True)

    if not rules:
        print("[Validator] No rules to validate.")
        return []

    validator = RuleValidator(use_gemini=True)
    report    = validator.validate(rules)

    validator.print_summary(report)
    validator.save_report(report, VALIDATION_REPORT_PATH)
    validator.save_clean_rules(report, CLEAN_RULES_PATH)

    return report["clean_rules"]


# ── Stage 4: Versioning ────────────────────────────────────────────────────────

def stage_version(clean_rules: list) -> list:
    """Diff against last snapshot, bump versions, save new snapshot."""
    print("\n" + "=" * 60)
    print("  STAGE 4: VERSIONING")
    print("=" * 60)

    if not clean_rules:
        return []

    detect_changes(clean_rules, SNAPSHOT_PATH)
    versioned = apply_versions(clean_rules, SNAPSHOT_PATH)
    save_snapshot(versioned, SNAPSHOT_PATH)

    with open(CLEAN_RULES_PATH, "w", encoding="utf-8") as f:
        json.dump(versioned, f, indent=2)
    print(f"[Versioning] Saved {len(versioned)} versioned rules → {CLEAN_RULES_PATH}")

    return versioned


# ── Stage 5: Firestore push ────────────────────────────────────────────────────

def stage_firestore(versioned_rules: list):
    """Push only new rules to Firestore — skip any whose hash already exists."""
    print("\n" + "=" * 60)
    print("  STAGE 5: FIRESTORE PUSH")
    print("=" * 60)

    if not versioned_rules:
        print("[Firestore] Nothing to push.")
        return

    try:
        client = FirestoreClient()
        stats  = client.push_rules(versioned_rules)
        print(
            f"[Firestore] Summary — "
            f"Total: {stats['total']} | "
            f"Pushed: {stats['pushed']} | "
            f"Skipped (unchanged): {stats['skipped']}"
        )
    except (ValueError, FileNotFoundError) as e:
        print(f"[Firestore] Configuration error: {e}")
        print("[Firestore] Add FIRESTORE_PROJECT_ID and FIREBASE_CREDENTIALS_PATH to .env")
    except Exception as e:
        print(f"[Firestore] Push failed: {e}")
        print("[Firestore] Clean rules saved locally at:", CLEAN_RULES_PATH)


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Stage 1: Chunk all sources (local PDFs + remote URLs) → agentic store
    stage_chunk()

    # Stage 2: Extract rules from agentic store only
    all_rules = stage_extract()

    # Stage 3: Validate, save report + clean rules
    clean_rules = stage_validate(all_rules)

    # Stage 4: Version rules against last snapshot
    versioned_rules = stage_version(clean_rules)

    # Stage 5: Push new rules to Firestore (skip duplicates by content_hash)
    stage_firestore(versioned_rules)

    print("\n[Pipeline] All stages complete.")
