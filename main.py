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
#   2. ENTITY EXTRACTION  (semantic + agentic stores)
#      - Reads every completed *.json in both chunk stores
#      - Classifies each source as airline or airport via DOMAIN_MAP
#      - Calls Gemini to extract a services array per entity
#      - Unrecognised sources (iata.org, eur-lex, etc.) are skipped
#      - Entities from both stores are merged by airline_id / airport_id
#      - Hash-cached: skips extraction if chunk files are unchanged
#      - Output saved to rules/extracted/all_entities.json
#
#   3. VALIDATION
#      - Runs structure and service-level checks on all entities
#      - Saves full report  → rules/validated/report.json
#      - Saves clean entities → rules/validated/clean_entities.json
#
#   4. FIRESTORE PUSH
#      - Airlines → v2_airlines collection
#      - Airports → v2_airports collection
#      - Document ID = airline_id / airport_id (overwrites existing doc)

import json
import hashlib
from pathlib import Path

from config import Config
from pipeline_flow_doc_process import Doc_Process_Pipeline
from extractor.rule_extractor import RuleExtractor
from validator.rule_validator import RuleValidator
from firestore.client import FirestoreClient

from parser.social_media.twitter_rapid import RapidXProvider

# ── Paths ──────────────────────────────────────────────────────────────────────
SOURCES_DIR    = Path("sources")
SOCIAL_RAW_DIR = Path("chunk_store") / "social" / "raw"
AGENTIC_DIR    = Path("chunk_store/agentic")
EXTRACTED_DIR  = Path("rules/extracted")
VALIDATED_DIR  = Path("rules/validated")

ALL_ENTITIES_PATH      = EXTRACTED_DIR / "all_entities.json"
MANIFEST_PATH          = EXTRACTED_DIR / ".manifest.json"
CLEAN_ENTITIES_PATH    = str(VALIDATED_DIR / "clean_entities.json")
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
        social_provider=RapidXProvider(), # add social provider
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
    pipeline.process_social(run_id="x_social")

    print("[Done] All sources processed")


# ── Stage 2: Entity extraction from both semantic + agentic stores ─────────────

def _file_hash(path: Path) -> str:
    """SHA-256 of a file's raw bytes — used to detect chunk file changes."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def _load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_manifest(manifest: dict):
    EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def _extract_from_file(extractor: RuleExtractor, chunk_file: Path, label: str) -> list:
    """Extract entity from one chunk file. Returns [entity] or []."""
    with open(chunk_file, "r", encoding="utf-8") as f:
        chunks = json.load(f)

    if not chunks:
        print(f"[Extractor] {label}/{chunk_file.stem}: empty — skipping")
        return []

    print(f"[Extractor] {label}/{chunk_file.stem}: {len(chunks)} chunks...")
    entities = extractor.run(chunks)
    print(f"[Extractor] {label}/{chunk_file.stem}: {len(entities)} entity/entities extracted")
    return entities


def _merge_entities(entities: list) -> list:
    """
    Merge entities from both stores that share the same airline_id or airport_id.
    Services are combined and deduplicated by type, keeping first occurrence.
    """
    merged: dict[str, dict] = {}
    for entity in entities:
        key = entity.get("airline_id") or entity.get("airport_id")
        if not key:
            continue
        if key not in merged:
            merged[key] = {**entity, "services": list(entity.get("services", []))}
        else:
            existing_types = {s["type"] for s in merged[key]["services"]}
            for svc in entity.get("services", []):
                if svc["type"] not in existing_types:
                    merged[key]["services"].append(svc)
                    existing_types.add(svc["type"])
    return list(merged.values())


def stage_extract() -> list:
    """
    Extract entities from BOTH chunk_store/semantic/ and chunk_store/agentic/.

    Hash-based caching:
      - .manifest.json maps each chunk file key → its SHA-256 hash.
      - All hashes match AND all_entities.json exists → return cached (no Gemini calls).
      - Any file is new or changed → re-extract everything, update manifest.
      - Bootstrap: if all_entities.json exists but manifest is missing, write manifest now.
    """
    print("\n" + "=" * 60)
    print("  STAGE 2: ENTITY EXTRACTION  (semantic + agentic → single file)")
    print("=" * 60)

    EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)

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

    current_hashes = {}
    all_chunk_files = (
        [(f, "semantic") for f in semantic_files] +
        [(f, "agentic")  for f in agentic_files]
    )
    for f, label in all_chunk_files:
        current_hashes[f"{label}/{f.stem}"] = _file_hash(f)

    manifest = _load_manifest()

    # Bootstrap: manifest missing but output already exists
    if not manifest and ALL_ENTITIES_PATH.exists():
        _save_manifest(current_hashes)
        print("[Extractor] Manifest bootstrapped — loading existing all_entities.json")
        with open(ALL_ENTITIES_PATH, "r", encoding="utf-8") as f:
            entities = json.load(f)
        print(f"[Extractor] {len(entities)} entities loaded (no changes detected)")
        return entities

    changed = [k for k, h in current_hashes.items() if manifest.get(k) != h]

    if not changed and ALL_ENTITIES_PATH.exists():
        print(f"[Extractor] All {len(current_hashes)} chunk files unchanged — skipping extraction")
        with open(ALL_ENTITIES_PATH, "r", encoding="utf-8") as f:
            entities = json.load(f)
        print(f"[Extractor] {len(entities)} entities loaded from cache")
        return entities

    if changed:
        print(f"[Extractor] {len(changed)} chunk file(s) changed: {', '.join(changed)}")

    extractor       = RuleExtractor()
    semantic_entities = []
    agentic_entities  = []

    for f in semantic_files:
        semantic_entities.extend(_extract_from_file(extractor, f, "semantic"))
    for f in agentic_files:
        agentic_entities.extend(_extract_from_file(extractor, f, "agentic"))

    print(f"\n[Extractor] Before merge — Semantic: {len(semantic_entities)} | Agentic: {len(agentic_entities)}")

    merged = _merge_entities(semantic_entities + agentic_entities)
    print(f"[Extractor] After merge — {len(merged)} unique entities")

    _save_manifest(current_hashes)
    with open(ALL_ENTITIES_PATH, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2)
    print(f"[Extractor] All entities saved → {ALL_ENTITIES_PATH}")

    return merged


# ── Stage 3: Validation ────────────────────────────────────────────────────────

def stage_validate(entities: list) -> list:
    """Validate entities — return only error-free ones."""
    print("\n" + "=" * 60)
    print("  STAGE 3: VALIDATION")
    print("=" * 60)

    VALIDATED_DIR.mkdir(parents=True, exist_ok=True)

    if not entities:
        print("[Validator] No entities to validate.")
        return []

    validator = RuleValidator()
    report    = validator.validate(entities)

    validator.print_summary(report)
    validator.save_report(report, VALIDATION_REPORT_PATH)
    validator.save_clean_entities(report, CLEAN_ENTITIES_PATH)

    return report["clean_entities"]


# ── Stage 4: Firestore push ────────────────────────────────────────────────────

def stage_firestore(entities: list):
    """Push entities to Firestore — airlines → v2_airlines, airports → v2_airports."""
    print("\n" + "=" * 60)
    print("  STAGE 4: FIRESTORE PUSH")
    print("=" * 60)

    if not entities:
        print("[Firestore] Nothing to push.")
        return

    try:
        client = FirestoreClient()
        stats  = client.push_entities(entities)
        print(
            f"[Firestore] Summary — "
            f"Total: {stats['total']} | "
            f"Airlines: {stats['airlines_pushed']} | "
            f"Airports: {stats['airports_pushed']} | "
            f"Errors: {stats['errors']}"
        )
    except (ValueError, FileNotFoundError) as e:
        print(f"[Firestore] Configuration error: {e}")
        print("[Firestore] Add FIRESTORE_PROJECT_ID and FIREBASE_CREDENTIALS_PATH to .env")
    except Exception as e:
        print(f"[Firestore] Push failed: {e}")
        print("[Firestore] Clean entities saved locally at:", CLEAN_ENTITIES_PATH)


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Stage 1: Chunk all sources (local PDFs + remote URLs)
    stage_chunk()

    # Stage 2: Extract airline/airport entities from chunk stores
    all_entities = stage_extract()

    # Stage 3: Validate, save report + clean entities
    clean_entities = stage_validate(all_entities)

    # Stage 4: Push entities to Firestore (v2_airlines / v2_airports)
    # stage_firestore(clean_entities)

    print("\n[Pipeline] All stages complete.")
