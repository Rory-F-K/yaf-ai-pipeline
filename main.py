import json
import hashlib
import time
from pathlib import Path

from config import Config
from pipeline_flow_doc_process import Doc_Process_Pipeline
from extractor.rule_extractor import RuleExtractor
from validator.rule_validator import RuleValidator
from firestore.client import FirestoreClient

# ── Paths ──────────────────────────────────────────────────────────────────────
SOURCES_DIR  = Path("sources")
AGENTIC_DIR  = Path("chunk_store/agentic")

EXTRACTED_DIR  = Path("rules/extracted")
VALIDATED_DIR  = Path("rules/validated")
PER_FILE_DIR   = EXTRACTED_DIR / "per_file"   # one JSON per agentic source

ALL_ENTITIES_PATH      = EXTRACTED_DIR / "all_entities.json"
CLEAN_ENTITIES_PATH    = str(VALIDATED_DIR / "clean_entities.json")
VALIDATION_REPORT_PATH = str(VALIDATED_DIR / "report.json")

LOCAL_EXTENSIONS = ["*.pdf", "*.txt", "*.html", "*.json"]


# ── Stage 1: Scrape + chunk ALL sources ───────────────────────────────────────

def stage_chunk():
    print("\n" + "=" * 60)
    print("  STAGE 1: SCRAPE & CHUNK")
    print("=" * 60)

    pipeline = Doc_Process_Pipeline(
        enable_agentic=Config.ENABLE_AGENT_CHUNKS,
        agentic_rpm=3,
        batch_size=3,
        checkpoint_every=10,
    )

    local_files = []
    for pattern in LOCAL_EXTENSIONS:
        local_files.extend(SOURCES_DIR.glob(pattern))

    for file_path in sorted(local_files):
        input_id = file_path.stem
        if (AGENTIC_DIR / f"{input_id}.json").exists():
            print(f"[Skip] Already chunked: {input_id}")
            continue
        print(f"[Chunk] Local: {file_path.name}")
        pipeline.process(str(file_path))

    print("[Done] Local files processed")

    for src in Config.SOURCES:
        input_id = src.get("id", "remote")
        if (AGENTIC_DIR / f"{input_id}.json").exists():
            print(f"[Skip] Already chunked: {input_id}")
            continue
        print(f"[Chunk] Remote: {input_id}")
        pipeline.process(src)

    pipeline.process_social(run_id="x_social")
    print("[Done] All sources processed")


# ── Stage 2: Extract — per-file caching ───────────────────────────────────────

def _file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def _load_per_file_cache(source_id: str) -> dict | None:
    """Load a previously extracted result for one source file, or None if missing."""
    cache_file = PER_FILE_DIR / f"{source_id}.json"
    if cache_file.exists():
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def _save_per_file_cache(source_id: str, data: dict):
    PER_FILE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = PER_FILE_DIR / f"{source_id}.json"
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _load_hash_manifest() -> dict:
    manifest_path = EXTRACTED_DIR / ".manifest.json"
    if manifest_path.exists():
        with open(manifest_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_hash_manifest(manifest: dict):
    EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = EXTRACTED_DIR / ".manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def stage_extract() -> list:
    """
    Extract PRM services from every agentic chunk file.

    Per-file caching:
      - Each file gets its own cache at rules/extracted/per_file/{source_id}.json
      - A .manifest.json tracks the SHA-256 hash of each agentic chunk file
      - If a file's hash is unchanged AND its per-file cache exists → use cache (no Gemini call)
      - If a file changed or has no cache → re-extract that file only

    Only files whose entity_type is 'airline' or 'airport' trigger a Gemini call.
    Regulatory/industry sources (IATA, EUR-LEX, etc.) are skipped automatically
    based on the entity_type field already set in each chunk by the pipeline.
    """
    print("\n" + "=" * 60)
    print("  STAGE 2: ENTITY EXTRACTION  (per-file cache)")
    print("=" * 60)

    EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)
    PER_FILE_DIR.mkdir(parents=True, exist_ok=True)

    agentic_files = sorted(
        p for p in AGENTIC_DIR.glob("*.json")
        if not p.name.endswith("_partial.json")
    )

    if not agentic_files:
        print("[Extractor] No agentic chunk files found.")
        return []

    print(f"[Extractor] Found {len(agentic_files)} agentic chunk file(s)")

    manifest = _load_hash_manifest()
    extractor = None   # initialise lazily (avoids Gemini init if all cached)
    all_entities = []
    first_extract = True

    for chunk_file in agentic_files:
        source_id   = chunk_file.stem
        current_hash = _file_hash(chunk_file)
        cached_data  = _load_per_file_cache(source_id)

        if manifest.get(source_id) == current_hash and cached_data is not None:
            print(f"[Extractor] {source_id}: unchanged — using cache")
            all_entities.append(cached_data)
            continue

        # File is new or changed — re-extract
        print(f"[Extractor] {source_id}: {'changed' if source_id in manifest else 'new'} — extracting...")

        with open(chunk_file, "r", encoding="utf-8") as f:
            chunks = json.load(f)

        if not chunks:
            print(f"[Extractor] {source_id}: empty file — skipping")
            manifest[source_id] = current_hash
            continue

        if extractor is None:
            extractor = RuleExtractor()

        # Pace calls to stay within Gemini rate limits
        if not first_extract:
            time.sleep(4)
        first_extract = False

        entity = extractor.extract_entity_from_chunks(chunks)

        manifest[source_id] = current_hash   # always update hash (even if skipped)

        if entity is None:
            continue   # non-airline/airport source — no cache entry needed

        _save_per_file_cache(source_id, entity)
        all_entities.append(entity)

    _save_hash_manifest(manifest)

    # Also merge results into the combined all_entities.json for reference
    with open(ALL_ENTITIES_PATH, "w", encoding="utf-8") as f:
        json.dump(all_entities, f, indent=2)

    print(f"[Extractor] Done — {len(all_entities)} entities ready")
    return all_entities


# ── Stage 3: Validation ────────────────────────────────────────────────────────

def stage_validate(entities: list) -> list:
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
    """
    1. Delete any wrong-format docs created by earlier pipeline versions.
    2. Find each entity's existing Firestore document by name (case-insensitive).
    3. Update ONLY the `services` field of that existing document.
    """
    print("\n" + "=" * 60)
    print("  STAGE 4: FIRESTORE PUSH")
    print("=" * 60)

    try:
        client = FirestoreClient()
        client.cleanup_wrong_format_docs()

        if not entities:
            print("[Firestore] Nothing to push.")
            return

        stats = client.push_entities(entities)
        print(
            f"[Firestore] Summary — "
            f"Total: {stats['total']} | "
            f"Updated: {stats['updated']} | "
            f"Skipped: {stats['skipped']} | "
            f"Errors: {stats['errors']}"
        )
    except (ValueError, FileNotFoundError) as e:
        print(f"[Firestore] Configuration error: {e}")
    except Exception as e:
        print(f"[Firestore] Push failed: {e}")
        print("[Firestore] Clean entities saved locally at:", CLEAN_ENTITIES_PATH)


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    stage_chunk()
    all_entities   = stage_extract()
    clean_entities = stage_validate(all_entities)
    stage_firestore(clean_entities)
    print("\n[Pipeline] All stages complete.")
