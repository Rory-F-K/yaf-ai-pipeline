# main.py - Entry point for the full pipeline
#
# Stages:
#
#   1. SCRAPE & CHUNK
#      - Local files (sources/*.pdf / *.txt) → semantic + agentic chunker
#      - Remote URLs from Config.SOURCES → chunker
#      - Output goes to chunk_store/agentic/
#      - Skips sources whose agentic output already exists (cached)
#
#   2. FETCH EXISTING FIRESTORE DATA
#      - Reads all docs from v2_airlines and v2_airports
#      - Returns {doc_id: data} used by Stage 3 for static-field preservation
#        and by Stage 4 for diff-based push
#
#   3. ENTITY EXTRACTION
#      - Reads agentic chunk files
#      - Classifies each source (airline/airport) via DOMAIN_MAP
#      - Calls Gemini to extract:
#          airports  → services array [{type, description:{en,ro}, is_presented}]
#          airlines  → rules object  {sub_rule_N: {crutches, walker, ...}}
#      - Merges extracted dynamic fields with existing Firestore doc (preserves static fields)
#      - Hash-cached: skips extraction if chunk files unchanged
#      - Saves to rules/extracted/all_entities.json + per-entity split files
#
#   4. FIRESTORE PUSH (compare-and-push)
#      - Diffs each entity's dynamic field against existing Firestore doc
#      - Pushes ONLY entities with changes (or new entities)
#      - Logs every changed field path with old → new values
#      - Skips unchanged entities entirely

import json
import hashlib
import time
from pathlib import Path

from config import Config
from pipeline_flow_doc_process import Doc_Process_Pipeline
from extractor.rule_extractor import RuleExtractor
from firestore.client import FirestoreClient

# ── Paths ──────────────────────────────────────────────────────────────────────
SOURCES_DIR   = Path("sources")
AGENTIC_DIR   = Path("chunk_store/agentic")
EXTRACTED_DIR = Path("rules/extracted")

ALL_ENTITIES_PATH  = EXTRACTED_DIR / "all_entities.json"
MANIFEST_PATH      = EXTRACTED_DIR / ".manifest.json"
CHANGELOG_PATH     = EXTRACTED_DIR / "changelog.json"
ENTITY_CACHE_DIR   = EXTRACTED_DIR / "cache"   # per-file entity cache (avoids re-calling Gemini for unchanged files)

LOCAL_EXTENSIONS = ["*.pdf", "*.txt", "*.html", "*.json"]


# ── Stage 1: Scrape + chunk ────────────────────────────────────────────────────

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

    for src in Config.SOURCES:
        input_id = src.get("id", "remote")
        if (AGENTIC_DIR / f"{input_id}.json").exists():
            print(f"[Skip] Already chunked: {input_id}")
            continue
        print(f"[Chunk] Remote: {input_id}")
        pipeline.process(src)

    print("[Done] All sources chunked")


# ── Stage 2: Fetch existing Firestore data ─────────────────────────────────────

def stage_fetch_existing(test_mode: bool = False) -> dict:
    """
    Fetch all existing Firestore documents from v2_airlines and v2_airports.
    Returns {doc_id: data} used for static-field preservation and diff-based push.
    Returns {} if Firestore is not configured.
    """
    print("\n" + "=" * 60)
    print("  STAGE 2: FETCH EXISTING FIRESTORE DATA")
    print("=" * 60)

    try:
        client = FirestoreClient(test_mode=test_mode)
        existing = client.fetch_all_entities()
        print(f"[Firestore] {len(existing)} existing documents loaded")
        return existing
    except (ValueError, FileNotFoundError) as e:
        print(f"[Firestore] Not configured — skipping fetch: {e}")
        print("[Firestore] Extraction will proceed without static-field preservation")
        return {}
    except Exception as e:
        print(f"[Firestore] Fetch error: {e}")
        return {}


# ── Stage 3: Entity extraction ─────────────────────────────────────────────────

def _file_hash(path: Path) -> str:
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


def stage_extract(existing_docs: dict) -> list:
    """
    Extract entities from agentic chunk files.

    Per-file cache (rules/extracted/cache/<stem>.json):
      Each chunk file has its own cached result. Only files whose hash changed
      since the last run call Gemini — unchanged files are loaded from cache.
      This prevents re-calling Gemini for Porto airport when only Lufthansa changed.
    """
    print("\n" + "=" * 60)
    print("  STAGE 3: ENTITY EXTRACTION")
    print("=" * 60)

    EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)
    ENTITY_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    agentic_files = sorted(
        p for p in AGENTIC_DIR.glob("*.json")
        if not p.name.endswith("_partial.json")
    )

    if not agentic_files:
        print("[Extractor] No agentic chunk files found.")
        return []

    print(f"[Extractor] Agentic chunk files: {len(agentic_files)}")

    current_hashes = {f.stem: _file_hash(f) for f in agentic_files}
    manifest = _load_manifest()
    changed_set = set(k for k, h in current_hashes.items() if manifest.get(k) != h)

    if not changed_set:
        print(f"[Extractor] All {len(current_hashes)} chunk files unchanged — using cache")
    else:
        print(f"[Extractor] {len(changed_set)} file(s) changed: {', '.join(sorted(changed_set))}")

    extractor = None  # lazy-init: only created if Gemini is actually needed
    entities  = []

    for chunk_file in agentic_files:
        stem = chunk_file.stem
        cache_file = ENTITY_CACHE_DIR / f"{stem}.json"

        # Serve from per-file cache if the chunk hasn't changed
        if stem not in changed_set and cache_file.exists():
            with open(cache_file, "r", encoding="utf-8") as f:
                cached = json.load(f)
            if cached:
                print(f"[Extractor] {stem}: {len(cached)} entity(s) from cache")
                entities.extend(cached)
            continue

        # Chunk changed (or no cache yet) — need to call Gemini
        with open(chunk_file, "r", encoding="utf-8") as f:
            chunks = json.load(f)

        if not chunks:
            print(f"[Extractor] {stem}: empty — skipping")
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump([], f)
            continue

        print(f"[Extractor] {stem}: {len(chunks)} chunks → extracting...")
        if extractor is None:
            extractor = RuleExtractor()

        extracted = extractor.run(chunks, existing_docs=existing_docs)
        entities.extend(extracted)

        # Save per-file cache so this file won't need Gemini next run
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(extracted, f, indent=2, ensure_ascii=False)

    print(f"\n[Extractor] {len(entities)} entities total")

    _save_manifest(current_hashes)
    if extractor:
        extractor.save(entities, str(ALL_ENTITIES_PATH))
        extractor.save_split(entities, str(EXTRACTED_DIR))
    elif not ALL_ENTITIES_PATH.exists():
        # First-ever run with all files cached — still write all_entities.json
        with open(ALL_ENTITIES_PATH, "w", encoding="utf-8") as f:
            json.dump(entities, f, indent=2, ensure_ascii=False)

    return entities


# ── Stage 4: Firestore compare-and-push ───────────────────────────────────────

def stage_firestore(entities: list, existing_docs: dict, test_mode: bool = False):
    """
    Compare each entity against its existing Firestore doc.
    Push only entities with changes. Log every changed field.
    """
    print("\n" + "=" * 60)
    print("  STAGE 4: FIRESTORE PUSH (compare-and-push)")
    print("=" * 60)

    if not entities:
        print("[Firestore] Nothing to push.")
        return

    try:
        client = FirestoreClient(test_mode=test_mode)
        stats  = client.compare_and_push(entities, existing_docs)

        # Save changelog
        if stats["changelog"]:
            EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)
            with open(CHANGELOG_PATH, "w", encoding="utf-8") as f:
                json.dump(stats["changelog"], f, indent=2, ensure_ascii=False)
            print(f"[Firestore] Changelog saved → {CHANGELOG_PATH}")
        else:
            print("[Firestore] No changes detected — nothing pushed.")

    except (ValueError, FileNotFoundError) as e:
        print(f"[Firestore] Configuration error: {e}")
        print("[Firestore] Set FIRESTORE_PROJECT_ID and FIREBASE_CREDENTIALS_PATH in .env")
    except Exception as e:
        print(f"[Firestore] Push failed: {e}")


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true",
                        help="Write to v2_airlines_test / v2_airports_test instead of real collections")
    parser.add_argument("--skip-chunk", action="store_true",
                        help="Skip Stage 1 (use existing chunk files)")
    args = parser.parse_args()

    if not args.skip_chunk:
        stage_chunk()

    existing_docs = stage_fetch_existing(test_mode=args.test)
    entities      = stage_extract(existing_docs)
    stage_firestore(entities, existing_docs, test_mode=args.test)

    print("\n[Pipeline] All stages complete.")
