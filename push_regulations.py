"""
push_regulations.py
-------------------
Standalone script that extracts PRM rules from all non-airline / non-airport
agentic chunk files (IATA docs, EUR-LEX, US DOT, local PDFs, etc.) and pushes
them to the Firestore collection  v2_regulations.

Each Firestore document:
  - ID         = source_id  (e.g. "iata_accessibility_fact_sheet")
  - entity      : source organisation name
  - entity_type : e.g. "industry_body", "regulator"
  - source_url  : original URL or file path
  - services    : [{type, description:{en, ro}, is_presented}]
  - updated_at  : ISO-8601 timestamp

Per-file cache:
  rules/regulations/per_file/{source_id}.json  — cached extraction result
  rules/regulations/.manifest.json             — SHA-256 hash per file
  Only files whose hash changed are re-extracted.

Run:
  python push_regulations.py
"""

import json
import hashlib
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import firestore
from google.oauth2 import service_account

from extractor.rule_extractor import RuleExtractor

load_dotenv()

# ── paths ──────────────────────────────────────────────────────────────────────
AGENTIC_DIR    = Path("chunk_store/agentic")
CACHE_DIR      = Path("rules/regulations/per_file")
MANIFEST_PATH  = Path("rules/regulations/.manifest.json")

COLLECTION     = "v2_regulations"

# entity_type values that belong to the main pipeline (skip them here)
_ENTITY_PIPELINE_TYPES = {"airline", "airport"}


# ── helpers ────────────────────────────────────────────────────────────────────

def _file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def _load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_manifest(manifest: dict):
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def _load_cache(source_id: str) -> dict | None:
    path = CACHE_DIR / f"{source_id}.json"
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return None


def _save_cache(source_id: str, data: dict):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_DIR / f"{source_id}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _firestore_client():
    project_id  = os.getenv("FIRESTORE_PROJECT_ID")
    creds_path  = os.getenv("FIREBASE_CREDENTIALS_PATH")
    if not project_id:
        raise ValueError("FIRESTORE_PROJECT_ID not set in .env")
    if not creds_path or not Path(creds_path).exists():
        raise FileNotFoundError(f"Firebase credentials not found: {creds_path}")
    creds = service_account.Credentials.from_service_account_file(creds_path)
    return firestore.Client(project=project_id, credentials=creds)


# ── stage: extract ─────────────────────────────────────────────────────────────

def extract_regulations() -> list:
    print("\n" + "=" * 60)
    print("  EXTRACT: Regulatory / Reference Sources")
    print("=" * 60)

    agentic_files = sorted(
        p for p in AGENTIC_DIR.glob("*.json")
        if not p.name.endswith("_partial.json")
    )

    # Filter to non-airline / non-airport sources only
    regulation_files = []
    for f in agentic_files:
        chunks = json.loads(f.read_text(encoding="utf-8"))
        if not chunks:
            continue
        et = chunks[0].get("entity_type")
        if et in _ENTITY_PIPELINE_TYPES:
            continue   # handled by main pipeline
        regulation_files.append((f, chunks))

    if not regulation_files:
        print("[Regulations] No regulatory chunk files found.")
        return []

    print(f"[Regulations] Found {len(regulation_files)} regulatory source(s)")

    manifest  = _load_manifest()
    extractor = None
    results   = []
    first     = True

    for chunk_file, chunks in regulation_files:
        source_id    = chunk_file.stem
        current_hash = _file_hash(chunk_file)
        cached       = _load_cache(source_id)

        if manifest.get(source_id) == current_hash and cached is not None:
            print(f"[Regulations] {source_id}: unchanged — using cache")
            results.append(cached)
            continue

        print(f"[Regulations] {source_id}: {'changed' if source_id in manifest else 'new'} — extracting...")

        if extractor is None:
            extractor = RuleExtractor()

        if not first:
            time.sleep(4)
        first = False

        doc = extractor.extract_reference_from_chunks(chunks)
        manifest[source_id] = current_hash

        if doc is None:
            continue

        _save_cache(source_id, doc)
        results.append(doc)

    _save_manifest(manifest)
    print(f"[Regulations] Extraction done — {len(results)} documents ready")
    return results


# ── stage: push ────────────────────────────────────────────────────────────────

def push_regulations(docs: list):
    print("\n" + "=" * 60)
    print("  PUSH: v2_regulations")
    print("=" * 60)

    if not docs:
        print("[Regulations] Nothing to push.")
        return

    db        = _firestore_client()
    timestamp = datetime.now(timezone.utc).isoformat()
    pushed    = 0
    errors    = 0

    for doc in docs:
        source_id = doc.get("source_id")
        if not source_id:
            print("[Regulations] Skipping doc with no source_id")
            errors += 1
            continue
        try:
            ref = db.collection(COLLECTION).document(source_id)
            ref.set({**doc, "updated_at": timestamp})
            print(f"[Regulations] Pushed '{source_id}' — {len(doc.get('services', []))} services")
            pushed += 1
        except Exception as e:
            print(f"[Regulations] Error pushing '{source_id}': {e}")
            errors += 1

    print(f"\n[Regulations] Summary — Pushed: {pushed} | Errors: {errors}")


# ── main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    docs = extract_regulations()
    push_regulations(docs)
    print("\n[Regulations] Done.")
