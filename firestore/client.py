# firestore/client.py
#
# Purpose:
#   Reads and writes airline/airport entities to Firestore.
#   Airlines → v2_airlines collection
#   Airports → v2_airports collection
#
# Key methods:
#   fetch_all(collection)          → {doc_id: data} for every doc in a collection
#   fetch_all_entities()           → combined {doc_id: data} across both collections
#   compare_and_push(entities)     → diff new vs existing, push only changed docs,
#                                    return a detailed change log
#
# Change detection:
#   Deep-diffs the dynamic fields (services for airports, rules for airlines).
#   Static fields (images, IDs, sub_rules metadata) are never pushed by the pipeline —
#   they live only in Firestore and are preserved by extract_entity() merging.
#
# Required .env variables:
#   FIRESTORE_PROJECT_ID       — Firebase project ID
#   FIREBASE_CREDENTIALS_PATH  — Path to service account JSON file

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from google.cloud import firestore
from google.oauth2 import service_account

load_dotenv()

FIRESTORE_PROJECT_ID      = os.getenv("FIRESTORE_PROJECT_ID")
FIREBASE_CREDENTIALS_PATH = os.getenv("FIREBASE_CREDENTIALS_PATH")

AIRLINES_COLLECTION = "v2_airlines"
AIRPORTS_COLLECTION = "v2_airports"
FIRESTORE_BATCH_SIZE = 500

# Internal metadata keys added by the pipeline — never pushed to Firestore
_PIPELINE_KEYS = {"_doc_id", "_entity_type"}


class FirestoreClient:
    def __init__(self, test_mode: bool = False):
        """
        Args:
            test_mode: if True, writes go to v2_airlines_test / v2_airports_test
                       instead of the real collections. Reads always use real collections.
        """
        if not FIRESTORE_PROJECT_ID:
            raise ValueError("FIRESTORE_PROJECT_ID not set in .env")
        if not FIREBASE_CREDENTIALS_PATH:
            raise ValueError("FIREBASE_CREDENTIALS_PATH not set in .env")

        creds_path = Path(FIREBASE_CREDENTIALS_PATH)
        if not creds_path.exists():
            raise FileNotFoundError(f"Firebase credentials file not found: {creds_path}")

        credentials = service_account.Credentials.from_service_account_file(str(creds_path))
        self.db = firestore.Client(project=FIRESTORE_PROJECT_ID, credentials=credentials)

        # In test_mode writes go to _test collections; reads always use real collections
        if test_mode:
            self._write_airlines = AIRLINES_COLLECTION + "_test"
            self._write_airports = AIRPORTS_COLLECTION + "_test"
            print(f"[Firestore] ⚠ TEST MODE — writes go to "
                  f"'{self._write_airlines}' / '{self._write_airports}' (real data untouched)")
        else:
            self._write_airlines = AIRLINES_COLLECTION
            self._write_airports = AIRPORTS_COLLECTION

    # ── fetch ──────────────────────────────────────────────────────────────────

    def fetch_all(self, collection: str) -> dict:
        """Return {doc_id: data_dict} for every document in a collection."""
        result = {}
        for doc in self.db.collection(collection).stream():
            result[doc.id] = doc.to_dict()
        print(f"[Firestore] Fetched {len(result)} docs from '{collection}'")
        return result

    def fetch_all_entities(self) -> dict:
        """Return {doc_id: data_dict} across both v2_airlines and v2_airports."""
        all_docs = {}
        all_docs.update(self.fetch_all(AIRLINES_COLLECTION))
        all_docs.update(self.fetch_all(AIRPORTS_COLLECTION))
        print(f"[Firestore] Total existing docs: {len(all_docs)}")
        return all_docs

    # ── deep diff ─────────────────────────────────────────────────────────────

    @staticmethod
    def _deep_diff(old: Any, new: Any, path: str = "") -> list[dict]:
        """
        Recursively compare two values and return a list of change dicts:
          {path, old_value, new_value}
        Only leaf-level changes are reported.
        """
        changes = []

        if type(old) != type(new):
            changes.append({"path": path, "old": old, "new": new})
            return changes

        if isinstance(new, dict):
            all_keys = set(old.keys()) | set(new.keys())
            for k in all_keys:
                child_path = f"{path}.{k}" if path else k
                if k not in old:
                    changes.append({"path": child_path, "old": None, "new": new[k]})
                elif k not in new:
                    changes.append({"path": child_path, "old": old[k], "new": None})
                else:
                    changes.extend(FirestoreClient._deep_diff(old[k], new[k], child_path))

        elif isinstance(new, list):
            if old != new:
                changes.append({"path": path, "old": old, "new": new})

        else:
            if old != new:
                changes.append({"path": path, "old": old, "new": new})

        return changes

    # ── push ──────────────────────────────────────────────────────────────────

    def _collection_for(self, entity: dict) -> tuple[str, str]:
        """Return (write_collection_name, doc_id) for an entity."""
        doc_id      = entity.get("_doc_id")
        entity_type = entity.get("_entity_type")
        if not doc_id:
            raise ValueError("Entity missing '_doc_id' pipeline metadata")
        collection = self._write_airlines if entity_type == "airline" else self._write_airports
        return collection, doc_id

    def compare_and_push(self, entities: list, existing_docs: dict) -> dict:
        """
        For each entity:
          1. Strip pipeline-internal keys (_doc_id, _entity_type)
          2. Compare new data against existing Firestore doc
          3. Push ONLY if something changed (or doc is new)
          4. Log every changed field path

        Args:
            entities:      list of entity dicts from RuleExtractor (with _doc_id/_entity_type)
            existing_docs: {doc_id: data} from fetch_all_entities()

        Returns:
            {
              total:     int,
              pushed:    int,
              skipped:   int,
              errors:    int,
              changelog: [{ doc_id, entity_type, name, changes: [{path, old, new}] }]
            }
        """
        timestamp  = datetime.now(timezone.utc).isoformat()
        pushed = skipped = errors = 0
        changelog = []

        for entity in entities:
            doc_id      = entity.get("_doc_id", "?")
            entity_type = entity.get("_entity_type", "?")
            name        = (entity.get("name") or entity.get("full_name") or doc_id)
            if isinstance(name, dict):
                name = name.get("en", doc_id)

            # Strip pipeline keys before comparison / push
            clean = {k: v for k, v in entity.items() if k not in _PIPELINE_KEYS}

            try:
                collection, doc_id_str = self._collection_for(entity)
            except ValueError as e:
                print(f"[Firestore] Skipping — {e}")
                errors += 1
                continue

            existing = existing_docs.get(doc_id_str, {})

            # Diff only the dynamic field(s)
            if entity_type == "airport":
                changes = self._deep_diff(
                    existing.get("services", []),
                    clean.get("services", []),
                    path="services"
                )
            else:
                changes = self._deep_diff(
                    existing.get("rules", {}),
                    clean.get("rules", {}),
                    path="rules"
                )

            if not changes and existing:
                print(f"[Firestore] No changes — skipping '{name}' ({doc_id_str})")
                skipped += 1
                continue

            # Something changed (or doc is new) → push
            try:
                doc_ref = self.db.collection(collection).document(doc_id_str)
                doc_ref.set({**clean, "updated_at": timestamp}, merge=True)
                pushed += 1

                change_summary = {"doc_id": doc_id_str, "entity_type": entity_type,
                                  "name": name, "changes": changes}
                changelog.append(change_summary)

                if changes:
                    print(f"[Firestore] Updated '{name}' — {len(changes)} field change(s):")
                    for c in changes[:5]:
                        print(f"    {c['path']}: {str(c['old'])[:60]!r} → {str(c['new'])[:60]!r}")
                    if len(changes) > 5:
                        print(f"    ... and {len(changes) - 5} more changes")
                else:
                    print(f"[Firestore] Created new doc '{name}' ({doc_id_str})")

            except Exception as e:
                print(f"[Firestore] Error pushing '{name}': {e}")
                errors += 1

        stats = {
            "total":     len(entities),
            "pushed":    pushed,
            "skipped":   skipped,
            "errors":    errors,
            "changelog": changelog,
        }
        print(
            f"\n[Firestore] Summary — Total: {len(entities)} | "
            f"Pushed: {pushed} | Skipped (no change): {skipped} | Errors: {errors}"
        )
        return stats

    # ── legacy push (kept for backward compat) ─────────────────────────────────

    def push_entities(self, entities: list) -> dict:
        """
        Legacy method — pushes all entities unconditionally (no diff check).
        Prefer compare_and_push() for the updated pipeline.
        """
        timestamp      = datetime.now(timezone.utc).isoformat()
        airlines_pushed = airports_pushed = errors = 0

        for batch_start in range(0, len(entities), FIRESTORE_BATCH_SIZE):
            batch_slice = entities[batch_start: batch_start + FIRESTORE_BATCH_SIZE]
            write_batch = self.db.batch()

            for entity in batch_slice:
                clean = {k: v for k, v in entity.items() if k not in _PIPELINE_KEYS}
                try:
                    collection, doc_id = self._collection_for(entity)
                    doc_ref = self.db.collection(collection).document(doc_id)
                    write_batch.set(doc_ref, {**clean, "pushed_at": timestamp})
                    if collection == AIRLINES_COLLECTION:
                        airlines_pushed += 1
                    else:
                        airports_pushed += 1
                except ValueError as e:
                    print(f"[Firestore] Skipping entity — {e}")
                    errors += 1

            write_batch.commit()

        total = airlines_pushed + airports_pushed
        print(f"[Firestore] Complete. Airlines: {airlines_pushed} | Airports: {airports_pushed} | Errors: {errors} | Total: {total}")
        return {"total": len(entities), "airlines_pushed": airlines_pushed,
                "airports_pushed": airports_pushed, "errors": errors}
