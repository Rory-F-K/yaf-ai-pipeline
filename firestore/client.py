import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import firestore
from google.oauth2 import service_account

load_dotenv()

FIRESTORE_PROJECT_ID      = os.getenv("FIRESTORE_PROJECT_ID")
FIREBASE_CREDENTIALS_PATH = os.getenv("FIREBASE_CREDENTIALS_PATH")

AIRLINES_COLLECTION = "v2_airlines"
AIRPORTS_COLLECTION = "v2_airports"


def _normalize(name: str) -> str:
    """Lowercase + strip for loose name matching."""
    return name.lower().strip()


class FirestoreClient:
    def __init__(self):
        if not FIRESTORE_PROJECT_ID:
            raise ValueError("FIRESTORE_PROJECT_ID not set in .env")
        if not FIREBASE_CREDENTIALS_PATH:
            raise ValueError("FIREBASE_CREDENTIALS_PATH not set in .env")

        creds_path = Path(FIREBASE_CREDENTIALS_PATH)
        if not creds_path.exists():
            raise FileNotFoundError(f"Firebase credentials not found: {creds_path}")

        credentials = service_account.Credentials.from_service_account_file(str(creds_path))
        self.db = firestore.Client(project=FIRESTORE_PROJECT_ID, credentials=credentials)

        # Build name→docRef index once on init (avoids repeated Firestore queries)
        self._airline_index: dict[str, firestore.DocumentReference] = {}
        self._airport_index: dict[str, firestore.DocumentReference] = {}
        self._build_index()

    # ── index ──────────────────────────────────────────────────────────────────

    def _build_index(self):
        """
        Read all existing docs in v2_airlines and v2_airports and build
        a normalised-name → doc-ref lookup so we can find docs by entity name
        without re-querying Firestore for every entity.

        Airlines:  name field is {"en": "...", "ro": "..."}  → index on name.en
        Airports:  name field is a plain string               → index on both
                   name and full_name
        """
        # Wrong-format doc IDs to skip during indexing (will be deleted in cleanup)
        _wrong_ids = {"lufthansa", "ryanair", "swiss", "vueling", "porto"}

        for doc in self.db.collection(AIRLINES_COLLECTION).stream():
            if doc.id in _wrong_ids:
                continue
            d = doc.to_dict()
            name_field = d.get("name", {})
            if isinstance(name_field, dict):
                en = name_field.get("en", "")
                if en:
                    self._airline_index[_normalize(en)] = doc.reference
            # plain string name → wrong-format doc, skip

        for doc in self.db.collection(AIRPORTS_COLLECTION).stream():
            if doc.id in _wrong_ids:
                continue
            d = doc.to_dict()
            for key in ("name", "full_name"):
                val = d.get(key, "")
                if val:
                    self._airport_index[_normalize(val)] = doc.reference

        print(f"[Firestore] Index built — {len(self._airline_index)} airlines, {len(self._airport_index)} airports")

    def _token_match(self, index: dict, entity_name: str) -> "firestore.DocumentReference | None":
        """
        Two-pass lookup:
        1. Exact normalised match.
        2. Token overlap: any single token of entity_name found as an index key
           (handles "Swiss International Air Lines" → index key "swiss").
        """
        key = _normalize(entity_name)
        if key in index:
            return index[key]
        for token in key.split():
            if token in index:
                return index[token]
        return None

    def _find_airline_ref(self, entity_name: str) -> "firestore.DocumentReference | None":
        return self._token_match(self._airline_index, entity_name)

    def _find_airport_ref(self, entity_name: str) -> "firestore.DocumentReference | None":
        return self._token_match(self._airport_index, entity_name)

    @staticmethod
    def _new_doc_template(entity_name: str, entity_type: str, services: list, timestamp: str) -> dict:
        """
        Build a new Firestore document in the same schema as the existing original docs
        for an entity that doesn't have an existing document yet.
        """
        name_obj = {"en": entity_name, "ro": entity_name}
        empty_contacts = {
            "email": "", "url": "", "phone": "",
            "availability": {"en": "", "ro": ""},
            "whatsapp": "",
        }
        if entity_type == "airline":
            return {
                "name":                   name_obj,
                "services":               services,
                "rules":                  {},
                "sub_rules":              [],
                "accessibility_info":     [],
                "accessibility_contacts": empty_contacts,
                "icon":                   "",
                "background_image":       "",
                "updated_at":             timestamp,
            }
        else:  # airport
            return {
                "name":             entity_name,
                "full_name":        entity_name,
                "services":         services,
                "accessibility_url": "",
                "airport_url":      "",
                "code":             "",
                "image":            "",
                "background_image": "",
                "phone":            "",
                "updated_at":       timestamp,
            }

    # ── public API ─────────────────────────────────────────────────────────────

    def cleanup_wrong_format_docs(self):
        """
        Delete documents that were pushed with the wrong format (name as doc ID,
        flat schema instead of the correct nested schema). These docs were created
        by earlier pipeline versions and should be removed.
        """
        wrong_ids = {
            AIRLINES_COLLECTION: ["lufthansa", "ryanair", "swiss", "vueling"],
            AIRPORTS_COLLECTION:  ["porto"],
        }
        deleted = 0
        for collection, ids in wrong_ids.items():
            for doc_id in ids:
                ref = self.db.collection(collection).document(doc_id)
                if ref.get().exists:
                    ref.delete()
                    print(f"[Firestore] Deleted wrong-format doc: {collection}/{doc_id}")
                    deleted += 1
        if deleted == 0:
            print("[Firestore] No wrong-format docs to clean up.")

    def push_entities(self, entities: list) -> dict:
        """
        For each entity, find the matching existing Firestore document by entity name
        and update ONLY its `services` field (airlines and airports both use this field).

        Airlines  → v2_airlines  — matched on name.en (case-insensitive)
        Airports  → v2_airports  — matched on name or full_name (case-insensitive)

        If no existing doc is found for an entity, a warning is printed and it is skipped.

        Args:
            entities: list of dicts from the extractor, each with:
                      entity_name, entity_type, services

        Returns:
            dict with counts: total, updated, skipped, errors
        """
        if not entities:
            print("[Firestore] No entities to push.")
            return {"total": 0, "updated": 0, "skipped": 0, "errors": 0}

        timestamp = datetime.now(timezone.utc).isoformat()
        updated = 0
        skipped = 0
        errors  = 0

        for entity in entities:
            entity_name = entity.get("entity_name", "")
            entity_type = entity.get("entity_type", "")
            services    = entity.get("services", [])

            try:
                if entity_type == "airline":
                    ref = self._find_airline_ref(entity_name)
                elif entity_type == "airport":
                    ref = self._find_airport_ref(entity_name)
                else:
                    print(f"[Firestore] Unknown entity_type={entity_type!r} for {entity_name} — skipping")
                    skipped += 1
                    continue

                if ref is None:
                    # Entity is not in the original Firestore set — create a new doc
                    # in the same schema as existing docs so the app can use it.
                    collection = AIRLINES_COLLECTION if entity_type == "airline" else AIRPORTS_COLLECTION
                    ref = self.db.collection(collection).document()
                    new_doc = self._new_doc_template(entity_name, entity_type, services, timestamp)
                    ref.set(new_doc)
                    print(f"[Firestore] Created new {entity_type} doc for '{entity_name}' — {len(services)} services")
                    updated += 1
                    continue

                ref.update({
                    "services":   services,
                    "updated_at": timestamp,
                })
                print(f"[Firestore] Updated {entity_type} '{entity_name}' — {len(services)} services")
                updated += 1

            except Exception as e:
                print(f"[Firestore] Error updating '{entity_name}': {e}")
                errors += 1

        print(
            f"[Firestore] Complete — "
            f"Updated: {updated} | Skipped: {skipped} | Errors: {errors}"
        )
        return {
            "total":   len(entities),
            "updated": updated,
            "skipped": skipped,
            "errors":  errors,
        }
