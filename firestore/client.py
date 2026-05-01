# firestore/client.py
#
# Purpose:
#   Pushes validated airline and airport entities to Firestore.
#   Airlines go to the 'v2_airlines' collection, airports to 'v2_airports'.
#   Document ID is the airline_id or airport_id — so each entity is always
#   overwritten with the latest extracted services on every pipeline run.
#
# Firestore document schema:
#   Airline doc  (v2_airlines/{airline_id}):
#     airline_id, name, source, services, pushed_at
#   Airport doc  (v2_airports/{airport_id}):
#     airport_id, name, source, services, pushed_at
#
# Required .env variables:
#   FIRESTORE_PROJECT_ID       — Firebase project ID
#   FIREBASE_CREDENTIALS_PATH  — Path to service account JSON file

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
FIRESTORE_BATCH_SIZE = 500


class FirestoreClient:
    def __init__(self):
        if not FIRESTORE_PROJECT_ID:
            raise ValueError("FIRESTORE_PROJECT_ID not set in .env")
        if not FIREBASE_CREDENTIALS_PATH:
            raise ValueError("FIREBASE_CREDENTIALS_PATH not set in .env")

        creds_path = Path(FIREBASE_CREDENTIALS_PATH)
        if not creds_path.exists():
            raise FileNotFoundError(
                f"Firebase credentials file not found: {creds_path}"
            )

        credentials = service_account.Credentials.from_service_account_file(
            str(creds_path)
        )
        self.db = firestore.Client(
            project=FIRESTORE_PROJECT_ID, credentials=credentials
        )

    # ── internal helpers ───────────────────────────────────────────────────────

    def _route(self, entity: dict) -> tuple[str, str]:
        """
        Returns (collection_name, document_id) for an entity.
        Raises ValueError if the entity has neither airline_id nor airport_id.
        """
        if "airline_id" in entity:
            return AIRLINES_COLLECTION, entity["airline_id"]
        if "airport_id" in entity:
            return AIRPORTS_COLLECTION, entity["airport_id"]
        raise ValueError(f"Entity has no airline_id or airport_id: {entity}")

    # ── public API ─────────────────────────────────────────────────────────────

    def push_entities(self, entities: list) -> dict:
        """
        Push airline and airport entities to their respective Firestore collections.
        Each entity is written using its airline_id/airport_id as the document ID,
        overwriting any existing document for that entity.

        Args:
            entities: list of entity dicts (each must have airline_id or airport_id)

        Returns:
            dict with keys: total, airlines_pushed, airports_pushed, errors
        """
        if not entities:
            print("[Firestore] No entities to push.")
            return {"total": 0, "airlines_pushed": 0, "airports_pushed": 0, "errors": 0}

        timestamp = datetime.now(timezone.utc).isoformat()
        airlines_pushed = 0
        airports_pushed = 0
        errors = 0

        for batch_start in range(0, len(entities), FIRESTORE_BATCH_SIZE):
            batch_slice = entities[batch_start : batch_start + FIRESTORE_BATCH_SIZE]
            write_batch = self.db.batch()

            for entity in batch_slice:
                try:
                    collection_name, doc_id = self._route(entity)
                    doc_ref = self.db.collection(collection_name).document(doc_id)
                    write_batch.set(doc_ref, {**entity, "pushed_at": timestamp})

                    if collection_name == AIRLINES_COLLECTION:
                        airlines_pushed += 1
                    else:
                        airports_pushed += 1
                except ValueError as e:
                    print(f"[Firestore] Skipping entity — {e}")
                    errors += 1

            write_batch.commit()
            batch_num = batch_start // FIRESTORE_BATCH_SIZE + 1
            print(f"[Firestore] Batch {batch_num} committed — {len(batch_slice)} entities")

        total = airlines_pushed + airports_pushed
        print(
            f"[Firestore] Complete. "
            f"Airlines: {airlines_pushed} | Airports: {airports_pushed} | "
            f"Errors: {errors} | Total pushed: {total}"
        )
        return {
            "total":           len(entities),
            "airlines_pushed": airlines_pushed,
            "airports_pushed": airports_pushed,
            "errors":          errors,
        }
