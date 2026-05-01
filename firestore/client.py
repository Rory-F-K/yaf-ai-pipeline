# firestore/client.py
#
# Purpose:
#   Pushes validated rules to Firestore with content-hash-based deduplication.
#   Uses the rule's content_hash as the Firestore document ID so that identical
#   rules are never written twice — even across pipeline runs.
#
# Dedup strategy:
#   1. Fetch all existing document IDs from the 'rules' collection (one list call).
#   2. For each rule: if its content_hash is already a doc ID → skip.
#   3. Write only new rules in Firestore WriteBatches (max 500 ops each).
#
# Firestore document schema:
#   Document ID : content_hash  (stable, content-based — natural dedup key)
#   Fields      : rule_id, category, title, description, source,
#                 version, content_hash, pushed_at (ISO timestamp)
#
# Required .env variables:
#   FIRESTORE_PROJECT_ID       — Firebase project ID
#   FIREBASE_CREDENTIALS_PATH  — Path to service account JSON file
#
# Dependencies:
#   google-cloud-firestore (already in requirements.txt)
#   google-oauth2-service-account

import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import firestore
from google.oauth2 import service_account

from utils.hashing import hash_rule_content

load_dotenv()

FIRESTORE_PROJECT_ID      = os.getenv("FIRESTORE_PROJECT_ID")
FIREBASE_CREDENTIALS_PATH = os.getenv("FIREBASE_CREDENTIALS_PATH")
FIRESTORE_COLLECTION      = "rules"
FIRESTORE_BATCH_SIZE      = 500   # Firestore hard limit per WriteBatch


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
        self.collection = self.db.collection(FIRESTORE_COLLECTION)

    # ── internal helpers ───────────────────────────────────────────────────────

    def _fetch_existing_hashes(self) -> set:
        """
        Fetch all document IDs from the Firestore rules collection.
        Document IDs equal content_hashes, so this is the complete dedup set.
        Only doc IDs are fetched — no field data is transferred.
        """
        print(f"[Firestore] Checking existing rules in '{FIRESTORE_COLLECTION}'...")
        existing = {doc.id for doc in self.collection.list_documents()}
        print(f"[Firestore] {len(existing)} rules already stored")
        return existing

    def _ensure_hash(self, rule: dict) -> dict:
        """Return rule with content_hash set (compute if missing)."""
        if rule.get("content_hash"):
            return rule
        return {
            **rule,
            "content_hash": hash_rule_content(
                rule.get("description", ""), rule.get("title", "")
            ),
        }

    # ── public API ─────────────────────────────────────────────────────────────

    def fetch_all_rules(self) -> list:
        """
        Fetch all rules from Firestore and return them as a list of dicts.
        Each dict contains all stored fields (rule_id, category, title,
        description, source, version, content_hash, pushed_at).
        """
        docs = self.collection.stream()
        return [doc.to_dict() for doc in docs]

    def push_rules(self, rules: list) -> dict:
        """
        Push validated rules to Firestore, skipping any whose content_hash
        already exists as a document ID.

        Uses WriteBatch for fast bulk writes (up to 500 ops per commit).

        Args:
            rules: list of rule dicts (must include content_hash or have
                   title + description to compute it from)

        Returns:
            dict with keys: total, pushed, skipped
        """
        if not rules:
            print("[Firestore] No rules to push.")
            return {"total": 0, "pushed": 0, "skipped": 0}

        existing_hashes = self._fetch_existing_hashes()

        # Separate new rules from already-stored ones
        to_push = []
        skipped = 0
        for rule in rules:
            rule = self._ensure_hash(rule)
            if rule["content_hash"] in existing_hashes:
                skipped += 1
            else:
                to_push.append(rule)

        print(
            f"[Firestore] {len(to_push)} new rules to push | "
            f"{skipped} skipped (unchanged)"
        )

        # Batch-write in chunks of FIRESTORE_BATCH_SIZE
        pushed = 0
        timestamp = datetime.now(timezone.utc).isoformat()

        for batch_start in range(0, len(to_push), FIRESTORE_BATCH_SIZE):
            batch_slice = to_push[batch_start : batch_start + FIRESTORE_BATCH_SIZE]
            write_batch = self.db.batch()

            for rule in batch_slice:
                doc_ref = self.collection.document(rule["content_hash"])
                write_batch.set(doc_ref, {**rule, "pushed_at": timestamp})

            write_batch.commit()
            pushed += len(batch_slice)
            batch_num = batch_start // FIRESTORE_BATCH_SIZE + 1
            print(f"[Firestore] Batch {batch_num} committed — {len(batch_slice)} rules")

        print(
            f"[Firestore] Complete. "
            f"Pushed: {pushed} | Skipped: {skipped} | Total: {len(rules)}"
        )
        return {"total": len(rules), "pushed": pushed, "skipped": skipped}
