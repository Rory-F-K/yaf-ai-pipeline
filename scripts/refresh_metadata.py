#!/usr/bin/env python3
# scripts/refresh_metadata.py
#
# Syncs entity_metadata.json from Firestore.
# Run this whenever you:
#   - Add a new airline/airport to Firestore
#   - Update static fields (images, contacts, sub_rules) in Firestore
#   - Add a new domain to DOMAIN_MAP in extractor/rule_extractor.py
#
# Usage:
#   python scripts/refresh_metadata.py
#
# What it does:
#   1. Reads DOMAIN_MAP from extractor/rule_extractor.py to get all known doc_ids
#   2. Fetches the current Firestore document for each doc_id
#   3. Strips dynamic fields (rules, services) — keeps only static metadata
#   4. Writes/updates entity_metadata.json in the project root
#
# For new entities (doc_id in DOMAIN_MAP but NOT yet in Firestore):
#   The script creates a placeholder entry in entity_metadata.json with empty
#   static fields so the pipeline can create a skeleton Firestore document.
#   Fill in the real values (images, contacts, sub_rules) in Firestore, then
#   run this script again to pull them into entity_metadata.json.

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from google.cloud import firestore
from google.oauth2 import service_account

load_dotenv(dotenv_path=".env")

from extractor.rule_extractor import DOMAIN_MAP, _airline_skeleton, _airport_skeleton

FIRESTORE_PROJECT_ID      = os.getenv("FIRESTORE_PROJECT_ID")
FIREBASE_CREDENTIALS_PATH = os.getenv("FIREBASE_CREDENTIALS_PATH")
METADATA_FILE = "entity_metadata.json"
DYNAMIC_FIELDS = {"rules", "services"}


def main():
    if not FIRESTORE_PROJECT_ID or not FIREBASE_CREDENTIALS_PATH:
        print("ERROR: Set FIRESTORE_PROJECT_ID and FIREBASE_CREDENTIALS_PATH in .env")
        sys.exit(1)

    creds = service_account.Credentials.from_service_account_file(FIREBASE_CREDENTIALS_PATH)
    db = firestore.Client(project=FIRESTORE_PROJECT_ID, credentials=creds)

    # Fetch all Firestore docs
    all_docs = {}
    for col in ["v2_airlines", "v2_airports"]:
        for doc in db.collection(col).stream():
            all_docs[doc.id] = doc.to_dict()
    print(f"[refresh] Fetched {len(all_docs)} Firestore docs")

    # Load existing metadata to preserve any manual additions
    existing_metadata = {}
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r", encoding="utf-8") as f:
            existing_metadata = json.load(f)

    metadata = {}
    new_count = updated_count = placeholder_count = 0

    for domain, info in DOMAIN_MAP.items():
        doc_id      = info["doc_id"]
        entity_type = info["entity_type"]

        if doc_id in all_docs:
            d = all_docs[doc_id]
            static = {k: v for k, v in d.items() if k not in DYNAMIC_FIELDS}
            is_new = doc_id not in existing_metadata

            metadata[doc_id] = {
                "entity_type": entity_type,
                "domain":      domain,
                "static":      static,
            }

            if is_new:
                new_count += 1
                name = (static.get("name") or {}).get("en", domain) if entity_type == "airline" else static.get("full_name", domain)
                print(f"[refresh] NEW   {entity_type}: {name} ({doc_id})")
            else:
                updated_count += 1
                name = (static.get("name") or {}).get("en", domain) if entity_type == "airline" else static.get("full_name", domain)
                print(f"[refresh] SYNC  {entity_type}: {name} ({doc_id})")

        else:
            # doc doesn't exist in Firestore yet — create placeholder
            name = domain
            if entity_type == "airline":
                static = _airline_skeleton(name)
            else:
                static = _airport_skeleton(name)

            metadata[doc_id] = {
                "entity_type": entity_type,
                "domain":      domain,
                "static":      static,
                "_placeholder": True,
            }
            placeholder_count += 1
            print(f"[refresh] ⚠ PLACEHOLDER {entity_type}: '{domain}' — not in Firestore yet (doc_id={doc_id})")
            print(f"           Create the Firestore doc, fill in static fields, then re-run this script.")

    with open(METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    print(
        f"\n[refresh] Done — {len(metadata)} entries written to {METADATA_FILE}\n"
        f"  New: {new_count} | Updated: {updated_count} | Placeholders: {placeholder_count}"
    )


if __name__ == "__main__":
    main()
