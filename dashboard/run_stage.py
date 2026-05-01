"""
CLI helper — run a single pipeline stage.
Called by the Streamlit dashboard via subprocess.

Usage:
  python dashboard/run_stage.py chunk
  python dashboard/run_stage.py extract
  python dashboard/run_stage.py validate
  python dashboard/run_stage.py version
  python dashboard/run_stage.py firestore
  python dashboard/run_stage.py all
"""
import sys
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import main  # noqa: E402  (needs sys.path set first)


def load_json(path: Path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


stage = sys.argv[1] if len(sys.argv) > 1 else "all"

if stage == "chunk":
    main.stage_chunk()

elif stage == "extract":
    rules = main.stage_extract()
    print(f"\n[Runner] Extracted {len(rules)} rules.")

elif stage == "validate":
    p = PROJECT_ROOT / "rules" / "extracted" / "all_rules.json"
    if not p.exists():
        print("[Runner][Error] rules/extracted/all_rules.json not found. Run extraction first.")
        sys.exit(1)
    rules = load_json(p)
    main.stage_validate(rules)

elif stage == "version":
    p = PROJECT_ROOT / "rules" / "validated" / "clean_rules.json"
    if not p.exists():
        print("[Runner][Error] rules/validated/clean_rules.json not found. Run validation first.")
        sys.exit(1)
    rules = load_json(p)
    main.stage_version(rules)

elif stage == "firestore":
    p = PROJECT_ROOT / "rules" / "validated" / "clean_rules.json"
    if not p.exists():
        print("[Runner][Error] rules/validated/clean_rules.json not found. Run validation first.")
        sys.exit(1)
    rules = load_json(p)
    main.stage_firestore(rules)

elif stage == "all":
    main.stage_chunk()
    all_rules   = main.stage_extract()
    clean_rules = main.stage_validate(all_rules)
    versioned   = main.stage_version(clean_rules)
    # main.stage_firestore(versioned)  # uncomment to enable Firestore push
    print("\n[Runner] All stages complete.")

else:
    print(f"[Runner][Error] Unknown stage: '{stage}'")
    print("Valid stages: chunk, extract, validate, version, firestore, all")
    sys.exit(1)
