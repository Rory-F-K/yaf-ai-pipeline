# utils/hashing.py
#
# Purpose:
#   Provides content hashing, snapshot management, and change detection for the
#   rules pipeline. Enables versioning of rules across runs — detecting which rules
#   were added, removed, or modified since the last snapshot was saved.
#
# Core functions:
#   hash_rule_content(description, title)
#       SHA-256 hash of a single rule's content. Used to detect edits between runs.
#       Only hashes meaningful content (title + description), not metadata like rule_id.
#
#   hash_ruleset(rules)
#       Single SHA-256 hash representing the entire ruleset. Quick dirty-check for
#       whether anything changed at all before running detailed comparison.
#
#   save_snapshot(rules, snapshot_path)
#       Saves current rule hashes and metadata to a JSON file for future comparison.
#
#   load_snapshot(snapshot_path)
#       Loads a previously saved snapshot. Returns empty dict if none exists.
#
#   detect_changes(current_rules, snapshot_path)
#       Compares current rules against the last saved snapshot and returns a report
#       with: added, removed, modified, and unchanged rule IDs.
#
#   apply_versions(current_rules, snapshot_path)
#       Returns rules with version numbers assigned:
#         - new rules start at version 1
#         - modified rules get their version incremented
#         - unchanged rules keep their existing version

import hashlib
import json
from pathlib import Path
from datetime import datetime, timezone


# ── Core hashing ───────────────────────────────────────────────────────────────

def hash_rule_content(description: str, title: str = "") -> str:
    """
    Stable SHA-256 hash of a rule's content.
    Used to detect when a rule has meaningfully changed between runs.
    Only hashes description + title (not rule_id, source, or version).
    """
    normalized = f"{title.strip().lower()}||{description.strip().lower()}"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def hash_ruleset(rules: list[dict]) -> str:
    """
    Single SHA-256 hash representing the entire ruleset.
    Useful for quickly checking if anything changed at all.
    """
    content = json.dumps(
        [{"rule_id": r.get("rule_id"), "hash": hash_rule_content(
            r.get("description", ""), r.get("title", "")
        )} for r in sorted(rules, key=lambda r: r.get("rule_id", ""))],
        sort_keys=True
    )
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


# ── Snapshot management ────────────────────────────────────────────────────────

def save_snapshot(rules: list[dict], snapshot_path: str):
    """
    Save a hashed snapshot of the current ruleset to disk.
    Each entry stores the rule_id, content_hash, and a timestamp.
    """
    snapshot = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ruleset_hash": hash_ruleset(rules),
        "rules": {
            r["rule_id"]: {
                "content_hash": hash_rule_content(r.get("description", ""), r.get("title", "")),
                "title": r.get("title", ""),
                "category": r.get("category", ""),
                "source": r.get("source", ""),
            }
            for r in rules
        }
    }
    Path(snapshot_path).parent.mkdir(parents=True, exist_ok=True)
    with open(snapshot_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)
    print(f"[Hashing] Snapshot saved → {snapshot_path}")


def load_snapshot(snapshot_path: str) -> dict:
    """Load a previously saved snapshot. Returns empty dict if none exists."""
    p = Path(snapshot_path)
    if not p.exists():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


# ── Change detection ───────────────────────────────────────────────────────────

def detect_changes(current_rules: list[dict], snapshot_path: str) -> dict:
    """
    Compare current rules against the last saved snapshot.

    Returns a change report with:
      - added:    rules not in the previous snapshot
      - removed:  rules in the previous snapshot but not now
      - modified: rules whose content hash changed
      - unchanged: rules with identical content
      - is_changed: True if any adds, removals, or modifications exist
    """
    snapshot = load_snapshot(snapshot_path)
    prev_rules = snapshot.get("rules", {})

    current_map = {
        r["rule_id"]: {
            "content_hash": hash_rule_content(r.get("description", ""), r.get("title", "")),
            "title": r.get("title", ""),
            "category": r.get("category", ""),
            "source": r.get("source", ""),
        }
        for r in current_rules
    }

    current_ids = set(current_map.keys())
    prev_ids    = set(prev_rules.keys())

    added    = sorted(current_ids - prev_ids)
    removed  = sorted(prev_ids - current_ids)
    modified = []
    unchanged = []

    for rid in current_ids & prev_ids:
        if current_map[rid]["content_hash"] != prev_rules[rid]["content_hash"]:
            modified.append(rid)
        else:
            unchanged.append(rid)

    is_changed = bool(added or removed or modified)

    report = {
        "snapshot_timestamp": snapshot.get("timestamp", "no previous snapshot"),
        "is_changed": is_changed,
        "added":     added,
        "removed":   removed,
        "modified":  modified,
        "unchanged": unchanged,
        "counts": {
            "added":     len(added),
            "removed":   len(removed),
            "modified":  len(modified),
            "unchanged": len(unchanged),
        }
    }

    _print_change_report(report)
    return report


def _print_change_report(report: dict):
    print("\n" + "=" * 50)
    print("  CHANGE DETECTION REPORT")
    print("=" * 50)
    print(f"  Previous snapshot : {report['snapshot_timestamp']}")
    print(f"  Changed           : {report['is_changed']}")
    print(f"  Added             : {report['counts']['added']}")
    print(f"  Removed           : {report['counts']['removed']}")
    print(f"  Modified          : {report['counts']['modified']}")
    print(f"  Unchanged         : {report['counts']['unchanged']}")

    if report["added"]:
        print(f"\n  NEW     : {', '.join(report['added'])}")
    if report["removed"]:
        print(f"  REMOVED : {', '.join(report['removed'])}")
    if report["modified"]:
        print(f"  CHANGED : {', '.join(report['modified'])}")

    print("=" * 50 + "\n")


# ── Version bumping ────────────────────────────────────────────────────────────

def apply_versions(current_rules: list[dict], snapshot_path: str) -> list[dict]:
    """
    Assign version numbers to rules based on change history.
    - New rules start at version 1.
    - Modified rules get their version incremented.
    - Unchanged rules keep their previous version.
    """
    snapshot = load_snapshot(snapshot_path)
    prev_rules = snapshot.get("rules", {})

    versioned = []
    for r in current_rules:
        rid = r.get("rule_id", "")
        current_hash = hash_rule_content(r.get("description", ""), r.get("title", ""))

        prev = prev_rules.get(rid)
        if prev is None:
            version = 1                          # new rule
        elif prev["content_hash"] != current_hash:
            version = r.get("version", 1) + 1   # content changed
        else:
            version = r.get("version", 1)        # unchanged

        versioned.append({**r, "version": version, "content_hash": current_hash})

    return versioned
