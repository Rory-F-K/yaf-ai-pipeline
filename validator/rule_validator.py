# validator/rule_validator.py
#
# Purpose:
#   Validates a list of airline/airport entities before they are pushed to Firestore.
#   Checks entity-level structure and each service within the services array.
#
# Checks performed:
#   1. entity_structure  — required top-level fields present and non-empty
#   2. entity_type       — entity has exactly one of airline_id or airport_id (not both/neither)
#   3. services_present  — services array exists and is non-empty
#   4. service_structure — each service has type, description (min 20 chars), is_presented (bool)
#   5. duplicate_types   — duplicate service types within the same entity
#
# Severity levels:
#   error   — entity is excluded from clean_entities output
#   warning — entity is kept but flagged for review
#
# Key class:
#   RuleValidator — main class with the following public methods:
#     - validate(entities)                  run all checks on a list of entity dicts
#     - validate_file(json_path)            load a JSON file and validate it
#     - save_report(report, output_path)    save full validation report as JSON
#     - save_clean_entities(report, path)   save only error-free entities as JSON
#     - print_summary(report)               print human-readable summary to stdout

import json
from collections import defaultdict
from pathlib import Path


# ── Issue model ────────────────────────────────────────────────────────────────

def _issue(entity_id: str, check: str, severity: str, message: str) -> dict:
    return {
        "entity_id": entity_id,
        "check":     check,
        "severity":  severity,
        "message":   message,
    }


def _entity_id(entity: dict) -> str:
    """Return a stable display ID for an entity dict."""
    return entity.get("airline_id") or entity.get("airport_id") or "UNKNOWN"


# ── Validation checks ──────────────────────────────────────────────────────────

def check_entity_structure(entities: list) -> list:
    """Check that required top-level fields are present and non-empty strings."""
    issues = []
    for e in entities:
        eid = _entity_id(e)
        for field in ("name", "source"):
            val = e.get(field)
            if not val or not str(val).strip():
                issues.append(_issue(eid, "entity_structure", "error",
                    f"Missing or empty required field: '{field}'"))
    return issues


def check_entity_type(entities: list) -> list:
    """Check that each entity has exactly one of airline_id or airport_id."""
    issues = []
    for e in entities:
        eid = _entity_id(e)
        has_airline = bool(e.get("airline_id", "").strip() if isinstance(e.get("airline_id"), str) else e.get("airline_id"))
        has_airport = bool(e.get("airport_id", "").strip() if isinstance(e.get("airport_id"), str) else e.get("airport_id"))

        if has_airline and has_airport:
            issues.append(_issue(eid, "entity_type", "error",
                "Entity has both airline_id and airport_id — must have exactly one"))
        elif not has_airline and not has_airport:
            issues.append(_issue(eid, "entity_type", "error",
                "Entity has neither airline_id nor airport_id"))
    return issues


def check_services_present(entities: list) -> list:
    """Check that each entity has a non-empty services array."""
    issues = []
    for e in entities:
        eid = _entity_id(e)
        services = e.get("services")
        if not isinstance(services, list):
            issues.append(_issue(eid, "services_present", "error",
                "Field 'services' is missing or not a list"))
        elif len(services) == 0:
            issues.append(_issue(eid, "services_present", "error",
                "Entity has an empty services array — no PRM services extracted"))
    return issues


def check_service_structure(entities: list) -> list:
    """Check each service dict for required fields and minimum description length."""
    issues = []
    for e in entities:
        eid = _entity_id(e)
        services = e.get("services", [])
        if not isinstance(services, list):
            continue
        for idx, svc in enumerate(services):
            prefix = f"services[{idx}]"
            if not isinstance(svc, dict):
                issues.append(_issue(eid, "service_structure", "error",
                    f"{prefix} is not a dict"))
                continue
            svc_type = str(svc.get("type", "")).strip()
            desc = str(svc.get("description", "")).strip()
            if not svc_type:
                issues.append(_issue(eid, "service_structure", "error",
                    f"{prefix}: 'type' is missing or empty"))
            if not desc:
                issues.append(_issue(eid, "service_structure", "error",
                    f"{prefix}: 'description' is missing or empty"))
            elif len(desc) < 20:
                issues.append(_issue(eid, "service_structure", "error",
                    f"{prefix}: description too short ({len(desc)} chars)"))
            if "is_presented" in svc and not isinstance(svc["is_presented"], bool):
                issues.append(_issue(eid, "service_structure", "warning",
                    f"{prefix}: 'is_presented' should be a boolean"))
    return issues


def check_duplicate_types(entities: list) -> list:
    """Flag duplicate service types within the same entity."""
    issues = []
    for e in entities:
        eid = _entity_id(e)
        services = e.get("services", [])
        if not isinstance(services, list):
            continue
        type_counts: dict[str, int] = defaultdict(int)
        for svc in services:
            if isinstance(svc, dict):
                t = str(svc.get("type", "")).strip().lower()
                if t:
                    type_counts[t] += 1
        for t, count in type_counts.items():
            if count > 1:
                issues.append(_issue(eid, "duplicate_types", "warning",
                    f"Service type '{t}' appears {count} times — consider merging"))
    return issues


# ── Main validator class ───────────────────────────────────────────────────────

class RuleValidator:
    def validate(self, entities: list) -> dict:
        """
        Run all validation checks on a list of entity dicts.

        Returns a report dict with:
          - total:          total entities checked
          - passed:         entities with no errors
          - issues:         list of all issue dicts
          - summary:        counts by check type and severity
          - clean_entities: entities that passed all error-level checks
        """
        if not entities:
            return {"total": 0, "passed": 0, "issues": [], "summary": {}, "clean_entities": []}

        print(f"[Validator] Running checks on {len(entities)} entities...")

        all_issues = []
        all_issues += check_entity_structure(entities)
        all_issues += check_entity_type(entities)
        all_issues += check_services_present(entities)
        all_issues += check_service_structure(entities)
        all_issues += check_duplicate_types(entities)

        summary: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for issue in all_issues:
            summary[issue["check"]][issue["severity"]] += 1

        error_ids = {i["entity_id"] for i in all_issues if i["severity"] == "error"}
        clean_entities = [e for e in entities if _entity_id(e) not in error_ids]
        passed = len(clean_entities)

        print(f"[Validator] {passed}/{len(entities)} entities passed | {len(all_issues)} issues found")

        return {
            "total":          len(entities),
            "passed":         passed,
            "issues":         all_issues,
            "summary":        {k: dict(v) for k, v in summary.items()},
            "clean_entities": clean_entities,
        }

    def validate_file(self, json_path: str) -> dict:
        """Load an entities JSON file and validate it."""
        with open(json_path, "r", encoding="utf-8") as f:
            entities = json.load(f)
        return self.validate(entities)

    def save_report(self, report: dict, output_path: str):
        """Save the full validation report to a JSON file."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"[Validator] Report saved → {output_path}")

    def save_clean_entities(self, report: dict, output_path: str):
        """Save only the entities that passed all error-level checks."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report["clean_entities"], f, indent=2)
        print(f"[Validator] Clean entities saved → {output_path}")

    def print_summary(self, report: dict):
        """Print a human-readable summary to stdout."""
        print("\n" + "=" * 50)
        print("  VALIDATION REPORT")
        print("=" * 50)
        print(f"  Total entities : {report['total']}")
        print(f"  Passed         : {report['passed']}")
        print(f"  Issues found   : {len(report['issues'])}")
        print()

        for check, severities in report["summary"].items():
            print(f"  [{check}]")
            for sev, count in severities.items():
                print(f"    {sev:<10} : {count}")

        if report["issues"]:
            print("\n  ISSUES:")
            for issue in report["issues"]:
                icon = {"error": "✖", "warning": "⚠"}.get(issue["severity"], "-")
                print(f"  {icon} [{issue['entity_id']}] ({issue['check']}) {issue['message']}")

        print("=" * 50 + "\n")
