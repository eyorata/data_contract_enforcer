"""
SchemaEvolutionAnalyzer — Phase 3

Diffs consecutive schema snapshots to detect and classify changes.
Produces migration impact reports for breaking changes.

Usage:
  python contracts/schema_analyzer.py \
    --contract-id week3-document-refinery-extractions \
    --since "7 days ago" \
    --output validation_reports/schema_evolution_week3.json
"""

import argparse
import json
import os
from datetime import datetime, timezone, timedelta

import yaml


def iso_now():
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def load_snapshots(contract_id, base_dir="schema_snapshots"):
    """Load all schema snapshots for a contract, sorted by timestamp."""
    snap_dir = os.path.join(base_dir, contract_id)
    if not os.path.exists(snap_dir):
        return []
    snapshots = []
    for fname in sorted(os.listdir(snap_dir)):
        if fname.endswith(".yaml"):
            fpath = os.path.join(snap_dir, fname)
            with open(fpath, "r", encoding="utf-8") as f:
                schema = yaml.safe_load(f)
            ts = fname.replace(".yaml", "")
            snapshots.append({"timestamp": ts, "path": fpath, "schema": schema})
    return snapshots


def parse_snapshot_ts(ts):
    """Parse snapshot timestamp like 20260404T072738Z."""
    try:
        return datetime.strptime(ts, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def parse_since(since):
    """Parse --since values like '7 days ago' or ISO date."""
    s = since.strip().lower()
    if "day" in s:
        try:
            days = int(s.split()[0])
            return datetime.now(timezone.utc) - timedelta(days=days)
        except Exception:
            return None
    try:
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def load_registry(path="contract_registry/subscriptions.yaml"):
    """Load the contract registry for blast radius."""
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("subscriptions", [])


# ── Change Classification ────────────────────────────────────────────────────

CHANGE_TYPES = {
    "add_nullable_field": {
        "compatible": True,
        "action": "None. Consumers can ignore new fields.",
    },
    "add_required_field": {
        "compatible": False,
        "action": (
            "Coordinate with all producers. "
            "Provide default or migration script."
        ),
    },
    "remove_field": {
        "compatible": False,
        "action": (
            "Two-sprint deprecation minimum. "
            "Each subscriber must acknowledge removal."
        ),
    },
    "rename_field": {
        "compatible": False,
        "action": (
            "Deprecation period with alias. "
            "Notify all registry subscribers."
        ),
    },
    "widen_type": {
        "compatible": True,
        "action": (
            "Validate no precision loss. "
            "Re-run statistical checks."
        ),
    },
    "narrow_type": {
        "compatible": False,
        "action": (
            "CRITICAL. Requires migration plan with rollback. "
            "Statistical baseline must be re-established."
        ),
    },
    "change_enum_add": {
        "compatible": True,
        "action": "Notify subscribers. Additive change is safe.",
    },
    "change_enum_remove": {
        "compatible": False,
        "action": (
            "Treat as breaking. "
            "Blast radius required before deployment."
        ),
    },
    "change_constraint": {
        "compatible": False,
        "action": (
            "Evaluate impact on downstream validation. "
            "Notify affected subscribers."
        ),
    },
}


def classify_field_change(field_name, old_def, new_def):
    """Classify a single field change."""
    changes = []

    if old_def is None and new_def is not None:
        required = False
        if isinstance(new_def, dict):
            required = new_def.get("required", False)
        change_type = (
            "add_required_field" if required
            else "add_nullable_field"
        )
        changes.append({
            "field": field_name,
            "change_type": change_type,
            "old": None,
            "new": new_def,
            **CHANGE_TYPES[change_type],
        })
        return changes

    if old_def is not None and new_def is None:
        changes.append({
            "field": field_name,
            "change_type": "remove_field",
            "old": old_def,
            "new": None,
            **CHANGE_TYPES["remove_field"],
        })
        return changes

    if not isinstance(old_def, dict) or not isinstance(new_def, dict):
        if old_def != new_def:
            changes.append({
                "field": field_name,
                "change_type": "change_constraint",
                "old": old_def,
                "new": new_def,
                **CHANGE_TYPES["change_constraint"],
            })
        return changes

    # Type changes
    old_type = old_def.get("type")
    new_type = new_def.get("type")
    if old_type and new_type and old_type != new_type:
        type_widening = {
            ("integer", "number"): True,
            ("int", "float"): True,
            ("string", "string"): True,
        }
        is_widen = type_widening.get(
            (old_type, new_type), False
        )
        change_type = "widen_type" if is_widen else "narrow_type"
        change = {
            "field": field_name,
            "change_type": change_type,
            "old": {"type": old_type},
            "new": {"type": new_type},
            **CHANGE_TYPES[change_type],
        }
        # Explicitly flag narrow numeric scale change as CRITICAL breaking
        if change_type == "narrow_type" and old_type in ("number", "float") and new_type in ("integer", "int"):
            change["severity"] = "CRITICAL"
            change["reason"] = "Numeric scale/type narrowed (e.g., 0.0–1.0 to 0–100) — breaking."
        changes.append(change)

    # Enum changes
    old_enum = set(old_def.get("enum", []))
    new_enum = set(new_def.get("enum", []))
    if old_enum or new_enum:
        added = new_enum - old_enum
        removed = old_enum - new_enum
        if added:
            changes.append({
                "field": field_name,
                "change_type": "change_enum_add",
                "old": sorted(old_enum),
                "new": sorted(new_enum),
                "added_values": sorted(added),
                **CHANGE_TYPES["change_enum_add"],
            })
        if removed:
            changes.append({
                "field": field_name,
                "change_type": "change_enum_remove",
                "old": sorted(old_enum),
                "new": sorted(new_enum),
                "removed_values": sorted(removed),
                **CHANGE_TYPES["change_enum_remove"],
            })

    # Pattern changes
    if old_def.get("pattern") != new_def.get("pattern"):
        if old_def.get("pattern") and new_def.get("pattern"):
            changes.append({
                "field": field_name,
                "change_type": "change_constraint",
                "old": {"pattern": old_def.get("pattern")},
                "new": {"pattern": new_def.get("pattern")},
                **CHANGE_TYPES["change_constraint"],
            })

    # Required changes
    if old_def.get("required") != new_def.get("required"):
        if new_def.get("required") and not old_def.get("required"):
            changes.append({
                "field": field_name,
                "change_type": "add_required_field",
                "old": {"required": False},
                "new": {"required": True},
                **CHANGE_TYPES["add_required_field"],
            })

    # Range constraint changes (explicitly flag scale shifts as CRITICAL)
    for bound in ("minimum", "maximum"):
        old_bound = old_def.get(bound)
        new_bound = new_def.get(bound)
        if old_bound != new_bound:
            change = {
                "field": field_name,
                "change_type": "change_constraint",
                "old": {bound: old_bound},
                "new": {bound: new_bound},
                **CHANGE_TYPES["change_constraint"],
            }
            # Explicit scale change detection (0.0–1.0 -> 0–100)
            if bound == "maximum":
                try:
                    if old_bound is not None and new_bound is not None:
                        if float(old_bound) <= 1.0 and float(new_bound) >= 10.0:
                            change["severity"] = "CRITICAL"
                            change["reason"] = "Confidence scale change detected (0.0–1.0 -> 0–100)."
                except Exception:
                    pass
            changes.append(change)

    return changes


def diff_schemas(old_schema, new_schema):
    """Diff two schema snapshots and classify all changes."""
    all_changes = []
    old_fields = set(old_schema.keys()) if old_schema else set()
    new_fields = set(new_schema.keys()) if new_schema else set()

    for field in sorted(old_fields | new_fields):
        old_def = old_schema.get(field) if old_schema else None
        new_def = new_schema.get(field) if new_schema else None
        changes = classify_field_change(field, old_def, new_def)
        all_changes.extend(changes)

    return all_changes


# ── Migration Impact Report ──────────────────────────────────────────────────

def generate_migration_impact(
    contract_id, changes, subscriptions
):
    """Generate a migration impact report for breaking changes."""
    breaking = [c for c in changes if not c.get("compatible")]
    if not breaking:
        return None

    # Find affected subscribers
    affected_subs = []
    failure_modes = []
    for sub in subscriptions:
        if sub.get("contract_id") != contract_id:
            continue
        affected_fields = []
        breaking_map = {
            bf["field"]: bf.get("reason", "breaking field")
            for bf in sub.get("breaking_fields", [])
        }
        for change in breaking:
            field = change["field"]
            consumed = sub.get("fields_consumed", [])
            breaking_fields = [
                bf["field"]
                for bf in sub.get("breaking_fields", [])
            ]
            if field in consumed or field in breaking_fields:
                affected_fields.append(field)
                failure_modes.append({
                    "subscriber_id": sub["subscriber_id"],
                    "field": field,
                    "failure_mode": breaking_map.get(field, "schema change may break consumer logic"),
                })
        if affected_fields:
            affected_subs.append({
                "subscriber_id": sub["subscriber_id"],
                "subscriber_team": sub.get("subscriber_team"),
                "affected_fields": affected_fields,
                "validation_mode": sub.get("validation_mode"),
                "contact": sub.get("contact"),
            })

    # Build migration checklist
    checklist = []
    for i, change in enumerate(breaking, 1):
        checklist.append({
            "step": i,
            "action": change.get("action", "Review required"),
            "field": change["field"],
            "change_type": change["change_type"],
        })
    checklist.append({
        "step": len(breaking) + 1,
        "action": "Notify all affected subscribers",
        "field": "all",
        "change_type": "notification",
    })
    checklist.append({
        "step": len(breaking) + 2,
        "action": "Re-run validation after migration",
        "field": "all",
        "change_type": "verification",
    })

    return {
        "contract_id": contract_id,
        "generated_at": iso_now(),
        "total_changes": len(changes),
        "breaking_changes": len(breaking),
        "compatible_changes": len(changes) - len(breaking),
        "compatibility_verdict": (
            "BREAKING" if breaking else "COMPATIBLE"
        ),
        "changes": changes,
        "blast_radius": {
            "affected_subscribers": affected_subs,
            "subscriber_count": len(affected_subs),
        },
        "consumer_failure_modes": failure_modes,
        "migration_checklist": checklist,
        "rollback_plan": (
            "Revert to previous schema snapshot. "
            "Restore baseline in schema_snapshots/baselines.json. "
            "Notify all subscribers of rollback."
        ),
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Analyze schema evolution between snapshots."
    )
    parser.add_argument(
        "--contract-id", required=True,
        help="Contract ID to analyze."
    )
    parser.add_argument(
        "--since", default="7 days ago",
        help="How far back to look for changes."
    )
    parser.add_argument(
        "--output",
        help="Output path for evolution report JSON."
    )
    args = parser.parse_args()

    contract_id = args.contract_id
    print(f"Analyzing schema evolution for: {contract_id}")

    snapshots = load_snapshots(contract_id)
    cutoff = parse_since(args.since)
    if cutoff:
        snapshots = [
            s for s in snapshots
            if parse_snapshot_ts(s["timestamp"]) and parse_snapshot_ts(s["timestamp"]) >= cutoff
        ]
    if len(snapshots) < 2:
        print(
            f"  Only {len(snapshots)} snapshot(s) found. "
            f"Need at least 2 to detect changes."
        )
        if len(snapshots) == 1:
            print(
                "  Run the generator again to create a "
                "second snapshot for comparison."
            )
        # Write a no-change report
        report = {
            "contract_id": contract_id,
            "generated_at": iso_now(),
            "snapshots_analyzed": len(snapshots),
            "total_changes": 0,
            "breaking_changes": 0,
            "compatible_changes": 0,
            "compatibility_verdict": "NO_CHANGES",
            "changes": [],
            "message": (
                "Insufficient snapshots for comparison. "
                "Run generator twice to enable evolution analysis."
            ),
        }
    else:
        # Compare consecutive snapshot pairs
        all_changes = []
        for i in range(len(snapshots) - 1):
            old_snap = snapshots[i]
            new_snap = snapshots[i + 1]
            changes = diff_schemas(
                old_snap["schema"], new_snap["schema"]
            )
            for c in changes:
                c["from_snapshot"] = old_snap["timestamp"]
                c["to_snapshot"] = new_snap["timestamp"]
            all_changes.extend(changes)

        print(f"  Compared {len(snapshots)} snapshots: "
              f"{len(all_changes)} change(s) detected")

        # Generate migration impact if breaking
        subscriptions = load_registry()
        impact = generate_migration_impact(
            contract_id, all_changes, subscriptions
        )

        if impact:
            report = impact
            # Write migration impact report separately
            impact_path = os.path.join(
                "validation_reports",
                f"migration_impact_{contract_id}_"
                f"{datetime.now(timezone.utc).strftime('%Y%m%d')}.json",
            )
            os.makedirs(
                os.path.dirname(impact_path), exist_ok=True
            )
            with open(impact_path, "w", encoding="utf-8") as f:
                json.dump(impact, f, indent=2)
            print(f"  Migration impact: {impact_path}")
        else:
            report = {
                "contract_id": contract_id,
                "generated_at": iso_now(),
                "snapshots_analyzed": len(snapshots),
                "total_changes": len(all_changes),
                "breaking_changes": 0,
                "compatible_changes": len(all_changes),
                "compatibility_verdict": "COMPATIBLE",
                "changes": all_changes,
            }

    # Write evolution report
    output_path = args.output
    if not output_path:
        output_path = os.path.join(
            "validation_reports",
            f"schema_evolution_{contract_id}.json",
        )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"  Report: {output_path}")


if __name__ == "__main__":
    main()
