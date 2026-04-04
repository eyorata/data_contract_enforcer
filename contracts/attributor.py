"""
ViolationAttributor — Phase 2B

Traces validation failures to their origin using:
  1. Contract registry for blast radius (authoritative subscriber list)
  2. Lineage graph traversal for enrichment (transitive contamination)
  3. Git blame for cause attribution

Usage:
  python contracts/attributor.py \
    --report validation_reports/week3_report.json \
    --output violation_log/violations.jsonl
"""

import argparse
import json
import os
import subprocess
import uuid
from collections import deque
from datetime import datetime, timezone

import yaml


def iso_now():
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def load_jsonl(path):
    records = []
    if not os.path.exists(path):
        return records
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def load_registry(path="contract_registry/subscriptions.yaml"):
    """Load the contract registry."""
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("subscriptions", [])


def load_lineage(path="outputs/week4/lineage_snapshots.jsonl"):
    """Load the latest lineage snapshot."""
    records = load_jsonl(path)
    if not records:
        return {"nodes": {}, "edges": [], "adj": {}, "reverse_adj": {}}
    latest = records[-1]
    nodes = {n["node_id"]: n for n in latest.get("nodes", [])}
    edges = latest.get("edges", [])

    adj = {}
    reverse_adj = {}
    for edge in edges:
        src = edge["source"]
        tgt = edge["target"]
        adj.setdefault(src, []).append((tgt, edge))
        reverse_adj.setdefault(tgt, []).append((src, edge))

    return {
        "nodes": nodes,
        "edges": edges,
        "adj": adj,
        "reverse_adj": reverse_adj,
    }


# ── Step 1: Registry Blast Radius ────────────────────────────────────────────

def registry_blast_radius(contract_id, failing_field, subscriptions):
    """Find all subscribers affected by a failing field."""
    affected = []
    for sub in subscriptions:
        if sub.get("contract_id") != contract_id:
            continue
        # Check if the failing field is in breaking_fields
        is_breaking = False
        for bf in sub.get("breaking_fields", []):
            if bf.get("field") == failing_field:
                is_breaking = True
                break
        # Also check fields_consumed
        consumed = sub.get("fields_consumed", [])
        base_field = failing_field.split("[")[0].split(".")[0]
        is_consumed = (
            failing_field in consumed
            or base_field in consumed
        )

        if is_breaking or is_consumed:
            affected.append({
                "subscriber_id": sub["subscriber_id"],
                "subscriber_team": sub.get("subscriber_team", "unknown"),
                "is_breaking": is_breaking,
                "validation_mode": sub.get("validation_mode", "AUDIT"),
                "contact": sub.get("contact", ""),
            })
    return affected


# ── Step 2: Lineage Traversal ────────────────────────────────────────────────

def find_upstream_producers(contract_id, lineage):
    """BFS upstream from the contract's file node to find producers."""
    # Map contract_id to likely file node
    file_map = {
        "week3-document-refinery-extractions": "file::outputs/week3/extractions.jsonl",
        "week5-event-sourcing-events": "file::outputs/week5/events.jsonl",
        "week4-cartographer-lineage": "file::outputs/week4/lineage_snapshots.jsonl",
        "week1-intent-classifier-records": "file::outputs/week1/intent_records.jsonl",
        "langsmith-traces": "file::outputs/traces/runs.jsonl",
    }
    start_node = file_map.get(contract_id)
    if not start_node:
        return []

    reverse_adj = lineage["reverse_adj"]
    nodes = lineage["nodes"]
    visited = set()
    queue = deque([(start_node, 0)])
    producers = []

    while queue:
        node, depth = queue.popleft()
        if node in visited:
            continue
        visited.add(node)

        node_info = nodes.get(node, {})
        if node_info.get("type") == "pipeline":
            meta = node_info.get("metadata", {})
            producers.append({
                "node_id": node,
                "file_path": meta.get("path", ""),
                "depth": depth,
            })

        # Traverse upstream (reverse edges)
        for upstream, edge in reverse_adj.get(node, []):
            if upstream not in visited:
                queue.append((upstream, depth + 1))

    return producers


def transitive_contamination(affected_subscribers, lineage):
    """Find transitive consumers from affected subscribers."""
    adj = lineage["adj"]
    nodes = lineage["nodes"]
    contaminated = []

    for sub in affected_subscribers:
        sub_id = sub["subscriber_id"]
        # Find matching node
        node_key = None
        for nid in nodes:
            if sub_id in nid:
                node_key = nid
                break
        if not node_key:
            continue

        # BFS forward from subscriber
        visited = set()
        queue = deque([(node_key, 0)])
        while queue:
            current, depth = queue.popleft()
            if current in visited or depth == 0:
                if depth == 0:
                    visited.add(current)
                    for downstream, _ in adj.get(current, []):
                        queue.append((downstream, depth + 1))
                continue
            visited.add(current)
            contaminated.append({
                "node_id": current,
                "contamination_depth": depth,
                "source_subscriber": sub_id,
            })
            for downstream, _ in adj.get(current, []):
                if downstream not in visited:
                    queue.append((downstream, depth + 1))

    return contaminated


# ── Step 3: Git Blame ────────────────────────────────────────────────────────

def git_blame_file(file_path, days=14, max_candidates=5):
    """Run git log and blame on a file to find recent changes."""
    candidates = []

    try:
        result = subprocess.run(
            [
                "git", "log", "--follow",
                f"--since={days} days ago",
                "--format=%H|%an|%ae|%ai|%s",
                "--", file_path,
            ],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            for line in result.stdout.strip().split("\n"):
                parts = line.split("|", 4)
                if len(parts) == 5:
                    commit_hash, author, email, ts, msg = parts
                    # Compute confidence score
                    try:
                        commit_dt = datetime.fromisoformat(
                            ts.strip().replace(" ", "T")
                        )
                        if commit_dt.tzinfo is None:
                            commit_dt = commit_dt.replace(
                                tzinfo=timezone.utc
                            )
                        now = datetime.now(timezone.utc)
                        days_ago = (now - commit_dt).days
                    except Exception:
                        days_ago = 7
                    confidence = max(
                        0.1, 1.0 - (days_ago * 0.1)
                    )
                    candidates.append({
                        "commit_hash": commit_hash,
                        "author": email.strip(),
                        "commit_timestamp": ts.strip(),
                        "commit_message": msg.strip(),
                        "confidence_score": round(confidence, 2),
                    })
    except Exception:
        pass

    # Rank by confidence (recency)
    candidates.sort(
        key=lambda x: x["confidence_score"], reverse=True
    )
    return candidates[:max_candidates] if candidates else [{
        "commit_hash": "unknown",
        "author": "unknown",
        "commit_timestamp": iso_now(),
        "commit_message": "no recent commits found",
        "confidence_score": 0.1,
    }]


# ── Main Attribution Pipeline ────────────────────────────────────────────────

def attribute_violation(failure, contract_id, subscriptions, lineage):
    """Full attribution for a single check failure."""
    check_id = failure.get("check_id", "unknown")
    column = failure.get("column_name", "")
    failing_field = column.split("[")[0] if column else ""

    # Step 1: Registry blast radius
    affected = registry_blast_radius(
        contract_id, column, subscriptions
    )
    if not affected:
        affected = registry_blast_radius(
            contract_id, failing_field, subscriptions
        )

    # Step 2: Lineage traversal for upstream producers
    producers = find_upstream_producers(contract_id, lineage)

    # Transitive contamination from affected subscribers
    contamination = transitive_contamination(affected, lineage)

    # Step 3: Git blame for each upstream producer
    blame_chain = []
    for rank, producer in enumerate(producers[:5], start=1):
        fp = producer["file_path"]
        candidates = git_blame_file(fp)
        for cand in candidates[:1]:  # top candidate per file
            conf = cand["confidence_score"]
            # Reduce by 0.2 per lineage hop
            conf = max(0.1, conf - (producer["depth"] * 0.2))
            blame_chain.append({
                "rank": rank,
                "file_path": fp,
                "commit_hash": cand["commit_hash"],
                "author": cand["author"],
                "commit_timestamp": cand["commit_timestamp"],
                "commit_message": cand["commit_message"],
                "confidence_score": round(conf, 2),
            })

    if not blame_chain:
        blame_chain.append({
            "rank": 1,
            "file_path": "unknown",
            "commit_hash": "unknown",
            "author": "unknown",
            "commit_timestamp": iso_now(),
            "commit_message": "no upstream producers found in lineage",
            "confidence_score": 0.1,
        })

    # Build violation record
    violation = {
        "violation_id": uuid.uuid4().hex,
        "check_id": check_id,
        "contract_id": contract_id,
        "column_name": column,
        "severity": failure.get("severity", "UNKNOWN"),
        "detected_at": iso_now(),
        "blame_chain": blame_chain,
        "blast_radius": {
            "registry_subscribers": affected,
            "affected_nodes": [
                p["node_id"] for p in producers
            ],
            "affected_pipelines": [
                p["file_path"] for p in producers if p["file_path"]
            ],
            "estimated_records": failure.get("records_failing", 0),
            "transitive_contamination": contamination,
        },
    }
    return violation


def main():
    parser = argparse.ArgumentParser(
        description="Attribute validation failures to their origin."
    )
    parser.add_argument(
        "--report", required=False,
        help="Path to validation report JSON."
    )
    parser.add_argument(
        "--violation", required=False,
        help="Alias for --report (compatibility with checklist)."
    )
    parser.add_argument(
        "--lineage", default="outputs/week4/lineage_snapshots.jsonl",
        help="Path to lineage snapshots JSONL."
    )
    parser.add_argument(
        "--registry", default="contract_registry/subscriptions.yaml",
        help="Path to registry file."
    )
    parser.add_argument(
        "--output", default="violation_log/violations.jsonl",
        help="Output path for violation log."
    )
    args = parser.parse_args()

    report_path = args.report or args.violation
    if not report_path:
        raise SystemExit("Missing --report (or --violation) path.")

    with open(report_path, "r", encoding="utf-8") as f:
        report = json.load(f)

    contract_id = report.get("contract_id", "unknown")
    failures = [
        r for r in report.get("results", [])
        if r.get("status") == "FAIL"
    ]

    if not failures:
        print("No failures to attribute.")
        return

    print(f"Attributing {len(failures)} failure(s) "
          f"for contract: {contract_id}")

    # Load registry and lineage
    subscriptions = load_registry(args.registry)
    lineage = load_lineage(args.lineage)
    print(f"  Registry: {len(subscriptions)} subscriptions")
    print(f"  Lineage: {len(lineage['nodes'])} nodes, "
          f"{len(lineage['edges'])} edges")

    # Process each failure
    violations = []
    for failure in failures:
        v = attribute_violation(
            failure, contract_id, subscriptions, lineage
        )
        violations.append(v)
        print(f"  Attributed: {v['check_id']} -> "
              f"{len(v['blame_chain'])} blame candidates, "
              f"{len(v['blast_radius']['registry_subscribers'])} "
              f"affected subscribers")

    # Write violation log
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "a", encoding="utf-8") as f:
        for v in violations:
            f.write(json.dumps(v) + "\n")

    print(f"\nWrote {len(violations)} violation(s) to {args.output}")


if __name__ == "__main__":
    main()
