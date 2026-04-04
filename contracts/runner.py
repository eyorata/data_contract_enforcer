"""
ValidationRunner — Phase 2A

Executes every clause in a contract YAML against a data snapshot.
Includes statistical drift detection using stored baselines.

Usage:
  python contracts/runner.py \
    --contract generated_contracts/week3_extractions.yaml \
    --data outputs/week3/extractions.jsonl \
    --output validation_reports/week3_report.json
"""

import argparse
import hashlib
import json
import os
import re
import statistics
from datetime import datetime, timezone

import yaml


BASELINES_PATH = "schema_snapshots/baselines.json"


def load_jsonl(path):
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def iso_now():
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_iso(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    s = value
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def extract_path_values(record, path):
    parts = path.split(".")
    values = [record]
    for part in parts:
        if part.endswith("[*]"):
            key = part[:-3]
            next_values = []
            for v in values:
                if isinstance(v, dict) and key in v and isinstance(v[key], list):
                    next_values.extend(v[key])
                else:
                    next_values.append(None)
            values = next_values
        else:
            next_values = []
            for v in values:
                if isinstance(v, dict) and part in v:
                    next_values.append(v[part])
                else:
                    next_values.append(None)
            values = next_values
    return values


def column_exists(record, path):
    parts = path.split(".")
    current = record
    for part in parts:
        if part.endswith("[*]"):
            key = part[:-3]
            if not isinstance(current, dict) or key not in current:
                return False
            if not isinstance(current[key], list) or not current[key]:
                return True
            current = current[key][0]
        else:
            if not isinstance(current, dict) or part not in current:
                return False
            current = current[part]
    return True


def result_template(check, status, actual, expected,
                    failing_count, samples, message):
    return {
        "check_id": check.get("id"),
        "column_name": check.get("column"),
        "check_type": check.get("type"),
        "status": status,
        "actual_value": actual,
        "expected": expected,
        "severity": check.get("severity", "LOW"),
        "records_failing": failing_count,
        "sample_failing": samples,
        "message": message,
    }


# ── Check implementations ────────────────────────────────────────────────────

def run_required(check, records, record_ids):
    path = check["column"]
    failing = []
    for rec, rec_id in zip(records, record_ids):
        vals = extract_path_values(rec, path)
        if all(v is None or v == "" for v in vals):
            failing.append(rec_id)
    status = "PASS" if not failing else "FAIL"
    return result_template(
        check, status, f"missing={len(failing)}",
        "required", len(failing), failing[:5], "value is required"
    )


def run_regex(check, records, record_ids):
    path = check["column"]
    pattern = re.compile(check["pattern"])
    failing = []
    for rec, rec_id in zip(records, record_ids):
        vals = extract_path_values(rec, path)
        for v in vals:
            if v is None:
                failing.append(rec_id)
                break
            if not isinstance(v, str) or not pattern.match(v):
                failing.append(rec_id)
                break
    status = "PASS" if not failing else "FAIL"
    return result_template(
        check, status, f"invalid={len(failing)}",
        f"pattern={check['pattern']}",
        len(failing), failing[:5], "value does not match pattern"
    )


def run_enum(check, records, record_ids):
    allowed = set(check.get("allowed", []))
    path = check["column"]
    failing = []
    for rec, rec_id in zip(records, record_ids):
        vals = extract_path_values(rec, path)
        for v in vals:
            if v is None or v not in allowed:
                failing.append(rec_id)
                break
    status = "PASS" if not failing else "FAIL"
    return result_template(
        check, status, f"invalid={len(failing)}",
        f"allowed={sorted(allowed)}",
        len(failing), failing[:5], "value not in allowed set"
    )


def run_range(check, records, record_ids):
    path = check["column"]
    min_v = check.get("min")
    max_v = check.get("max")
    failing = []
    observed = []
    for rec, rec_id in zip(records, record_ids):
        vals = extract_path_values(rec, path)
        for v in vals:
            if v is None:
                failing.append(rec_id)
                break
            if not isinstance(v, (int, float)):
                failing.append(rec_id)
                break
            observed.append(float(v))
            if min_v is not None and v < min_v:
                failing.append(rec_id)
                break
            if max_v is not None and v > max_v:
                failing.append(rec_id)
                break
    status = "PASS" if not failing else "FAIL"
    actual = "no_values"
    if observed:
        actual = f"min={min(observed)}, max={max(observed)}"
    if max_v is None:
        expected = f"min>={min_v}"
    else:
        expected = f"min>={min_v}, max<={max_v}"
    return result_template(
        check, status, actual, expected,
        len(failing), failing[:5], "value out of range"
    )


def run_min_items(check, records, record_ids):
    path = check["column"]
    min_items = check.get("min_items", 1)
    failing = []
    for rec, rec_id in zip(records, record_ids):
        vals = extract_path_values(rec, path)
        ok = False
        for v in vals:
            if isinstance(v, list) and len(v) >= min_items:
                ok = True
                break
        if not ok:
            failing.append(rec_id)
    status = "PASS" if not failing else "FAIL"
    return result_template(
        check, status, f"invalid={len(failing)}",
        f"min_items>={min_items}",
        len(failing), failing[:5], "array has too few items"
    )


def run_iso8601(check, records, record_ids):
    path = check["column"]
    failing = []
    for rec, rec_id in zip(records, record_ids):
        vals = extract_path_values(rec, path)
        for v in vals:
            if parse_iso(v) is None:
                failing.append(rec_id)
                break
    status = "PASS" if not failing else "FAIL"
    return result_template(
        check, status, f"invalid={len(failing)}",
        "ISO-8601", len(failing), failing[:5], "invalid timestamp"
    )


def run_gte_field(check, records, record_ids):
    path = check["column"]
    other = check["other_column"]
    failing = []
    for rec, rec_id in zip(records, record_ids):
        a_vals = extract_path_values(rec, path)
        b_vals = extract_path_values(rec, other)
        a = parse_iso(a_vals[0]) if a_vals else None
        b = parse_iso(b_vals[0]) if b_vals else None
        if a is None or b is None or a < b:
            failing.append(rec_id)
    status = "PASS" if not failing else "FAIL"
    return result_template(
        check, status, f"invalid={len(failing)}",
        f"{path}>={other}",
        len(failing), failing[:5],
        "recorded_at is before occurred_at"
    )


def run_type(check, records, record_ids):
    path = check["column"]
    expected = check.get("expected_type")
    failing = []
    for rec, rec_id in zip(records, record_ids):
        vals = extract_path_values(rec, path)
        v = vals[0] if vals else None
        ok = False
        if expected == "object":
            ok = isinstance(v, dict)
        elif expected == "array":
            ok = isinstance(v, list)
        if not ok:
            failing.append(rec_id)
    status = "PASS" if not failing else "FAIL"
    return result_template(
        check, status, f"invalid={len(failing)}",
        f"type={expected}",
        len(failing), failing[:5], "type mismatch"
    )


def run_relation_in_set(check, records, record_ids):
    path = check["column"]
    set_path = check["set_column"]
    failing = []
    for rec, rec_id in zip(records, record_ids):
        refs = extract_path_values(rec, path)
        refs_flat = []
        for v in refs:
            if isinstance(v, list):
                refs_flat.extend(v)
            elif v is not None:
                refs_flat.append(v)
        valid_set = set(extract_path_values(rec, set_path))
        valid_set.discard(None)
        if any(r not in valid_set for r in refs_flat):
            failing.append(rec_id)
    status = "PASS" if not failing else "FAIL"
    return result_template(
        check, status, f"invalid={len(failing)}",
        f"{path} subset of {set_path}",
        len(failing), failing[:5],
        "entity_refs not found in entities"
    )


def run_monotonic_sequence(check, records, record_ids):
    group_by = check["group_by"]
    path = check["column"]
    by_group = {}
    for rec, rec_id in zip(records, record_ids):
        g = extract_path_values(rec, group_by)[0]
        v = extract_path_values(rec, path)[0]
        by_group.setdefault(g, []).append((rec_id, v))

    failing = []
    for _, seqs in by_group.items():
        values = [v for _, v in seqs]
        if not values:
            continue
        start = min(values)
        expected = list(range(start, start + len(values)))
        if values != expected:
            for rec_id, _ in seqs:
                failing.append(rec_id)
    status = "PASS" if not failing else "FAIL"
    return result_template(
        check, status,
        f"invalid_groups={len(set(failing))}",
        "monotonic no gaps",
        len(failing), failing[:5],
        "sequence numbers are not strictly increasing"
    )


def run_token_sum_check(check, records, record_ids):
    """Check total_tokens == prompt_tokens + completion_tokens."""
    col = check["column"]
    prompt_col = check.get("prompt_col", "prompt_tokens")
    completion_col = check.get("completion_col", "completion_tokens")
    failing = []
    for rec, rec_id in zip(records, record_ids):
        total = extract_path_values(rec, col)
        prompt = extract_path_values(rec, prompt_col)
        completion = extract_path_values(rec, completion_col)
        t = total[0] if total else None
        p = prompt[0] if prompt else None
        c = completion[0] if completion else None
        if t is not None and p is not None and c is not None:
            if t != p + c:
                failing.append(rec_id)
        elif t is not None:
            failing.append(rec_id)
    status = "PASS" if not failing else "FAIL"
    return result_template(
        check, status, f"invalid={len(failing)}",
        "total = prompt + completion",
        len(failing), failing[:5],
        "token sum mismatch"
    )


def run_error_output_check(check, records, record_ids):
    """If error is not null, outputs should be empty/null."""
    col = check["column"]
    output_col = check.get("output_col", "outputs")
    failing = []
    for rec, rec_id in zip(records, record_ids):
        error = extract_path_values(rec, col)
        outputs = extract_path_values(rec, output_col)
        e = error[0] if error else None
        o = outputs[0] if outputs else None
        if e is not None and o is not None and o != {} and o != []:
            failing.append(rec_id)
    status = "PASS" if not failing else "FAIL"
    return result_template(
        check, status, f"invalid={len(failing)}",
        "error implies no output",
        len(failing), failing[:5],
        "error present but outputs not empty"
    )


def run_custom_acyclic(check, records, record_ids):
    """Check that the graph defined by edges is acyclic."""
    failing = []
    for rec, rec_id in zip(records, record_ids):
        edges = extract_path_values(rec, "edges")
        edge_list = edges[0] if edges and isinstance(edges[0], list) else edges
        if not edge_list or not isinstance(edge_list, list):
            continue
        # Build adjacency and detect cycle via DFS
        adj = {}
        for e in edge_list:
            if isinstance(e, dict):
                s = e.get("source", "")
                t = e.get("target", "")
                adj.setdefault(s, []).append(t)
        visited = set()
        in_stack = set()
        has_cycle = False

        def dfs(node):
            nonlocal has_cycle
            if has_cycle:
                return
            visited.add(node)
            in_stack.add(node)
            for nb in adj.get(node, []):
                if nb in in_stack:
                    has_cycle = True
                    return
                if nb not in visited:
                    dfs(nb)
            in_stack.discard(node)

        for node in adj:
            if node not in visited:
                dfs(node)
        if has_cycle:
            failing.append(rec_id)
    status = "PASS" if not failing else "FAIL"
    return result_template(
        check, status, f"cyclic_graphs={len(failing)}",
        "acyclic graph",
        len(failing), failing[:5],
        "graph contains cycles"
    )


# ── Statistical Drift Detection ──────────────────────────────────────────────

def load_baselines():
    if os.path.exists(BASELINES_PATH):
        with open(BASELINES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_baselines(baselines):
    os.makedirs(os.path.dirname(BASELINES_PATH), exist_ok=True)
    with open(BASELINES_PATH, "w", encoding="utf-8") as f:
        json.dump(baselines, f, indent=2)


def run_statistical_drift(contract_id, records, checks):
    """For every numeric column with a baseline, check for drift."""
    baselines = load_baselines()
    contract_baselines = baselines.get(contract_id, {})
    drift_results = []

    # Find numeric columns from checks
    numeric_paths = []
    for check in checks:
        if check.get("type") == "range":
            numeric_paths.append(check["column"])

    for path in numeric_paths:
        values = []
        for rec in records:
            vals = extract_path_values(rec, path)
            for v in vals:
                if isinstance(v, (int, float)):
                    values.append(float(v))

        if not values:
            continue

        current_mean = statistics.mean(values)
        current_stddev = statistics.stdev(values) if len(values) > 1 else 0

        baseline_key = path
        if baseline_key not in contract_baselines:
            # First run — establish baseline
            contract_baselines[baseline_key] = {
                "mean": current_mean,
                "stddev": current_stddev,
                "count": len(values),
                "established_at": iso_now(),
            }
            drift_results.append({
                "check_id": f"{contract_id}.{path}.drift",
                "column_name": path,
                "check_type": "statistical_drift",
                "status": "BASELINE_SET",
                "actual_value": (
                    f"mean={round(current_mean, 4)}, "
                    f"stddev={round(current_stddev, 4)}"
                ),
                "expected": "baseline established",
                "severity": "LOW",
                "records_failing": 0,
                "sample_failing": [],
                "message": "Baseline established for drift detection",
            })
        else:
            bl = contract_baselines[baseline_key]
            bl_mean = bl["mean"]
            bl_stddev = bl["stddev"]
            if bl_stddev == 0:
                bl_stddev = 0.001  # avoid division by zero

            deviation = abs(current_mean - bl_mean) / bl_stddev
            if deviation > 3:
                status = "FAIL"
                severity = "HIGH"
                msg = (
                    f"Statistical drift CRITICAL: mean shifted "
                    f"{round(deviation, 2)} stddev from baseline"
                )
            elif deviation > 2:
                status = "WARN"
                severity = "MEDIUM"
                msg = (
                    f"Statistical drift WARNING: mean shifted "
                    f"{round(deviation, 2)} stddev from baseline"
                )
            else:
                status = "PASS"
                severity = "LOW"
                msg = "Within baseline tolerance"

            drift_results.append({
                "check_id": f"{contract_id}.{path}.drift",
                "column_name": path,
                "check_type": "statistical_drift",
                "status": status,
                "actual_value": (
                    f"mean={round(current_mean, 4)}, "
                    f"stddev={round(current_stddev, 4)}, "
                    f"deviation={round(deviation, 2)}"
                ),
                "expected": (
                    f"baseline_mean={round(bl_mean, 4)}, "
                    f"baseline_stddev={round(bl_stddev, 4)}"
                ),
                "severity": severity,
                "records_failing": 0,
                "sample_failing": [],
                "message": msg,
            })

    # Save updated baselines
    baselines[contract_id] = contract_baselines
    save_baselines(baselines)

    return drift_results


# ── Check dispatcher ─────────────────────────────────────────────────────────

def run_check(check, records, record_ids):
    check_type = check.get("type")
    dispatch = {
        "required": run_required,
        "regex": run_regex,
        "enum": run_enum,
        "range": run_range,
        "min_items": run_min_items,
        "iso8601": run_iso8601,
        "gte_field": run_gte_field,
        "type": run_type,
        "relation_in_set": run_relation_in_set,
        "monotonic_sequence": run_monotonic_sequence,
        "token_sum_check": run_token_sum_check,
        "error_output_check": run_error_output_check,
        "custom_acyclic": run_custom_acyclic,
    }
    handler = dispatch.get(check_type)
    if handler:
        return handler(check, records, record_ids)
    return result_template(
        check, "ERROR", "unsupported", "supported",
        0, [], f"unsupported check type: {check_type}"
    )


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Run contract checks against JSONL data."
    )
    parser.add_argument("--contract", required=True,
                        help="Path to contract YAML.")
    parser.add_argument("--data", required=True,
                        help="Path to JSONL data.")
    parser.add_argument("--output", required=False,
                        help="Output path for validation report JSON.")
    args = parser.parse_args()

    with open(args.contract, "r", encoding="utf-8") as f:
        contract = yaml.safe_load(f)

    records = load_jsonl(args.data)
    primary_key = contract.get("primary_key", "id")
    record_ids = []
    for i, rec in enumerate(records):
        record_ids.append(rec.get(primary_key, f"row_{i}"))

    checks = contract.get("quality", {}).get("checks", [])
    results = []

    for check in checks:
        col = check.get("column")
        if col and records:
            if not column_exists(records[0], col):
                results.append(result_template(
                    check, "ERROR", "column_missing",
                    "column_present", 0, [],
                    "column does not exist",
                ))
                continue
        results.append(run_check(check, records, record_ids))

    # Statistical drift detection
    contract_id = contract.get("id", "unknown")
    drift_results = run_statistical_drift(
        contract_id, records, checks
    )
    results.extend(drift_results)

    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    warned = sum(
        1 for r in results
        if r["status"] in ("WARN", "WARNING")
    )
    errored = sum(1 for r in results if r["status"] == "ERROR")

    report = {
        "report_id": __import__("uuid").uuid4().hex,
        "contract_id": contract_id,
        "snapshot_id": sha256_file(args.data),
        "run_timestamp": iso_now(),
        "total_checks": len(results),
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "errored": errored,
        "results": results,
    }

    output_path = args.output
    if not output_path:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(
            "validation_reports", f"{contract_id}_{ts}.json"
        )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"Wrote validation report: {output_path}")
    print(
        f"  Total: {len(results)} | "
        f"Passed: {passed} | Failed: {failed} | "
        f"Warned: {warned} | Errors: {errored}"
    )


if __name__ == "__main__":
    main()
