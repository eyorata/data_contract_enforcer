"""
AI Contract Extensions — Phase 4A

Three AI-specific contract clauses:
  1. Embedding Drift Detection
  2. Prompt Input Schema Validation
  3. LLM Output Schema Violation Rate

Usage:
  python contracts/ai_extensions.py \
    --week3-data outputs/week3/extractions.jsonl \
    --week2-data outputs/week2/verdicts.jsonl \
    --traces-data outputs/traces/runs.jsonl \
    --output validation_reports/ai_extensions.json
"""

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

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


# ── Extension 1: Embedding Drift Detection ───────────────────────────────────

def embed_texts_local(texts, model_name="all-MiniLM-L6-v2"):
    """
    Embed texts using sentence-transformers (local).
    Falls back to a simple hash-based pseudo-embedding if not available.
    """
    try:
        from sentence_transformers import SentenceTransformer
        import numpy as np
        model = SentenceTransformer(model_name)
        embeddings = model.encode(texts, show_progress_bar=False)
        return np.array(embeddings)
    except ImportError:
        pass

    # Try OpenAI-compatible local endpoint (LM Studio)
    try:
        import requests
        import numpy as np
        response = requests.post(
            "http://localhost:1234/v1/embeddings",
            json={"input": texts, "model": "embedding-model"},
            timeout=30,
        )
        if response.status_code == 200:
            data = response.json()
            embeddings = [d["embedding"] for d in data["data"]]
            return np.array(embeddings)
    except Exception:
        pass

    # Fallback: hash-based pseudo-embeddings (deterministic, for demo)
    import hashlib
    import numpy as np
    embeddings = []
    for text in texts:
        h = hashlib.sha256(text.encode()).hexdigest()
        vec = [int(h[i:i+2], 16) / 255.0 for i in range(0, 64, 2)]
        embeddings.append(vec)
    return np.array(embeddings)


def check_embedding_drift(
    texts, baseline_path, threshold=0.15, sample_size=200
):
    """
    Extension 1: Embedding drift detection.
    Embed a sample, compute centroid, compare to baseline.
    """
    import numpy as np

    if not texts:
        return {
            "check_id": "ai.embedding_drift",
            "status": "ERROR",
            "message": "No texts provided",
            "drift_score": 0.0,
            "threshold": threshold,
        }

    # Sample
    import random
    sample = (
        random.sample(texts, min(sample_size, len(texts)))
    )

    current = embed_texts_local(sample)
    current_centroid = np.mean(current, axis=0)

    baseline_file = Path(baseline_path)
    if not baseline_file.exists():
        baseline_file.parent.mkdir(parents=True, exist_ok=True)
        np.savez(str(baseline_file), centroid=current_centroid)
        return {
            "check_id": "ai.embedding_drift",
            "status": "BASELINE_SET",
            "message": (
                f"Baseline established from {len(sample)} samples"
            ),
            "drift_score": 0.0,
            "threshold": threshold,
            "sample_size": len(sample),
        }

    baseline_data = np.load(str(baseline_file))
    baseline_centroid = baseline_data["centroid"]

    # Cosine distance
    dot = np.dot(current_centroid, baseline_centroid)
    norm_curr = np.linalg.norm(current_centroid)
    norm_base = np.linalg.norm(baseline_centroid)
    cosine_sim = dot / (norm_curr * norm_base + 1e-9)
    drift = 1 - cosine_sim

    status = "FAIL" if drift > threshold else "PASS"

    return {
        "check_id": "ai.embedding_drift",
        "status": status,
        "drift_score": round(float(drift), 4),
        "threshold": threshold,
        "cosine_similarity": round(float(cosine_sim), 4),
        "sample_size": len(sample),
        "message": (
            f"Drift={round(float(drift), 4)} "
            f"(threshold={threshold})"
        ),
    }


# ── Extension 2: Prompt Input Schema Validation ─────────────────────────────

PROMPT_INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["doc_id", "source_path"],
    "properties": {
        "doc_id": {
            "type": "string",
            "minLength": 36,
            "maxLength": 36,
        },
        "source_path": {
            "type": "string",
            "minLength": 1,
        },
    },
}


def validate_prompt_input(record, schema=None):
    """Validate a record against the prompt input schema."""
    if schema is None:
        schema = PROMPT_INPUT_SCHEMA
    errors = []
    required = schema.get("required", [])
    properties = schema.get("properties", {})

    for field in required:
        if field not in record or record[field] is None:
            errors.append(f"missing required field: {field}")

    for field, rules in properties.items():
        val = record.get(field)
        if val is None:
            continue
        if rules.get("type") == "string":
            if not isinstance(val, str):
                errors.append(
                    f"{field}: expected string, got {type(val).__name__}"
                )
            else:
                min_len = rules.get("minLength", 0)
                max_len = rules.get("maxLength", float("inf"))
                if len(val) < min_len:
                    errors.append(
                        f"{field}: length {len(val)} < "
                        f"minLength {min_len}"
                    )
                if len(val) > max_len:
                    errors.append(
                        f"{field}: length {len(val)} > "
                        f"maxLength {max_len}"
                    )

    return errors


def check_prompt_inputs(records, quarantine_dir="outputs/quarantine"):
    """
    Extension 2: Validate prompt inputs, quarantine non-conforming.
    """
    valid = []
    quarantined = []

    for rec in records:
        errors = validate_prompt_input(rec)
        if errors:
            quarantined.append({
                "record": rec,
                "errors": errors,
            })
        else:
            valid.append(rec)

    # Write quarantined records
    if quarantined:
        os.makedirs(quarantine_dir, exist_ok=True)
        q_path = os.path.join(quarantine_dir, "quarantined.jsonl")
        with open(q_path, "w", encoding="utf-8") as f:
            for q in quarantined:
                f.write(json.dumps(q) + "\n")

    total = len(records)
    q_count = len(quarantined)
    status = "PASS" if q_count == 0 else "WARN" if q_count < total * 0.05 else "FAIL"

    return {
        "check_id": "ai.prompt_input_validation",
        "status": status,
        "total_records": total,
        "valid": len(valid),
        "quarantined": q_count,
        "quarantine_rate": (
            round(q_count / max(total, 1), 4)
        ),
        "sample_errors": [
            q["errors"] for q in quarantined[:3]
        ],
        "message": (
            f"{q_count}/{total} records quarantined"
        ),
    }


# ── Extension 3: LLM Output Schema Violation Rate ───────────────────────────

def check_output_schema_violation_rate(
    verdict_records, baseline_rate=None, warn_threshold=0.02
):
    """
    Extension 3: Track structured output violation rate.
    """
    valid_verdicts = {"PASS", "FAIL", "WARN"}
    total = len(verdict_records)
    violations = sum(
        1 for v in verdict_records
        if v.get("overall_verdict") not in valid_verdicts
    )
    rate = violations / max(total, 1)

    trend = "unknown"
    if baseline_rate is not None:
        trend = "rising" if rate > baseline_rate * 1.5 else "stable"

    status = "WARN" if rate > warn_threshold else "PASS"

    # Find sample violations
    sample_violations = []
    for v in verdict_records:
        if v.get("overall_verdict") not in valid_verdicts:
            sample_violations.append({
                "verdict_id": v.get("verdict_id", "unknown"),
                "actual_verdict": v.get("overall_verdict"),
            })
            if len(sample_violations) >= 5:
                break

    return {
        "check_id": "ai.output_schema_violation_rate",
        "status": status,
        "total_outputs": total,
        "schema_violations": violations,
        "violation_rate": round(rate, 4),
        "trend": trend,
        "warn_threshold": warn_threshold,
        "sample_violations": sample_violations,
        "message": (
            f"Violation rate: {round(rate * 100, 2)}% "
            f"({violations}/{total})"
        ),
    }


# ── LangSmith Trace Quality ─────────────────────────────────────────────────

def check_trace_quality(traces):
    """Additional AI-specific checks on LangSmith traces."""
    results = []

    # Error rate
    total = len(traces)
    errors = sum(
        1 for t in traces
        if t.get("status") == "error" or t.get("error") is not None
    )
    error_rate = errors / max(total, 1)
    results.append({
        "check_id": "ai.trace_error_rate",
        "status": "WARN" if error_rate > 0.1 else "PASS",
        "error_rate": round(error_rate, 4),
        "errors": errors,
        "total": total,
        "message": (
            f"Error rate: {round(error_rate * 100, 2)}% "
            f"({errors}/{total})"
        ),
    })

    # Latency distribution
    latencies = [
        t.get("latency_ms", 0) for t in traces
        if t.get("latency_ms") is not None
    ]
    if latencies:
        import statistics
        mean_lat = statistics.mean(latencies)
        p95 = sorted(latencies)[int(len(latencies) * 0.95)]
        results.append({
            "check_id": "ai.trace_latency",
            "status": "WARN" if p95 > 10000 else "PASS",
            "mean_latency_ms": round(mean_lat, 1),
            "p95_latency_ms": p95,
            "message": (
                f"Mean latency: {round(mean_lat, 1)}ms, "
                f"P95: {p95}ms"
            ),
        })

    return results


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Run AI-specific contract extension checks."
    )
    parser.add_argument(
        "--week3-data",
        default="outputs/week3/extractions.jsonl",
    )
    parser.add_argument(
        "--week2-data",
        default="outputs/week2/verdicts.jsonl",
    )
    parser.add_argument(
        "--traces-data",
        default="outputs/traces/runs.jsonl",
    )
    parser.add_argument(
        "--output",
        default="validation_reports/ai_extensions.json",
    )
    args = parser.parse_args()

    print("Running AI Contract Extensions...")
    results = []

    # Extension 1: Embedding Drift
    print("\n  Extension 1: Embedding Drift Detection")
    week3 = load_jsonl(args.week3_data)
    texts = []
    for r in week3:
        for fact in r.get("extracted_facts", []):
            text = fact.get("text", "")
            if text:
                texts.append(text)
    if texts:
        drift_result = check_embedding_drift(
            texts,
            baseline_path=(
                "schema_snapshots/embedding_baseline.npz"
            ),
        )
        results.append(drift_result)
        print(f"    Status: {drift_result['status']}, "
              f"Drift: {drift_result['drift_score']}")
    else:
        results.append({
            "check_id": "ai.embedding_drift",
            "status": "SKIP",
            "message": "No text data available for embedding",
        })
        print("    SKIP: No text data")

    # Extension 2: Prompt Input Validation
    print("\n  Extension 2: Prompt Input Schema Validation")
    if week3:
        prompt_result = check_prompt_inputs(week3)
        results.append(prompt_result)
        print(f"    Status: {prompt_result['status']}, "
              f"Quarantined: {prompt_result['quarantined']}")
    else:
        results.append({
            "check_id": "ai.prompt_input_validation",
            "status": "SKIP",
            "message": "No Week 3 data",
        })

    # Extension 3: LLM Output Schema Violation Rate
    print("\n  Extension 3: LLM Output Schema Violation Rate")
    verdicts = load_jsonl(args.week2_data)
    if verdicts:
        violation_result = check_output_schema_violation_rate(
            verdicts
        )
        results.append(violation_result)
        print(f"    Status: {violation_result['status']}, "
              f"Rate: {violation_result['violation_rate']}")
    else:
        results.append({
            "check_id": "ai.output_schema_violation_rate",
            "status": "SKIP",
            "message": "No Week 2 verdict data",
        })

    # Bonus: LangSmith trace quality
    print("\n  Bonus: LangSmith Trace Quality")
    traces = load_jsonl(args.traces_data)
    if traces:
        trace_results = check_trace_quality(traces)
        results.extend(trace_results)
        for tr in trace_results:
            print(f"    {tr['check_id']}: {tr['status']}")
    else:
        results.append({
            "check_id": "ai.trace_quality",
            "status": "SKIP",
            "message": "No trace data",
        })

    # Build report
    report = {
        "report_id": __import__("uuid").uuid4().hex,
        "report_type": "ai_extensions",
        "run_timestamp": iso_now(),
        "total_checks": len(results),
        "passed": sum(
            1 for r in results if r.get("status") == "PASS"
        ),
        "failed": sum(
            1 for r in results if r.get("status") == "FAIL"
        ),
        "warned": sum(
            1 for r in results
            if r.get("status") in ("WARN", "WARNING")
        ),
        "results": results,
    }

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"\nWrote AI extensions report: {args.output}")


if __name__ == "__main__":
    main()
