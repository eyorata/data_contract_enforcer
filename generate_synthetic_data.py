"""
Generate synthetic data for empty JSONL files:
- outputs/week1/intent_records.jsonl
- outputs/week2/verdicts.jsonl
- outputs/week4/lineage_snapshots.jsonl
- outputs/traces/runs.jsonl
"""

import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

random.seed(42)

def uid():
    return str(uuid.uuid4())

def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

BASE_TIME = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)


def write_jsonl(path, records):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"Wrote {len(records)} records to {path}")


def generate_week1_intent_records():
    """Week 1 Intent Classifier outputs."""
    intents = ["extract_financials", "summarize_document", "classify_risk",
               "verify_identity", "detect_fraud", "assess_compliance"]
    symbols = ["extract_facts", "parse_document", "run_classifier",
               "validate_input", "score_risk", "check_compliance"]
    files = ["src/week3/extractor.py", "src/week3/parser.py",
             "src/week1/classifier.py", "src/week5/event_handler.py",
             "src/week4/cartographer.py", "src/week2/judge.py"]
    records = []
    for i in range(80):
        t = BASE_TIME + timedelta(minutes=i * 15)
        n_refs = random.randint(1, 4)
        code_refs = []
        for _ in range(n_refs):
            ls = random.randint(1, 200)
            code_refs.append({
                "file": random.choice(files),
                "line_start": ls,
                "line_end": ls + random.randint(5, 30),
                "symbol": random.choice(symbols),
                "confidence": round(random.uniform(0.6, 0.99), 4)
            })
        records.append({
            "intent_id": uid(),
            "session_id": uid(),
            "intent": random.choice(intents),
            "code_refs": code_refs,
            "resolved_at": iso(t),
            "source_text": f"Sample intent input text for record {i}",
            "model_version": random.choice(["claude-3-5-sonnet-20241022", "claude-3-haiku-20240307"]),
        })
    return records


def generate_week2_verdicts():
    """Week 2 Verdict records — LLM structured outputs."""
    verdict_values = ["PASS", "FAIL", "WARN"]
    categories = ["financial_accuracy", "compliance_check", "identity_verification",
                   "risk_assessment", "document_completeness"]
    records = []
    for i in range(100):
        t = BASE_TIME + timedelta(minutes=i * 10)
        # 2% intentional schema violations for testing
        if random.random() < 0.02:
            verdict = "INVALID_VERDICT"
        else:
            verdict = random.choice(verdict_values)
        records.append({
            "verdict_id": uid(),
            "doc_id": uid(),
            "overall_verdict": verdict,
            "category": random.choice(categories),
            "reasoning": f"Assessment reasoning for document {i}.",
            "confidence": round(random.uniform(0.55, 0.98), 4),
            "sub_verdicts": [
                {
                    "criterion": f"criterion_{j}",
                    "verdict": random.choice(verdict_values),
                    "weight": round(random.uniform(0.1, 0.5), 2),
                }
                for j in range(random.randint(2, 5))
            ],
            "model_version": "claude-3-5-sonnet-20241022",
            "prompt_version": random.choice(["v1.0", "v1.1", "v1.2"]),
            "judged_at": iso(t),
            "latency_ms": random.randint(800, 5000),
        })
    return records


def generate_week4_lineage():
    """Week 4 Cartographer lineage snapshots."""
    # Build a realistic lineage graph
    nodes = []
    edges = []

    # File nodes
    file_nodes = [
        ("file::outputs/week1/intent_records.jsonl", "file", {"path": "outputs/week1/intent_records.jsonl", "owner": "week1-team"}),
        ("file::outputs/week3/extractions.jsonl", "file", {"path": "outputs/week3/extractions.jsonl", "owner": "week3-team"}),
        ("file::outputs/week4/lineage_snapshots.jsonl", "file", {"path": "outputs/week4/lineage_snapshots.jsonl", "owner": "week4-team"}),
        ("file::outputs/week5/events.jsonl", "file", {"path": "outputs/week5/events.jsonl", "owner": "week5-team"}),
        ("file::outputs/traces/runs.jsonl", "file", {"path": "outputs/traces/runs.jsonl", "owner": "observability-team"}),
        ("file::outputs/week2/verdicts.jsonl", "file", {"path": "outputs/week2/verdicts.jsonl", "owner": "week2-team"}),
    ]

    # Pipeline/code nodes
    pipeline_nodes = [
        ("pipeline::week1-intent-classifier", "pipeline", {"path": "src/week1/classifier.py", "owner": "week1-team"}),
        ("pipeline::week3-document-refinery", "pipeline", {"path": "src/week3/extractor.py", "owner": "week3-team"}),
        ("pipeline::week4-cartographer", "pipeline", {"path": "src/week4/cartographer.py", "owner": "week4-team"}),
        ("pipeline::week5-event-sourcing", "pipeline", {"path": "src/week5/event_handler.py", "owner": "week5-team"}),
        ("pipeline::week2-judge", "pipeline", {"path": "src/week2/judge.py", "owner": "week2-team"}),
        ("pipeline::langsmith-tracer", "pipeline", {"path": "src/observability/tracer.py", "owner": "observability-team"}),
    ]

    for nid, ntype, meta in file_nodes + pipeline_nodes:
        nodes.append({
            "node_id": nid,
            "type": ntype,
            "metadata": meta,
        })

    # Edges: pipeline PRODUCES file, file CONSUMED_BY pipeline
    edge_defs = [
        ("pipeline::week1-intent-classifier", "file::outputs/week1/intent_records.jsonl", "PRODUCES"),
        ("file::outputs/week1/intent_records.jsonl", "pipeline::week3-document-refinery", "CONSUMED_BY"),
        ("pipeline::week3-document-refinery", "file::outputs/week3/extractions.jsonl", "PRODUCES"),
        ("file::outputs/week3/extractions.jsonl", "pipeline::week4-cartographer", "CONSUMED_BY"),
        ("pipeline::week4-cartographer", "file::outputs/week4/lineage_snapshots.jsonl", "PRODUCES"),
        ("pipeline::week2-judge", "file::outputs/week2/verdicts.jsonl", "PRODUCES"),
        ("file::outputs/week3/extractions.jsonl", "pipeline::week5-event-sourcing", "CONSUMED_BY"),
        ("file::outputs/week4/lineage_snapshots.jsonl", "pipeline::week5-event-sourcing", "CONSUMED_BY"),
        ("pipeline::week5-event-sourcing", "file::outputs/week5/events.jsonl", "PRODUCES"),
        ("pipeline::langsmith-tracer", "file::outputs/traces/runs.jsonl", "PRODUCES"),
    ]

    for src, tgt, rel in edge_defs:
        edges.append({
            "edge_id": uid(),
            "source": src,
            "target": tgt,
            "relationship": rel,
        })

    # Produce multiple snapshots (one per day for 7 days)
    records = []
    for day in range(7):
        t = BASE_TIME + timedelta(days=day)
        records.append({
            "snapshot_id": uid(),
            "captured_at": iso(t),
            "nodes": nodes,
            "edges": edges,
            "node_count": len(nodes),
            "edge_count": len(edges),
        })
    return records


def generate_langsmith_traces():
    """LangSmith trace/run records."""
    run_types = ["llm", "chain", "tool", "retriever", "embedding"]
    model_names = ["claude-3-5-sonnet-20241022", "claude-3-haiku-20240307",
                   "gpt-4-turbo-2024-04-09"]
    statuses = ["success", "success", "success", "success", "error"]  # 20% error rate
    records = []
    for i in range(150):
        t_start = BASE_TIME + timedelta(minutes=i * 5)
        duration_ms = random.randint(200, 15000)
        t_end = t_start + timedelta(milliseconds=duration_ms)
        prompt_tokens = random.randint(50, 2000)
        completion_tokens = random.randint(20, 1500)
        total_tokens = prompt_tokens + completion_tokens
        run_type = random.choice(run_types)
        status = random.choice(statuses)
        error = None
        outputs = {"result": f"output for run {i}"}
        if status == "error":
            error = {"message": "Rate limit exceeded", "type": "RateLimitError"}
            outputs = {}

        records.append({
            "id": uid(),
            "name": f"run_{i}",
            "run_type": run_type,
            "parent_run_id": uid() if random.random() > 0.4 else None,
            "session_id": uid(),
            "start_time": iso(t_start),
            "end_time": iso(t_end),
            "status": status,
            "error": error,
            "inputs": {"query": f"input query {i}"},
            "outputs": outputs if outputs else None,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "model_name": random.choice(model_names) if run_type == "llm" else None,
            "latency_ms": duration_ms,
            "tags": random.sample(["production", "staging", "debug", "week3", "week5"], k=random.randint(1, 3)),
        })
    return records


if __name__ == "__main__":
    write_jsonl("outputs/week1/intent_records.jsonl", generate_week1_intent_records())
    write_jsonl("outputs/week2/verdicts.jsonl", generate_week2_verdicts())
    write_jsonl("outputs/week4/lineage_snapshots.jsonl", generate_week4_lineage())
    write_jsonl("outputs/traces/runs.jsonl", generate_langsmith_traces())
    print("Done.")
