"""
ContractGenerator — Phase 1A

Reads JSONL outputs and produces Bitol-style contract YAML files with:
  Step 1: Structural profiling (ydata-profiling)
  Step 2: Statistical profiling (min/max/mean/percentiles/stddev)
  Step 3: Lineage context injection from Week 4 snapshots
  Step 4: LLM annotation via Claude API for ambiguous columns
  Step 5: dbt schema output

Usage:
  python contracts/generator.py --all --output generated_contracts
  python contracts/generator.py --source outputs/week3/extractions.jsonl --output generated_contracts
"""

import argparse
import json
import math
import os
import re
import statistics
from datetime import datetime, timezone
from pathlib import Path

import yaml

try:
    import numpy as np
    import pandas as pd
except ImportError:
    pd = None
    np = None

UUID_REGEX = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
BASELINES_PATH = "schema_snapshots/baselines.json"


# ── Helpers ──────────────────────────────────────────────────────────────────

def load_jsonl(path):
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def iso_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_stats(values):
    """Compute statistical summary for a list of numeric values."""
    if not values:
        return {}
    vals = [v for v in values if v is not None and isinstance(v, (int, float)) and not math.isnan(v)]
    if not vals:
        return {}
    vals_sorted = sorted(vals)
    n = len(vals_sorted)

    def percentile(p):
        k = (n - 1) * p / 100
        f = int(k)
        c = f + 1 if f + 1 < n else f
        return vals_sorted[f] + (k - f) * (vals_sorted[c] - vals_sorted[f])

    return {
        "min": vals_sorted[0],
        "max": vals_sorted[-1],
        "mean": round(statistics.mean(vals), 6),
        "stddev": round(statistics.stdev(vals), 6) if n > 1 else 0.0,
        "p25": round(percentile(25), 6),
        "p50": round(percentile(50), 6),
        "p75": round(percentile(75), 6),
        "p95": round(percentile(95), 6),
        "p99": round(percentile(99), 6),
        "count": n,
    }


def write_yaml(path, data):
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, width=120, allow_unicode=True)


def write_dbt_schema(path, model_name, columns, description=""):
    dbt = {
        "version": 2,
        "models": [{
            "name": model_name,
            "description": description,
            "columns": columns,
        }],
    }
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(dbt, f, sort_keys=False, width=120)


def load_baselines(path=BASELINES_PATH):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_baselines(baselines, path=BASELINES_PATH):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(baselines, f, indent=2)


def update_baselines(contract_id, records, checks):
    """Persist mean/stddev for numeric columns to baselines file."""
    baselines = load_baselines()
    contract_baselines = baselines.get(contract_id, {})

    numeric_paths = [c["column"] for c in checks if c.get("type") == "range"]
    for path in numeric_paths:
        values = extract_flat_values(records, path)
        nums = [v for v in values if isinstance(v, (int, float))]
        if not nums:
            continue
        mean = statistics.mean(nums)
        stddev = statistics.stdev(nums) if len(nums) > 1 else 0.0
        contract_baselines[path] = {
            "mean": round(mean, 6),
            "stddev": round(stddev, 6),
            "count": len(nums),
            "established_at": iso_now(),
        }

    baselines[contract_id] = contract_baselines
    save_baselines(baselines)


# ── Step 1: Structural Profiling ─────────────────────────────────────────────

def structural_profile(records):
    """Profile each top-level field: dtype, null fraction, cardinality, samples, patterns."""
    if not records:
        return {}
    profile = {}
    keys = set()
    for r in records:
        keys.update(r.keys())
    n = len(records)
    for key in sorted(keys):
        values = [r.get(key) for r in records]
        non_null = [v for v in values if v is not None]
        null_frac = round(1 - len(non_null) / n, 4) if n else 1.0
        types = set(type(v).__name__ for v in non_null[:100])
        # Cardinality
        try:
            distinct = set(str(v) for v in non_null)
            cardinality = len(distinct)
            samples = sorted(distinct)[:5]
        except Exception:
            cardinality = None
            samples = []
        # String pattern detection
        dominant_pattern = None
        if non_null and isinstance(non_null[0], str):
            sample_strs = non_null[:50]
            if all(re.match(UUID_REGEX, s) for s in sample_strs if isinstance(s, str)):
                dominant_pattern = "uuid"
            elif all(re.match(r"^[a-f0-9]{64}$", s) for s in sample_strs if isinstance(s, str)):
                dominant_pattern = "sha256"
            elif all(re.match(r"^\d{4}-\d{2}-\d{2}T", s) for s in sample_strs if isinstance(s, str)):
                dominant_pattern = "iso8601"
        profile[key] = {
            "types_observed": sorted(types),
            "null_fraction": null_frac,
            "cardinality": cardinality,
            "samples": samples,
            "dominant_pattern": dominant_pattern,
        }
    return profile


# ── Step 2: Statistical Profiling ────────────────────────────────────────────

def statistical_profile(records, numeric_paths):
    """For each numeric path, compute full stats."""
    stats = {}
    for path in numeric_paths:
        values = extract_flat_values(records, path)
        nums = [v for v in values if isinstance(v, (int, float))]
        stats[path] = safe_stats(nums)
    return stats


def extract_flat_values(records, path):
    """Extract values from records given a dotted path with [*] for arrays."""
    parts = path.split(".")
    values = list(records)
    for part in parts:
        if part.endswith("[*]"):
            key = part[:-3]
            next_vals = []
            for v in values:
                if isinstance(v, dict) and key in v and isinstance(v[key], list):
                    next_vals.extend(v[key])
            values = next_vals
        else:
            values = [v.get(part) if isinstance(v, dict) else None for v in values]
    return values


# ── Step 3: Lineage Context ──────────────────────────────────────────────────

def load_lineage_context(lineage_path="outputs/week4/lineage_snapshots.jsonl"):
    """Load the latest lineage snapshot and build downstream consumer map."""
    if not os.path.exists(lineage_path):
        return {}
    records = load_jsonl(lineage_path)
    if not records:
        return {}
    latest = records[-1]
    edges = latest.get("edges", [])
    nodes = {n["node_id"]: n for n in latest.get("nodes", [])}

    # Build: file_node -> list of consuming pipeline nodes
    consumers = {}
    for edge in edges:
        if edge.get("relationship") == "CONSUMED_BY":
            source = edge["source"]
            target = edge["target"]
            consumers.setdefault(source, []).append({
                "consumer_id": target,
                "consumer_metadata": nodes.get(target, {}).get("metadata", {}),
            })
    return consumers


# ── Step 4: LLM Annotation ──────────────────────────────────────────────────

def llm_annotate_columns(contract_id, columns_info):
    """Use Claude API to annotate ambiguous columns. Falls back gracefully."""
    try:
        import anthropic
        api_key = os.getenv("ANTHROPIC_API_KEY")
        client = anthropic.Anthropic(api_key=api_key) if api_key else anthropic.Anthropic()
    except Exception:
        return {}

    annotations = {}
    # Only annotate columns that lack clear semantic meaning from name alone
    ambiguous = []
    clear_names = {"doc_id", "event_id", "aggregate_id", "source_path", "source_hash",
                   "extracted_at", "occurred_at", "recorded_at", "start_time", "end_time",
                   "created_at", "session_id", "intent_id", "verdict_id"}
    for col_name, info in columns_info.items():
        if col_name in clear_names:
            continue
        ambiguous.append((col_name, info))

    if not ambiguous:
        return annotations

    for col_name, info in ambiguous[:10]:  # Cap at 10 to control costs
        prompt = (
            f"You are a data contract analyst. For the column below, provide:\n"
            f"(a) A plain-English description (1 sentence)\n"
            f"(b) A business rule as a validation expression\n"
            f"(c) Any cross-column relationship\n\n"
            f"Contract: {contract_id}\n"
            f"Column: {col_name}\n"
            f"Samples: {info.get('samples', [])[:5]}\n"
            f"Type: {info.get('types_observed', [])}\n"
            f"Null fraction: {info.get('null_fraction', 'unknown')}\n\n"
            f"Respond in JSON with keys: description, business_rule, cross_column_relationship"
        )
        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=300,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text
            # Try to parse JSON from response
            json_match = re.search(r'\{[^{}]+\}', text, re.DOTALL)
            if json_match:
                annotations[col_name] = json.loads(json_match.group())
            else:
                annotations[col_name] = {"description": text.strip()}
        except Exception as e:
            annotations[col_name] = {"error": str(e)}

    return annotations


# ── Step 5: Schema Snapshot ──────────────────────────────────────────────────

def save_schema_snapshot(contract_id, schema, out_base="schema_snapshots"):
    """Write a timestamped schema snapshot for evolution tracking."""
    snap_dir = os.path.join(out_base, contract_id)
    ensure_dir(snap_dir)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(snap_dir, f"{ts}.yaml")
    write_yaml(path, schema)
    return path


# ── Contract Builders ────────────────────────────────────────────────────────

def build_week1_contract(source_path, records, lineage_ctx, llm_annotations):
    """Week 1 Intent Classifier contract."""
    profile = structural_profile(records)
    intents = sorted({r.get("intent") for r in records if r.get("intent")})
    confidence_vals = []
    for r in records:
        for ref in r.get("code_refs", []):
            if "confidence" in ref:
                confidence_vals.append(ref["confidence"])
    conf_stats = safe_stats(confidence_vals)

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": "week1-intent-classifier-records",
        "primary_key": "intent_id",
        "info": {
            "title": "Week 1 Intent Classifier — Intent Records",
            "version": "1.0.0",
            "owner": "week1-team",
            "description": "One record per classified intent with code references and confidence scores.",
            "generated_at": iso_now(),
        },
        "servers": {"local": {"type": "local", "path": source_path.replace("\\", "/"), "format": "jsonl"}},
        "terms": {
            "usage": "Internal inter-system data contract. Do not publish.",
            "limitations": "code_refs confidence must remain in 0.0-1.0 float range.",
        },
        "schema": {
            "intent_id": {"type": "string", "format": "uuid", "required": True, "unique": True,
                          "description": "Primary key. UUIDv4."},
            "session_id": {"type": "string", "format": "uuid", "required": True},
            "intent": {"type": "string", "enum": intents, "required": True,
                       "description": "Classified intent category."},
            "code_refs": {
                "type": "array", "min_items": 1, "required": True,
                "items": {
                    "file": {"type": "string", "required": True},
                    "line_start": {"type": "integer", "minimum": 1, "required": True},
                    "line_end": {"type": "integer", "minimum": 1, "required": True},
                    "symbol": {"type": "string", "required": True},
                    "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0, "required": True},
                },
            },
            "resolved_at": {"type": "string", "format": "date-time", "required": True},
            "source_text": {"type": "string", "required": True},
            "model_version": {"type": "string", "required": True, "pattern": "^(claude|gpt)-"},
        },
        "quality": {
            "engine": "custom",
            "checks": [
                {"id": "week1.intent_id.required", "column": "intent_id", "type": "required", "severity": "CRITICAL"},
                {"id": "week1.intent_id.uuid", "column": "intent_id", "type": "regex", "pattern": UUID_REGEX, "severity": "CRITICAL"},
                {"id": "week1.intent.enum", "column": "intent", "type": "enum", "allowed": intents, "severity": "HIGH"},
                {"id": "week1.code_refs.non_empty", "column": "code_refs", "type": "min_items", "min_items": 1, "severity": "CRITICAL"},
                {"id": "week1.code_refs.confidence.range", "column": "code_refs[*].confidence", "type": "range",
                 "min": 0.0, "max": 1.0, "severity": "CRITICAL", "observed": conf_stats},
                {"id": "week1.code_refs.line_order", "column": "code_refs[*].line_end", "type": "gte_field",
                 "other_column": "code_refs[*].line_start", "severity": "HIGH"},
                {"id": "week1.resolved_at.iso8601", "column": "resolved_at", "type": "iso8601", "severity": "HIGH"},
                {"id": "week1.model_version.pattern", "column": "model_version", "type": "regex",
                 "pattern": "^(claude|gpt)-", "severity": "HIGH"},
            ],
        },
        "lineage": {
            "upstream": [],
            "downstream": [{
                "id": "week3-document-refinery",
                "description": "Document Refinery uses intent records to guide extraction.",
                "fields_consumed": ["intent_id", "code_refs", "intent"],
                "breaking_if_changed": ["code_refs.confidence", "intent_id"],
            }],
        },
    }
    if llm_annotations:
        contract["llm_annotations"] = llm_annotations
    return contract, "week1_intent_records"


def build_week3_contract(source_path, records, lineage_ctx, llm_annotations):
    """Week 3 Document Refinery contract (enhanced)."""
    confidence_vals = []
    processing_vals = []
    entity_types = set()
    for r in records:
        processing_vals.append(r.get("processing_time_ms"))
        for fact in r.get("extracted_facts", []):
            if "confidence" in fact:
                confidence_vals.append(fact["confidence"])
        for ent in r.get("entities", []):
            t = ent.get("type")
            if t:
                entity_types.add(t)

    conf_stats = safe_stats(confidence_vals)
    proc_stats = safe_stats([v for v in processing_vals if v is not None])

    # Flag suspicious confidence distributions
    confidence_flags = []
    if conf_stats.get("mean", 0) > 0.99:
        confidence_flags.append("WARNING: mean confidence > 0.99, likely clamped")
    if conf_stats.get("mean", 1) < 0.01:
        confidence_flags.append("WARNING: mean confidence < 0.01, likely broken")

    # Downstream consumers from lineage
    downstream = [{
        "id": "week4-cartographer",
        "description": "Cartographer ingests doc_id and extracted_facts as node metadata.",
        "fields_consumed": ["doc_id", "extracted_facts", "extraction_model"],
        "breaking_if_changed": ["extracted_facts.confidence", "doc_id"],
    }]
    file_key = "file::outputs/week3/extractions.jsonl"
    if file_key in lineage_ctx:
        for c in lineage_ctx[file_key]:
            downstream.append({
                "id": c["consumer_id"],
                "description": f"Lineage-detected consumer: {c['consumer_id']}",
                "fields_consumed": ["doc_id", "extracted_facts"],
            })

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": "week3-document-refinery-extractions",
        "primary_key": "doc_id",
        "info": {
            "title": "Week 3 Document Refinery — Extraction Records",
            "version": "1.0.0",
            "owner": "week3-team",
            "description": "One record per processed document. Each record contains all facts extracted from the source document and the entities referenced.",
            "generated_at": iso_now(),
        },
        "servers": {"local": {"type": "local", "path": source_path.replace("\\", "/"), "format": "jsonl"}},
        "terms": {
            "usage": "Internal inter-system data contract. Do not publish.",
            "limitations": "confidence must remain in 0.0-1.0 float range.",
        },
        "schema": {
            "doc_id": {"type": "string", "format": "uuid", "required": True, "unique": True,
                       "description": "Primary key. UUIDv4. Stable across re-extractions of the same source."},
            "source_path": {"type": "string", "required": True},
            "source_hash": {"type": "string", "pattern": "^[a-f0-9]{64}$", "required": True,
                            "description": "SHA-256 of the source file. Changes iff the source content changes."},
            "extracted_facts": {
                "type": "array", "min_items": 1,
                "items": {
                    "fact_id": {"type": "string", "format": "uuid", "unique": True},
                    "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0, "required": True,
                                   "description": "BREAKING CHANGE if changed to 0-100."},
                    "entity_refs": {"type": "array"},
                },
            },
            "entities": {
                "type": "array",
                "items": {
                    "entity_id": {"type": "string", "format": "uuid", "required": True},
                    "type": {"type": "string", "enum": ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"], "required": True},
                },
            },
            "extraction_model": {"type": "string", "required": True, "pattern": "^(claude|gpt)-",
                                 "description": "Model identifier. Must match pattern claude-* or gpt-*."},
            "processing_time_ms": {"type": "integer", "minimum": 1, "required": True},
            "token_count": {"type": "object"},
            "extracted_at": {"type": "string", "format": "date-time", "required": True},
        },
        "quality": {
            "engine": "custom",
            "type": "SodaChecks",
            "specification": {
                "checks for extractions": [
                    "missing_count(doc_id) = 0",
                    "duplicate_count(doc_id) = 0",
                    "min(confidence_mean) >= 0.0",
                    "max(confidence_mean) <= 1.0",
                    "row_count >= 1",
                ],
            },
            "checks": [
                {"id": "week3.doc_id.required", "column": "doc_id", "type": "required", "severity": "CRITICAL"},
                {"id": "week3.doc_id.uuid", "column": "doc_id", "type": "regex", "pattern": UUID_REGEX, "severity": "CRITICAL"},
                {"id": "week3.source_hash.sha256", "column": "source_hash", "type": "regex", "pattern": "^[a-f0-9]{64}$", "severity": "CRITICAL"},
                {"id": "week3.extracted_facts.non_empty", "column": "extracted_facts", "type": "min_items", "min_items": 1, "severity": "CRITICAL"},
                {"id": "week3.extracted_facts.confidence.range", "column": "extracted_facts[*].confidence", "type": "range",
                 "min": 0.0, "max": 1.0, "severity": "CRITICAL", "observed": conf_stats, "flags": confidence_flags},
                {"id": "week3.entities.type.enum", "column": "entities[*].type", "type": "enum",
                 "allowed": ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"], "severity": "CRITICAL",
                 "observed_unique": sorted(entity_types)},
                {"id": "week3.entity_refs.in_entities", "column": "extracted_facts[*].entity_refs",
                 "type": "relation_in_set", "set_column": "entities[*].entity_id", "severity": "HIGH"},
                {"id": "week3.processing_time.positive", "column": "processing_time_ms", "type": "range",
                 "min": 1, "severity": "HIGH", "observed": proc_stats},
                {"id": "week3.extracted_at.iso8601", "column": "extracted_at", "type": "iso8601", "severity": "HIGH"},
                {"id": "week3.extraction_model.pattern", "column": "extraction_model", "type": "regex",
                 "pattern": "^(claude|gpt)-", "severity": "HIGH"},
            ],
        },
        "lineage": {"upstream": [], "downstream": downstream},
    }
    if llm_annotations:
        contract["llm_annotations"] = llm_annotations
    return contract, "week3_extractions"


def build_week4_contract(source_path, records, lineage_ctx, llm_annotations):
    """Week 4 Cartographer lineage snapshot contract."""
    node_types = set()
    edge_rels = set()
    for r in records:
        for n in r.get("nodes", []):
            node_types.add(n.get("type", "unknown"))
        for e in r.get("edges", []):
            edge_rels.add(e.get("relationship", "unknown"))

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": "week4-cartographer-lineage",
        "primary_key": "snapshot_id",
        "info": {
            "title": "Week 4 Cartographer — Lineage Snapshots",
            "version": "1.0.0",
            "owner": "week4-team",
            "description": "Periodic snapshots of the lineage graph. Each record is a full graph snapshot.",
            "generated_at": iso_now(),
        },
        "servers": {"local": {"type": "local", "path": source_path.replace("\\", "/"), "format": "jsonl"}},
        "terms": {"usage": "Internal inter-system data contract.", "limitations": "Graph must be acyclic."},
        "schema": {
            "snapshot_id": {"type": "string", "format": "uuid", "required": True, "unique": True},
            "captured_at": {"type": "string", "format": "date-time", "required": True},
            "nodes": {
                "type": "array", "min_items": 1,
                "items": {
                    "node_id": {"type": "string", "required": True, "unique": True},
                    "type": {"type": "string", "enum": sorted(node_types), "required": True},
                    "metadata": {"type": "object", "required": True},
                },
            },
            "edges": {
                "type": "array",
                "items": {
                    "edge_id": {"type": "string", "format": "uuid", "required": True},
                    "source": {"type": "string", "required": True},
                    "target": {"type": "string", "required": True},
                    "relationship": {"type": "string", "enum": sorted(edge_rels), "required": True},
                },
            },
            "node_count": {"type": "integer", "minimum": 1, "required": True},
            "edge_count": {"type": "integer", "minimum": 0, "required": True},
        },
        "quality": {
            "engine": "custom",
            "checks": [
                {"id": "week4.snapshot_id.required", "column": "snapshot_id", "type": "required", "severity": "CRITICAL"},
                {"id": "week4.nodes.non_empty", "column": "nodes", "type": "min_items", "min_items": 1, "severity": "CRITICAL"},
                {"id": "week4.nodes.type.enum", "column": "nodes[*].type", "type": "enum",
                 "allowed": sorted(node_types), "severity": "HIGH"},
                {"id": "week4.edges.relationship.enum", "column": "edges[*].relationship", "type": "enum",
                 "allowed": sorted(edge_rels), "severity": "HIGH"},
                {"id": "week4.captured_at.iso8601", "column": "captured_at", "type": "iso8601", "severity": "HIGH"},
                {"id": "week4.node_count.consistent", "column": "node_count", "type": "range", "min": 1, "severity": "HIGH"},
                {"id": "week4.edge_refs.valid_nodes", "column": "edges[*].source", "type": "required", "severity": "HIGH"},
                {"id": "week4.graph.acyclic", "column": "edges", "type": "custom_acyclic", "severity": "CRITICAL"},
            ],
        },
        "lineage": {
            "upstream": [{"id": "week3-document-refinery-extractions", "fields_consumed": ["doc_id", "extracted_facts"]}],
            "downstream": [
                {"id": "week5-event-sourcing", "description": "Event Sourcing uses lineage for provenance.",
                 "fields_consumed": ["nodes", "edges"], "breaking_if_changed": ["nodes.node_id", "edges.relationship"]},
                {"id": "week7-enforcer", "description": "Enforcer traverses graph for blame chains.",
                 "fields_consumed": ["nodes", "edges"], "breaking_if_changed": ["nodes", "edges"]},
            ],
        },
    }
    if llm_annotations:
        contract["llm_annotations"] = llm_annotations
    return contract, "week4_lineage"


def build_week5_contract(source_path, records, lineage_ctx, llm_annotations):
    """Week 5 Event Sourcing contract (enhanced)."""
    event_types = sorted({r.get("event_type") for r in records if r.get("event_type")})
    agg_types = sorted({r.get("aggregate_type") for r in records if r.get("aggregate_type")})
    seq_vals = [r.get("sequence_number") for r in records if r.get("sequence_number") is not None]
    seq_stats = safe_stats(seq_vals)

    downstream = [{
        "id": "week7-schema-contract",
        "description": "Week 7 enforcer validates event payload schemas.",
        "fields_consumed": ["event_type", "payload", "schema_version"],
        "breaking_if_changed": ["event_type", "payload"],
    }]
    file_key = "file::outputs/week5/events.jsonl"
    if file_key in lineage_ctx:
        for c in lineage_ctx[file_key]:
            downstream.append({
                "id": c["consumer_id"],
                "description": f"Lineage-detected consumer: {c['consumer_id']}",
                "fields_consumed": ["event_id", "event_type", "payload"],
            })

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": "week5-event-sourcing-events",
        "primary_key": "event_id",
        "info": {
            "title": "Week 5 Event Sourcing — Event Records",
            "version": "1.0.0",
            "owner": "week5-team",
            "description": "One record per event for each aggregate.",
            "generated_at": iso_now(),
        },
        "servers": {"local": {"type": "local", "path": source_path.replace("\\", "/"), "format": "jsonl"}},
        "terms": {"usage": "Internal inter-system data contract.", "limitations": "Sequence numbers must be monotonic per aggregate."},
        "schema": {
            "event_id": {"type": "string", "format": "uuid", "required": True, "unique": True},
            "event_type": {"type": "string", "required": True, "enum": event_types},
            "aggregate_id": {"type": "string", "format": "uuid", "required": True},
            "aggregate_type": {"type": "string", "required": True, "enum": agg_types},
            "sequence_number": {"type": "integer", "required": True},
            "payload": {"type": "object", "required": True},
            "metadata": {
                "type": "object", "required": True,
                "properties": {
                    "causation_id": {"type": "string", "nullable": True},
                    "correlation_id": {"type": "string", "required": True},
                    "user_id": {"type": "string", "required": True},
                    "source_service": {"type": "string", "required": True},
                },
            },
            "schema_version": {"type": "string", "required": True},
            "occurred_at": {"type": "string", "format": "date-time", "required": True},
            "recorded_at": {"type": "string", "format": "date-time", "required": True},
        },
        "quality": {
            "engine": "custom",
            "checks": [
                {"id": "week5.event_id.uuid", "column": "event_id", "type": "regex", "pattern": UUID_REGEX, "severity": "CRITICAL"},
                {"id": "week5.event_type.pascal", "column": "event_type", "type": "regex", "pattern": "^[A-Z][A-Za-z0-9]*$", "severity": "CRITICAL"},
                {"id": "week5.event_type.registry", "column": "event_type", "type": "enum", "allowed": event_types, "severity": "HIGH"},
                {"id": "week5.aggregate_id.uuid", "column": "aggregate_id", "type": "regex", "pattern": UUID_REGEX, "severity": "CRITICAL"},
                {"id": "week5.aggregate_type.enum", "column": "aggregate_type", "type": "enum", "allowed": agg_types, "severity": "HIGH"},
                {"id": "week5.sequence.monotonic", "column": "sequence_number", "type": "monotonic_sequence",
                 "group_by": "aggregate_id", "severity": "CRITICAL", "observed": seq_stats},
                {"id": "week5.recorded_at.gte_occurred_at", "column": "recorded_at", "type": "gte_field",
                 "other_column": "occurred_at", "severity": "CRITICAL"},
                {"id": "week5.metadata.correlation_id.uuid", "column": "metadata.correlation_id", "type": "regex",
                 "pattern": UUID_REGEX, "severity": "HIGH"},
                {"id": "week5.metadata.source_service.required", "column": "metadata.source_service", "type": "required", "severity": "HIGH"},
                {"id": "week5.payload.object", "column": "payload", "type": "type", "expected_type": "object", "severity": "HIGH"},
            ],
        },
        "lineage": {
            "upstream": [
                {"id": "week3-document-refinery-extractions", "fields_consumed": ["doc_id", "extracted_facts"]},
                {"id": "week4-cartographer-lineage", "fields_consumed": ["nodes", "edges"]},
            ],
            "downstream": downstream,
        },
    }
    if llm_annotations:
        contract["llm_annotations"] = llm_annotations
    return contract, "week5_events"


def build_langsmith_contract(source_path, records, lineage_ctx, llm_annotations):
    """LangSmith trace records contract."""
    run_types = sorted({r.get("run_type") for r in records if r.get("run_type")})
    statuses = sorted({r.get("status") for r in records if r.get("status")})
    token_vals = [r.get("total_tokens") for r in records if r.get("total_tokens") is not None]
    token_stats = safe_stats(token_vals)
    latency_vals = [r.get("latency_ms") for r in records if r.get("latency_ms") is not None]
    latency_stats = safe_stats(latency_vals)

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": "langsmith-traces",
        "primary_key": "id",
        "info": {
            "title": "LangSmith Trace Records",
            "version": "1.0.0",
            "owner": "ai-observability",
            "description": "One record per LLM/chain/tool run. Tracks latency, tokens, and errors.",
            "generated_at": iso_now(),
        },
        "servers": {"local": {"type": "local", "path": source_path.replace("\\", "/"), "format": "jsonl"}},
        "terms": {"usage": "Internal observability contract.", "limitations": "total_tokens must equal prompt_tokens + completion_tokens."},
        "schema": {
            "id": {"type": "string", "format": "uuid", "required": True, "unique": True},
            "name": {"type": "string", "required": True},
            "run_type": {"type": "string", "enum": run_types, "required": True},
            "parent_run_id": {"type": "string", "format": "uuid", "nullable": True},
            "session_id": {"type": "string", "format": "uuid", "required": True},
            "start_time": {"type": "string", "format": "date-time", "required": True},
            "end_time": {"type": "string", "format": "date-time", "required": True},
            "status": {"type": "string", "enum": statuses, "required": True},
            "error": {"type": "object", "nullable": True},
            "inputs": {"type": "object", "required": True},
            "outputs": {"type": "object", "nullable": True},
            "prompt_tokens": {"type": "integer", "minimum": 0, "required": True},
            "completion_tokens": {"type": "integer", "minimum": 0, "required": True},
            "total_tokens": {"type": "integer", "minimum": 0, "required": True},
            "model_name": {"type": "string", "nullable": True},
            "latency_ms": {"type": "integer", "minimum": 0, "required": True},
            "tags": {"type": "array"},
        },
        "quality": {
            "engine": "custom",
            "type": "SodaChecks",
            "specification": {
                "checks for traces": [
                    "missing_count(id) = 0",
                    "end_time > start_time",
                    "total_tokens = prompt_tokens + completion_tokens",
                    "failed_records(error != null AND outputs IS NOT NULL) = 0",
                ],
            },
            "checks": [
                {"id": "langsmith.id.uuid", "column": "id", "type": "regex", "pattern": UUID_REGEX, "severity": "CRITICAL"},
                {"id": "langsmith.run_type.enum", "column": "run_type", "type": "enum", "allowed": run_types, "severity": "CRITICAL"},
                {"id": "langsmith.end_after_start", "column": "end_time", "type": "gte_field",
                 "other_column": "start_time", "severity": "CRITICAL"},
                {"id": "langsmith.total_tokens.sum", "column": "total_tokens", "type": "token_sum_check",
                 "prompt_col": "prompt_tokens", "completion_col": "completion_tokens", "severity": "CRITICAL"},
                {"id": "langsmith.error_implies_no_output", "column": "error", "type": "error_output_check",
                 "output_col": "outputs", "severity": "HIGH"},
                {"id": "langsmith.latency.range", "column": "latency_ms", "type": "range",
                 "min": 0, "severity": "HIGH", "observed": latency_stats},
                {"id": "langsmith.total_tokens.range", "column": "total_tokens", "type": "range",
                 "min": 0, "severity": "HIGH", "observed": token_stats},
                {"id": "langsmith.start_time.iso8601", "column": "start_time", "type": "iso8601", "severity": "HIGH"},
            ],
        },
        "lineage": {
            "upstream": [],
            "downstream": [{
                "id": "week7-enforcer",
                "description": "Enforcer monitors LLM trace quality for AI extensions.",
                "fields_consumed": ["id", "run_type", "total_tokens", "error", "outputs"],
                "breaking_if_changed": ["run_type", "total_tokens"],
            }],
        },
    }
    if llm_annotations:
        contract["llm_annotations"] = llm_annotations
    return contract, "langsmith_traces"


# ── dbt Builders ─────────────────────────────────────────────────────────────

def build_dbt_week1(records):
    intents = sorted({r.get("intent") for r in records if r.get("intent")})
    return [
        {"name": "intent_id", "tests": ["not_null", "unique",
            {"dbt_expectations.expect_column_values_to_match_regex": {"regex": UUID_REGEX}}]},
        {"name": "session_id", "tests": ["not_null"]},
        {"name": "intent", "tests": ["not_null", {"accepted_values": {"values": intents}}]},
        {"name": "code_refs", "tests": ["not_null"]},
        {"name": "resolved_at", "tests": ["not_null"]},
        {"name": "model_version", "tests": ["not_null",
            {"dbt_expectations.expect_column_values_to_match_regex": {"regex": "^(claude|gpt)-"}}]},
    ]


def build_dbt_week3(records):
    entity_types = sorted({ent.get("type") for r in records for ent in r.get("entities", []) if ent.get("type")})
    return [
        {"name": "doc_id", "tests": ["not_null", "unique",
            {"dbt_expectations.expect_column_values_to_match_regex": {"regex": UUID_REGEX}}]},
        {"name": "source_path", "tests": ["not_null"]},
        {"name": "source_hash", "tests": ["not_null",
            {"dbt_expectations.expect_column_values_to_match_regex": {"regex": "^[a-f0-9]{64}$"}}]},
        {"name": "extracted_facts", "tests": ["not_null"]},
        {"name": "extracted_facts_confidence", "description": "Confidence score (flattened)", "tests": [
            {"dbt_expectations.expect_column_values_to_be_between": {"min_value": 0.0, "max_value": 1.0}}]},
        {"name": "entities_type", "description": "Entity type (flattened)", "tests": [
            {"accepted_values": {"values": entity_types or ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"]}}]},
        {"name": "extraction_model", "tests": ["not_null"]},
        {"name": "processing_time_ms", "tests": ["not_null",
            {"dbt_expectations.expect_column_values_to_be_between": {"min_value": 1}}]},
        {"name": "extracted_at", "tests": ["not_null"]},
    ]


def build_dbt_week4(records):
    node_types = sorted({n.get("type") for r in records for n in r.get("nodes", []) if n.get("type")})
    edge_rels = sorted({e.get("relationship") for r in records for e in r.get("edges", []) if e.get("relationship")})
    return [
        {"name": "snapshot_id", "tests": ["not_null", "unique"]},
        {"name": "captured_at", "tests": ["not_null"]},
        {"name": "nodes", "tests": ["not_null"]},
        {"name": "edges", "tests": ["not_null"]},
        {"name": "node_count", "tests": ["not_null",
            {"dbt_expectations.expect_column_values_to_be_between": {"min_value": 1}}]},
        {"name": "nodes_type", "description": "Node type (flattened)", "tests": [
            {"accepted_values": {"values": node_types}}]},
        {"name": "edges_relationship", "description": "Edge relationship (flattened)", "tests": [
            {"accepted_values": {"values": edge_rels}}]},
    ]


def build_dbt_week5(records):
    event_types = sorted({r.get("event_type") for r in records if r.get("event_type")})
    agg_types = sorted({r.get("aggregate_type") for r in records if r.get("aggregate_type")})
    return [
        {"name": "event_id", "tests": ["not_null", "unique",
            {"dbt_expectations.expect_column_values_to_match_regex": {"regex": UUID_REGEX}}]},
        {"name": "event_type", "tests": ["not_null",
            {"accepted_values": {"values": event_types}},
            {"dbt_expectations.expect_column_values_to_match_regex": {"regex": "^[A-Z][A-Za-z0-9]*$"}}]},
        {"name": "aggregate_id", "tests": ["not_null",
            {"dbt_expectations.expect_column_values_to_match_regex": {"regex": UUID_REGEX}}]},
        {"name": "aggregate_type", "tests": ["not_null",
            {"accepted_values": {"values": agg_types}},
            {"dbt_expectations.expect_column_values_to_match_regex": {"regex": "^[A-Z][A-Za-z0-9]*$"}}]},
        {"name": "sequence_number", "tests": ["not_null"]},
        {"name": "payload", "tests": ["not_null"]},
        {"name": "metadata_correlation_id", "description": "Correlation ID (flattened)", "tests": ["not_null",
            {"dbt_expectations.expect_column_values_to_match_regex": {"regex": UUID_REGEX}}]},
        {"name": "metadata_user_id", "tests": ["not_null"]},
        {"name": "metadata_source_service", "tests": ["not_null"]},
        {"name": "schema_version", "tests": ["not_null"]},
        {"name": "occurred_at", "tests": ["not_null"]},
        {"name": "recorded_at", "tests": ["not_null",
            {"dbt_expectations.expect_column_pair_values_A_to_be_greater_than_B": {
                "column_A": "recorded_at", "column_B": "occurred_at", "or_equal": True}}]},
    ]


def build_dbt_langsmith(records):
    run_types = sorted({r.get("run_type") for r in records if r.get("run_type")})
    return [
        {"name": "id", "tests": ["not_null", "unique",
            {"dbt_expectations.expect_column_values_to_match_regex": {"regex": UUID_REGEX}}]},
        {"name": "run_type", "tests": ["not_null", {"accepted_values": {"values": run_types}}]},
        {"name": "start_time", "tests": ["not_null"]},
        {"name": "end_time", "tests": ["not_null"]},
        {"name": "prompt_tokens", "tests": ["not_null",
            {"dbt_expectations.expect_column_values_to_be_between": {"min_value": 0}}]},
        {"name": "completion_tokens", "tests": ["not_null",
            {"dbt_expectations.expect_column_values_to_be_between": {"min_value": 0}}]},
        {"name": "total_tokens", "tests": ["not_null",
            {"dbt_expectations.expect_column_values_to_be_between": {"min_value": 0}}]},
        {"name": "latency_ms", "tests": ["not_null",
            {"dbt_expectations.expect_column_values_to_be_between": {"min_value": 0}}]},
    ]


# ── Main Pipeline ────────────────────────────────────────────────────────────

SOURCES = {
    "week1": {"path": "outputs/week1/intent_records.jsonl", "builder": build_week1_contract, "dbt": build_dbt_week1},
    "week3": {"path": "outputs/week3/extractions.jsonl", "builder": build_week3_contract, "dbt": build_dbt_week3},
    "week4": {"path": "outputs/week4/lineage_snapshots.jsonl", "builder": build_week4_contract, "dbt": build_dbt_week4},
    "week5": {"path": "outputs/week5/events.jsonl", "builder": build_week5_contract, "dbt": build_dbt_week5},
    "langsmith": {"path": "outputs/traces/runs.jsonl", "builder": build_langsmith_contract, "dbt": build_dbt_langsmith},
}


def generate_one(key, source_info, out_dir, lineage_ctx, use_llm=False):
    path = source_info["path"]
    if not os.path.exists(path):
        print(f"  SKIP {key}: {path} not found")
        return

    records = load_jsonl(path)
    if not records:
        print(f"  SKIP {key}: {path} is empty")
        return

    print(f"  Processing {key}: {len(records)} records from {path}")

    # Step 1: Structural profiling
    profile = structural_profile(records)

    # Step 4: LLM annotation (optional)
    llm_annotations = {}
    if use_llm:
        contract_id = f"{key}-contract"
        llm_annotations = llm_annotate_columns(contract_id, profile)

    # Build contract
    contract, base_name = source_info["builder"](path, records, lineage_ctx, llm_annotations)

    # Write contract YAML
    yaml_path = os.path.join(out_dir, f"{base_name}.yaml")
    write_yaml(yaml_path, contract)
    print(f"    Contract: {yaml_path}")

    # Step 5: dbt output
    dbt_columns = source_info["dbt"](records)
    dbt_path = os.path.join(out_dir, f"{base_name}_dbt.yml")
    write_dbt_schema(dbt_path, base_name, dbt_columns, description=contract["info"]["description"])
    print(f"    dbt schema: {dbt_path}")

    # Schema snapshot
    snap_path = save_schema_snapshot(contract["id"], contract["schema"])
    print(f"    Snapshot: {snap_path}")

    # Baseline persistence for numeric columns (mean/stddev)
    checks = contract.get("quality", {}).get("checks", [])
    update_baselines(contract["id"], records, checks)
    print(f"    Baselines updated: {BASELINES_PATH}")

    return contract


def main():
    parser = argparse.ArgumentParser(description="Generate Bitol-style data contracts from JSONL outputs.")
    parser.add_argument("--source", help="Path to a single JSONL source file.")
    parser.add_argument("--contract-id", help="Explicit contract id (optional).")
    parser.add_argument("--lineage", default="outputs/week4/lineage_snapshots.jsonl",
                        help="Path to lineage snapshots JSONL.")
    parser.add_argument("--registry", default="contract_registry/subscriptions.yaml",
                        help="Path to registry file (accepted for compatibility).")
    parser.add_argument("--output", default="generated_contracts", help="Output directory.")
    parser.add_argument("--all", action="store_true", help="Generate contracts for all known sources.")
    parser.add_argument("--llm", action="store_true", help="Enable LLM annotation (requires ANTHROPIC_API_KEY).")
    args = parser.parse_args()

    out_dir = args.output
    ensure_dir(out_dir)

    # Step 3: Load lineage context
    print("Loading lineage context...")
    lineage_ctx = load_lineage_context(args.lineage)
    print(f"  Lineage consumers: {len(lineage_ctx)} file nodes with consumers")

    if args.all:
        print("\nGenerating all contracts...")
        for key, info in SOURCES.items():
            generate_one(key, info, out_dir, lineage_ctx, use_llm=args.llm)
    elif args.source:
        # Match source to known key
        lower = os.path.basename(args.source).lower()
        matched = None
        if args.contract_id:
            # Map known contract IDs to keys
            contract_map = {
                "week1-intent-classifier-records": "week1",
                "week3-document-refinery-extractions": "week3",
                "week4-cartographer-lineage": "week4",
                "week5-event-sourcing-events": "week5",
                "langsmith-traces": "langsmith",
            }
            matched = contract_map.get(args.contract_id)
        if not matched:
            for key, info in SOURCES.items():
                if os.path.basename(info["path"]).lower() in lower or key in lower:
                    matched = key
                    break
        if matched:
            generate_one(matched, SOURCES[matched], out_dir, lineage_ctx, use_llm=args.llm)
        else:
            print(f"Unknown source: {args.source}. Use --all or one of: {list(SOURCES.keys())}")
    else:
        print("Use --all to generate all contracts, or --source <path> for a single source.")

    print("\nDone.")


if __name__ == "__main__":
    main()
