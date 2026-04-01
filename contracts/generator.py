import argparse
import json
import os
import re
import statistics
from datetime import datetime, timezone

import yaml


def load_jsonl(path):
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def iso_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_mean(values, default=0.0):
    if not values:
        return default
    return float(statistics.mean(values))


def week3_contract(source_path, records):
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

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": "week3-document-refinery-extractions",
        "primary_key": "doc_id",
        "info": {
            "title": "Week 3 Document Refinery — Extraction Records",
            "version": "1.0.0",
            "owner": "week3-team",
            "description": "One record per processed document with extracted facts and entities.",
            "generated_at": iso_now(),
        },
        "servers": {
            "local": {
                "type": "local",
                "path": source_path.replace("\\", "/"),
                "format": "jsonl",
            }
        },
        "schema": {
            "doc_id": {"type": "string", "format": "uuid", "required": True, "unique": True},
            "source_path": {"type": "string", "required": True},
            "source_hash": {"type": "string", "pattern": "^[a-f0-9]{64}$", "required": True},
            "extracted_facts": {
                "type": "array",
                "min_items": 1,
                "items": {
                    "fact_id": {"type": "string", "format": "uuid", "required": True},
                    "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0, "required": True},
                    "entity_refs": {"type": "array"},
                },
            },
            "entities": {
                "type": "array",
                "items": {
                    "entity_id": {"type": "string", "format": "uuid", "required": True},
                    "type": {
                        "type": "string",
                        "enum": ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"],
                        "required": True,
                    },
                },
            },
            "extraction_model": {"type": "string", "required": True},
            "processing_time_ms": {"type": "integer", "minimum": 1, "required": True},
            "token_count": {"type": "object"},
            "extracted_at": {"type": "string", "format": "date-time", "required": True},
        },
        "quality": {
            "engine": "custom",
            "checks": [
                {
                    "id": "week3.doc_id.required",
                    "column": "doc_id",
                    "type": "required",
                    "severity": "CRITICAL",
                },
                {
                    "id": "week3.doc_id.uuid",
                    "column": "doc_id",
                    "type": "regex",
                    "pattern": "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$",
                    "severity": "CRITICAL",
                },
                {
                    "id": "week3.source_hash.sha256",
                    "column": "source_hash",
                    "type": "regex",
                    "pattern": "^[a-f0-9]{64}$",
                    "severity": "CRITICAL",
                },
                {
                    "id": "week3.extracted_facts.non_empty",
                    "column": "extracted_facts",
                    "type": "min_items",
                    "min_items": 1,
                    "severity": "CRITICAL",
                },
                {
                    "id": "week3.extracted_facts.confidence.range",
                    "column": "extracted_facts[*].confidence",
                    "type": "range",
                    "min": 0.0,
                    "max": 1.0,
                    "severity": "CRITICAL",
                    "observed": {
                        "min": min(confidence_vals) if confidence_vals else None,
                        "max": max(confidence_vals) if confidence_vals else None,
                        "mean": round(safe_mean(confidence_vals), 6),
                    },
                },
                {
                    "id": "week3.entities.type.enum",
                    "column": "entities[*].type",
                    "type": "enum",
                    "allowed": ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"],
                    "severity": "CRITICAL",
                    "observed_unique": sorted(entity_types),
                },
                {
                    "id": "week3.entity_refs.in_entities",
                    "column": "extracted_facts[*].entity_refs",
                    "type": "relation_in_set",
                    "set_column": "entities[*].entity_id",
                    "severity": "HIGH",
                },
                {
                    "id": "week3.processing_time.positive",
                    "column": "processing_time_ms",
                    "type": "range",
                    "min": 1,
                    "severity": "HIGH",
                    "observed": {
                        "min": min(processing_vals) if processing_vals else None,
                        "max": max(processing_vals) if processing_vals else None,
                    },
                },
                {
                    "id": "week3.extracted_at.iso8601",
                    "column": "extracted_at",
                    "type": "iso8601",
                    "severity": "HIGH",
                },
            ],
        },
        "lineage": {
            "upstream": [],
            "downstream": [
                {
                    "id": "week4-cartographer",
                    "description": "Cartographer ingests doc_id and extracted_facts as node metadata.",
                    "fields_consumed": ["doc_id", "extracted_facts", "extraction_model"],
                    "breaking_if_changed": ["extracted_facts.confidence", "doc_id"],
                }
            ],
        },
    }
    return contract, "week3_extractions"


def week5_contract(source_path, records):
    event_types = []
    for r in records:
        if r.get("event_type"):
            event_types.append(r["event_type"])

    registry = sorted(set(event_types))[:25]

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
        "servers": {
            "local": {
                "type": "local",
                "path": source_path.replace("\\", "/"),
                "format": "jsonl",
            }
        },
        "schema": {
            "event_id": {"type": "string", "format": "uuid", "required": True, "unique": True},
            "event_type": {"type": "string", "required": True},
            "aggregate_id": {"type": "string", "format": "uuid", "required": True},
            "aggregate_type": {"type": "string", "required": True},
            "sequence_number": {"type": "integer", "required": True},
            "payload": {"type": "object", "required": True},
            "metadata": {
                "type": "object",
                "required": True,
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
                {
                    "id": "week5.event_id.uuid",
                    "column": "event_id",
                    "type": "regex",
                    "pattern": "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$",
                    "severity": "CRITICAL",
                },
                {
                    "id": "week5.event_type.pascal",
                    "column": "event_type",
                    "type": "regex",
                    "pattern": "^[A-Z][A-Za-z0-9]*$",
                    "severity": "CRITICAL",
                },
                {
                    "id": "week5.event_type.registry",
                    "column": "event_type",
                    "type": "enum",
                    "allowed": registry,
                    "severity": "HIGH",
                },
                {
                    "id": "week5.aggregate_id.uuid",
                    "column": "aggregate_id",
                    "type": "regex",
                    "pattern": "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$",
                    "severity": "CRITICAL",
                },
                {
                    "id": "week5.aggregate_type.pascal",
                    "column": "aggregate_type",
                    "type": "regex",
                    "pattern": "^[A-Z][A-Za-z0-9]*$",
                    "severity": "HIGH",
                },
                {
                    "id": "week5.sequence.monotonic",
                    "column": "sequence_number",
                    "type": "monotonic_sequence",
                    "group_by": "aggregate_id",
                    "severity": "CRITICAL",
                },
                {
                    "id": "week5.recorded_at.gte_occurred_at",
                    "column": "recorded_at",
                    "type": "gte_field",
                    "other_column": "occurred_at",
                    "severity": "CRITICAL",
                },
                {
                    "id": "week5.metadata.correlation_id.uuid",
                    "column": "metadata.correlation_id",
                    "type": "regex",
                    "pattern": "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$",
                    "severity": "HIGH",
                },
                {
                    "id": "week5.metadata.source_service.required",
                    "column": "metadata.source_service",
                    "type": "required",
                    "severity": "HIGH",
                },
                {
                    "id": "week5.payload.object",
                    "column": "payload",
                    "type": "type",
                    "expected_type": "object",
                    "severity": "HIGH",
                },
            ],
        },
        "lineage": {
            "upstream": [],
            "downstream": [
                {
                    "id": "week7-schema-contract",
                    "description": "Week 7 enforcer validates event payload schemas.",
                    "fields_consumed": ["event_type", "payload", "schema_version"],
                    "breaking_if_changed": ["event_type", "payload"],
                }
            ],
        },
    }
    return contract, "week5_events"


def write_yaml(path, data):
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, width=120)


def write_dbt_schema(path, model_name, columns):
    dbt = {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "columns": columns,
            }
        ],
    }
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(dbt, f, sort_keys=False, width=120)


def main():
    parser = argparse.ArgumentParser(description="Generate Bitol-style data contracts from JSONL outputs.")
    parser.add_argument("--source", required=True, help="Path to JSONL source file.")
    parser.add_argument("--output", required=True, help="Output directory for generated contracts.")
    args = parser.parse_args()

    source = args.source
    out_dir = args.output
    ensure_dir(out_dir)

    records = load_jsonl(source)
    lower = os.path.basename(source).lower()

    uuid_regex = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"

    if "extractions" in lower:
        contract, base_name = week3_contract(source, records)
        entity_types = sorted({ent.get("type") for r in records for ent in r.get("entities", []) if ent.get("type")})
        dbt_columns = [
            {"name": "doc_id", "tests": [
                "not_null", "unique",
                {"dbt_expectations.expect_column_values_to_match_regex": {"regex": uuid_regex}},
            ]},
            {"name": "source_path", "tests": ["not_null"]},
            {"name": "source_hash", "tests": [
                "not_null",
                {"dbt_expectations.expect_column_values_to_match_regex": {"regex": "^[a-f0-9]{64}$"}},
            ]},
            {"name": "extracted_facts", "tests": [
                "not_null",
                "dbt_expectations.expect_column_values_to_not_be_null",
            ]},
            {"name": "extracted_facts_confidence", "description": "Confidence score (flattened)", "tests": [
                {"dbt_expectations.expect_column_values_to_be_between": {"min_value": 0.0, "max_value": 1.0}},
            ]},
            {"name": "entities_type", "description": "Entity type (flattened)", "tests": [
                {"accepted_values": {"values": entity_types if entity_types else ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"]}},
            ]},
            {"name": "extraction_model", "tests": ["not_null"]},
            {"name": "processing_time_ms", "tests": [
                "not_null",
                {"dbt_expectations.expect_column_values_to_be_between": {"min_value": 1}},
            ]},
            {"name": "extracted_at", "tests": ["not_null"]},
        ]
    elif "events" in lower:
        contract, base_name = week5_contract(source, records)
        registry = sorted({r.get("event_type") for r in records if r.get("event_type")})[:25]
        dbt_columns = [
            {"name": "event_id", "tests": [
                "not_null", "unique",
                {"dbt_expectations.expect_column_values_to_match_regex": {"regex": uuid_regex}},
            ]},
            {"name": "event_type", "tests": [
                "not_null",
                {"accepted_values": {"values": registry}},
                {"dbt_expectations.expect_column_values_to_match_regex": {"regex": "^[A-Z][A-Za-z0-9]*$"}},
            ]},
            {"name": "aggregate_id", "tests": [
                "not_null",
                {"dbt_expectations.expect_column_values_to_match_regex": {"regex": uuid_regex}},
            ]},
            {"name": "aggregate_type", "tests": [
                "not_null",
                {"dbt_expectations.expect_column_values_to_match_regex": {"regex": "^[A-Z][A-Za-z0-9]*$"}},
            ]},
            {"name": "sequence_number", "tests": ["not_null"]},
            {"name": "payload", "tests": ["not_null"]},
            {"name": "metadata_correlation_id", "description": "Correlation ID (flattened)", "tests": [
                "not_null",
                {"dbt_expectations.expect_column_values_to_match_regex": {"regex": uuid_regex}},
            ]},
            {"name": "metadata_user_id", "description": "User ID (flattened)", "tests": ["not_null"]},
            {"name": "metadata_source_service", "description": "Source service (flattened)", "tests": ["not_null"]},
            {"name": "schema_version", "tests": ["not_null"]},
            {"name": "occurred_at", "tests": ["not_null"]},
            {"name": "recorded_at", "tests": [
                "not_null",
                {"dbt_expectations.expect_column_pair_values_A_to_be_greater_than_B": {
                    "column_A": "recorded_at", "column_B": "occurred_at", "or_equal": True,
                }},
            ]},
        ]
    else:
        raise ValueError("Unsupported source file. Expected extractions.jsonl or events.jsonl.")

    yaml_path = os.path.join(out_dir, f"{base_name}.yaml")
    write_yaml(yaml_path, contract)

    dbt_path = os.path.join(out_dir, f"{base_name}_dbt.yml")
    write_dbt_schema(dbt_path, base_name, dbt_columns)

    print(f"Generated contract: {yaml_path}")
    print(f"Generated dbt schema: {dbt_path}")


if __name__ == "__main__":
    main()
