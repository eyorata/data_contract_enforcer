# Week 7 — Data Contract Enforcer: Interim Report

**Author:** TenX Academy Trainee
**Date:** 2026-04-01

---

## 1. Data Flow Diagram

The diagram below models the platform as five interacting systems. Each arrow is annotated with the schema name or data artifact transferred at that interface.

```
 ┌─────────────────────┐          intent_records.jsonl          ┌─────────────────────┐
 │                     │  ─────────────────────────────────────► │                     │
 │    Week 1           │   (code_refs, intent, confidence)       │    Week 3           │
 │    Intent           │                                         │    Document         │
 │    Classifier       │                                         │    Refinery         │
 │                     │                                         │                     │
 └─────────────────────┘                                         └──────────┬──────────┘
                                                                    │       │
                                               extractions.jsonl    │       │  extraction events
                                   (doc_id, extracted_facts,        │       │  (ExtractedFacts
                                    entities, confidence)           │       │   Consumed)
                                                                    │       │
                                                                    ▼       ▼
 ┌─────────────────────┐     lineage_snapshots.jsonl       ┌─────────────────────┐
 │                     │  ◄─────────────────────────────── │                     │
 │    Week 4           │    (nodes, edges, metadata)        │    Week 5           │
 │    Cartographer     │  ──────────────────────────────►  │    Event            │
 │                     │   extractions.jsonl                │    Sourcing         │
 │                     │   (doc_id, facts, entities)        │                     │
 └──────────┬──────────┘                                    └──────────┬──────────┘
            │                                                          │
            │   lineage graph                        events.jsonl      │
            │   (nodes, edges                  (event_id, event_type,  │
            │    — for blame chain)             aggregate_id,          │
            │                                   sequence_number,       │
            ▼                                   payload, metadata)     ▼
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │                                                                              │
 │                     Week 7 — Data Contract Enforcer                          │
 │                                                                              │
 │   Validates extractions.jsonl (9 checks) and events.jsonl (10 checks).      │
 │   Uses lineage graph for blame chain attribution via BFS traversal.          │
 │                                                                              │
 └──────────────────────────────────────────────────────────────────────────────┘
```

### Interface Summary

| # | Interface | Artifact | Schema / Fields Transferred |
|---|-----------|----------|-----------------------------|
| 1 | Week 1 → Week 3 | `intent_records.jsonl` | `intent_id`, `code_refs[]` (file, line_start, line_end, symbol, confidence), `intent`, `resolved_at` |
| 2 | Week 3 → Week 4 | `extractions.jsonl` | `doc_id`, `extracted_facts[]` (fact_id, confidence, entity_refs), `entities[]` (entity_id, type), `extraction_model` |
| 3 | Week 3 → Week 5 | Extraction events | `ExtractedFactsConsumed` event_type flowing into event store with extraction payload |
| 4 | Week 4 → Week 5 | `lineage_snapshots.jsonl` | `nodes[]` (node_id, type, metadata), `edges[]` (source, target, relationship) |
| 5 | Week 3 → Week 7 | `extractions.jsonl` | Validated by `week3_extractions.yaml` — 9 quality checks |
| 6 | Week 5 → Week 7 | `events.jsonl` | Validated by `week5_events.yaml` — 10 quality checks |
| 7 | Week 4 → Week 7 | Lineage graph | Consumed for BFS blame chain traversal, not validated as a dataset |

---

## 2. Contract Coverage Table

| # | Interface | Artifact | Status | Rationale / Notes |
|---|-----------|----------|:------:|-------------------|
| 1 | Week 1 → Week 3 | `intent_records.jsonl` (intent_id, code_refs, intent, resolved_at) | **No** | Week 1 output file is empty (0 records in repo). Cannot profile field distributions or infer validation clauses. Deferred until intent records are populated. The missing contract means we cannot detect schema drift at the classifier→refinery boundary. |
| 2 | Week 3 → Week 4 | `extractions.jsonl` (doc_id, extracted_facts, entities) | **Yes** | Contract: `week3_extractions.yaml` — 9 quality checks: UUID format on doc_id, SHA-256 regex on source_hash, confidence range [0.0, 1.0], min 1 extracted fact per record, entity type enum (PERSON, ORG, LOCATION, DATE, AMOUNT, OTHER), referential integrity (entity_refs ⊂ entity_ids), processing_time_ms ≥ 1, and ISO-8601 on extracted_at. |
| 3 | Week 3 → Week 5 | Extraction events (`ExtractedFactsConsumed`) | **Partial** | The event_type is covered by Week 5's event type registry check. However, no dedicated contract validates the *payload* of `ExtractedFactsConsumed` events (e.g., that doc_id and fact_ids inside the payload match valid Week 3 records). Deferred: need to profile the payload schema for this specific event type and add cross-system referential checks. |
| 4 | Week 4 → Week 5 | `lineage_snapshots.jsonl` (nodes, edges, metadata) | **No** | Week 4 output file is empty (0 records in repo). Cannot profile node types, edge relationships, or metadata structure. Deferred until lineage snapshots are populated. This is a blocking dependency — without lineage data, blame chain attribution in Week 7 cannot function. |
| 5 | Week 5 → Week 7 | `events.jsonl` (event_id, event_type, aggregate_id, sequence_number, payload, metadata) | **Yes** | Contract: `week5_events.yaml` — 10 quality checks: UUID format on event_id and aggregate_id, PascalCase regex on event_type and aggregate_type, event type registry enum (25 known types), monotonic sequence per aggregate_id, temporal ordering (recorded_at ≥ occurred_at), UUID on metadata.correlation_id, required metadata.source_service, and payload type=object. |
| 6 | Week 4 → Week 7 | Lineage graph (nodes + edges for blame chain) | **No** | The lineage graph is consumed by the Enforcer for BFS blame chain traversal — it is an input to the attribution algorithm, not a dataset being validated. A structural contract for lineage_snapshots (node_id uniqueness, valid edge references, required metadata.path) is deferred until Week 4 outputs are available. Without this contract, malformed lineage data could produce incorrect blame chains silently. |
| 7 | Week 1 → Week 7 | `intent_records.jsonl` (indirect, via Week 3) | **No** | No direct data flow from Week 1 to Week 7 exists today. However, if the Enforcer later traces blame chains through Week 3 back to Week 1 intent records, a contract on intent_records would be needed. Deferred: depends on both Week 1 data availability and lineage graph completion. |

**Coverage summary:** 2 of 7 interfaces have full contracts (**Yes**). 1 is partially covered (**Partial**). 4 are deferred (**No**) — 2 because upstream data files are empty and cannot be profiled, 1 because the lineage graph is an attribution input rather than a validated dataset, and 1 because the indirect dependency path does not yet exist.

---

## 3. First Validation Run Results

### 3.1 Week 3 — Document Refinery Extractions

- **Contract:** `week3-document-refinery-extractions`
- **Data:** `extractions.jsonl` (50 records)
- **Run timestamp:** 2026-04-01T17:45:44Z

| Check ID | Type | Column | Severity | Status | Detail |
|----------|------|--------|----------|:------:|--------|
| `week3.doc_id.required` | required | doc_id | CRITICAL | **PASS** | missing=0 |
| `week3.doc_id.uuid` | regex | doc_id | CRITICAL | **PASS** | invalid=0 |
| `week3.source_hash.sha256` | regex | source_hash | CRITICAL | **PASS** | invalid=0 |
| `week3.extracted_facts.non_empty` | min_items | extracted_facts | CRITICAL | **PASS** | invalid=0 |
| `week3.extracted_facts.confidence.range` | range | extracted_facts[*].confidence | CRITICAL | **PASS** | min=0.825, max=0.993 |
| `week3.entities.type.enum` | enum | entities[*].type | CRITICAL | **PASS** | invalid=0 |
| `week3.entity_refs.in_entities` | relation_in_set | extracted_facts[*].entity_refs | HIGH | **PASS** | invalid=0 |
| `week3.processing_time.positive` | range | processing_time_ms | HIGH | **PASS** | min=85557, max=343487 |
| `week3.extracted_at.iso8601` | iso8601 | extracted_at | HIGH | **PASS** | invalid=0 |

**Result: 9/9 checks passed. No violations found.**

**Interpretation:** The Week 3 data is structurally sound. All doc_ids are valid UUIDs, source hashes match SHA-256 format, confidence values fall within [0.825, 0.993] (well within the 0.0–1.0 contract range), entity types are restricted to the allowed enum, all entity_refs resolve to valid entity_ids (referential integrity holds), and all timestamps are valid ISO-8601. This confirms the Document Refinery is producing clean output.

---

### 3.2 Week 5 — Event Sourcing Events

- **Contract:** `week5-event-sourcing-events`
- **Data:** `events.jsonl` (1,847 records)
- **Run timestamp:** 2026-04-01T19:47:42Z

| Check ID | Type | Column | Severity | Status | Detail |
|----------|------|--------|----------|:------:|--------|
| `week5.event_id.uuid` | regex | event_id | CRITICAL | **PASS** | invalid=0 |
| `week5.event_type.pascal` | regex | event_type | CRITICAL | **PASS** | invalid=0 |
| `week5.event_type.registry` | enum | event_type | HIGH | **FAIL** | invalid=291 |
| `week5.aggregate_id.uuid` | regex | aggregate_id | CRITICAL | **PASS** | invalid=0 |
| `week5.aggregate_type.pascal` | regex | aggregate_type | HIGH | **PASS** | invalid=0 |
| `week5.sequence.monotonic` | monotonic_sequence | sequence_number | CRITICAL | **PASS** | invalid_groups=0 |
| `week5.recorded_at.gte_occurred_at` | gte_field | recorded_at | CRITICAL | **PASS** | invalid=0 |
| `week5.metadata.correlation_id.uuid` | regex | metadata.correlation_id | HIGH | **PASS** | invalid=0 |
| `week5.metadata.source_service.required` | required | metadata.source_service | HIGH | **PASS** | invalid=0 |
| `week5.payload.object` | type | payload | HIGH | **PASS** | invalid=0 |

**Result: 9/10 checks passed. 1 violation found.**

#### Violation: `week5.event_type.registry` — FAIL

**What happened:** 291 out of 1,847 event records (15%) contain `event_type` values outside the known registry of 25 allowed types. The contract was generated by profiling the data and capping the registry at 25 event types, but the actual dataset contains 34 distinct event types. The 9 unregistered types (including `PackageCreated`, `FraudScreeningInitiated`, etc.) are legitimate events from aggregate types like `DocumentPackage` and `FraudScreening` that were not captured in the initial profiling window.

**Why this matters:** This violation is a *real contract gap*, not a data quality issue. It reveals that the generator's event type registry is incomplete — it captured only the first 25 alphabetically sorted types. In a production setting, this would mean new event types introduced by downstream services would silently fail validation. The fix is to either expand the registry or convert this check to a warning-level drift detector.

**Risk assessment:** Severity is HIGH. The violation does not indicate corrupt data (all event_type values pass the PascalCase format check), but it does indicate that the contract's event type registry needs to be regenerated with the full dataset or updated to allow dynamic registration.

---

## 4. Reflection

*(Max 400 words)*

Writing data contracts forced me to confront assumptions I did not know I was making. Three discoveries stand out.

**First, profiling is not the same as understanding.** The generator profiles the data and produces contracts automatically — but profiling only sees what is present, not what *should* be present. The Week 5 event type registry violation proved this: the generator capped the registry at 25 types because that was a design choice I made for tractability. The real dataset has 34 event types, and 291 records were flagged as violations. These are not bad records — they are legitimate events from aggregates like `DocumentPackage` and `FraudScreening` that my profiling window missed. I assumed the sample was representative. It was not.

**Second, empty upstream outputs are a hidden risk.** Week 1 (`intent_records`) and Week 4 (`lineage_snapshots`) have zero records in my repository. Before writing contracts, I treated these as "not yet implemented" and moved on. But contract thinking forced me to ask: what happens to downstream systems when upstream data is absent? The answer is that the Cartographer cannot build a lineage graph, and the Enforcer cannot construct blame chains. These are not just missing features — they are silent failures. The coverage table makes this gap visible and trackable, which is the first step toward fixing it.

**Third, referential integrity across system boundaries is fragile.** The Week 3 contract includes a `relation_in_set` check verifying that every `entity_ref` in `extracted_facts` points to a valid `entity_id`. This check passed on my data — but writing it forced me to realize that if Week 3 and Week 5 share entity references (via the `ExtractedFactsConsumed` event), there is no cross-system contract ensuring that entity_ids remain consistent across the boundary. A valid `entity_ref` in Week 3 could become a dangling pointer in Week 5 if the entity list is filtered or transformed during event creation. This cross-boundary integrity gap is not visible from either system alone — it only appears when you try to formalize what each system promises to the next.

The core lesson is that contracts are not documentation — they are executable assumptions. When those assumptions are wrong, the contract fails loudly. When they are missing, failures are silent. The hardest part is not writing the checks; it is discovering which assumptions you forgot to check.
