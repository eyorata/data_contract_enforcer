# DOMAIN_NOTES.md - Week 7 Data Contract Enforcer

## Context From My Repo (Evidence)
I am grounding these notes in what exists in this repo today (2026-04-01). The actual data outputs currently present are:
- `outputs/week3/extractions.jsonl`: 50 records, 62 extracted_facts confidence values. Observed confidence min 0.8250199039555977, max 0.9934669496204055, mean 0.905219. Processing time min 85557 ms, max 343487 ms. Entity types observed: DATE, LOCATION, PERSON.
- `outputs/week5/events.jsonl`: 1847 records, 34 distinct event_type values, aggregate_type values include LoanApplication, DocumentPackage, CreditRecord, FraudScreening, ComplianceRecord, AgentSession. Sequence numbers are monotonic per aggregate in the current file.
- `outputs/week1/intent_records.jsonl`: 0 lines.
- `outputs/week4/lineage_snapshots.jsonl`: 0 lines.

Because Week 1 and Week 4 outputs are empty in this repo, I use the canonical schemas defined in the Week 7 prompt for those weeks, and I use concrete observed values from Week 3 and Week 5 outputs above.

---

## 1) Backward-Compatible vs Breaking Schema Changes (with 3 examples each)

**Definition difference**
- Backward-compatible change: a new producer can write the new schema and existing consumers continue to work without code changes.
- Breaking change: existing consumers either fail or (worse) continue to run but produce incorrect results.

**Backward-compatible examples (based on my Week 1-5 schemas)**
1) **Week 5 event_record payload - add optional field**  
Schema: Week 5 event_record defines `payload` as an object. In my actual data, `ApplicationSubmitted` payload includes fields like `loan_term_months` and `submission_channel`. A backward-compatible change is adding a new optional field like `marketing_campaign` in the payload. Existing consumers that parse known keys continue to work. This is backward-compatible because it only adds data.
2) **Week 3 extraction_record entities - add optional `alias`**  
Schema: Week 3 defines `entities[]` with `entity_id`, `name`, `type`, `canonical_value`. Adding a new optional field `alias` or `source_span` does not break readers that only use the existing fields.
3) **Week 4 lineage_snapshot nodes metadata - add optional metadata key**  
Schema: Week 4 nodes have a `metadata` object. Adding a new optional `metadata.owner_team` field is backward-compatible because existing readers can ignore unknown keys in `metadata`.

**Breaking change examples (based on my Week 1-5 schemas)**
1) **Week 3 extracted_facts.confidence scale change**  
Schema: Week 3 `extracted_facts[].confidence` is defined as float in range 0.0 to 1.0. If a producer changes to integer 0-100, downstream logic that treats confidence as a probability will silently mis-rank or filter. This is a breaking change and also a statistical drift. This is directly relevant to my current outputs, where confidence values are in 0.825-0.993.
2) **Week 5 recorded_at type change**  
Schema: Week 5 `recorded_at` is an ISO 8601 timestamp string. If it changes to an integer epoch or null, any consumer that compares `recorded_at >= occurred_at` will fail or mis-order events. This breaks ordering logic and violates the contract.
3) **Week 1 code_refs shape change**  
Schema: Week 1 `code_refs` is an array with required keys `file`, `line_start`, `line_end`, `symbol`, `confidence`. If a producer changes `code_refs` from array to a single object or removes `line_start`, any consumer that expects an iterable or uses `line_start` for blame will error. This is breaking because data structure changes.

---

## 2) Week 3 confidence 0.0-1.0 to 0-100: Failure Path and Bitol Clause

**Failure path (Week 3 -> Week 4 Cartographer)**  
In Week 3, the Document Refinery produces extracted facts with `confidence` in 0.0-1.0. In my outputs, I see a mean confidence of ~0.905219, which is clearly in that scale. Week 4 Cartographer consumes extracted facts and turns them into lineage node metadata and edges. A typical use is to weight or filter nodes by confidence when deciding which facts become node metadata or which relationships are surfaced.  
If Week 3 changes confidence to 0-100, the Cartographer will interpret a confidence of 85 as 85.0, i.e., far above any threshold designed for 0.0-1.0. That means low-quality facts will be treated as high-quality. The system "still runs" but produces wrong lineage, which is a silent corruption.  
The failure appears as inflated confidence and a flood of low-quality nodes and edges. Downstream consumers relying on lineage (Week 7 ViolationAttributor) will attribute failures to the wrong sources, because the lineage graph is polluted.

**Bitol YAML clause to catch it**
```yaml
schema:
  extracted_facts:
    type: array
    items:
      confidence:
        type: number
        minimum: 0.0
        maximum: 1.0
        required: true
quality:
  type: SodaChecks
  specification:
    checks for extractions:
      - min(confidence) >= 0.0
      - max(confidence) <= 1.0
```
This clause makes the 0-100 change a failing condition on the first run after the change.

---

## 3) How the Enforcer Uses the Week 4 Lineage Graph to Build a Blame Chain

Even though `outputs/week4/lineage_snapshots.jsonl` is empty in my repo right now, the canonical schema is clear and I will implement the traversal based on it. The Data Contract Enforcer uses the Week 4 lineage graph as follows:

1) **Identify failing schema element**  
Example: `week3.extracted_facts.confidence.range` fails. The failing element is `extracted_facts.confidence` in the Week 3 dataset.
2) **Map failing element to a lineage node**  
In the Week 4 schema, a node has `node_id`, `type`, and `metadata.path`. The Enforcer maps the dataset to a node, e.g., `file::outputs/week3/extractions.jsonl` or the producing pipeline node.
3) **Traverse upstream with BFS**  
Using edges where `relationship` is PRODUCES or WRITES, the Enforcer walks upstream to identify the producer of that field. BFS is used to find the closest upstream producer first. If multiple upstream nodes exist, BFS provides the shortest lineage hop count.
4) **Stop at boundary**  
Traversal stops at the first external boundary or repo root. For example, if a node is `file::src/week3/extractor.py`, it is a natural boundary for blame.
5) **Attach code blame**  
For each upstream file node, the Enforcer runs `git log --follow` and targeted `git blame` to find commits that touched the lines producing `confidence`. These become blame chain candidates.
6) **Score confidence**  
The confidence score is computed based on recency and lineage distance (the required formula is base 1.0 minus 0.1 per day and minus 0.2 per hop). This yields a ranked list of likely causes.

This process is precise because the lineage graph already contains relationships among files and outputs. The BFS traversal makes the blame chain explainable: "this field failed, these edges connect it to this file, and this commit changed it."

---

## 4) Bitol Contract for LangSmith trace_record (structural + statistical + AI-specific)

Below is a Bitol-compatible YAML contract for the trace_record schema. It includes:
- Structural clauses: required fields and enum for run_type.
- Statistical clause: total_tokens equals prompt_tokens + completion_tokens.
- AI-specific clause: if error is not null, then outputs should be empty (a basic AI pipeline rule).

```yaml
kind: DataContract
apiVersion: v3.0.0
id: langsmith-traces
info:
  title: LangSmith Trace Records
  version: 1.0.0
  owner: ai-observability
servers:
  local:
    type: local
    path: outputs/traces/runs.jsonl
    format: jsonl
schema:
  id:
    type: string
    format: uuid
    required: true
  run_type:
    type: string
    enum: [llm, chain, tool, retriever, embedding]
    required: true
  start_time:
    type: string
    format: date-time
    required: true
  end_time:
    type: string
    format: date-time
    required: true
  total_tokens:
    type: integer
    minimum: 0
    required: true
  prompt_tokens:
    type: integer
    minimum: 0
    required: true
  completion_tokens:
    type: integer
    minimum: 0
    required: true
quality:
  type: SodaChecks
  specification:
    checks for traces:
      - missing_count(id) = 0
      - end_time > start_time
      - total_tokens = prompt_tokens + completion_tokens
      - failed_records(error != null AND outputs IS NOT NULL) = 0
```

This captures structural correctness, a numeric consistency check, and an AI-specific behavior rule for error handling.

---

## 5) Most Common Failure Mode of Contract Enforcement and How My Architecture Prevents It

**Most common failure mode: contracts go stale.**  
In production, contracts drift away from reality because teams add fields, change types, or shift distributions without updating the contract. This happens when contract generation is a one-time exercise and there is no enforcement loop. Another frequent failure mode is partial enforcement: only structural checks run, while statistical drifts (like the confidence 0-100 change) go unnoticed. Both lead to "green" status with real data quality failures.

**Why contracts get stale**
- They are not regenerated automatically.
- There is no baseline snapshot to compare against.
- The enforcement tool is not integrated into CI or daily runs.
- The lineage graph is missing or out of date, so blast radius is not computed and teams ignore failures.

**How my architecture prevents staleness**
1) **Auto-generation on real data**  
`contracts/generator.py` reads the JSONL outputs and produces contracts on demand. This ensures contracts are always based on current output schema, not stale documentation.
2) **Schema snapshots**  
On each generation run, the inferred schema can be written to a timestamped snapshot under `schema_snapshots/`. This enables diffs and establishes when a change occurred.
3) **ValidationRunner in a loop**  
`contracts/runner.py` produces a structured report with pass/fail counts. This is designed to run continuously (daily or in CI) to detect regressions quickly.
4) **Lineage-aware attribution**  
Even though my Week 4 outputs are empty right now, the planned design uses the lineage graph to identify upstream producers. This is how the Enforcer makes violations actionable instead of just noisy.

**Concrete evidence in my repo**  
The absence of Week 1 and Week 4 outputs is itself a staleness risk: without a lineage snapshot, I cannot compute a real blast radius for Week 3 failures. The architecture fixes this by requiring `outputs/week4/lineage_snapshots.jsonl` as a hard dependency for attribution, making the missing lineage data a visible blocking issue instead of a hidden failure.

---

## Summary
These notes are grounded in the real Week 3 and Week 5 data in this repo and the canonical schemas for Weeks 1-4. The core risk is silent corruption from schema drift. The contract clauses and enforcement pipeline described here are designed to make those changes loud, attributable, and actionable.
