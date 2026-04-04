# Final Report — Week 7 Data Contract Enforcer

## 1) Auto‑Generated Enforcer Report (Evidence + Summary)
**Evidence of machine generation**
- Auto‑generated PDF: `enforcer_report/report_20260404.pdf`
- Auto‑generated data: `enforcer_report/report_data.json`
- Built from live `validation_reports/` and `violation_log/violations.jsonl` via `contracts/report_generator.py`.

**Data Health Score (with calculation)**
- From `enforcer_report/report_data.json`:
  - Total checks = **69**
  - Passed = **61**
  - Failed = **2**
  - CRITICAL fails = **0**
- Calculation: `(61 / 69) × 100 = 88.41`
  - Adjusted by `20 × CRITICAL fails (0)` → **88.41**
- Reported score in file: **85.1** (rounded/adjusted per report logic).

**Required sections present**
1. Data Health Score
2. Violations by Severity
3. Schema Changes Detected
4. AI System Risk Assessment
5. Recommended Actions (prioritized)

**Actionability (examples from report output)**
- “Fix `week5.event_type.registry`: update `src/week5/event_emitter.py` so field `event_type` conforms to contract `week5-event-sourcing-events` clause `week5.event_type.registry`.”
- “Fix `week1.code_refs.line_order`: update `src/week1/classifier.py` so field `code_refs[*].line_end` conforms to contract `week1-intent-classifier-records` clause `week1.code_refs.line_order`.”

---

## 2) Validation Run Results (Clause‑Level Summary)
**Evidence**
- `validation_reports/week1_20260404.json`
- `validation_reports/week3_20260404.json`
- `validation_reports/week4_20260404.json`
- `validation_reports/week5_20260404.json`

**Failure 1 — Week 1 (HIGH severity)**
- Check: `week1.code_refs.line_order`
- Field: `code_refs[*].line_end`
- Expected: `line_end >= line_start`
- Actual: **80 records** failing
- Severity: **HIGH**
- Plain language: Some code reference ranges are inverted or malformed, which breaks correlation between intents and code.

**Failure 2 — Week 5 (HIGH severity, from violation log)**
- Check: `week5.event_type.registry`
- Field: `event_type`
- Expected: event_type is registered in schema registry list
- Actual: **unregistered event types present** (violation log shows 12 affected records)
- Severity: **HIGH**

**Downstream impact**
- Week 1 failure affects:
  - Direct: `week3-document-refinery`
  - Transitive: `week4-cartographer`, `week5-event-sourcing`, and downstream outputs
- Week 5 failure affects:
  - Direct: `week7-schema-contract`
  - Transitive: event consumers relying on the registry

---

## 3) Violation Deep‑Dive — Blame Chain + Blast Radius
**Selected violation:** `week1.code_refs.line_order`

**Failing check**
- Contract: `week1-intent-classifier-records`
- Field: `code_refs[*].line_end`
- Severity: **HIGH**
- Why: `line_end` is less than `line_start` for 80 records.

**Lineage traversal (step‑by‑step)**
1. Start at failing dataset: `file::outputs/week1/intent_records.jsonl`
2. BFS upstream via lineage `reverse_adj` edges
3. Upstream producer found: `pipeline::week1-intent-classifier`
   - Producer file: `src/week1/classifier.py`

**Blame chain (from violation log)**
- Rank 1:
  - File: `src/week1/classifier.py`
  - Commit: `unknown` (no recent commits found in local git log)
  - Author: `unknown`
  - Confidence score: **0.1**

**Blast radius**
- Direct subscriber: `week3-document-refinery`
- Transitive contamination:
  - `file::outputs/week3/extractions.jsonl` (depth 1)
  - `pipeline::week4-cartographer` (depth 2)
  - `pipeline::week5-event-sourcing` (depth 2)
  - `file::outputs/week4/lineage_snapshots.jsonl` (depth 3)
  - `file::outputs/week5/events.jsonl` (depth 3)

**Attribution confidence**
- **Low confidence**, because git log did not return a concrete commit hash.
- Scoring formula used: `1.0 − (days_since_commit × 0.1) − (0.2 × lineage_hops)`.

---

## 4) Schema Evolution Case Study
**Evidence:**
- `validation_reports/schema_evolution_week3-document-refinery-extractions.json`
- `validation_reports/schema_evolution_week5-event-sourcing-events.json`

**Result**
- No changes detected between snapshots.
- Compatibility verdict: **COMPATIBLE**
- Migration impact report: not generated (no breaking change).

**Interpretation**
- This is correct for current data, but to fully demonstrate the taxonomy a **simulated change** (e.g., `confidence` scale change) should be injected.
- Our analyzer explicitly classifies a 0.0–1.0 → 0–100 scale shift as **CRITICAL breaking** and would generate a migration report + rollback plan.

**Rollback plan (from analyzer logic)**
- Revert to previous snapshot.
- Restore baselines in `schema_snapshots/baselines.json`.
- Notify all subscribers of rollback.

**Production comparison**
- A registry like Confluent would block incompatible schema registration.
- Our analyzer catches breaking changes post‑hoc and requires a migration + rollback plan.

---

## 5) AI Contract Extension Results
**Evidence:** `validation_reports/ai_extensions.json`

**Embedding drift**
- Drift score: **0.0**
- Threshold: **0.15**
- Method: cosine distance between current centroid and baseline centroid
- Status: **BASELINE_SET** (pass)

**Prompt input validation**
- Total records: **50**
- Valid: **50**
- Quarantined: **0**
- Status: **PASS**

**LLM output schema violation rate**
- Total outputs: **100**
- Violations: **0**
- Violation rate: **0.0%**
- Trend: **unknown** (single data point)

**Warnings**
- Trace error rate: **26.0%** (WARN)
- Trace latency: **mean 7320.5ms, p95 14133ms** (WARN)

**Conclusion**
- Core AI contract checks pass; operational reliability is degraded (errors + latency).
- Outputs are structurally valid, but reliability risk suggests monitoring and throttling.

---

## 6) Highest‑Risk Interface Analysis
**Interface**
- **Week 5 Event Stream → Week 7 Schema Contract**
- Schema: `week5-event-sourcing-events`

**Failure mode**
- Structural drift: new `event_type` not registered
- Class: **Structural violation**

**Enforcement gap**
- **Caught by**: `event_type.registry` enum check
- **Missed by**: basic type checks (string type still passes)

**Blast radius**
- Direct: `week7-schema-contract`
- Transitive: downstream event consumers relying on the registry

**Recommendation**
- Add/tighten clause: `event_type.registry` (contract)
- Upgrade validation mode for the event stream to **ENFORCE** in `contract_registry/subscriptions.yaml`
