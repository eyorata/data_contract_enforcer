# Final Report — Week 7 Data Contract Enforcer

## 1) Auto‑Generated Enforcer Report (Evidence + Summary)
**Evidence of machine generation**
- Auto‑generated PDF: `enforcer_report/report_20260404.pdf`
- Auto‑generated data: `enforcer_report/report_data.json`
- Built from live `validation_reports/` and `violation_log/violations.jsonl` via `contracts/report_generator.py`.

**Data Health Score (with calculation)**
- From `enforcer_report/report_data.json`:
  - Total checks = **86**
  - Passed = **74**
  - Failed = **4**
  - CRITICAL fails = **2**
- Calculation: `(74 / 86) × 100 = 86.05`
  - Adjusted by `20 × CRITICAL fails (2)` → **46.05**
- Reported score in file: **46.0** (rounded).

**Required sections present**
1. Data Health Score
2. Violations by Severity
3. Schema Changes Detected
4. AI System Risk Assessment
5. Recommended Actions (prioritized)

**Actionability (examples from report output)**
- “Fix `week3.extracted_facts.confidence.range`: update `src/week3/extractor.py` so field `extracted_facts[*].confidence` conforms to contract `week3-document-refinery-extractions` clause `week3.extracted_facts.confidence.range`.”
- “Fix `week5.event_type.registry`: update `src/week5/event_emitter.py` so field `event_type` conforms to contract `week5-event-sourcing-events` clause `week5.event_type.registry`.”

---

## 2) Validation Run Results (Clause‑Level Summary)
**Evidence**
- `validation_reports/violated.json`
- `validation_reports/week1_20260404.json`
- `validation_reports/week5_20260404.json`

**Failure 1 — Week 3 (CRITICAL structural)**
- Check: `week3.extracted_facts.confidence.range`
- Field: `extracted_facts[*].confidence`
- Expected: `min>=0.0, max<=1.0`
- Actual: **violated data contains 0–100 scale** (10 records failing)
- Severity: **CRITICAL**
- Plain language: Confidence values were written in percent, which breaks the 0.0–1.0 contract and corrupts downstream ranking.

**Failure 2 — Week 3 (CRITICAL statistical drift)**
- Check: `week3-document-refinery-extractions.extracted_facts[*].confidence.drift`
- Field: `extracted_facts[*].confidence`
- Expected: baseline mean ± 3 stddev
- Actual: mean shift beyond 3 stddev (baseline mismatch)
- Severity: **CRITICAL**
- Plain language: Even if types pass, the distribution shifted sharply, indicating silent corruption.

**Failure 3 — Week 5 (HIGH structural)**
- Check: `week5.event_type.registry`
- Field: `event_type`
- Expected: event_type registered in schema registry list
- Actual: **unregistered event types present** (291 records failing)
- Severity: **HIGH**

**Downstream impact (named consumers)**
- Week 3 confidence failures affect:
  - Direct: `week4-cartographer`, `week5-event-sourcing`
  - Transitive: `file::outputs/week4/lineage_snapshots.jsonl`, `file::outputs/week5/events.jsonl`
- Week 5 registry failures affect:
  - Direct: `week7-schema-contract`

---

## 3) Violation Deep‑Dive — Blame Chain + Blast Radius
**Selected violation:** `week3.extracted_facts.confidence.range`

**Failing check**
- Contract: `week3-document-refinery-extractions`
- Field: `extracted_facts[*].confidence`
- Severity: **CRITICAL**

**Lineage traversal (step‑by‑step)**
1. Start at failing dataset: `file::outputs/week3/extractions.jsonl`
2. BFS upstream via lineage `reverse_adj` edges
3. Upstream producers found:
   - `pipeline::week3-document-refinery` → `src/week3/extractor.py`
   - `pipeline::week1-intent-classifier` → `src/week1/classifier.py`

**Blame chain (ranked candidates)**
- Rank 1:
  - Commit: `2954f933009a62c5aaf905b512129b62a7dc92d9`
  - Author: `eyuel.nebiyu@gmail.com`
  - File: `src/week3/extractor.py`
  - Confidence score: **0.8**
- Rank 2:
  - Commit: `2954f933009a62c5aaf905b512129b62a7dc92d9`
  - Author: `eyuel.nebiyu@gmail.com`
  - File: `src/week1/classifier.py`
  - Confidence score: **0.4**

**Scoring formula**
`base = 1.0 − (days_since_commit × 0.1)` and `−0.2 per lineage hop`

**Blast radius**
- Direct subscribers:
  - `week4-cartographer`
  - `week5-event-sourcing`
- Transitive contamination (with depth):
  - `file::outputs/week4/lineage_snapshots.jsonl` (depth 1)
  - `pipeline::week5-event-sourcing` (depth 2)
  - `file::outputs/week5/events.jsonl` (depth 3)

**Attribution confidence**
- **Moderate confidence** (recent commit, ranked by formula; multiple lineage hops reduce score).

---

## 4) Schema Evolution Case Study
**Evidence:**
- `validation_reports/schema_evolution_week3.json`
- `validation_reports/migration_impact_week3-document-refinery-extractions_20260404.json`

**Before/After Diff (human‑readable)**
- **Before:** `extracted_facts.items.confidence` → type `number`, max `1.0`
- **After:** `extracted_facts.items.confidence` → type `integer`, max `100`

**Taxonomy classification**
- **Narrow type (number → integer)** — **BREAKING, CRITICAL**
- **Constraint change (max 1.0 → 100)** — **BREAKING, CRITICAL**

**Migration impact (from report)**
- Affected subscribers:
  - `week4-cartographer` (ranking logic depends on confidence scale)
  - `week7-enforcer` (AI drift baselines depend on confidence)
- Required actions (examples):
  1. Re‑establish baselines for confidence after migration.
  2. Notify all subscribers and re‑run validation.

**Rollback plan**
- Revert to the previous schema snapshot.
- Restore baselines in `schema_snapshots/baselines.json`.
- Notify all subscribers of rollback.

**Production comparison**
- A registry like Confluent would block this at schema registration.
- Our analyzer catches it post‑hoc and generates a migration + rollback plan.

---

## 5) AI Contract Extension Results
**Evidence:** `validation_reports/ai_extensions.json`

**Embedding drift**
- Drift score: **0.0**
- Threshold: **0.15**
- Method: cosine distance between centroids
- Status: **PASS**

**Prompt input validation**
- Total records: **50**
- Valid: **50**
- Quarantined: **0**
- Status: **PASS**

**LLM output schema violation rate**
- Total outputs: **100**
- Violations: **0**
- Violation rate: **0.0%**
- Trend: **stable** (baseline persisted across runs)

**Warnings (explain cause)**
- Trace error rate: **26.0%** (WARN) — likely due to retry/test runs in trace export
- Trace latency: **mean 7320.5ms, p95 14133ms** (WARN) — suggests slow tool chains or large prompt sizes

**Conclusion**
- AI outputs are structurally valid, but operational reliability is degraded. Monitoring and throttling are recommended.

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
- Upgrade validation mode for event stream to **ENFORCE** in `contract_registry/subscriptions.yaml`
