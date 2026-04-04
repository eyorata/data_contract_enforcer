# Data Contract Enforcer - Week 7

This repo implements the Week 7 Data Contract Enforcer. Below is a recipe-style runbook aligned with the Day‑3 checklist.

## Prereqs
- Python 3.10+
- Install dependencies:
  - `pip install pyyaml pandas reportlab numpy`

## Environment
Create a `.env` file (already scaffolded) and add your keys/URLs:
- `ANTHROPIC_API_KEY` for optional LLM annotations in `contracts/generator.py --llm`
- `LM_STUDIO_EMBEDDINGS_URL` and `LM_STUDIO_EMBEDDINGS_MODEL` for local embeddings
- `EMBEDDINGS_LOCAL_ONLY=true` to force local embeddings only

## Inputs
Required input data is already included under `outputs/`.

## Step 1: Generate contracts (two systems minimum)
Generate Bitol-style contracts and dbt schema files (Week 3 + Week 5).

```bash
python contracts/generator.py --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml --output generated_contracts/

python contracts/generator.py --source outputs/week5/events.jsonl \
  --contract-id week5-event-sourcing-events \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml --output generated_contracts/
```

Expected outputs:
- `generated_contracts/week3_extractions.yaml`
- `generated_contracts/week3_extractions_dbt.yml`
- `generated_contracts/week5_events.yaml`
- `generated_contracts/week5_events_dbt.yml`
- `schema_snapshots/week3-document-refinery-extractions/<timestamp>.yaml`
- `schema_snapshots/week5-event-sourcing-events/<timestamp>.yaml`

## Step 2: Baseline validation on clean data
Run contract checks and emit a structured validation report.

```bash
python contracts/runner.py --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl --mode AUDIT \
  --output validation_reports/clean.json
```

Expected output:
- `validation_reports/clean.json`
- `schema_snapshots/baselines.json` updated

## Step 3: Inject violation and validate again
```bash
python create_violation.py
python contracts/runner.py --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl --mode ENFORCE \
  --output validation_reports/violated.json
```

Expected output:
- `validation_reports/violated.json`

## Step 4: Attribute the violation
Produce a blame chain for a failing check using lineage + git context.

```bash
python contracts/attributor.py \
  --violation validation_reports/violated.json \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml \
  --output violation_log/violations.jsonl
```

Expected output:
- `violation_log/violations.jsonl` (appended)

## Step 5: Run schema evolution analysis
Diff two snapshots and classify breaking changes.

```bash
python contracts/schema_analyzer.py --contract-id week3-document-refinery-extractions --since "7 days ago" --output validation_reports/schema_evolution_week3.json
```

Expected output:
- `validation_reports/schema_evolution_week3.json`

## Step 6: Run AI extensions
Run embedding drift, prompt input validation, and LLM output schema checks.

```bash
python contracts/ai_extensions.py \
  --extractions outputs/week3/extractions.jsonl \
  --verdicts outputs/week2/verdicts.jsonl \
  --output validation_reports/ai_extensions.json
```

Expected output:
- `validation_reports/ai_extensions.json`

## Step 7: Generate the Enforcer report
Generate the Enforcer report (PDF + JSON) from live data.

```bash
python contracts/report_generator.py --output enforcer_report/report_20260404.pdf
```

Expected outputs:
- `enforcer_report/report_20260404.pdf`
- `enforcer_report/report_20260404.json`
- `enforcer_report/report_data.json`

## Verification
Open `enforcer_report/report_data.json` and confirm:
- `data_health_score` is between 0 and 100
- Recommended actions reference real file paths in this repo

## Notes
- `violation_log/violations.jsonl` contains real and injected violations (injection documented in the first comment line).
- `schema_snapshots/` contains at least two snapshots per contract for evolution tracking.
