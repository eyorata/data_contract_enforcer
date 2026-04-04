# Data Contract Enforcer - Week 7

This repo implements the Week 7 Data Contract Enforcer. Below is a quick-start for evaluators to run every required entry point end-to-end on a fresh clone.

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

## 1) ContractGenerator
Generate Bitol-style contracts and dbt schema files.

```bash
python contracts/generator.py --source outputs/week3/extractions.jsonl --output generated_contracts
python contracts/generator.py --source outputs/week5/events.jsonl --output generated_contracts
```

Expected outputs:
- `generated_contracts/week3_extractions.yaml`
- `generated_contracts/week3_extractions_dbt.yml`
- `generated_contracts/week5_events.yaml`
- `generated_contracts/week5_events_dbt.yml`

## 2) ValidationRunner
Run contract checks and emit a structured validation report.

```bash
python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions.jsonl
```

Expected output:
- `validation_reports/week3-document-refinery-extractions_<timestamp>.json`

## 3) ViolationAttributor
Produce a blame chain for a failing check using lineage + git context.

```bash
python contracts/attributor.py --violation-log violation_log/violations.jsonl --output violation_log/violations_attributed.jsonl
```

Expected output:
- `violation_log/violations_attributed.jsonl`

## 4) SchemaEvolutionAnalyzer
Diff two snapshots and classify breaking changes.

```bash
python contracts/schema_analyzer.py --contract-id week3-document-refinery-extractions --since "7 days ago" --output validation_reports/schema_evolution_week3.json
```

Expected output:
- `validation_reports/schema_evolution_week3.json`

## 5) AI Extensions
Run embedding drift, prompt input validation, and LLM output schema checks.

```bash
python contracts/ai_extensions.py
```

Expected output:
- `validation_reports/ai_extensions.json`

## 6) ReportGenerator
Generate the Enforcer report (PDF + JSON) from live data.

```bash
python contracts/report_generator.py --output enforcer_report/report_20260404.pdf
```

Expected outputs:
- `enforcer_report/report_20260404.pdf`
- `enforcer_report/report_20260404.json`
- `enforcer_report/report_data.json`

## Notes
- `violation_log/violations.jsonl` contains real and injected violations (injection documented in the first comment line).
- `schema_snapshots/` contains at least two snapshots per contract for evolution tracking.
