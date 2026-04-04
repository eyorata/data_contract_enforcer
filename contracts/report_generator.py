"""
EnforcerReport — Phase 4B

Generates a stakeholder-facing PDF report with:
  1. Data Health Score (0-100)
  2. Violations this week (by severity)
  3. Schema changes detected
  4. AI system risk assessment
  5. Recommended actions

Usage:
  python contracts/report_generator.py \
    --output enforcer_report/report_20260401.pdf
"""

import argparse
import glob
import json
import os
from datetime import datetime, timezone

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
)


def iso_now():
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def load_json(path):
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)


def load_all_validation_reports(dir_path="validation_reports"):
    """Load all validation report JSONs."""
    reports = []
    if not os.path.exists(dir_path):
        return reports
    for fpath in sorted(glob.glob(os.path.join(dir_path, "*.json"))):
        fname = os.path.basename(fpath)
        # Skip non-validation reports
        if fname.startswith("schema_evolution"):
            continue
        if fname.startswith("migration_impact"):
            continue
        if fname.startswith("ai_extensions"):
            continue
        try:
            reports.append(load_json(fpath))
        except Exception:
            pass
    return reports


def load_ai_report(path="validation_reports/ai_extensions.json"):
    if os.path.exists(path):
        return load_json(path)
    return None


def load_schema_evolution_reports(dir_path="validation_reports"):
    """Load all schema evolution reports."""
    reports = []
    if not os.path.exists(dir_path):
        return reports
    for fpath in sorted(
        glob.glob(os.path.join(dir_path, "schema_evolution_*.json"))
    ):
        try:
            reports.append(load_json(fpath))
        except Exception:
            pass
    return reports


def load_violation_log(path="violation_log/violations.jsonl"):
    if not os.path.exists(path):
        return []
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip().lstrip("\ufeff")
            if not line:
                continue
            if line.startswith("#"):
                continue
            if line:
                records.append(json.loads(line))
    return records


# ── Data Health Score ────────────────────────────────────────────────────────

def compute_health_score(validation_reports, ai_report):
    """
    Formula: (checks_passed / total_checks) * 100
    Adjusted down by 20 points per CRITICAL violation.
    """
    total_checks = 0
    passed = 0
    critical_failures = 0

    for report in validation_reports:
        total_checks += report.get("total_checks", 0)
        passed += report.get("passed", 0)
        for r in report.get("results", []):
            if (
                r.get("status") == "FAIL"
                and r.get("severity") == "CRITICAL"
            ):
                critical_failures += 1

    if ai_report:
        total_checks += ai_report.get("total_checks", 0)
        passed += ai_report.get("passed", 0)

    if total_checks == 0:
        return 0, "No checks executed"

    base_score = (passed / total_checks) * 100
    adjusted = max(0, base_score - (critical_failures * 20))

    if adjusted >= 90:
        narrative = "Data health is excellent. All critical systems are operating within contract bounds."
    elif adjusted >= 70:
        narrative = "Data health is good with minor issues. Some non-critical violations detected."
    elif adjusted >= 50:
        narrative = "Data health needs attention. Multiple violations detected across systems."
    else:
        narrative = "Data health is critical. Immediate action required on failing contracts."

    return round(adjusted, 1), narrative


# ── Build PDF ────────────────────────────────────────────────────────────────

def build_pdf(output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "Title2", parent=styles["Title"],
        fontSize=20, spaceAfter=20,
        textColor=colors.HexColor("#1A237E"),
    )
    h1 = ParagraphStyle(
        "H1a", parent=styles["Heading1"],
        fontSize=16, spaceBefore=16, spaceAfter=8,
        textColor=colors.HexColor("#283593"),
    )
    h2 = ParagraphStyle(
        "H2a", parent=styles["Heading2"],
        fontSize=13, spaceBefore=12, spaceAfter=6,
        textColor=colors.HexColor("#37474F"),
    )
    body = ParagraphStyle(
        "Body2", parent=styles["Normal"],
        fontSize=10, leading=14, spaceAfter=8,
    )
    body_small = ParagraphStyle(
        "BodySm", parent=styles["Normal"],
        fontSize=9, leading=12, spaceAfter=6,
        textColor=colors.HexColor("#444444"),
    )

    story = []

    # Load all data
    val_reports = load_all_validation_reports()
    ai_report = load_ai_report()
    schema_reports = load_schema_evolution_reports()
    violations = load_violation_log()
    violation_map = {}
    for v in violations:
        key = (v.get("contract_id"), v.get("check_id"))
        violation_map[key] = v

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── Title ──
    story.append(Paragraph(
        "Data Contract Enforcer — Weekly Report", title_style
    ))
    story.append(Paragraph(
        f"Generated: {iso_now()} &nbsp;|&nbsp; Report Date: {today}",
        body_small,
    ))
    story.append(Spacer(1, 16))

    # ── Section 1: Data Health Score ──
    story.append(Paragraph("1. Data Health Score", h1))
    score, narrative = compute_health_score(
        val_reports, ai_report
    )

    if score >= 80:
        score_color = "#2E7D32"
    elif score >= 60:
        score_color = "#E65100"
    else:
        score_color = "#C62828"

    story.append(Paragraph(
        f"<font size=36 color='{score_color}'>"
        f"<b>{score}</b></font>"
        f"<font size=14> / 100</font>",
        body,
    ))
    story.append(Paragraph(narrative, body))

    # Summary table
    total_c = sum(r.get("total_checks", 0) for r in val_reports)
    total_p = sum(r.get("passed", 0) for r in val_reports)
    total_f = sum(r.get("failed", 0) for r in val_reports)
    total_w = sum(r.get("warned", 0) for r in val_reports)
    total_e = sum(r.get("errored", 0) for r in val_reports)

    summary_data = [
        ["Metric", "Value"],
        ["Total Checks", str(total_c)],
        ["Passed", str(total_p)],
        ["Failed", str(total_f)],
        ["Warned", str(total_w)],
        ["Errors", str(total_e)],
        ["Contracts Validated", str(len(val_reports))],
    ]
    tbl = Table(summary_data, colWidths=[2 * inch, 2 * inch])
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#283593")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCCCCC")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.HexColor("#F5F5F5"), colors.white]),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 12))

    # ── Section 2: Violations This Week ──
    story.append(Paragraph("2. Violations This Week", h1))

    all_failures = []
    for report in val_reports:
        cid = report.get("contract_id", "unknown")
        for r in report.get("results", []):
            if r.get("status") in ("FAIL", "WARN", "WARNING"):
                all_failures.append({**r, "contract_id": cid})

    # Count by severity
    severity_counts = {}
    for f in all_failures:
        sev = f.get("severity", "UNKNOWN")
        severity_counts[sev] = severity_counts.get(sev, 0) + 1

    if severity_counts:
        sev_data = [["Severity", "Count"]]
        for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
            if sev in severity_counts:
                sev_data.append([sev, str(severity_counts[sev])])
        tbl_sev = Table(sev_data, colWidths=[2 * inch, 2 * inch])
        tbl_sev.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#C62828")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCCCCC")),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(tbl_sev)
        story.append(Spacer(1, 8))
    else:
        story.append(Paragraph(
            "<b>No violations detected.</b> "
            "All checks passed.", body
        ))

    # Top 3 most significant violations
    critical_first = sorted(
        all_failures,
        key=lambda x: (
            {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
            .get(x.get("severity", "LOW"), 4),
            -x.get("records_failing", 0),
        ),
    )

    if critical_first:
        story.append(Paragraph(
            "<b>Top Violations:</b>", body
        ))
        for i, v in enumerate(critical_first[:3], 1):
            v_key = (v.get("contract_id"), v.get("check_id"))
            vlog = violation_map.get(v_key, {})
            blast = vlog.get("blast_radius", {})
            affected_nodes = blast.get("affected_nodes", [])
            affected_pipes = blast.get("affected_pipelines", [])
            impact = " | ".join(filter(None, [
                f"nodes: {', '.join(affected_nodes[:3])}" if affected_nodes else "",
                f"pipelines: {', '.join(affected_pipes[:3])}" if affected_pipes else "",
            ]))
            desc = (
                f"<b>{i}. [{v.get('severity')}] "
                f"{v.get('check_id', 'unknown')}</b> "
                f"(Contract: {v.get('contract_id', 'unknown')})<br/>"
                f"Failing field: <i>{v.get('column_name', 'N/A')}</i> | "
                f"Failing records: {v.get('records_failing', 0)}<br/>"
                f"Detail: {v.get('message', '')} — "
                f"Actual: {v.get('actual_value', 'N/A')}, "
                f"Expected: {v.get('expected', 'N/A')}<br/>"
                f"Downstream impact: {impact if impact else 'not available'}"
            )
            story.append(Paragraph(desc, body_small))
            story.append(Spacer(1, 4))

    story.append(PageBreak())

    # ── Section 3: Schema Changes ──
    story.append(Paragraph("3. Schema Changes Detected", h1))

    if schema_reports:
        for sr in schema_reports:
            cid = sr.get("contract_id", "unknown")
            verdict = sr.get("compatibility_verdict", "UNKNOWN")
            n_changes = sr.get("total_changes", 0)
            n_breaking = sr.get("breaking_changes", 0)

            if verdict == "NO_CHANGES":
                color = "#2E7D32"
            elif verdict == "COMPATIBLE":
                color = "#1565C0"
            else:
                color = "#C62828"

            story.append(Paragraph(
                f"<b>{cid}</b>: "
                f"<font color='{color}'>{verdict}</font> "
                f"({n_changes} changes, "
                f"{n_breaking} breaking)",
                body,
            ))

            for change in sr.get("changes", [])[:5]:
                story.append(Paragraph(
                    f"&nbsp;&nbsp;• <i>{change.get('field')}</i>: "
                    f"{change.get('change_type')} — "
                    f"{change.get('action', 'Review required')}",
                    body_small,
                ))
    else:
        story.append(Paragraph(
            "No schema evolution reports available. "
            "Run schema_analyzer.py to detect changes.",
            body,
        ))
    story.append(Spacer(1, 12))

    # ── Section 4: AI System Risk Assessment ──
    story.append(Paragraph("4. AI System Risk Assessment", h1))

    if ai_report:
        for r in ai_report.get("results", []):
            cid = r.get("check_id", "unknown")
            status = r.get("status", "UNKNOWN")
            if status == "PASS":
                color = "#2E7D32"
            elif status in ("WARN", "WARNING"):
                color = "#E65100"
            elif status == "FAIL":
                color = "#C62828"
            else:
                color = "#666666"

            story.append(Paragraph(
                f"<b>{cid}</b>: "
                f"<font color='{color}'>{status}</font> — "
                f"{r.get('message', '')}",
                body,
            ))
    else:
        story.append(Paragraph(
            "No AI extension report available. "
            "Run ai_extensions.py first.",
            body,
        ))
    story.append(Spacer(1, 12))

    # ── Section 5: Recommended Actions ──
    story.append(Paragraph("5. Recommended Actions", h1))

    actions = []
    # Generate actions from violations
    for v in critical_first[:3]:
        v_key = (v.get("contract_id"), v.get("check_id"))
        vlog = violation_map.get(v_key, {})
        blame = (vlog.get("blame_chain") or [{}])[0]
        file_path = blame.get("file_path") or "unknown_producer"
        check_id = v.get("check_id", "unknown")
        col = v.get("column_name", "unknown")
        cid = v.get("contract_id", "unknown")
        sev = v.get("severity", "LOW")
        if sev == "CRITICAL":
            priority = "P0"
        elif sev == "HIGH":
            priority = "P1"
        else:
            priority = "P2"

        actions.append({
            "priority": priority,
            "action": (
                f"Fix {check_id}: update `{file_path}` so field "
                f"`{col}` conforms to contract `{cid}` clause `{check_id}`. "
                f"Currently {v.get('records_failing', 0)} "
                f"records failing."
            ),
        })

    # Add schema evolution action if breaking
    for sr in schema_reports:
        if sr.get("breaking_changes", 0) > 0:
            actions.append({
                "priority": "P0",
                "action": (
                    f"Address breaking schema change in "
                    f"{sr.get('contract_id')}: "
                    f"{sr.get('breaking_changes')} breaking "
                    f"change(s) detected. Review migration "
                    f"impact report and notify subscribers."
                ),
            })

    # AI action
    if ai_report:
        for r in ai_report.get("results", []):
            if r.get("status") in ("FAIL", "WARN"):
                actions.append({
                    "priority": "P1",
                    "action": (
                        f"Investigate {r.get('check_id')}: "
                        f"{r.get('message', 'AI check flagged')}"
                    ),
                })

    if not actions:
        actions.append({
            "priority": "P3",
            "action": (
                "No immediate actions required. "
                "Continue monitoring."
            ),
        })

    # Sort by priority
    actions.sort(key=lambda x: x["priority"])

    for i, a in enumerate(actions[:5], 1):
        story.append(Paragraph(
            f"<b>{i}. [{a['priority']}]</b> {a['action']}",
            body,
        ))
        story.append(Spacer(1, 4))

    # ── Build PDF ──
    doc = SimpleDocTemplate(
        output_path, pagesize=letter,
        topMargin=0.6 * inch, bottomMargin=0.6 * inch,
        leftMargin=0.6 * inch, rightMargin=0.6 * inch,
    )
    doc.build(story)
    print(f"Generated Enforcer Report: {output_path}")

    # Also save data as JSON for programmatic access
    json_path = output_path.replace(".pdf", ".json")
    report_data = {
        "generated_at": iso_now(),
        "health_score": score,
        "health_narrative": narrative,
        "total_checks": total_c,
        "passed": total_p,
        "failed": total_f,
        "warned": total_w,
        "errors": total_e,
        "violations_by_severity": severity_counts,
        "top_violations": [
            {
                "check_id": v.get("check_id"),
                "severity": v.get("severity"),
                "contract_id": v.get("contract_id"),
                "records_failing": v.get("records_failing"),
            }
            for v in critical_first[:3]
        ],
        "recommended_actions": actions[:5],
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=2)
    print(f"Generated data file: {json_path}")

    # Required fixed filename for Sunday submission
    required_json_path = os.path.join("enforcer_report", "report_data.json")
    with open(required_json_path, "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=2)
    print(f"Generated data file: {required_json_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate the Enforcer stakeholder report."
    )
    parser.add_argument(
        "--output",
        help="Output PDF path.",
    )
    args = parser.parse_args()

    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    output_path = args.output or os.path.join(
        "enforcer_report", f"report_{today}.pdf"
    )
    build_pdf(output_path)


if __name__ == "__main__":
    main()
