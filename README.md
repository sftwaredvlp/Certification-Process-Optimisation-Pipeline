# MID Certification Process Analytics

This project simulates a real-world MID (Measuring Instruments Directive)
certification analytics pipeline to identify process bottlenecks and reduce
rework using outcome-based analysis.

## The Problem

Certification bodies processing MID applications face a recurring challenge:
too many submissions fail on first review. Each failure triggers a revision
cycle—additional documentation requests, re-testing, and re-audits—adding
weeks to turnaround time and consuming auditor capacity.

This project analyses a multi-year certification dataset to answer:
- Which failure reasons cause the most rework?
- Do certain instrument types or MID modules have worse outcomes?
- Can a pre-submission checklist reduce revision cycles?

## Why This Matters

In regulated industries, certification is a bottleneck. A manufacturer cannot
sell measuring instruments in the EU without MID approval. Delays cost money—
both for manufacturers waiting on market access and for certification bodies
managing audit backlogs.

The data shows:
- Applications that fail initial review take **~24 days longer** on average
- Each revision adds roughly **~14 days** to total turnaround
- **43%** of applications require at least one revision

Small improvements in first-time pass rates translate directly into higher
throughput and reduced operational pressure.

## Data Model

Four tables reflecting typical certification operations:

| Table | Purpose |
|------|---------|
| `clients` | Manufacturer information |
| `applications` | Submissions with instrument type, MID module, risk class |
| `certification_results` | Outcomes: pass/fail, revisions, certification dates |
| `audit_results` | Individual audit events with documented failure reasons |

Relationships:  
`clients` → `applications` → `certification_results` / `audit_results`

The SQLite database includes reporting-ready views:
- `v_application_details` — Flattened application-level view
- `v_failures` — Failed audits with module context
- `v_monthly_throughput` — Certifications completed by month
- `v_module_comparison` — Module B vs Module D performance
- `v_client_performance` — Client-level success metrics

## Key KPIs

| KPI | Value |
|-----|-------|
| First-time pass rate | 56.8% |
| Avg turnaround (overall) | 49.8 days |
| Avg turnaround (pass first time) | 39.2 days |
| Avg turnaround (with revisions) | 63.7 days |
| Revision rate | 43.2% |

## Key Findings

### Module B vs Module D

| Metric | Module B | Module D |
|------|----------|----------|
| First-time pass rate | 51.4% | 68.1% |
| Avg revisions | 0.92 | 0.50 |

Module B (Type Examination) shows significantly worse outcomes, driven by
technical documentation issues such as incomplete technical files and test
report gaps.

Module D (Production Quality Assurance) failures are primarily QMS-related,
including training records, internal audits, and management reviews.

### Revision Impact

Each revision cycle adds approximately **14 days** to turnaround time.

Preventing even a single revision per application results in measurable
throughput gains.

### Top Failure Reasons

Top three failure categories account for **48%** of all failures:
- Technical file incomplete
- Documentation inconsistencies
- Test report gaps

## Pre-Audit Checklist

Based on historical failure patterns, a prioritised pre-audit checklist was
derived.

**Module B (Type Examination)**
- Technical file completeness
- Document consistency
- Test report coverage
- Clear metrological definitions

**Module D (Production QA)**
- Management review records
- Training documentation
- NCR handling and closure
- Internal audit execution

**Projected impact**: Preventing 50% of top failure causes increases first-time
pass rate from ~57% to ~72%.

Checklist items are exported to `checklist_items.csv` for integration with
submission portals or client guidance.

## Tech Stack

Python, Pandas, SQLite, CSV  
Runs locally with no external APIs or cloud dependencies.

## Project Structure

(unchanged)

## Limitations

- Data is synthetic (realistic distributions, not real clients)
- Sample size: ~300 applications
- Impact estimates are projections

The analytical logic mirrors real certification workflows.

## Next Steps

- Power BI dashboard integration
- Client-level risk scoring
- Measuring checklist adoption impact
