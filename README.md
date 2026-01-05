# MID Certification Process Analytics

Analytics pipeline for optimising MID (Measuring Instruments Directive) certification processes.

## Background

This project analyses historical certification data to identify improvement opportunities in:
- First-time certification success rates
- Turnaround times  
- Rework cycles (revisions)
- Common failure patterns

The data model reflects typical certification body operations processing meters and measuring instruments under MID Module B (Type Examination) and Module D (Production QA).

## Project Structure

```
├── data/
│   └── raw/           # Source CSV files
├── scripts/
│   ├── 01_generate_mock_data.py      # Creates synthetic dataset
│   └── 02_data_quality_checks.py     # Validates data integrity
└── README.md
```

## Data Model

Four tables linked by application_id:

- **clients** - Manufacturer dimension (company info, size, sector)
- **applications** - Central fact table (instrument type, MID module, risk class)
- **certification_results** - Outcomes (pass/fail, revisions, dates)
- **audit_results** - Audit events with failure reasons

## Usage

```bash
# Generate mock data
python scripts/01_generate_mock_data.py

# Run quality checks
python scripts/02_data_quality_checks.py
```

## Tech Stack

- Python (Pandas)
- SQLite (coming)
- Power BI-ready outputs
