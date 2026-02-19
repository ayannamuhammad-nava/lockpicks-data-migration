# Data Validation Agent - MVP

A lightweight, modular **Data Validation Agent** that builds trust in legacy-to-modern data migrations through automated validation across **pre-migration**, **migration**, and **post-migration** phases.

## Purpose

Detect structural/schema risks before migration, monitor anomalies during migration, and prove integrity after migration completes. Produces evidence artifacts, confidence scores, and uses RAG/metadata to explain schema mismatches.

## Features

- **Pre-migration validation**: Structural & governance checks
- **During-migration monitoring**: Drift & anomaly detection
- **Post-migration reconciliation**: Integrity proof & confidence scoring
- **Evidence artifacts**: Markdown reports, CSV metrics, JSON logs
- **RAG-powered explanations**: Semantic search for schema mappings

## Technology Stack

- Python 3.10+
- pandas, pandera (schema validation)
- PostgreSQL (psycopg2)
- sentence-transformers (RAG/embeddings)
- YAML configuration

## Installation

1. **Prerequisites**:
   - PostgreSQL 12+ installed and running
   - Python 3.10+ with pip

2. **Setup**:
   ```bash
   # Create virtual environment
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate

   # Install dependencies
   pip install -r requirements.txt

   # Setup databases (see setup/README.md)
   cd setup
   # Follow database setup instructions
   ```

## Usage

### Pre-Migration Validation
```bash
python main.py --phase pre --dataset claimants --sample 500
```

### Post-Migration Reconciliation
```bash
python main.py --phase post --dataset claimants
```

## Configuration

Edit `config.yaml` to customize:
- Database connections (legacy/modern) — supports `${VAR:default}` environment variable syntax
- Validation rules (PII keywords, naming conventions, null thresholds)
- Confidence score weights
- RAG similarity thresholds (`rag.explain_threshold`, `rag.mapping_threshold`)

## Artifacts

Validation runs generate timestamped artifacts in `artifacts/run_<timestamp>/`:
- `readiness_report.md` - Pre-migration findings
- `schema_diff.md` - Schema comparison
- `governance_report.csv` - PII, naming, null checks
- `reconciliation_report.md` - Post-migration integrity
- `confidence_score.txt` - Final score and traffic light status

## Project Structure

```
.
├── agents/              # Validation agents (pre, during, post)
├── tools/               # Shared utilities (db, sampling, governance, RAG)
├── schemas/             # Pandera schema definitions (legacy/modern)
├── metadata/            # Column glossary and mapping rationale
├── setup/               # Database setup scripts
├── artifacts/           # Generated reports (gitignored)
├── config.yaml          # Configuration
└── main.py              # CLI entry point
```

## Success Criteria

✅ Detects schema mismatches and data quality issues
✅ Generates readable Markdown reports
✅ Produces confidence score with traffic light (GREEN/YELLOW/RED)
✅ Fail-loud behavior for bad data
✅ End-to-end run < 10 minutes

## License

MIT
