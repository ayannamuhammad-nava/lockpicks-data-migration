# Unemployment Claims Analysis — Setup & Walkthrough

This document captures every step taken to set up the **Lockpicks Data Migration (DM)** toolkit against the COBOL-based unemployment claims legacy system, run rationalization and validation pipelines, and launch the interactive Streamlit dashboard.

---

## Table of Contents

1. [Project Initialization](#1-project-initialization)
2. [Install the DM CLI](#2-install-the-dm-cli)
3. [Scaffold a Migration Project](#3-scaffold-a-migration-project)
4. [Start Infrastructure (PostgreSQL + OpenMetadata)](#4-start-infrastructure-postgresql--openmetadata)
5. [Load Legacy & Modern Demo Data](#5-load-legacy--modern-demo-data)
6. [Register Tables in OpenMetadata](#6-register-tables-in-openmetadata)
7. [Run Migration Rationalization](#7-run-migration-rationalization)
8. [Discover & Enrich Metadata](#8-discover--enrich-metadata)
9. [Generate Modern Schema](#9-generate-modern-schema)
10. [Convert Schema to Target Platform](#10-convert-schema-to-target-platform)
11. [Run Pre & Post Migration Validations](#11-run-pre--post-migration-validations)
12. [Launch the Dashboard](#12-launch-the-dashboard)
13. [Fixes Applied](#13-fixes-applied)
14. [Architecture Overview](#14-architecture-overview)

---

## 1. Project Initialization

Created a Python project with `uv` and installed dependencies for AI (Anthropic SDK) and the dashboard (Streamlit).

```bash
# Create pyproject.toml (requires-python >= 3.12)
# with optional dependency groups: [ai] and [dashboard]

uv sync --extra ai          # Installs anthropic SDK
uv sync --extra dashboard    # Installs streamlit
uv sync --extra ai --extra dashboard  # Install both together
```

**`pyproject.toml` extras:**
- `ai` = `["anthropic"]` — Claude API integration for AI-assisted migration
- `dashboard` = `["streamlit"]` — Interactive Streamlit dashboard

---

## 2. Install the DM CLI

Cloned the Lockpicks Data Migration toolkit and installed it in editable mode with all extras.

```bash
# Clone the repo into the project directory
git clone https://github.com/navapbc/lockpicks-data-migration.git

# Install the DM CLI with all optional dependencies
cd lockpicks-data-migration
uv pip install -e ".[all]"
```

This installs the `dm` CLI entry point (defined in `pyproject.toml` as `dm = "dm.cli:cli"`). The CLI is available at `.venv/bin/dm`.

**Verify installation:**
```bash
.venv/bin/dm --help
```

**Available commands:**
| Command | Description |
|---------|-------------|
| `dm init` | Scaffold a new migration project |
| `dm discover` | Introspect databases and generate metadata |
| `dm rationalize` | Analyze legacy catalog and recommend migration scope |
| `dm generate-schema` | Generate normalized modern schema from enriched metadata |
| `dm convert` | Translate legacy SQL/ETL to modern target platform |
| `dm validate` | Run pre- or post-migration validation |
| `dm prove` | Generate migration proof report |
| `dm dashboard` | Launch the Streamlit dashboard |
| `dm status` | Show latest run scores across all datasets |

---

## 3. Scaffold a Migration Project

```bash
.venv/bin/dm init unemployment-claims-analysis
```

**What it creates:**
```
projects/unemployment-claims-analysis/
  project.yaml          # Main configuration (connections, datasets, scoring)
  plugins/my_plugin.py  # Template for custom domain rules
```

**Key configuration in `project.yaml`:**
- **connections** — Legacy and modern database connection strings
- **datasets** — Table mappings (legacy_table -> modern_table)
- **validation** — Sample size, PII keywords, governance rules
- **scoring** — Weights for structure (40%), integrity (40%), governance (20%)
- **openmetadata** — OM server host, auth token, service/database/schema names
- **schema_generation** — Target platform, naming conventions, PII handling

We updated `project.yaml` with:
- Actual database credentials (`app` / `secret123` instead of default `postgres`)
- Real dataset names (claimants, employers, claims, benefit_payments) instead of placeholder `my_table`
- OpenMetadata JWT auth token

---

## 4. Start Infrastructure (PostgreSQL + OpenMetadata)

### Existing PostgreSQL

A PostgreSQL instance was already running via Docker on port 5432:
- **Container:** `ayannasandbox-ayannasandbox-database-1`
- **User:** `app`
- **Password:** `secret123`

### OpenMetadata Stack

Created `docker-compose-openmetadata.yml` and started the full OpenMetadata stack:

```bash
docker compose -f docker-compose-openmetadata.yml up -d
```

**Services started:**
| Service | Port | Description |
|---------|------|-------------|
| `om-postgresql` | 5433 (host) -> 5432 (container) | OpenMetadata's internal Postgres (separate from app DB) |
| `elasticsearch` | 9200 | Search index for OM catalog |
| `execute-migrate-all` | — | One-shot migration job for OM schema setup |
| `openmetadata-server` | 8585 (API), 8586 (admin) | OpenMetadata REST API |
| `openmetadata_ingestion` | 8080 | Airflow-based ingestion pipelines |

**Image version:** `docker.getcollate.io/openmetadata/*:1.6.2` (stable release)

**Verify OM is running:**
```bash
curl -s http://localhost:8585/api/v1/system/version
# {"version":"1.6.2", ...}
```

**Get an auth token:**
```bash
B64_PASS=$(echo -n 'admin' | base64)
curl -s -X POST http://localhost:8585/api/v1/users/login \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"admin@open-metadata.org\",\"password\":\"$B64_PASS\"}"
```

---

## 5. Load Legacy & Modern Demo Data

The `setup/` directory contains SQL scripts simulating the **LOOPS NJ** (NJ Department of Labor Unemployment Insurance System) migration.

### Create databases

```bash
docker exec -e PGPASSWORD=secret123 ayannasandbox-ayannasandbox-database-1 \
  psql -U app -d postgres \
  -c "CREATE DATABASE legacy_db;" \
  -c "CREATE DATABASE modern_db;"
```

### Create schemas and load data

```bash
# Copy SQL scripts into the container
docker cp setup/create_databases.sql <container>:/tmp/
docker cp setup/load_legacy_data.sql <container>:/tmp/
docker cp setup/load_modern_data.sql <container>:/tmp/

# Load data
docker exec -e PGPASSWORD=secret123 <container> psql -U app -d legacy_db -f /tmp/load_legacy_data.sql
docker exec -e PGPASSWORD=secret123 <container> psql -U app -d modern_db -f /tmp/load_modern_data.sql
```

### Data overview

| Table | Legacy Rows | Modern Rows | Description |
|-------|------------|-------------|-------------|
| claimants | 10 | 9 | Unemployed individuals filing for benefits |
| employers | 5 | 5 | Employers where claimants were separated from |
| claims | 15 | 12 | Individual unemployment claims |
| benefit_payments | 20 | 14 | Weekly benefit payments processed |

### Intentional data quality issues (for validation testing)

**Legacy database:**
- Duplicate SSNs (cl_recid 1 and 5)
- NULL required fields (cl_recid 3, 8 have NULL emails)
- PII in plaintext (`cl_ssn`, `cl_bact`, `cl_brtn`)
- Mixed date formats (ISO, US, text, 2-digit year)
- Status inconsistencies (ACTIVE, active, Active, ACT)
- Orphan claims referencing non-existent claimants
- Future filing dates, overpayments, negative amounts

**Schema differences (Legacy -> Modern):**
- Column renames: `cl_recid` -> `claimant_id`, `cl_fnam` -> `first_name`
- Type changes: `cl_phon` VARCHAR -> `phone_number` BIGINT
- Removed columns: `cl_bact`, `cl_brtn` (security)
- Added constraints: PRIMARY KEYs, FOREIGN KEYs, NOT NULL

---

## 6. Register Tables in OpenMetadata

Registered the legacy database service, database, schema, and all 4 tables in OpenMetadata via REST API.

```bash
TOKEN="<jwt_token>"

# 1. Create database service
curl -X PUT http://localhost:8585/api/v1/services/databaseServices \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my_legacy_service",
    "serviceType": "Postgres",
    "connection": {
      "config": {
        "type": "Postgres",
        "hostPort": "host.docker.internal:5432",
        "username": "app",
        "authType": {"password": "secret123"},
        "database": "legacy_db"
      }
    }
  }'

# 2. Create database entity
curl -X PUT http://localhost:8585/api/v1/databases \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "my_database", "service": "my_legacy_service"}'

# 3. Create schema entity
curl -X PUT http://localhost:8585/api/v1/databaseSchemas \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "public", "database": "my_legacy_service.my_database"}'

# 4. Register tables (repeat for each table with column definitions)
curl -X PUT http://localhost:8585/api/v1/tables \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "claimants",
    "databaseSchema": "my_legacy_service.my_database.public",
    "columns": [
      {"name":"cl_recid","dataType":"INT","description":"CLAIMANT-RECORD-ID"},
      {"name":"cl_fnam","dataType":"CHAR","dataLength":30,"description":"CLAIMANT-FIRST-NAME"},
      ...
    ]
  }'
```

Tables registered:
- `my_legacy_service.my_database.public.claimants` (17 columns)
- `my_legacy_service.my_database.public.employers` (10 columns)
- `my_legacy_service.my_database.public.claims` (13 columns)
- `my_legacy_service.my_database.public.benefit_payments` (8 columns)

---

## 7. Run Migration Rationalization

Analyzes the legacy catalog via OpenMetadata and recommends migration scope for each table (Migrate / Review / Archive).

```bash
.venv/bin/dm rationalize -p projects/unemployment-claims-analysis
```

**Output:**
```
Tables analyzed: 4
  Migrate:      0
  Review:       0
  Archive:      4
Scope reduction: 100%
```

All tables scored 32.5/100 and were classified as ARCHIVE because the fresh OM instance has no query activity, profiler data, or lineage. In a production environment with usage history, scores would be higher.

**Scoring dimensions:**
| Dimension | Score | Reason |
|-----------|-------|--------|
| Query Activity | 50 | Default (no usage data) |
| Downstream | 0 | No downstream consumers registered |
| Freshness | 25 | No profiler data |
| Completeness | 50 | Default (no profiler data) |
| Tier | 50 | No tier assigned |

**Artifacts generated:**
- `metadata/rationalization_report.md` — Human-readable report
- `metadata/rationalization_report.json` — Structured JSON
- `metadata/migration_scope.yaml` — Table classifications

---

## 8. Discover & Enrich Metadata

Introspects the legacy tables via OpenMetadata and generates a business glossary and field mapping knowledge base.

```bash
.venv/bin/dm discover --enrich -p projects/unemployment-claims-analysis
```

**Output:**
- `metadata/glossary.json` — 48 entries (column-level metadata for all 4 tables)
- `metadata/mappings.json` — 48 field mappings (legacy -> modern column mappings with rationale)

These artifacts power the RAG chat feature in the dashboard and the field mapping table in the schema diff view.

---

## 9. Generate Modern Schema

Generates a normalized PostgreSQL schema from the OM-enriched metadata, applying naming conventions, type optimization, and normalization analysis.

```bash
.venv/bin/dm generate-schema --all -p projects/unemployment-claims-analysis
```

**Output:**
```
Tables generated: 6
Confidence:       0.88/1.00
```

6 tables were generated (4 source + 2 from normalization — `claimant_addresses` and `employer_addresses` were extracted).

**Artifacts in `artifacts/generated_schema/`:**
| File | Description |
|------|-------------|
| `full_schema.sql` | Complete DDL for all generated tables |
| `claimants.sql` | Individual table DDL |
| `claimants_transforms.sql` | ETL transform SQL |
| `diff_report.json` | Schema diff analysis |
| `updated_mappings.json` | Updated field mappings |
| `updated_datasets.yaml` | Dataset config with new tables |

---

## 10. Convert Schema to Target Platform

Translates the generated schema SQL to the target platform dialect using sqlglot with regex fallback.

```bash
.venv/bin/dm convert \
  --source projects/unemployment-claims-analysis/artifacts/generated_schema/full_schema.sql \
  --target postgres \
  -p projects/unemployment-claims-analysis
```

**Output:**
```
Source:    .../generated_schema/full_schema.sql
Target:    postgres
Warnings:  0
```

Converted SQL saved to `artifacts/converted/postgres/full_schema.sql`.

---

## 11. Run Pre & Post Migration Validations

Validates data quality, schema compatibility, governance compliance, and data integrity for each dataset.

```bash
# Pre-migration (checks legacy data readiness)
.venv/bin/dm validate --phase pre --dataset claimants -p projects/unemployment-claims-analysis
.venv/bin/dm validate --phase pre --dataset employers -p projects/unemployment-claims-analysis
.venv/bin/dm validate --phase pre --dataset claims -p projects/unemployment-claims-analysis
.venv/bin/dm validate --phase pre --dataset benefit_payments -p projects/unemployment-claims-analysis

# Post-migration (reconciles legacy vs modern)
.venv/bin/dm validate --phase post --dataset claimants -p projects/unemployment-claims-analysis
.venv/bin/dm validate --phase post --dataset employers -p projects/unemployment-claims-analysis
.venv/bin/dm validate --phase post --dataset claims -p projects/unemployment-claims-analysis
.venv/bin/dm validate --phase post --dataset benefit_payments -p projects/unemployment-claims-analysis
```

**Results:**

| Dataset | Pre-Migration | Post-Migration |
|---------|--------------|----------------|
| claimants | RED - Failed | GREEN - Passed |
| employers | YELLOW - Warning | GREEN - Passed |
| claims | YELLOW - Warning | YELLOW - Warning |
| benefit_payments | YELLOW - Warning | RED - Failed |

**Artifacts per run (in `artifacts/run_<timestamp>/`):**

| File | Phase | Description |
|------|-------|-------------|
| `run_metadata.json` | Both | Scores, status, timestamps |
| `readiness_report.md` | Pre | Schema diff, data quality findings |
| `schema_diff.md` | Pre | Missing/added columns between legacy and modern |
| `governance_report.csv` | Pre | PII detection, naming violations, null checks |
| `reconciliation_report.md` | Post | Row counts, value comparisons, integrity checks |

**Scoring weights:**
- Structure: 40% (schema compatibility)
- Integrity: 40% (data quality, referential integrity)
- Governance: 20% (PII, naming conventions, compliance)

**Thresholds:**
- GREEN (>= 90): Safe to proceed
- YELLOW (70-89): Review recommended
- RED (< 70): Fix issues first

---

## 12. Launch the Dashboard

The Streamlit dashboard reads all validation artifacts and provides an interactive view.

```bash
STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
  .venv/bin/streamlit run dashboard.py --server.headless true \
  -- --project projects/unemployment-claims-analysis
```

**Access:** http://localhost:8501

**Dashboard features:**
- **Confidence gauge** — Traffic-light score visualization
- **Run selector** — Browse all validation runs from the sidebar
- **Readiness Report** (pre-migration) — Schema compatibility findings
- **Schema Diff & Mappings** — Missing/added columns with RAG-powered field mapping table
- **Governance** — PII violations, naming convention checks, null percentage analysis
- **Reconciliation Report** (post-migration) — Row counts, value drift, integrity checks
- **RAG Chat** — Ask questions about any schema field or mapping (e.g., "What is cl_bact?", "Why did cl_ssn become ssn_hash?")
- **Run New Validation** — Trigger validations directly from the UI

---

## 13. Fixes Applied

Four compatibility fixes were applied to the DM codebase for OpenMetadata 1.6.2:

### Fix 1: `owner` -> `owners` field name (OM API change)

**File:** `dm/discovery/openmetadata_enricher.py` line 138

```python
# Before
entity = self._get_table_entity(table, fields="owner,tags,followers")

# After
entity = self._get_table_entity(table, fields="owners,tags")
```

Also updated the owner extraction to handle the `owners` array format.

### Fix 2: Handle 500 errors for missing profiler data

**File:** `dm/discovery/openmetadata_enricher.py` line 171

```python
# Before (only handled 404)
if e.response.status_code == 404:

# After (also handles 500 when no profiler data exists)
if e.response.status_code in (404, 500):
```

### Fix 3: `ErrorLevel` -> `ParseError` exception class

**File:** `dm/conversion/rule_engine.py` line 230

```python
# Before (ErrorLevel is not an exception class)
except sqlglot.errors.ErrorLevel:

# After
except sqlglot.errors.ParseError:
```

### Fix 4: Missing `output_path` attribute on ConversionResult

**File:** `dm/pipeline.py` line 656

```python
# Before
"output_path": result.output_path,

# After
"output_path": getattr(result, "output_path", result.source_path),
```

---

## 14. Architecture Overview

```
unemployment-claims-analysis/
├── pyproject.toml                          # Python project config
├── .venv/                                  # Virtual environment
├── lockpicks-data-migration/               # DM toolkit (cloned)
│   ├── dm/                                 # CLI source code
│   │   ├── cli.py                          # Click CLI entry point
│   │   ├── pipeline.py                     # Orchestration layer
│   │   ├── discovery/                      # Schema discovery & metadata
│   │   ├── rationalization/                # Migration scope analysis
│   │   ├── conversion/                     # SQL translation engine
│   │   ├── connectors/                     # Database connectors
│   │   ├── validators/                     # Data quality checks
│   │   ├── kb/                             # RAG knowledge base
│   │   └── ai/                             # Claude API integration
│   ├── dashboard.py                        # Streamlit dashboard
│   ├── docker-compose-openmetadata.yml     # OM infrastructure
│   ├── setup/                              # Demo database SQL scripts
│   └── projects/
│       └── unemployment-claims-analysis/   # Our project
│           ├── project.yaml                # Configuration
│           ├── metadata/                   # Glossary, mappings, reports
│           │   ├── glossary.json
│           │   ├── mappings.json
│           │   ├── rationalization_report.md
│           │   ├── rationalization_report.json
│           │   ├── migration_scope.yaml
│           │   └── normalization_plan.json
│           └── artifacts/                  # Validation & schema artifacts
│               ├── generated_schema/       # Modern DDL
│               ├── converted/postgres/     # Platform-converted SQL
│               └── run_*/                  # Validation run outputs
└── unemployment-claims-project/            # Original COBOL source files
    ├── src/cbl/                            # COBOL programs
    │   ├── getclaim.cbl
    │   └── unemplclm.cbl
    ├── jcl/                                # JCL job control
    └── output/                             # COBOL program output
```

### Data Flow

```
Legacy DB (COBOL schema)
    |
    |-- dm discover --enrich --> glossary.json + mappings.json
    |
    |-- dm rationalize -------> migration_scope.yaml (migrate/review/archive)
    |
    |-- dm generate-schema ---> full_schema.sql (normalized modern DDL)
    |
    |-- dm convert -----------> converted SQL for target platform
    |
    |-- dm validate --phase pre --> readiness_report.md + governance_report.csv
    |
    v
Modern DB (PostgreSQL)
    |
    +-- dm validate --phase post --> reconciliation_report.md
                                         |
                                         v
                                    Dashboard (localhost:8501)
```

### Running Services

| Service | URL | Purpose |
|---------|-----|---------|
| PostgreSQL (app) | localhost:5432 | Legacy & modern databases |
| PostgreSQL (OM) | localhost:5433 | OpenMetadata internal DB |
| OpenMetadata API | localhost:8585 | Metadata catalog |
| OpenMetadata Admin | localhost:8586 | Health checks |
| Elasticsearch | localhost:9200 | OM search index |
| Airflow (Ingestion) | localhost:8080 | OM ingestion pipelines |
| Streamlit Dashboard | localhost:8501 | Interactive validation UI |
