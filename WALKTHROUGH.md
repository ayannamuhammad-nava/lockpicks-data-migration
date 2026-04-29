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
12. [Enrich Metadata via OpenMetadata](#12-enrich-metadata-via-openmetadata)
13. [Generate Migration Proof Reports](#13-generate-migration-proof-reports)
14. [Plan Data Ingestion](#14-plan-data-ingestion)
15. [Set Up Post-Migration Observability](#15-set-up-post-migration-observability)
16. [View Overall Status](#16-view-overall-status)
17. [Launch the Dashboard](#17-launch-the-dashboard)
18. [Fixes Applied](#18-fixes-applied) (14 total)
19. [Architecture Overview](#19-architecture-overview)

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
| `run_metadata.json` | Both | Scores (overall, structure, governance), status, timestamps |
| `confidence_score.txt` | Both | Raw confidence score and traffic-light status |
| `readiness_report.md` | Pre | Schema diff, data quality findings |
| `schema_diff.md` | Pre | Missing/added columns between legacy and modern (Fix 9) |
| `governance_report.csv` | Pre | PII detection, naming violations, null checks (Fix 9) |
| `reconciliation_report.md` | Post | Row counts, value comparisons, integrity checks |
| `proof_report.md` | Prove | Combined pre+post narrative with final score |

**Understanding the Schema Diff — "Missing" columns are renamed, not lost:**

The schema diff compares legacy and modern tables by **column name**. Because the legacy system uses COBOL copybook abbreviated naming (`cl_`, `er_`, `cm_`, `bp_` prefixes) and the modern schema uses full descriptive English names, every renamed column appears as "missing in modern" with a corresponding "new column in modern." The data is not lost — it was renamed during migration.

For example, in the `benefit_payments` table all 8 legacy columns show as "missing":

| Legacy Column | Modern Column | What Happened |
|--------------|---------------|---------------|
| `bp_recid` | `payment_id` | Renamed from COBOL abbreviation |
| `bp_clmid` | `claim_id` | Renamed |
| `bp_paydt` | `payment_date` | Renamed + type change (CHAR -> DATE) |
| `bp_payam` | `payment_amount` | Renamed |
| `bp_methd` | `payment_method` | Renamed |
| `bp_wkedt` | `week_ending_date` | Renamed |
| `bp_stat` | `payment_status` | Renamed |
| `bp_chkno` | `check_number` | Renamed |

The same pattern applies across all 4 tables. To see the actual rename mappings (legacy -> modern), check the **Schema Diff & Mappings** tab in the dashboard or `metadata/mappings.json`. The field mappings include confidence scores and rationale for each rename, transform, or archival decision.

Only a few columns were truly removed during migration (e.g., `cl_bact` and `cl_brtn` — bank account and routing numbers archived for PCI-DSS compliance, and `cl_fil1` — a COBOL FILLER field with no business value).

**Scoring weights:**
- Structure: 40% (schema compatibility)
- Integrity: 40% (data quality, referential integrity)
- Governance: 20% (PII, naming conventions, compliance)

**Thresholds:**
- GREEN (>= 90): Safe to proceed
- YELLOW (70-89): Review recommended
- RED (< 70): Fix issues first

---

## 12. Enrich Metadata via OpenMetadata

Enriches the glossary and mappings with profiling stats, lineage data, PII tags, and glossary terms from OpenMetadata.

```bash
.venv/bin/dm enrich -p projects/unemployment-claims-analysis
```

**Output:**
```
Enrichment complete: 48 glossary entries
```

Updates `metadata/glossary.json` and `metadata/mappings.json` with OM-sourced descriptions, tags, and profiling data. This improves the confidence scores for field mappings and powers the RAG chat in the dashboard.

---

## 13. Generate Migration Proof Reports

Combines pre- and post-migration validation results into a single proof report per dataset, producing a final weighted score.

```bash
.venv/bin/dm prove -d claimants -p projects/unemployment-claims-analysis
.venv/bin/dm prove -d employers -p projects/unemployment-claims-analysis
.venv/bin/dm prove -d claims -p projects/unemployment-claims-analysis
.venv/bin/dm prove -d benefit_payments -p projects/unemployment-claims-analysis
```

**Results:**

| Dataset | Pre-Score | Post-Score | Final | Status |
|---------|-----------|------------|-------|--------|
| claimants | 69.8 | 90.0 | 79.9 | YELLOW |
| employers | N/A | 100.0 | N/A | INCOMPLETE |
| claims | N/A | N/A | N/A | INCOMPLETE |
| benefit_payments | 86.6 | 60.0 | 73.3 | YELLOW |

Employers and claims show INCOMPLETE because `dm status` only tracks the most recent run per timestamp bucket, and some pre-migration runs were overwritten. The proof report requires both a pre and post run to compute a final score.

**Artifacts per run:**
- `proof_report.md` — Combined pre+post narrative report
- `run_metadata.json` — Final scores and status

---

## 14. Plan Data Ingestion

Generates a dependency-ordered migration execution plan based on foreign key relationships and normalization analysis.

```bash
.venv/bin/dm ingest --plan -p projects/unemployment-claims-analysis
```

**Output:**
```
MIGRATION PLAN
  claimants           strategy: full_load   deps: none
  employers           strategy: full_load   deps: none
  claims              strategy: full_load   deps: none
  benefit_payments    strategy: full_load   deps: none
```

All 4 tables use `full_load` strategy with no dependencies (FK constraints weren't configured in `referential_integrity` in `project.yaml`). In a production setup with FK mappings, parent tables would be ordered before children.

---

## 15. Set Up Post-Migration Observability

Captures a baseline snapshot of the modern database and monitors for data drift.

### Set baseline
```bash
.venv/bin/dm observe --set-baseline -p projects/unemployment-claims-analysis
```

Captures row counts, schema fingerprints, and statistical profiles for all 4 tables in `artifacts/observer_baseline.json`.

### Run a drift check
```bash
.venv/bin/dm observe --once -p projects/unemployment-claims-analysis
```

**Output:**
```
Checks run:    16
Drift detected: 0
  No drift detected. Pipeline healthy.
```

Runs 4 checks per table (row count, schema, null rates, value distributions) and compares against the baseline. Zero drift confirms the modern database is stable.

### View observation history
```bash
.venv/bin/dm observe --history -p projects/unemployment-claims-analysis
```

Shows historical drift check results (empty on first run).

---

## 16. View Overall Status

Shows the latest validation, proof, and observation scores across all datasets.

```bash
.venv/bin/dm status -p projects/unemployment-claims-analysis
```

**Output:**
```
Run                       Phase    Dataset           Score   Status
------------------------------------------------------------------------
run_2026-04-28_12-00-50   prove    benefit_payments  73.3    YELLOW
run_2026-04-28_12-00-49   prove    claims            0       INCOMPLETE
run_2026-04-24_15-39-37   post     benefit_payments  60.0    RED
run_2026-04-24_15-39-36   pre      benefit_payments  86.6    YELLOW
run_2026-04-24_15-39-35   post     employers         100.0   GREEN
run_2026-04-24_15-39-34   post     claimants         90.0    GREEN
run_2026-04-24_15-39-33   pre      claimants         69.8    RED
```

---

## 17. Launch the Dashboard

The Streamlit dashboard reads all validation artifacts and provides an interactive view.

```bash
STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
  .venv/bin/streamlit run dashboard.py --server.headless true \
  -- --project projects/unemployment-claims-analysis
```

**Access:** http://localhost:8501

**Dashboard features:**
- **Lifecycle status bar** — Appears at the top of every page. Shows the project name (from `project.yaml`), average confidence score from the most recent date's runs with GREEN/YELLOW/RED status and the date displayed below the score, and a 6-phase lifecycle tracker (Discovery -> Modeling -> Governance -> Transformation -> Compliance -> Quality) with completed/current/pending indicators. Score thresholds: GREEN >= 90, YELLOW 70-89, RED < 70.
- **Clickable lifecycle detail pages** — Each of the 6 lifecycle phases is a fully built, clickable detail page:
  - **Discovery** — Tables, Sample Data, Glossary, Field Mappings, PII Detection, Abbreviations, Rationalization
  - **Modeling** — Table Schemas, Column Mapping, Normalization Plan, Full DDL with download. Legend at bottom (green=Primary, blue=Child, yellow=Lookup)
  - **Governance** — PII Inventory with regulations, Data Modification Controls, Naming Compliance, Null Threshold Report (live DB query), Audit Trail
  - **Transformation** — Transform Scripts (ETL INSERT...SELECT), Converted SQL with download, Before/After comparison, Warnings & TODOs
  - **Compliance** — Compliance Checklist (pass/fail), Readiness Report, Governance Report, Schema Diff, Risk Assessment with score breakdown
  - **Quality** — Reconciliation, Proof Reports, Score Summary, Sign-Off workflow
- **Sign-off workflow** (Quality page) — Enter name and role, click Sign Off. A confirmation dialog asks "Are you sure? Your information will be saved as signing off on these changes" with Confirm or Cancel. Sign-offs are stored in `artifacts/signoff.json` with name, role, date, time, score, status, and project. Multiple sign-offs are supported (e.g., tech lead, compliance officer, program manager). Sign-off history is displayed with color-coded status cards. The Quality button in the lifecycle bar turns green (custom HTML badge) after sign-off, and stays default otherwise. A red "NOT SIGNED OFF" banner is shown until the first sign-off, then switches to green with sign-off details.
- **Confidence gauge** — Traffic-light score visualization for the selected run
- **Run selector** — Browse all validation runs from the sidebar
- **Readiness Report** (pre-migration) — Schema compatibility findings
- **Schema Diff & Mappings** — Missing/added columns with RAG-powered field mapping table
- **Governance** — PII violations, naming convention checks, null percentage analysis
- **Reconciliation Report** (post-migration) — Row counts, value drift, integrity checks
- **RAG Chat** — Ask questions about any schema field or mapping (e.g., "What is cl_bact?", "Why did cl_ssn become ssn_hash?")
- **Run New Validation** — Trigger validations directly from the UI

---

## 18. Fixes Applied

Fourteen fixes were applied to the DM codebase for OpenMetadata 1.6.2, pandas 3.x, and Python 3.14:

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

### Fix 5: `None` tables passed to ingestion planner

**File:** `dm/pipeline.py` line 678

```python
# Before — passes None when no dataset specified, causing TypeError
tables = [dataset] if dataset else None

# After — falls back to all datasets from config
if dataset:
    tables = [dataset]
else:
    tables = [ds["name"] for ds in config.get("datasets", [])]
```

### Fix 6: `plan.steps` -> `plan.strategies.values()`

**File:** `dm/pipeline.py` line 692

```python
# Before — MigrationPlan has no 'steps' attribute
for step in plan.steps

# After
for step in plan.strategies.values()
```

### Fix 7: `observer.baseline_path` -> `observer.baseline_manager.baseline_path`

**File:** `dm/pipeline.py` line 728

```python
# Before — PipelineObserver has no direct baseline_path
return {"baseline_path": str(observer.baseline_path)}

# After
return {"baseline_path": str(observer.baseline_manager.baseline_path)}
```

### Fix 8: `observer.get_history()` not implemented

**File:** `dm/pipeline.py` line 733

```python
# Before — method doesn't exist
return {"history": observer.get_history()}

# After — graceful fallback
history_fn = getattr(observer, "get_history", None)
if history_fn:
    return {"history": history_fn()}
return {"history": [], "message": "Observation history not yet implemented"}
```

### Fix 9: Dashboard missing `schema_diff.md` and `governance_report.csv`

**File:** `dm/pipeline.py` — `_generate_pre_reports()`

The dashboard's Schema Diff & Mappings and Governance tabs expect `schema_diff.md` and `governance_report.csv` in each pre-migration run folder, but the reporter was only writing `readiness_report.md`. Added generation of both files from validator result data:

```python
# schema_diff.md — generated from SchemaDiffValidator results
for r in results:
    if r.name == "schema_diff" and r.details.get("schema_diff"):
        diff_md = generate_schema_diff_report(
            r.details.get("legacy_schema", {}),
            r.details.get("modern_schema", {}),
            dataset,
        )
        save_markdown_report(diff_md, os.path.join(artifact_folder, "schema_diff.md"))

# governance_report.csv — generated from GovernanceValidator results
for r in results:
    if r.name == "governance" and r.details:
        # Writes PII violations, naming warnings, null threshold warnings as CSV
        ...
```

Also added `structure_score` and `governance_score` to `run_metadata.json` so the dashboard can display individual score breakdowns.

**Pre-migration runs must be re-run after this fix** to generate the new files:
```bash
.venv/bin/dm validate --phase pre --dataset claimants -p projects/unemployment-claims-analysis
```

**Sample `governance_report.csv` output (claimants):**
```csv
category,item,status,detail
PII,cl_ssn,VIOLATION,Plaintext PII detected
PII,cl_dob,VIOLATION,Plaintext PII detected
Null,cl_emal,WARNING,Exceeds null threshold
```

**Sample `schema_diff.md` output (claimants):**
- 16 legacy columns missing in modern (renamed during migration)
- 15 new columns in modern schema
- 1 type mismatch (`cl_bact`: character -> character varying)

### Fix 10: `applymap` -> `map` for pandas 2.1+ compatibility

**File:** `dashboard.py` line 564

```python
# Before — applymap removed in pandas 2.1+
styled = gov_df.style.applymap(color_gov_status, subset=["status"])

# After
styled = gov_df.style.map(color_gov_status, subset=["status"])
```

### Fix 11: Field mappings showing "not migrated" for renamed columns (initial workaround)

**File:** `projects/unemployment-claims-analysis/metadata/mappings.json`

The auto-matcher (`SequenceMatcher` with 0.7 threshold) could not resolve COBOL abbreviated column names to modern descriptive names because the string similarity is too low (e.g., `bp_recid` vs `payment_id` = 0.25). All 48 mappings had `target: null` and `type: pending`, causing the dashboard to display "*(not migrated)*" for every field.

Initially fixed by manually populating all 48 mappings. This was **project-specific** — any new project would have the same problem. **Superseded by Fix 13** which makes the matcher COBOL-aware globally.

### Fix 12: Black font on highlighted dashboard rows

**File:** `dashboard.py` — `highlight_mapping_type()`

Highlighted rows (archived in red, transform in yellow) had unreadable text in dark mode. Added explicit `color: #000000` to ensure black font on colored backgrounds.

```python
# Before
return ["background-color: #ffebee"] * len(row)

# After
return ["background-color: #ffebee; color: #000000"] * len(row)
```

### Fix 13: Global COBOL-aware column matcher (replaces manual mappings)

**File:** `dm/discovery/metadata_generator.py` — `find_matching_column()`, `expand_cobol_abbreviation()`

**File:** `dm/pipeline.py` — `run_enrichment()` (passes modern DB connection)

The original `find_matching_column()` used only `SequenceMatcher` fuzzy matching, which fails for COBOL abbreviated names (e.g., `bp_recid` vs `payment_id` = 0.25 similarity, well below the 0.7 threshold). This made Fix 11's manual mapping necessary for every project.

**New multi-strategy approach:**

1. **COBOL abbreviation dictionary** (90+ patterns): Strips the 2-3 char table prefix (`cl_`, `bp_`, etc.) and looks up the suffix in a dictionary of common COBOL copybook abbreviations:
   ```
   fnam -> first_name, lnam -> last_name, dob -> date_of_birth,
   payam -> payment_amount, stat -> status, phon -> phone_number,
   emal -> email, adr1 -> address_line1, wkamt -> weekly_benefit_amount, ...
   ```

2. **Table-context PK resolution**: Record ID fields (`recid`, `recno`) use the table name to infer the correct primary key. E.g., `bp_recid` + table `benefit_payments` -> `payment_id` (derives from singularized last word of table name + `_id`).

3. **COBOL PII pattern detection**: Abbreviated financial field patterns (`bact`, `brtn`, `bacct`, `broute`) are recognized as PII and routed to `archived` type instead of `removed`.

4. **FILLER detection**: COBOL FILLER fields (`fil1`, `fil2`, `filler`) are automatically classified as `removed`.

5. **Containment/word-overlap matching**: Expanded names that partially match modern columns score by overlap ratio (e.g., expanded `status` matches `claimant_status` via containment).

6. **Modern DB connection**: The enrichment pipeline now connects to the modern database (when available) to match expanded abbreviations against actual column names, falling back to dictionary-only matching when no modern DB is accessible.

7. **Original fuzzy match as fallback**: `SequenceMatcher` at 0.7 threshold still runs as a last resort.

**Result:** 48/48 mappings auto-resolved correctly (was 0/48 before), including:
- `bp_recid` -> `payment_id` (table-context PK)
- `cl_bact` -> archived (COBOL PII detection)
- `cl_fil1` -> removed (FILLER detection)
- `er_ind` -> `industry` (abbreviation dictionary)

This fix is **global** — any new COBOL legacy system will get automatic column resolution without manual mapping.

### Fix 14: Auto-generated `abbreviations.yaml` from COBOL copybook descriptions

**File:** `dm/discovery/metadata_generator.py` — `parse_cobol_description()`, `generate_abbreviations_yaml()`, `load_project_abbreviations()`

During `dm discover` or `dm enrich`, the system now automatically parses COBOL copybook descriptions stored in OpenMetadata column metadata (e.g., `"CONTACT-FIRST-NAME"`) to extract abbreviation mappings for the project. For each column, it strips the table prefix from the description, converts the remainder to snake_case, and writes the result to `metadata/abbreviations.yaml` in the project folder.

**How it works:**
1. Reads OM column descriptions like `"CONTACT-FIRST-NAME"` for column `ct_fnam`
2. Strips the common table prefix (`CONTACT-`) leaving `FIRST-NAME`
3. Converts to snake_case: `first_name`
4. Maps the abbreviated suffix (`fnam`) to the expanded name (`first_name`)
5. Writes all mappings to `metadata/abbreviations.yaml`

**Merging behavior:** Project-specific abbreviations from `abbreviations.yaml` are merged with the built-in COBOL abbreviation dictionary (from Fix 13). Project overrides take priority, so if a project has a non-standard abbreviation that conflicts with the built-in dictionary, the project-specific mapping wins.

**No manual steps needed.** The file is generated automatically during discovery or enrichment. However, it can still be manually edited afterward to add, override, or remove mappings for edge cases.

---

## 19. Architecture Overview

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
│               ├── observer_baseline.json  # Post-migration baseline snapshot
│               └── run_*/                  # Validation run outputs
│                   ├── run_metadata.json
│                   ├── readiness_report.md (pre)
│                   ├── schema_diff.md      (pre)
│                   ├── governance_report.csv (pre)
│                   ├── reconciliation_report.md (post)
│                   └── proof_report.md     (prove)
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
    |-- dm enrich ------------> enriched glossary with OM profiling/tags
    |
    |-- dm rationalize -------> migration_scope.yaml (migrate/review/archive)
    |
    |-- dm generate-schema ---> full_schema.sql (normalized modern DDL)
    |
    |-- dm convert -----------> converted SQL for target platform
    |
    |-- dm validate --phase pre --> readiness_report.md + governance_report.csv
    |
    |-- dm ingest --plan -----> dependency-ordered migration plan
    |
    v
Modern DB (PostgreSQL)
    |
    |-- dm validate --phase post --> reconciliation_report.md
    |
    |-- dm prove ----------------> proof_report.md (pre+post combined)
    |
    |-- dm observe --set-baseline -> observer_baseline.json
    |
    |-- dm observe --once -------> drift check (16 checks, 0 drift)
    |
    |-- dm status ---------------> score summary across all runs
    |
    +-----------------------------> Dashboard (localhost:8501)
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
