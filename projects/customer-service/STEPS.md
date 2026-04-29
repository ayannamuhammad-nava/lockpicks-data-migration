# Customer Service — Migration Step-by-Step

Complete step-by-step guide for migrating the Contact Management System (COBOL) to modern PostgreSQL using the Lockpicks DM pipeline.

---

## Step 1: Scaffold the Project

```bash
dm init customer-service
```

Creates the project folder with `project.yaml` template and plugin stubs at `projects/customer-service/`.

**Status:** Done

---

## Step 2: Configure project.yaml

Edit `projects/customer-service/project.yaml` to set:

- **Database credentials** — legacy and modern connection details
- **Dataset names** — `contacts` (legacy_table and modern_table)
- **OpenMetadata connection** — host, auth token, service/database/schema
- **PII keywords** — ssn, email, phone, dob, drivers_license, bank, etc.
- **Scoring weights** — structure (40%), integrity (40%), governance (20%)

**Status:** Done

---

## Step 3: Load Legacy Data into PostgreSQL

Copy SQL files into the database container and execute them to create the legacy `contacts` table and load 15 COBOL records.

```bash
docker cp projects/customer-service/COBOL_DATA/create_contacts_legacy.sql <container>:/tmp/
docker cp projects/customer-service/COBOL_DATA/load_contacts_legacy.sql <container>:/tmp/

docker exec -e PGPASSWORD=secret123 <container> psql -U app -d legacy_db -f /tmp/create_contacts_legacy.sql
docker exec -e PGPASSWORD=secret123 <container> psql -U app -d legacy_db -f /tmp/load_contacts_legacy.sql
```

This loads the COBOL-style data with 41 abbreviated fields (`ct_fnam`, `ct_ptel`, `ct_adtyp`, etc.) and 12 intentional data quality issues.

**Status:** Not started

---

## Step 4: Create Modern Target Table

Create the modern `contacts` table with descriptive column names and proper types.

```bash
docker cp projects/customer-service/COBOL_DATA/create_contacts_modern.sql <container>:/tmp/

docker exec -e PGPASSWORD=secret123 <container> psql -U app -d modern_db -f /tmp/create_contacts_modern.sql
```

**Status:** Not started

---

## Step 5: Register Legacy Service in OpenMetadata

Create the database service in OpenMetadata so the DM pipeline can discover tables.

```bash
TOKEN="<jwt_token>"

curl -X PUT http://localhost:8585/api/v1/services/databaseServices \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "customer_service_legacy",
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
```

Then create the database and schema entities:

```bash
curl -X PUT http://localhost:8585/api/v1/databases \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "customer_db", "service": "customer_service_legacy"}'

curl -X PUT http://localhost:8585/api/v1/databaseSchemas \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "public", "database": "customer_service_legacy.customer_db"}'
```

**Status:** Not started

---

## Step 6: Register Legacy Tables in OpenMetadata

Register the `contacts` table with all 41 column definitions so the DM pipeline can introspect it.

```bash
curl -X PUT http://localhost:8585/api/v1/tables \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "contacts",
    "databaseSchema": "customer_service_legacy.customer_db.public",
    "columns": [
      {"name":"ct_recid","dataType":"INT","description":"CONTACT-RECORD-ID"},
      {"name":"ct_fnam","dataType":"CHAR","dataLength":25,"description":"CONTACT-FIRST-NAME"},
      {"name":"ct_mnam","dataType":"CHAR","dataLength":25,"description":"CONTACT-MIDDLE-NAME"},
      {"name":"ct_lnam","dataType":"CHAR","dataLength":30,"description":"CONTACT-LAST-NAME"},
      ...all 41 columns...
    ]
  }'
```

**Status:** Not started

---

## Step 7: Rationalize Migration Scope

Analyze the legacy catalog and recommend which tables are worth migrating.

```bash
dm rationalize -p projects/customer-service
```

Scores each table on query activity, downstream lineage, freshness, completeness, and tier. Classifies as Migrate (>=70), Review (40-69), or Archive (<40).

**Expected output:** `metadata/rationalization_report.md`, `migration_scope.yaml`

**Status:** Not started

---

## Step 8: Discover and Enrich Metadata

Pull schema metadata from OpenMetadata and generate the business glossary and field mappings.

```bash
dm discover --enrich -p projects/customer-service
```

Generates `metadata/glossary.json` (column-level metadata), `metadata/mappings.json` (legacy-to-modern field mappings with COBOL abbreviation resolution), and `metadata/abbreviations.yaml` (auto-generated abbreviation mappings parsed from COBOL copybook descriptions in OpenMetadata).

**Expected output:** `metadata/glossary.json`, `metadata/mappings.json`, `metadata/abbreviations.yaml`

**Status:** Not started

---

## Step 9: Enrich Metadata

Enrich the glossary with profiling stats, lineage data, PII tags, and COBOL-aware column matching against the modern database. Also updates `metadata/abbreviations.yaml` with any additional abbreviation mappings discovered during enrichment.

```bash
dm enrich -p projects/customer-service
```

The COBOL-aware matcher auto-resolves abbreviated names (e.g., `ct_fnam` -> `first_name`, `ct_ptel` -> `primary_phone`, `ct_bact` -> archived).

**Status:** Not started

> **Note on `abbreviations.yaml`:** This file is auto-generated during Steps 8 and 9 by parsing COBOL copybook descriptions from OpenMetadata (e.g., `"CONTACT-FIRST-NAME"` for column `ct_fnam`). It maps abbreviated suffixes to their expanded snake_case names and is merged with the built-in COBOL abbreviation dictionary, with project-specific entries taking priority. No manual configuration is needed, but you can review and edit the file at `metadata/abbreviations.yaml` to add overrides or handle edge cases.

---

## Step 10: Generate Modern Schema

Generate a normalized PostgreSQL schema from the enriched metadata.

```bash
dm generate-schema --all -p projects/customer-service
```

Expands COBOL abbreviations, normalizes tables, optimizes types, handles PII fields, and infers constraints.

**Expected output:** `artifacts/generated_schema/full_schema.sql`

**Status:** Not started

---

## Step 11: Convert to Target Platform

Translate the generated schema SQL to the target platform dialect.

```bash
dm convert --source projects/customer-service/artifacts/generated_schema/full_schema.sql \
  --target postgres -p projects/customer-service
```

Rule engine handles ~80% of conversions. Optional `--ai-refine` flag uses Claude AI for the rest.

**Expected output:** `artifacts/converted/postgres/full_schema.sql`

**Status:** Not started

---

## Step 12: Pre-Migration Validation

Check if the data is safe to migrate — schema compatibility, PII exposure, governance, data quality.

```bash
dm validate --phase pre --dataset contacts -p projects/customer-service
```

Runs 6 validators: schema diff, Pandera, governance, data quality, profile risk, ETL tests. Produces a 0-100 confidence score (GREEN >= 90, YELLOW 70-89, RED < 70).

**Expected output:** `artifacts/run_*/readiness_report.md`, `schema_diff.md`, `governance_report.csv`

**Status:** Not started

---

## Step 13: Plan Data Ingestion

Generate a dependency-ordered migration execution plan.

```bash
dm ingest --plan -p projects/customer-service
```

Builds a dependency graph from FK relationships and normalization plan, then topologically sorts tables.

**Status:** Not started

---

## Step 14: Post-Migration Validation

Verify the data survived migration — row counts, checksums, FK integrity, sample comparison.

```bash
dm validate --phase post --dataset contacts -p projects/customer-service
```

Runs 9 validators including row count, referential integrity, archived leakage, and encoding checks.

**Expected output:** `artifacts/run_*/reconciliation_report.md`

**Status:** Not started

---

## Step 15: Generate Migration Proof

Combine pre + post validation into a single audit proof report.

```bash
dm prove -d contacts -p projects/customer-service
```

Final score = average of pre-score and post-score. GREEN >= 90, YELLOW >= 70, RED < 70.

**Expected output:** `artifacts/run_*/proof_report.md`

**Status:** Not started

---

## Step 16: Set Observation Baseline

Capture a baseline snapshot of the modern database for drift monitoring.

```bash
dm observe --set-baseline -p projects/customer-service
```

Snapshots row counts, schema fingerprints, and statistical profiles.

**Expected output:** `artifacts/observer_baseline.json`

**Status:** Not started

---

## Step 17: Check for Drift

Run a one-time observation check against the baseline.

```bash
dm observe --once -p projects/customer-service
```

Checks schema drift, volume anomalies, data freshness, and FK integrity.

**Status:** Not started

---

## Step 18: View Overall Status

Show the latest scores across all datasets and runs.

```bash
dm status -p projects/customer-service
```

**Status:** Not started

---

## Step 19: Launch the Dashboard

Start the interactive Streamlit dashboard.

```bash
STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
  .venv/bin/streamlit run dashboard.py --server.headless true \
  -- --project projects/customer-service
```

**Access:** http://localhost:8501

**Dashboard experience:**

The dashboard opens with a lifecycle status bar at the top showing the project name, average confidence score, and 6 clickable lifecycle phases. Each phase is a fully built detail page:

- **Discovery** — Tables, Sample Data, Glossary, Field Mappings, PII Detection, Abbreviations, Rationalization
- **Modeling** — Table Schemas, Column Mapping, Normalization Plan, Full DDL with download. Legend at bottom (green=Primary, blue=Child, yellow=Lookup)
- **Governance** — PII Inventory with regulations, Data Modification Controls, Naming Compliance, Null Threshold Report (live DB query), Audit Trail
- **Transformation** — Transform Scripts (ETL INSERT...SELECT), Converted SQL with download, Before/After comparison, Warnings & TODOs
- **Compliance** — Compliance Checklist (pass/fail), Readiness Report, Governance Report, Schema Diff, Risk Assessment with score breakdown
- **Quality** — Reconciliation, Proof Reports, Score Summary, Sign-Off workflow

**Sign-off workflow (Quality page):**

1. Enter name and role, click Sign Off
2. Confirmation dialog: "Are you sure? Your information will be saved as signing off on these changes"
3. Confirm or Cancel
4. Stored in `artifacts/signoff.json` with name, role, date, time, score, status, and project
5. Multiple sign-offs supported (tech lead, compliance officer, program manager)
6. Sign-off history displayed with color-coded status cards
7. Quality button in lifecycle bar turns green (custom HTML badge) after sign-off, stays default otherwise
8. Red "NOT SIGNED OFF" banner until first sign-off, then green with details

Additional features: confidence gauge, run selector sidebar, readiness report, schema diff & mappings, governance tab, reconciliation report, RAG chat, run new validation from UI.

**Status:** Not started
