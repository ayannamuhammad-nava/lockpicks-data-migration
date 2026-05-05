# Changelog — Unemployment Claims Analysis Project

This document explains every change made to the Lockpicks Data Migration toolkit during the unemployment claims (LOOPS NJ) implementation, why each change was necessary, and how it affects future projects.

---

## Context

The Lockpicks DM toolkit was designed for a general data migration lifecycle but had not been tested end-to-end against a live OpenMetadata instance with a real COBOL legacy dataset. Running the full pipeline — from `dm init` through `dm observe` — against the NJ unemployment claims system revealed 13 issues that prevented the toolkit from completing its workflow. Every fix below was applied to the core toolkit code (not project-specific configuration), making them available to any future migration project.

---

## Fixes by Category

### OpenMetadata 1.6.2 API Compatibility (Fixes 1-2)

The toolkit was written against an older OpenMetadata API. Version 1.6.2 introduced breaking changes.

**Fix 1 — `owner` field renamed to `owners`**
- **File:** `dm/discovery/openmetadata_enricher.py`
- **Problem:** The OM 1.6.2 API renamed the `owner` field to `owners` (now an array) and removed the `followers` field. Requesting `?fields=owner,tags,followers` returned 400 Bad Request, causing `dm rationalize` to fail on every table.
- **Why it matters:** Without table metadata (owner, tags), the rationalization engine cannot score tables for migration scope. Every table fell back to a 0.0 score with an error rationale.
- **Fix:** Changed the fields parameter to `owners,tags` and updated the owner extraction to handle both array and dict formats for backward compatibility.

**Fix 2 — 500 errors for missing profiler data**
- **File:** `dm/discovery/openmetadata_enricher.py`
- **Problem:** When a table has no profiler data (common in fresh OM instances), the `/tableProfile/latest` endpoint returns 500 Internal Server Error instead of 404. The enricher only caught 404, so the 500 propagated as an unhandled exception and crashed the rationalization pipeline.
- **Why it matters:** Profiler data is optional — its absence should degrade scores gracefully, not crash the pipeline. A fresh OM instance will always hit this on first run.
- **Fix:** Extended the error handler to treat both 404 and 500 as "no profiler data available" and return empty profile defaults.

---

### SQL Conversion Engine (Fixes 3-4)

The code conversion pipeline had two bugs that prevented `dm convert` from completing.

**Fix 3 — Wrong exception class in sqlglot error handler**
- **File:** `dm/conversion/rule_engine.py`
- **Problem:** The error handler caught `sqlglot.errors.ErrorLevel`, which is an enum, not an exception class. Python 3.14 raises `TypeError: catching classes that do not inherit from BaseException is not allowed`, crashing the converter on any SQL that sqlglot can't parse (including the generated schema with inline comments).
- **Why it matters:** The converter is designed to fall back to regex-based translation when sqlglot fails. This bug prevented the fallback from ever triggering.
- **Fix:** Changed to catch `sqlglot.errors.ParseError`, the actual exception class.

**Fix 4 — Missing `output_path` on ConversionResult**
- **File:** `dm/pipeline.py`
- **Problem:** The pipeline referenced `result.output_path` but the `ConversionResult` dataclass doesn't have that attribute. The conversion itself succeeded and wrote the output file, but the CLI crashed when trying to display the result.
- **Why it matters:** Users would see a traceback after a successful conversion, with no indication of where the output was written.
- **Fix:** Used `getattr` with a fallback to `result.source_path`.

---

### Pipeline Orchestration (Fixes 5-8)

Several pipeline commands (`dm ingest`, `dm observe`) had attribute errors and missing fallbacks that prevented them from running.

**Fix 5 — `None` tables passed to ingestion planner**
- **File:** `dm/pipeline.py`
- **Problem:** When no `--dataset` flag is provided, `run_ingestion` passed `None` to the planner, which tried to iterate over it and raised `TypeError: 'NoneType' object is not iterable`.
- **Why it matters:** `dm ingest --plan` without specifying a dataset should plan all datasets, not crash.
- **Fix:** Falls back to all datasets from `project.yaml` when no dataset is specified.

**Fix 6 — `plan.steps` doesn't exist on MigrationPlan**
- **File:** `dm/pipeline.py`
- **Problem:** The pipeline referenced `plan.steps` but the `MigrationPlan` dataclass uses `strategies` (a dict keyed by table name).
- **Why it matters:** `dm ingest --plan` generated the plan correctly (visible in logs) but crashed when formatting the output.
- **Fix:** Changed to iterate `plan.strategies.values()`.

**Fix 7 — `observer.baseline_path` doesn't exist on PipelineObserver**
- **File:** `dm/pipeline.py`
- **Problem:** The pipeline referenced `observer.baseline_path` but the baseline path lives on the nested `BaselineManager` object at `observer.baseline_manager.baseline_path`.
- **Why it matters:** `dm observe --set-baseline` captured the baseline successfully but crashed when reporting the output path.
- **Fix:** Changed to `observer.baseline_manager.baseline_path`.

**Fix 8 — `observer.get_history()` not implemented**
- **File:** `dm/pipeline.py`
- **Problem:** The pipeline called `observer.get_history()` but this method doesn't exist on `PipelineObserver`.
- **Why it matters:** `dm observe --history` crashed instead of showing an empty history.
- **Fix:** Added a graceful fallback using `getattr` that returns an empty list with a message when the method isn't available.

---

### Dashboard Artifact Generation (Fix 9)

**Fix 9 — Missing `schema_diff.md` and `governance_report.csv`**
- **File:** `dm/pipeline.py` — `_generate_pre_reports()`
- **Problem:** The dashboard's Schema Diff & Mappings tab reads `schema_diff.md` and the Governance tab reads `governance_report.csv` from each run folder. The pre-migration reporter only wrote `readiness_report.md` — the other two files were never generated, leaving those dashboard tabs empty.
- **Why it matters:** Two of the four dashboard tabs showed "No data found" even after successful validation runs. The data existed in the validator results but was never written to disk.
- **Fix:** Added generation of both files by extracting data from the `SchemaDiffValidator` and `GovernanceValidator` results. Also added `structure_score` and `governance_score` to `run_metadata.json` for the dashboard's score breakdown display.

---

### pandas Compatibility (Fix 10)

**Fix 10 — `applymap` renamed to `map` in pandas 2.1+**
- **File:** `dashboard.py`
- **Problem:** The governance table styling used `gov_df.style.applymap()`, which was deprecated in pandas 2.0 and removed in 2.1+. Our environment installed pandas 3.x, causing `AttributeError: 'Styler' object has no attribute 'applymap'`.
- **Why it matters:** The Governance tab crashed on load instead of displaying the color-coded violation/warning/pass table.
- **Fix:** Changed to `gov_df.style.map()`.

---

### COBOL Column Mapping (Fixes 11-13)

This was the most significant issue. The toolkit's column matcher could not handle the naming gap between COBOL legacy systems and modern schemas.

**Fix 11 — Manual mapping workaround (project-specific, superseded by Fix 13)**
- **File:** `projects/unemployment-claims-analysis/metadata/mappings.json`
- **Problem:** All 48 field mappings had `target: null` and `type: pending`, causing the dashboard to show "*(not migrated)*" for every column. The fuzzy matcher (`SequenceMatcher` at 0.7 threshold) scored `bp_recid` vs `payment_id` at 0.25 — far below the threshold.
- **Initial fix:** Manually wrote all 48 mappings with correct targets, types, and rationale. This worked but was project-specific — every new COBOL project would need the same manual effort.
- **Status:** Superseded by Fix 13.

**Fix 12 — Black font on highlighted dashboard rows**
- **File:** `dashboard.py`
- **Problem:** Highlighted rows (archived in red, transform in yellow) used default font color which was unreadable in dark mode.
- **Fix:** Added `color: #000000` to highlighted row styles.

**Fix 13 — Global COBOL-aware column matcher**
- **Files:** `dm/discovery/metadata_generator.py`, `dm/pipeline.py`
- **Problem:** The core issue behind Fix 11. The `find_matching_column()` function relied solely on string similarity, which fundamentally cannot bridge the gap between COBOL abbreviated names and modern descriptive names. This affected every COBOL migration, not just the unemployment claims project.
- **Why it matters:** Without correct mappings, the dashboard shows every column as "not migrated," the RAG chat can't explain field relationships, and the schema diff is misleading. Manual mapping for every project is not scalable.

**Solution — multi-strategy matcher:**

| Strategy | Example | How it works |
|----------|---------|-------------|
| COBOL abbreviation dictionary | `cl_fnam` -> `first_name` | 90+ common patterns. Strips 2-3 char table prefix, looks up suffix. |
| Table-context PK resolution | `bp_recid` + table `benefit_payments` -> `payment_id` | Singularizes table name, tries `{singular}_id` and `{last_word}_id`. |
| COBOL PII detection | `cl_bact` -> archived | Recognizes abbreviated financial fields (`bact`, `brtn`) as PII. |
| FILLER detection | `cl_fil1` -> removed | COBOL FILLER fields (`fil1`, `fil2`, `filler`) have no business value. |
| Containment matching | expanded `status` -> `claimant_status` | Scores by character overlap when expanded name is substring of target. |
| Word overlap | expanded `first_name` -> `first_name` | Scores by shared underscore-delimited word components. |
| Modern DB matching | connects to target DB | Matches expanded abbreviations against actual modern column names. |
| Fuzzy fallback | original `SequenceMatcher` | Runs at 0.7 threshold as last resort. |

**Result:** 48/48 mappings auto-resolved correctly. New COBOL projects get automatic column resolution without any manual mapping.

**Abbreviation dictionary includes:**
- Name fields: `fnam`, `lnam`, `mnam`
- Dates: `dob`, `fildt`, `paydt`, `rgdt`, `lupdt`, `bystr`, `byend`
- Contact: `phon`, `emal`, `adr1`, `city`, `st`, `zip`
- Financial: `payam`, `wkamt`, `mxamt`, `totpd`, `bact`, `brtn`
- Status/codes: `stat`, `ind`, `methd`, `typ`
- References: `clmnt`, `clmid`, `emplr`
- Misc: `dcsd`, `chkno`, `wkedt`, `seprs`, `wkcnt`

---

## Infrastructure Added

**OpenMetadata Docker Compose** (`docker-compose-openmetadata.yml`)
- Full OM 1.6.2 stack: PostgreSQL (port 5433), Elasticsearch (9200), OM Server (8585), Ingestion/Airflow (8080)
- Configured to avoid port conflict with existing app PostgreSQL on 5432
- Required for `dm rationalize`, `dm discover --enrich`, and `dm enrich`

---

## Summary

| # | Fix | Scope | Impact |
|---|-----|-------|--------|
| 1 | OM `owner` -> `owners` | Global | Unblocks rationalization |
| 2 | OM 500 for missing profiles | Global | Prevents crash on fresh OM |
| 3 | sqlglot exception class | Global | Enables SQL conversion fallback |
| 4 | Missing `output_path` | Global | Shows conversion output path |
| 5 | Null tables in ingestion | Global | Enables `dm ingest --plan` without `--dataset` |
| 6 | `plan.steps` attribute | Global | Enables ingestion plan display |
| 7 | Observer baseline path | Global | Enables `dm observe --set-baseline` |
| 8 | Observer history method | Global | Enables `dm observe --history` |
| 9 | Missing dashboard artifacts | Global | Populates Schema Diff and Governance tabs |
| 10 | pandas `applymap` | Global | Fixes Governance tab rendering |
| 11 | Manual mappings | Project | Initial workaround (superseded by 13) |
| 12 | Dashboard font color | Global | Readable highlighted rows |
| 13 | COBOL-aware matcher | Global | Auto-resolves all COBOL column mappings |

**12 of 13 fixes are global** and apply to any future migration project. Fix 11 (manual mappings) was project-specific and has been superseded by Fix 13's global COBOL-aware matcher.

---

## Enhancements

### Signoff Changed from JSON to Plain Text Log

**Files:** `dashboard.py`, `.gitignore`

Changed `signoff.json` to `signoff.log` — a plain text log file where each sign-off appends one line:

```
SIGNOFF | 2026-04-29 | 18:19:09 | Ayanna Muhammad | Data Migration Lead | 94.5/100 | GREEN | Customer Service
```

**Why:** A log file is easier to read, email, grep, and attach to audit packages than JSON. No parsing needed — anyone can open it and understand the sign-off history. The file is committed to git so the audit trail is preserved in version control.

### Repo Name in Sidebar

**File:** `dashboard.py`

The sidebar now shows the repo name (auto-detected from `git remote get-url origin`) below the project name.

### Clickable Lifecycle Detail Pages

**File:** `dashboard.py`

All 6 lifecycle phases in the status bar are now fully built as clickable detail pages with comprehensive drill-down views:

| Phase | Detail Page Contents |
|-------|---------------------|
| Discovery | Tables, Sample Data, Glossary, Field Mappings, PII Detection, Abbreviations, Rationalization |
| Modeling | Table Schemas, Column Mapping, Normalization Plan, Full DDL with download. Legend at bottom (green=Primary, blue=Child, yellow=Lookup) |
| Governance | PII Inventory with regulations, Data Modification Controls, Naming Compliance, Null Threshold Report (live DB query), Audit Trail |
| Transformation | Transform Scripts (ETL INSERT...SELECT), Converted SQL with download, Before/After comparison, Warnings & TODOs |
| Compliance | Compliance Checklist (pass/fail), Readiness Report, Governance Report, Schema Diff, Risk Assessment with score breakdown |
| Quality | Reconciliation, Proof Reports, Score Summary, Sign-Off workflow |

### Sign-Off Workflow

**File:** `dashboard.py`

Added a formal sign-off workflow to the Quality lifecycle page. Authorized personnel can sign off on migration results directly from the dashboard.

**How it works:**
1. Enter name and role, click Sign Off
2. Confirmation dialog: "Are you sure? Your information will be saved as signing off on these changes"
3. Confirm or Cancel
4. Sign-off record stored in `artifacts/signoff.json` with name, role, date, time, score, status, and project

**Features:**
- Multiple sign-offs supported (e.g., tech lead, compliance officer, program manager)
- Sign-off history displayed with color-coded status cards
- Quality button in lifecycle bar turns green (custom HTML badge) after sign-off, stays default otherwise
- Red "NOT SIGNED OFF" banner shown until first sign-off, then green with sign-off details

### Green Quality Badge

**File:** `dashboard.py`

The Quality phase button in the lifecycle status bar now uses a custom HTML badge that turns green after at least one sign-off has been recorded. Before any sign-off, the button remains in its default state. This provides immediate visual feedback on whether the migration has been formally approved.

### Lifecycle Status Bar

**File:** `dashboard.py`

Added a Data Migration Lifecycle status bar that appears at the top of every dashboard page, inspired by the lifecycle diagram in the README. Provides at-a-glance context for where the project stands in the migration process.

**Features:**
- **Project name** — Reads from `project.yaml` instead of displaying the full filesystem path
- **Average confidence score** — Computed from the most recent date's validation runs only (not all historical runs). Color-coded: GREEN (>= 90), YELLOW (70-89), RED (< 70). The date of the score is displayed below the average for context.
- **6 lifecycle phases** — Discovery, Modeling, Governance, Transformation, Compliance, Quality. Each phase shows as completed (green), current (highlighted), or pending (gray) based on which artifacts exist and which validations have been run
- **Clean title** — Status bar shows only the project name. Phase and dataset context is displayed in the run header below, avoiding duplication.

### Auto-Generated Abbreviations

**File:** `dm/discovery/metadata_generator.py`

Added automatic generation of `abbreviations.yaml` from COBOL copybook descriptions during `dm discover` or `dm enrich`. The system parses OpenMetadata column descriptions (e.g., `"CONTACT-FIRST-NAME"` for column `ct_fnam`), strips the table prefix, converts to snake_case, and writes the abbreviation-to-expansion mappings to `metadata/abbreviations.yaml` in the project folder.

**Why it matters:** Previously, the built-in COBOL abbreviation dictionary (Fix 13) handled common patterns, but project-specific or non-standard abbreviations required manual intervention. This enhancement eliminates manual abbreviation configuration entirely by deriving mappings directly from the copybook descriptions already stored in OpenMetadata. Project-specific abbreviations are merged with the built-in dictionary, with project overrides taking priority.

**New functions:**
- `parse_cobol_description()` — Extracts the abbreviated suffix and expanded name from a COBOL copybook description
- `generate_abbreviations_yaml()` — Writes all parsed mappings to `metadata/abbreviations.yaml`
- `load_project_abbreviations()` — Loads and merges project abbreviations with the built-in dictionary

The generated file can still be manually reviewed and edited to handle edge cases, but no manual step is required.

**How current phase is determined:**
| Phase | Completed when |
|-------|---------------|
| Discovery | `metadata/glossary.json` exists |
| Modeling | `artifacts/generated_schema/` exists |
| Governance | Pre-validation runs exist |
| Transformation | `artifacts/converted/` exists |
| Compliance | Pre-validation runs exist |
| Sign-Off | `signoff.log` has entries |
| Post-Migration | Post-validation runs exist |

---

## Multi-Target Platform Support (2026-05-04)

Major feature: the toolkit now supports multiple target database platforms. Users can select a target from the dashboard and see DDL, scores, and transform scripts tailored to that platform.

### Target Adapters

**Files:** `dm/targets/snowflake.py`, `dm/targets/oracle.py`, `dm/targets/redshift.py`, `dm/targets/postgres.py`

Added three new target platform adapters alongside the existing PostgreSQL adapter:

| Target | Dialect | Key Differences |
|--------|---------|-----------------|
| **PostgreSQL** | postgres | Full type support, JSONB, enforced FK/CHECK constraints |
| **Snowflake** | snowflake | `CREATE OR REPLACE TABLE`, `VARIANT` for JSON, `NUMBER(38,0) AUTOINCREMENT`, FK/CHECK not enforced |
| **Oracle** | oracle | `VARCHAR2`, `NUMBER(10) GENERATED ALWAYS AS IDENTITY`, `CLOB` for JSON, no native BOOLEAN (NUMBER(1)) |
| **AWS (Redshift)** | redshift | `INTEGER IDENTITY(1,1)`, `SUPER` for JSON, `DISTSTYLE KEY`/`DISTKEY`/`SORTKEY`, nothing enforced |

Each adapter implements the full `BaseTargetAdapter` interface: type mapping, DDL rendering, INSERT...SELECT rendering, and function translation.

Registered in `BUILTIN_TARGETS` with aliases (`aws` maps to `redshift`). A `get_available_targets()` helper returns display names for the dashboard selector. A `TARGET_DISPLAY_NAMES` dict provides human-readable labels.

### Target-Aware Schema Generation

**Files:** `dm/discovery/schema_gen.py`, `dm/pipeline.py`

- `SchemaGenerator` accepts an optional `target_adapter` parameter
- `_map_base_type()` delegates to the adapter's `map_type()` when set
- `render_ddl()` delegates to the adapter's `render_create_table()` when set
- `render_transforms()` generates dialect-specific hash expressions (`SHA2()` for Snowflake, `DBMS_CRYPTO.HASH()` for Oracle, `encode(sha256())` for Postgres), cast expressions, and boolean literals
- `render_full_ddl()` includes the target platform name in the header comment
- `save_all_targets()` generates DDL for all four platforms into subfolders (`postgres/`, `snowflake/`, `oracle/`, `redshift/`)
- `run_schema_generation()` in pipeline.py calls `save_all_targets()` automatically after the default save

### Target-Aware Scoring

**File:** `dm/scoring.py`

Each target platform has different capabilities that affect migration confidence. The scoring engine now applies platform-specific penalties:

| Target | Structure | Integrity | Governance | Notes |
|--------|-----------|-----------|------------|-------|
| PostgreSQL | 0 | 0 | 0 | Full support |
| Snowflake | -2 | -5 | -4 | FK/CHECK not enforced, no native UUID |
| Oracle | -3 | 0 | 0 | No native BOOLEAN, JSON→CLOB |
| AWS (Redshift) | -3 | -8 | -4 | Nothing enforced, no binary type |

- `calculate_confidence()` accepts optional `target` parameter
- `calculate_confidence_all_targets()` returns scores for all four platforms
- Each result includes `target_notes` explaining why points were deducted

### Dashboard Target Platform Selector

**File:** `dashboard.py`

- **Sidebar**: "Target Platform" dropdown (PostgreSQL, Snowflake, Oracle, AWS Redshift) with expandable platform capability notes
- **Score display**: Recalculates live when target changes; shows target name under the gauge
- **Modeling page**: Reads DDL from target-specific subfolder; download button labeled per-platform; tabbed comparison view showing all four DDLs side-by-side
- **Compliance page**: "Score by Target Platform" comparison table

---

## Multi-Source Database Support (2026-05-04)

### Config Helpers

**File:** `dm/config.py`

Added three new functions for per-dataset source/target resolution:

- `get_dataset_source(config, dataset)` — returns the source connection name (defaults to `"legacy"`)
- `get_dataset_target(config, dataset)` — returns the target connection name (defaults to `"modern"`)
- `get_all_sources(config)` — returns deduplicated list of all source connection names

Updated `get_connection_config()` error message to list available connections.

### Call Site Updates

Removed all hardcoded `config["connections"]["legacy"]` and `config["connections"]["modern"]` references (9 call sites across 5 files):

| File | Change |
|------|--------|
| `dm/pipeline.py` (run_validation) | Resolves source/target per-dataset |
| `dm/pipeline.py` (run_enrichment) | Resolves first dataset's target for column matching |
| `dm/pipeline.py` (observer) | Resolves target from config or defaults to `"modern"` |
| `dm/cli.py` (discover) | Uses `get_all_sources()[0]` instead of `"legacy"` |
| `dm/ingestion/executor.py` | `_get_modern_connection()` accepts optional dataset for per-dataset target |
| `dm/conversion/converter.py` | Dialect detection uses first source connection type |

### New project.yaml Format

Datasets can now specify which connection to use:

```yaml
connections:
  eligibility_db: { type: db2, host: mainframe.state.gov }
  claims_db: { type: db2, host: mainframe.state.gov }
  modern: { type: postgres, host: localhost }

datasets:
  - name: claimants
    source: eligibility_db
    target: modern
  - name: claims
    source: claims_db
    target: modern
```

Fully backward compatible — existing configs with just `legacy`/`modern` work unchanged.

---

## DB2 and Oracle Source Connectors (2026-05-04)

**Files:** `dm/connectors/db2.py`, `dm/connectors/oracle.py`, `dm/connectors/postgres.py`, `pyproject.toml`

Added two new source database connectors for reading from legacy systems:

### DB2 Connector (`dm/connectors/db2.py`)
- Uses `ibm_db` / `ibm_db_dbi` (standard IBM Python driver)
- Schema introspection via `SYSCAT.COLUMNS`
- Column hashing via `HASH()` (DB2 LUW 11.1+) with fallback
- `FETCH FIRST N ROWS ONLY` for row limiting
- Optional SSL and schema config

### Oracle Connector (`dm/connectors/oracle.py`)
- Uses `oracledb` (thin mode — no Oracle client install required)
- Supports both `service_name` and `sid` connection styles
- Schema introspection via `ALL_TAB_COLUMNS`
- Column hashing via `STANDARD_HASH()` (12c+) with `ORA_HASH()` fallback
- `ROWNUM <= N` for row limiting

### Registry
- Both lazy-loaded in the connector factory — `ibm-db` and `oracledb` only imported when actually used
- Added `[db2]` and `[oracle]` optional dependency groups to `pyproject.toml`
- Factory updated to handle lazy-loaded connectors

---

## Cross-Source Referential Integrity (2026-05-04)

**Files:** `dm/validators/post/referential.py`, `dm/observer/checks/integrity.py`

When the child and parent tables live in different databases (e.g., claims in `claims_db` references claimants in `eligibility_db`), the validator now opens connections to both sources and checks for orphans in Python.

### Config Format

```yaml
referential_integrity:
  claims:
    - child_table: claims
      child_source: claims_db
      parent_table: claimants
      parent_source: eligibility_db
      fk_column: claimant_id
      pk_column: claimant_id
```

When `child_source` and `parent_source` are omitted or identical, falls back to original single-connection JOIN.

### How It Works
1. Opens separate connections to both databases
2. Pulls distinct FK values from child table
3. Pulls distinct PK values from parent table
4. Computes orphans as `child_fks - parent_pks` in Python
5. Cleans up both connections

Results include `"cross_source": true` flag. Both the post-migration validator and the observer integrity check support this.

---

## Local Profiling Fallback (2026-05-04)

**File:** `dm/pipeline.py`

When OpenMetadata has no profiler data (common with OM 1.6.2 which can't persist profiles via REST API), the schema generation pipeline now falls back to `metadata/profiling_stats.json` — a local file containing column-level statistics (null %, distinct count, max length, value frequencies) computed directly from the database.

---

## Ingestion Planner Fix (2026-05-04)

**File:** `dm/ingestion/planner.py`

Fixed `_build_dependency_graph()` to handle both flat list and per-dataset dict formats for `referential_integrity` config. Previously crashed with `AttributeError: 'str' object has no attribute 'get'` when the config used the dict format.

---

## Dashboard Overhaul (2026-05-04)

### Python 3.9 Compatibility
- Replaced `dict | None`, `str | None`, `list[str]` type hints with `Optional[dict]`, `Optional[str]`, `List[str]` from `typing`

### Gated Workflow
The dashboard now enforces a strict workflow: **PRE → Sign-Off → POST → Prove**

- **Sidebar**: Only shows "Run PRE Validation"
- **Sign-Off page**: New dedicated lifecycle phase with PRE score summary, sign-off form, confirmation dialog, and history
- **Post-Migration page**: New dedicated lifecycle phase with run buttons (single dataset + run all with progress bar), reconciliation reports with timestamps, proof report generation, and score summary
- POST validation and Prove are locked until sign-off is recorded
- All actions that were previously in the sidebar (POST, Prove) now live on the Post-Migration page

### Lifecycle Bar Updated to 7 Phases
```
Discovery → Modeling → Governance → Transformation → Compliance → Sign-Off → Post-Migration
```

Sign-Off shows as locked (🔒) until PRE results are signed off. Post-Migration is disabled until sign-off.

### Latest Results in Sidebar
- Groups runs by dataset, shows only latest PRE and POST scores per dataset
- Shows `⏳ Awaiting Sign-Off` for POST when sign-off hasn't been recorded
- Shows actual POST scores once runs exist
- "View Run" dropdown shows only latest runs; older runs in collapsible "Run History"

### Removed Row Highlighting
Removed background color highlighting from all dataframe rows across governance, compliance, and modeling pages. Tables now use plain text for readability.

### Removed Abbreviations Tab
Removed the standalone "Abbreviations" tab from Discovery — abbreviation expansion is already shown inline in the Modeling page's column mapping.

### Updated Discovery Summary
Changed from "Legacy Tables, Legacy Columns, Modern Columns, PII Fields" to more meaningful metrics:
- Tables Discovered
- Columns Mapped (e.g., 44/48)
- Mapping Confidence (average %)
- PII Fields
- Migrate / Archive (e.g., 0/4)

### Reconciliation Report Timestamps
Each reconciliation report title now includes the run date and time.

---

## COBOL Copybook Parser & Flat File Connector (2026-05-04)

Enables the toolkit to ingest mainframe data directly from copybooks and flat file extracts — no database connection required.

### Copybook Parser (`dm/connectors/copybook_parser.py`)

Parses COBOL `.cpy` files to extract field definitions:

- Extracts field name, PIC clause, byte offset, and length
- Handles group levels (01, 05, 10), REDEFINES, OCCURS, FILLER
- Maps PIC clauses to SQL types:

| PIC Clause | SQL Type |
|------------|----------|
| `PIC X(25)` | `VARCHAR(25)` |
| `PIC 9(5)` | `INTEGER` |
| `PIC S9(7)V99` | `NUMERIC(9,2)` |
| `PIC 9(15)` | `BIGINT` |
| `PIC X` | `CHAR(1)` |
| `PIC A(10)` | `VARCHAR(10)` |
| `PIC 9(3)` | `SMALLINT` |

- Skips FILLER fields and 88-level condition names
- Works from file path or raw text string
- Outputs `CopybookLayout` with `to_schema()` for direct use in the pipeline

### Flat File Connector (`dm/connectors/flatfile.py`)

Implements the full `BaseConnector` interface for reading mainframe extracts:

- **Fixed-width files** — parsed using copybook layout (field offsets and lengths)
- **CSV/TSV files** — configurable delimiter
- **EBCDIC encoding** — auto-decodes CP037 (US/Canada mainframes) to UTF-8
- **All validation helpers** — row count, null %, distinct count, column hash, checksums, duplicate detection
- Registered as three connector types: `flatfile`, `copybook`, `csv`

Config format:
```yaml
connections:
  mainframe_extract:
    type: copybook
    copybook: /data/CLAIMANT.cpy
    datafile: /data/CLAIMANT.dat
    encoding: ebcdic
    format: fixed
    table_name: claimants

  federal_feed:
    type: flatfile
    datafile: /data/qc_sample.csv
    format: csv
    table_name: federal_sample
```

---

## Git Repo Loader (`dm/repo_loader.py`) (2026-05-04)

Enables the toolkit to ingest mainframe artifacts directly from a git repository URL.

### How It Works

1. **Clone** — `clone_repo(url)` clones a git repo (or pulls latest if already cloned)
2. **Scan** — `scan_repo(path)` auto-detects mainframe artifacts:
   - `.cpy` / `.cob` → COBOL copybooks
   - `.dat` / `.bin` / `.raw` → fixed-width data files
   - `.csv` / `.tsv` → delimited data
   - `.sql` → legacy DDL/DML
   - `.txt` → auto-detected as fixed-width (same-length lines) or CSV (delimiters)
3. **Pair** — matches copybooks to data files by filename (e.g., `CLAIMANT.cpy` + `CLAIMANT.dat`)
4. **Generate** — creates full `project.yaml` with connections, datasets, validation config

### CLI Integration

```bash
# From a git repo
dm init my-project --repo https://github.com/agency/mainframe-extracts.git

# From a local directory
dm init my-project --data /path/to/mainframe/files

# With a specific target platform
dm init my-project --repo <url> --target snowflake
```

The generated project is immediately ready for the pipeline:
```bash
dm discover --project projects/my-project
dm generate-schema --all --project projects/my-project
dm validate --phase pre --dataset claimants --project projects/my-project
```

---

## Full Toolkit Capabilities Summary (2026-05-04)

### What the toolkit can do now

**Input Sources — read from any legacy system:**

| Source | Connector | How |
|--------|-----------|-----|
| IBM DB2 mainframe | `dm/connectors/db2.py` | Direct JDBC connection via `ibm_db` |
| Oracle Database | `dm/connectors/oracle.py` | Direct connection via `oracledb` (thin mode) |
| PostgreSQL | `dm/connectors/postgres.py` | Direct connection via `psycopg2` |
| COBOL copybook + flat file | `dm/connectors/flatfile.py` | Parses `.cpy` layout, reads fixed-width `.dat` files |
| CSV / TSV files | `dm/connectors/flatfile.py` | Standard delimited file reading |
| EBCDIC-encoded files | `dm/connectors/flatfile.py` | Auto-decodes CP037 to UTF-8 |
| Git repository | `dm/repo_loader.py` | Clones repo, auto-detects and pairs artifacts |
| Multiple databases | `dm/config.py` | Per-dataset `source`/`target` in `project.yaml` |

**Output Targets — generate schemas for any platform:**

| Target | Adapter | DDL Features |
|--------|---------|-------------|
| PostgreSQL | `dm/targets/postgres.py` | SERIAL, JSONB, TIMESTAMPTZ, enforced FK/CHECK |
| Snowflake | `dm/targets/snowflake.py` | CREATE OR REPLACE, VARIANT, NUMBER AUTOINCREMENT |
| Oracle | `dm/targets/oracle.py` | VARCHAR2, NUMBER IDENTITY, CLOB for JSON |
| AWS Redshift | `dm/targets/redshift.py` | IDENTITY, SUPER for JSON, DISTSTYLE/DISTKEY/SORTKEY |

**10-Phase Pipeline:**

| Phase | Command | What it does |
|-------|---------|-------------|
| **Init** | `dm init --repo <url>` | Clone repo, scan artifacts, scaffold project |
| **Profile** | Local profiler | Column stats: null %, distinct count, value frequencies |
| **Discover** | `dm discover --enrich` | Schema introspection, COBOL abbreviation expansion, PII tagging |
| **Rationalize** | `dm rationalize` | Score tables for migration scope (migrate/review/archive) |
| **Generate Schema** | `dm generate-schema --all` | Normalized DDL for all 4 target platforms |
| **Convert** | `dm convert` | Legacy SQL → target dialect (80% rule engine, 20% AI) |
| **PRE Validate** | `dm validate --phase pre` | Structure, governance, PII, naming, data quality checks |
| **Sign-Off** | Dashboard | Formal approval with name, role, timestamp |
| **POST Validate** | `dm validate --phase post` | Row counts, checksums, FK integrity, aggregates |
| **Prove** | `dm prove` | Combined pre+post audit package |

**Scoring — target-aware confidence:**

- Base formula: `confidence = (0.4 × structure) + (0.4 × integrity) + (0.2 × governance)`
- Platform penalties: PostgreSQL (0), Snowflake (-11), Oracle (-3), Redshift (-15)
- Traffic lights: GREEN >= 90, YELLOW 70-89, RED < 70
- Cross-platform comparison table in dashboard

**Dashboard — 7-phase gated workflow:**

```
Discovery → Modeling → Governance → Transformation → Compliance → Sign-Off → Post-Migration
```

- Target platform selector (live DDL and score switching)
- PRE validation from sidebar
- POST validation and Proof generation from Post-Migration page
- Gated: POST requires sign-off, Prove requires POST
- Per-dataset score summary, run history, reconciliation reports with timestamps

**Cross-Source Features:**

- Multiple databases per project (each dataset can point to a different source)
- Cross-database referential integrity (FK checks across DB2 + Oracle, etc.)
- Mixed source types (DB2 for core, flat files for federal feeds, CSV for extracts)

**COBOL-Aware:**

- 90+ built-in abbreviation patterns
- Auto-generates abbreviations from copybook descriptions
- PIC clause → SQL type mapping
- EBCDIC → UTF-8 decoding
- FILLER field detection and removal
- Copybook-driven fixed-width file parsing

**What a new mainframe migration needs:**

1. `dm init my-project --repo <url>` (or `--data /path`)
2. `dm discover --enrich --project projects/my-project`
3. `dm rationalize --project projects/my-project`
4. `dm generate-schema --all --project projects/my-project`
5. `dm validate --phase pre --dataset <name> --project projects/my-project`
6. Sign off in the dashboard
7. `dm validate --phase post` and `dm prove` from the Post-Migration page

No project-specific code required. The plugin system handles edge cases.

---

## Scoring Improvements (2026-05-04)

Three fixes that directly improved migration confidence scores.

### Rationalization Local Profiling Fallback

**File:** `dm/rationalization/discoverer.py`

When OpenMetadata has no profiler data, the rationalization engine now falls back to `metadata/profiling_stats.json` — the same local profiling data used by schema generation.

- `MigrationRationalizer` constructor accepts optional `config` parameter
- `_evaluate_table()` checks if OM profile is empty, loads local stats if available
- `_load_local_profile()` reads from `profiling_stats.json`, converts file mtime to `profiled_at` timestamp
- Pipeline passes `config` to the rationalizer

**Impact:** Tables went from `0 migrate / 0 review / 4 archive` to `0 migrate / 4 review / 0 archive`. The completeness scores now reflect actual data (low null %) and freshness scores reflect when profiling was run.

### Target-Specific Type Optimization

**File:** `dm/discovery/schema_gen.py`

The `optimize_data_type()` method now delegates profiling-based type decisions to the target adapter instead of hardcoding PostgreSQL types.

| Optimization | Before (hardcoded) | After (target-aware) |
|---|---|---|
| Boolean detection | Always `BOOLEAN` | Oracle: `NUMBER(1)`, others: adapter's boolean type |
| VARCHAR right-sizing | Always `VARCHAR(n)` | Oracle: `VARCHAR2(n)`, Snowflake: `VARCHAR(n)` |
| Integer narrowing | Always `INTEGER`/`BIGINT` | Oracle: `NUMBER(10)`/`NUMBER(19)`, Snowflake: `NUMBER(38,0)` |
| Date heuristic | Always `DATE` | Delegates to `adapter.map_type("date")` |
| Timestamp | Always `TIMESTAMPTZ` | Delegates to `adapter.map_type("timestamp")` |

**Impact:** Claimants PRE score improved from 89.0 YELLOW to 90.6 GREEN due to fewer type mismatch penalties for non-PostgreSQL targets.

### `dm profile` Command

**File:** `dm/cli.py`

New first-class CLI command that profiles legacy tables and saves column-level statistics to `metadata/profiling_stats.json`.

```bash
dm profile --project projects/my-project
```

Computes per column: null %, distinct count, max length, min/max values, and top 10 value frequencies. Uses per-dataset source resolution (supports multi-source configs).

Runs automatically as step 2 in `dm bootstrap` (6-step pipeline: init → **profile** → discover → rationalize → generate-schema → validate).

**Impact:** Profiling data is now always available before discovery and rationalization, eliminating the dependency on OM's profiler pipeline.

---

## Dashboard Setup Screen (2026-05-05)

**File:** `dashboard.py`

Added a setup screen that appears when no project is loaded. Users enter a git repo URL and click one button to run the entire pipeline.

### How It Works

1. User opens `http://localhost:8501` with no `--project` flag
2. Setup screen shows: repo URL input, project name, target platform dropdown
3. User clicks **Run Migration Analysis**
4. Live progress shows each step:
   - Cloning repository
   - Scanning for copybooks and data files (paired by record length)
   - Configuring project with only usable datasets
   - Running discovery, profiling, schema generation for all 4 targets
   - Running PRE validation on every dataset
5. Page auto-reloads into the full dashboard with all results

The `.dm_active_project` marker file tells the dashboard which project to load on subsequent visits.

### Start Over Button

Added to the sidebar bottom. Shows a confirmation dialog, then deletes the project directory and marker file, returning to the setup screen.

---

## Flat File Pipeline (2026-05-05)

**File:** `dm/pipeline_flatfile.py`

A single function (`run_flatfile_pipeline`) that generates all artifacts from copybook and flat file sources — identical output to the OM-backed pipeline but without requiring OpenMetadata or a database.

### Artifacts Generated

| Artifact | Description |
|----------|-------------|
| `metadata/profiling_stats.json` | Column-level statistics (null %, distinct count, max length, value frequencies) |
| `metadata/glossary.json` | Column metadata with PII flags and descriptions |
| `metadata/mappings.json` | Source-to-target column mappings with types (rename/transform/archived) |
| `metadata/abbreviations.yaml` | Field name abbreviation mappings |
| `metadata/normalization_plan.json` | Entity decomposition with address sub-entities |
| `metadata/rationalization_report.json` + `.md` | Migration scope scoring |
| `metadata/migration_scope.yaml` | Migrate/review/archive classification |
| `artifacts/generated_schema/{target}/` | DDL + transform SQL for all 4 targets |
| `artifacts/generated_schema/diff_report.json` | Column mapping summary |
| `artifacts/generated_schema/updated_datasets.yaml` | Dataset config |

### Auto-Detection

`dm discover` now auto-detects flat file sources by checking connection types in `project.yaml`. If all sources are `copybook`, `flatfile`, or `csv`, it routes to the flat file pipeline instead of trying OpenMetadata.

---

## Record-Length Copybook Matching (2026-05-05)

**File:** `dm/repo_loader.py`

When copybooks and data files have different names (e.g., `CVCUS01Y.cpy` vs `custdata.txt`), the repo scanner now matches them by record length:

1. Parses each unpaired copybook to get its record length
2. Reads the first line of each unpaired data file to get its line length
3. Matches when line length = record length (or within +/- 2 byte tolerance)

Also improved `.txt` file detection: files in `data/` directories are automatically classified as data files.

**Impact:** The AWS CardDemo repo (where copybooks and data files use completely different naming conventions) went from 0 usable datasets to 9 paired datasets.

---

## Customer Service Demo Repo (2026-05-05)

Created standalone repo for customer service demo data:

**https://github.com/ayannamuhammad-nava/customer-service-data**

Contains:
- `CONTACTS.cpy` — COBOL copybook (42 fields)
- `CONTACTS.dat` — fixed-width data file
- `create_contacts_legacy.sql` — legacy DDL
- `create_contacts_modern.sql` — modern DDL
- `load_contacts_legacy.sql` — sample data

Can be used directly with the setup screen or `dm init --repo`.

---

## Score Notes Tab (2026-05-05)

**File:** `dashboard.py`

Added a **Score Notes** tab to the PRE and POST validation run views, positioned before the Ask the Agent tab.

Contents:
- **Scoring formula** — `confidence = (0.4 x structure) + (0.4 x integrity) + (0.2 x governance)`
- **Component scores** — table showing each component's score and what it measures
- **Target platform impact** — shows penalties applied for the selected platform with explanations
- **How to improve** — context-specific recommendations based on which components scored low
- **Score thresholds** — GREEN/YELLOW/RED definitions

---

## Setup Screen Layout Fix (2026-05-05)

**File:** `dashboard.py`

- Setup screen now uses **centered layout** with collapsed sidebar (clean, focused input form)
- Dashboard uses **wide layout** with expanded sidebar (full data view)
- Layout is determined before `set_page_config` by checking if a project exists
- After setup completes, page auto-reruns and switches to wide layout with the latest PRE run selected
- `.dm_show_latest_run` marker ensures the dashboard lands on the run view, not a lifecycle page
