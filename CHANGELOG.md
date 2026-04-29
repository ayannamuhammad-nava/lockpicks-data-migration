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
| Quality | Post-validation runs exist |
