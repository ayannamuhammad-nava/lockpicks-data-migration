# Lockpicks Data Migration — What Is This?

## The Problem

Migrating data from legacy systems (COBOL, DB2, mainframes) to modern platforms (PostgreSQL, cloud databases) is one of the highest-risk steps in modernization. Teams face hard questions:

- **Which tables actually need to be migrated?** Legacy systems accumulate hundreds of tables over decades. Many are unused, stale, or duplicates. Migrating everything wastes time and money.
- **Is the data safe to move?** Legacy schemas often contain plaintext PII (Social Security numbers, bank accounts), inconsistent formats, orphaned records, and undocumented business rules.
- **Did the data survive?** After migration, how do you prove that row counts match, values are intact, and no data was lost or corrupted?
- **How do you explain this to non-technical stakeholders?** Auditors, compliance officers, and program managers need clear evidence — not database logs.

## What Lockpicks Does

Lockpicks Data Migration is a CLI toolkit that automates the entire migration lifecycle with a **deterministic-first, AI-second** approach. Every step produces auditable artifacts and a quantified 0-100 confidence score.

### The 10-Step Process

```
LEGACY DATABASE (COBOL/DB2)
        |
        v
  1. RATIONALIZE -----> Which tables are worth migrating?
        |                Scores each table on usage, lineage, freshness.
        |                Recommends: Migrate / Review / Archive.
        v
  2. DISCOVER ----------> What does this data look like?
        |                Pulls schemas, column profiles, PII tags,
        |                glossary terms from OpenMetadata catalog.
        v
  3. ENRICH ------------> Fill in the gaps and map columns
        |                COBOL-aware matcher auto-resolves abbreviated
        |                names (cl_fnam -> first_name, bp_payam ->
        |                payment_amount) using a 90+ pattern dictionary,
        |                table-context PK inference, and PII detection.
        |                Auto-generates abbreviations.yaml from COBOL
        |                copybook descriptions in OpenMetadata.
        v
  4. GENERATE SCHEMA ---> Design the modern database
        |                Expands COBOL abbreviations (cl_fnam -> first_name),
        |                normalizes tables, optimizes types, handles PII.
        v
  5. CONVERT -----------> Translate legacy SQL to modern SQL
        |                Rule engine handles ~80% automatically.
        |                Claude AI refines the rest (optional).
        v
  6. PRE-VALIDATE ------> Is it safe to migrate?
        |                Checks schema compatibility, PII exposure,
        |                data quality, naming conventions.
        |                Score: 0-100 (RED / YELLOW / GREEN)
        v
  7. INGEST ------------> Execute the migration
        |                Dependency-ordered table loading with
        |                checkpoint/resume support.
        v
  8. POST-VALIDATE -----> Did the data survive?
        |                Row counts, checksums, FK integrity,
        |                sample comparison, aggregate verification.
        |                Score: 0-100 (RED / YELLOW / GREEN)
        v
  9. PROVE -------------> Generate the audit package
        |                Combines pre + post scores into a final
        |                proof report for stakeholders and auditors.
        v
  10. OBSERVE -----------> Monitor for drift
                          Tracks schema changes, volume anomalies,
                          data freshness after go-live.

        |
        v
  MODERN DATABASE (PostgreSQL)
        |
        v
  DASHBOARD (localhost:8501)
        Interactive Streamlit UI with lifecycle status bar,
        confidence scores, schema diffs, governance findings,
        and a RAG chat for asking questions about any field.
        All 6 lifecycle phases (Discovery, Modeling, Governance,
        Transformation, Compliance, Quality) are clickable detail
        pages with full drill-down views. The Quality page includes
        a sign-off workflow where authorized personnel (tech lead,
        compliance officer, program manager) can formally sign off
        on migration results — stored in artifacts/signoff.json
        with name, role, date, time, score, and status.
```

### What Makes It Different

- **Deterministic-first:** Rule engines produce complete output at every stage. AI refines but is never required.
- **Quantified confidence:** Every step produces a 0-100 score with GREEN/YELLOW/RED status — not just pass/fail.
- **Auditable artifacts:** Every run creates timestamped folders with markdown reports, CSV findings, and JSON metadata.
- **COBOL-aware:** Built-in dictionary of 90+ COBOL copybook abbreviations automatically maps legacy column names to modern equivalents — no manual mapping needed.
- **Pluggable:** 19 hook points let you add domain-specific rules (custom validators, scoring overrides, PII patterns) without modifying the toolkit.
- **OpenMetadata-native:** Uses OM as the metadata backbone for schema discovery, profiling, lineage, and PII classification.

## Confidence Scoring

Every validation produces a weighted score:

| Component | Weight | What It Measures |
|-----------|--------|-----------------|
| Structure | 40% | Schema compatibility between legacy and modern |
| Integrity | 40% | Data completeness, referential integrity, value accuracy |
| Governance | 20% | PII compliance, naming conventions, null thresholds |

| Score | Status | Meaning |
|-------|--------|---------|
| 90-100 | GREEN | Safe to proceed |
| 70-89 | YELLOW | Review recommended |
| 0-69 | RED | Fix issues before proceeding |

## Understanding the Schema Diff

When viewing the dashboard or schema diff reports, you'll see columns listed as "missing in modern." This does **not** mean data was lost. The legacy system uses COBOL copybook abbreviated naming (`cl_fnam`, `bp_payam`, `er_ein`) while the modern schema uses full descriptive names (`first_name`, `payment_amount`, `employer_ein`). The diff compares by column name, so every rename appears as "missing" + "new."

To see the actual rename mappings, check the **Field Mappings** table in the dashboard or `metadata/mappings.json`.

Only a few columns are truly removed during migration — typically PII fields archived for compliance (e.g., `cl_bact` bank account, `cl_brtn` routing number) and COBOL FILLER fields with no business value.

## The Demo: Unemployment Claims (LOOPS NJ)

This repo includes a complete working example using the **NJ Department of Labor Unemployment Insurance System** — a COBOL/DB2 legacy system with 4 tables:

| Table | Rows | Description |
|-------|------|-------------|
| claimants | 10 | Unemployed individuals filing for benefits |
| employers | 5 | Employers where claimants were separated from |
| claims | 15 | Individual unemployment claims |
| benefit_payments | 20 | Weekly benefit payments |

The demo data includes 13 intentional data quality issues (duplicate SSNs, orphan records, PII in plaintext, mixed date formats, etc.) to showcase the toolkit's detection capabilities.

## Next Steps

| What you want to do | Where to go |
|---------------------|-------------|
| **Step through the full process hands-on** | [WALKTHROUGH.md](WALKTHROUGH.md) — 19 detailed steps with every command, output, and fix |
| **Understand the technical details** | [README.md](README.md) — Architecture, CLI reference, plugin system, AI integration |
| **Just run it** | `dm init my-project` then follow the Quick Start in [README.md](README.md#quick-start-bootstrap-a-new-project) |

## Requirements

- Python 3.11+
- Docker (for OpenMetadata and demo databases)
- PostgreSQL (target database)
- OpenMetadata instance (provided via docker-compose in this repo)
- Anthropic API key (optional — for AI-assisted conversion)
