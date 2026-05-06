"""
AI Prompt Templates

String constants with {placeholder} fields for use with str.format().
These prompts drive AI-assisted operations throughout DM.
"""

# ── Schema Refinement ─────────────────────────────────────────────────

SCHEMA_REFINEMENT_PROMPT = """\
Review the following generated DDL and suggest improvements.

## Generated DDL

```sql
{ddl}
```

## Business Glossary

```json
{glossary}
```

## Instructions

1. Check that column names follow the business glossary terminology.
2. Verify that data types are appropriate for the target platform.
3. Identify missing constraints (NOT NULL, UNIQUE, CHECK) that the data \
implies.
4. Suggest index recommendations based on likely query patterns.
5. Flag any naming inconsistencies or abbreviations that should be expanded.

Return the refined DDL with inline comments explaining each change. \
If no changes are needed, return the original DDL with a comment confirming \
it looks correct."""

# ── Code Conversion ───────────────────────────────────────────────────

CODE_CONVERSION_PROMPT = """\
Review the following SQL code conversion and correct any issues.

## Original Source SQL

```sql
{source_sql}
```

## Machine-Translated SQL (target: {target_platform})

```sql
{translated_sql}
```

## Instructions

1. Verify that the translated SQL is syntactically valid for {target_platform}.
2. Check that all functions, operators, and data types are correctly mapped.
3. Ensure date/time handling, string functions, and NULL semantics are \
preserved.
4. Identify any logic changes introduced by the translation.
5. Confirm that performance characteristics are reasonable.

Return the corrected SQL. If the translation is already correct, return it \
unchanged with a brief confirmation comment."""

# ── Drift Explanation ─────────────────────────────────────────────────

DRIFT_EXPLANATION_PROMPT = """\
Analyze the following data drift observation and provide an explanation.

## Drift Details

- **Check:** {check_name}
- **Table:** {table}
- **Severity:** {severity}

## Check Results

```json
{details}
```

## Baseline Context

```json
{baseline_context}
```

## Instructions

1. Explain the most likely root cause of this drift.
2. Assess whether this is expected (e.g., organic growth) or unexpected \
(e.g., data pipeline failure, schema migration).
3. Suggest concrete remediation steps.
4. Rate the urgency: IMMEDIATE, SOON, or MONITOR.

Provide a concise, actionable summary."""

# ── Rationalization ───────────────────────────────────────────────────

RATIONALIZATION_PROMPT = """\
Analyze the following table and determine whether it should be included \
in the migration scope.

## Table Information

- **Table Name:** {table_name}
- **Row Count:** {row_count}
- **Column Count:** {column_count}
- **Last Updated:** {last_updated}

## Schema

```json
{schema}
```

## Profiling Summary

```json
{profiling}
```

## Relevance Score

Calculated score: {relevance_score}/100

## Instructions

1. Explain why this table scored {relevance_score}/100 for migration relevance.
2. Identify whether this table contains: active business data, reference/lookup \
data, archive/historical data, or system/temp data.
3. Recommend one of: MIGRATE, ARCHIVE, REFERENCE_ONLY, EXCLUDE.
4. Provide a brief rationale (2-3 sentences) suitable for a stakeholder review.

Format your response as:
- **Category:** <category>
- **Recommendation:** <recommendation>
- **Rationale:** <rationale>"""

# ── Column Name Understanding ────────────────────────────────────────

COLUMN_UNDERSTANDING_PROMPT = """\
You are analyzing a COBOL copybook from a legacy mainframe system. \
The fields use abbreviated names that need to be mapped to modern, \
descriptive column names for a database migration.

## Copybook Fields

{fields}

## Context

This is a {context} record from a {domain} system.

## Instructions

For each field, provide the modern column name and a brief description.

Respond ONLY with a JSON array, one object per field:
```json
[
  {{"source": "CT-FNAM", "modern_name": "first_name", "description": "Contact first name", "data_type_suggestion": "VARCHAR(25)"}},
  ...
]
```

Rules:
- Use snake_case for all modern names
- Expand all abbreviations to full English words
- Keep names concise but descriptive (2-4 words max)
- Suggest appropriate SQL data types based on the PIC clause and field semantics
- For PIC X(1) fields that look like flags (Y/N), suggest BOOLEAN
- For date fields (PIC X(10) with date-like names), suggest DATE
- For timestamp fields (PIC X(26) with datetime names), suggest TIMESTAMPTZ
- Return ONLY the JSON array, no other text"""

# ── Normalization Review ─────────────────────────────────────────────

NORMALIZATION_REVIEW_PROMPT = """\
You are a database architect reviewing a normalization plan for a \
legacy-to-modern data migration.

## Source Table: {table_name}

{column_count} columns from a COBOL copybook.

## All Columns

{columns}

## Rule-Based Normalization (already proposed)

{proposed_plan}

## Profiling Summary

{profiling}

## Instructions

Review the proposed normalization and suggest improvements:

1. Should any groups be merged into a single table with a type discriminator?
2. Are there column groups the rules missed?
3. Should any fields be moved between entities?
4. Are the lookup table candidates appropriate?
5. Should any sensitive fields be isolated for compliance?

Respond with a JSON object:
```json
{{
  "approved": true/false,
  "changes": [
    {{"action": "merge|split|move|add_entity|remove_entity", "details": "...", "reason": "..."}}
  ],
  "rationale": "Overall assessment in 2-3 sentences"
}}
```

Return ONLY the JSON object, no other text."""

# ── Data Quality Assessment ──────────────────────────────────────────

DATA_QUALITY_PROMPT = """\
You are a data quality analyst reviewing profiling statistics for a \
legacy mainframe data migration.

## Table: {table_name}

{row_count} rows, {column_count} columns.

## Column Profiling Stats

{profiling_stats}

## Sample Values (first 5 rows)

{sample_data}

## Instructions

Analyze the data for quality issues that automated rules cannot catch:

1. Identify placeholder or default values (e.g., 0000-00-00, 999-99-9999, test@test.com)
2. Flag suspicious duplicates (e.g., same SSN on different records)
3. Detect encoding issues (garbled characters, truncated values)
4. Identify business logic violations (e.g., deceased status + recent activity date)
5. Flag columns where the data type doesn't match the values

Respond with a JSON array of findings:
```json
[
  {{"column": "ct_ssn", "severity": "HIGH", "finding": "3 duplicate SSNs detected", "recommendation": "Investigate for duplicate contact records"}},
  ...
]
```

If no issues are found, return an empty array [].
Return ONLY the JSON array, no other text."""
