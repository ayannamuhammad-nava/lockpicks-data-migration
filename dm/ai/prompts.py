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
