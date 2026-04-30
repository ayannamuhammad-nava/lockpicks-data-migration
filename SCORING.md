# Scoring Methodology

This document explains how every score in the Lockpicks Data Migration toolkit is calculated — from table rationalization through migration confidence to final proof reports.

---

## Table of Contents

1. [Migration Confidence Score](#1-migration-confidence-score)
2. [Structure Score](#2-structure-score)
3. [Integrity Score](#3-integrity-score)
4. [Governance Score](#4-governance-score)
5. [Rationalization Relevance Score](#5-rationalization-relevance-score)
6. [Proof Score](#6-proof-score)
7. [Traffic Light Thresholds](#7-traffic-light-thresholds)
8. [Configuring Weights and Thresholds](#8-configuring-weights-and-thresholds)
9. [Examples](#9-examples)

---

## 1. Migration Confidence Score

The core metric. Produced by every `dm validate` run.

**Formula:**

```
confidence = (0.4 x structure) + (0.4 x integrity) + (0.2 x governance)
```

| Component | Weight | Phase | What It Measures |
|-----------|--------|-------|-----------------|
| Structure | 40% | Pre-migration | Schema compatibility between legacy and modern |
| Integrity | 40% | Post-migration | Data completeness, FK integrity, value accuracy |
| Governance | 20% | Pre-migration | PII compliance, naming conventions, null thresholds |

**Pre-migration runs:** Integrity defaults to 100 (not applicable yet), so the score is effectively `(0.4 x structure) + (0.4 x 100) + (0.2 x governance)`.

**Post-migration runs:** All three components are active.

---

## 2. Structure Score

Measures schema compatibility between legacy and modern databases. Calculated during `dm validate --phase pre`.

**Formula:**

```
structure_score = 100 - total_penalties
```

**Penalties per column:**

| Mapping Type | Penalty | Explanation |
|-------------|---------|-------------|
| `rename` | 0 pts | Column was renamed — no data risk |
| `transform` | 0 pts | Column was renamed + type changed — handled by ETL |
| `archived` | 1 pt | PII field archived for compliance — intentional, low risk |
| `removed` | 4 pts | Column has no modern equivalent — potential data loss |
| Type mismatch | 5 pts | Same column name but different data type |

**How it works:**

1. Compare legacy and modern schemas column by column
2. For each column missing in modern, look up its mapping type from `mappings.json`
3. Apply the penalty based on mapping type
4. Add 5 points for each type mismatch on shared columns
5. Subtract total penalties from 100

**Example:**
- 2 archived columns (cl_bact, cl_brtn): 2 x 1 = 2 pts
- 1 removed column (cl_fil1): 1 x 4 = 4 pts
- 1 type mismatch (cl_bact CHAR vs VARCHAR): 1 x 5 = 5 pts
- Structure score = 100 - 2 - 4 - 5 = **89**

---

## 3. Integrity Score

Measures data survival after migration. Calculated during `dm validate --phase post`.

**Formula:**

```
integrity_score = 100 - total_penalties
```

**Penalties by validator:**

### Row Count Validator
| Condition | Penalty |
|-----------|---------|
| Counts match | 0 pts |
| Counts differ | min(difference_percentage, 30) pts |

Example: Legacy has 100 rows, modern has 95 → 5% difference → 5 pt penalty.

### Checksum Validator
- Compares MD5 checksums per column between legacy and modern
- Skips archived/transformed columns
- Penalty per mismatched column

### Referential Integrity Validator
- Checks FK constraints in modern schema
- Counts orphan records (child rows with no parent)
- Penalty based on orphan count

### Sample Compare Validator
- Draws random sample, matches by primary key
- Compares field values with format-aware tolerance:
  - Y/N → boolean normalization
  - Date format normalization
  - Numeric epsilon tolerance
- Penalty per mismatched field

### Aggregate Validator
- Compares SUM/COUNT/AVG for configured columns
- Configurable tolerance (default: exact match)
- Penalty per aggregate drift

### Archived Leakage Validator
- Verifies PCI/HIPAA archived columns contain NO data in modern schema
- Any data in an archived column = penalty

### Unmapped Columns Validator
- Checks modern columns have source mappings or are auto-generated (created_at, updated_at)
- Unmapped columns = potential ungoverned data

### Normalization Integrity Validator
- Validates child tables have no orphaned FKs
- All parent PKs are present in child table FK columns

### Encoding Validator
- Detects encoding mismatches (EBCDIC → UTF-8)
- Checks for mojibake patterns

---

## 4. Governance Score

Measures compliance with data governance rules. Calculated during `dm validate --phase pre`.

**Formula:**

```
governance_score = 100 - total_penalties
```

**Penalties:**

| Check | Penalty Per Violation | Cap |
|-------|----------------------|-----|
| PII in plaintext | 5 pts | 30 pts max |
| Naming convention violation | 2 pts | 10 pts max |
| Missing required field | 10 pts | 30 pts max |
| Null threshold breach | 3 pts | 15 pts max |

**Maximum total governance penalty:** 85 pts (minimum score: 15)

### PII Detection
- Matches column names against configured keywords: `ssn, email, phone, dob, credit_card, account_number, drivers_license, bank`
- Also matches COBOL abbreviated patterns: `bact, brtn, bacct, broute`
- Each PII column in plaintext = 5 pt penalty

### Naming Convention
- Checks modern column names against regex (default: `^[a-z0-9_]+$`)
- Each violation = 2 pt penalty

### Required Fields
- Configured per dataset in `project.yaml` under `validation.governance.required_fields`
- Each missing required field = 10 pt penalty

### Null Thresholds
- Samples data and calculates null percentage per column
- Default threshold: 10%
- Each column exceeding threshold = 3 pt penalty

---

## 5. Rationalization Relevance Score

Separate from the confidence score. Used by `dm rationalize` to classify tables as Migrate / Review / Archive.

**Formula:**

```
relevance = (0.35 x query_activity) + (0.25 x downstream) + (0.20 x freshness)
           + (0.10 x completeness) + (0.10 x tier)
```

### Query Activity (35% weight)

| Condition | Score |
|-----------|-------|
| 1000+ queries | 100 |
| 100 queries | 80 |
| 10 queries | 50 |
| 1 query | 20 |
| No data — tagged "frequentlyUsed" | 100 |
| No data — tagged "active" | 75 |
| No data — tagged "moderatelyUsed" | 50 |
| No data — tagged "rarelyUsed" | 25 |
| No data — tagged "unused" or "deprecated" | 0 |
| No usage data at all | 50 (default) |

Uses logarithmic scale: `score = 20 + 25 x log10(query_count)`

### Downstream Lineage (25% weight)

| Downstream Consumers | Score |
|---------------------|-------|
| 0 | 0 |
| 1 | 30 |
| 2-3 | 55 |
| 4-5 | 75 |
| 6-10 | 90 |
| 11+ | 100 |

### Freshness (20% weight)

| Last Profiled | Score |
|--------------|-------|
| Within 7 days | 100 |
| Within 30 days | 85 |
| Within 90 days | 65 |
| Within 180 days | 45 |
| Within 1 year | 25 |
| Over 1 year | 10 |
| No profiling data | 25 |

### Completeness (10% weight)

```
score = 100 - average_null_percentage
```

If no profiling data: defaults to 50.

### Tier (10% weight)

| OpenMetadata Tier | Score |
|-------------------|-------|
| Tier 1 (most critical) | 100 |
| Tier 2 | 80 |
| Tier 3 | 60 |
| Tier 4 | 40 |
| Tier 5 (least critical) | 20 |
| No tier assigned | 50 (default) |

### Classification Thresholds

| Score | Recommendation |
|-------|---------------|
| >= 70 | **Migrate** — Table is actively used and critical |
| 40-69 | **Review** — Manual decision needed |
| < 40 | **Archive** — Table is stale, unused, or low-value |

---

## 6. Proof Score

Generated by `dm prove`. Combines pre and post validation into a single audit score.

**Formula:**

```
proof_score = (pre_score + post_score) / 2
```

Both pre_score and post_score are migration confidence scores (section 1). The proof score uses the same traffic light thresholds.

If either pre or post score is missing, the proof report shows `INCOMPLETE`.

---

## 7. Traffic Light Thresholds

Applied to all scores (confidence, proof, rationalization):

| Score | Status | Color | Meaning |
|-------|--------|-------|---------|
| 90-100 | GREEN | #2e7d32 | Safe to proceed |
| 70-89 | YELLOW | #f57f17 | Review recommended |
| 0-69 | RED | #c62828 | Fix issues before proceeding |

---

## 8. Configuring Weights and Thresholds

All weights and thresholds are configurable in `project.yaml`:

```yaml
scoring:
  weights:
    structure: 0.4      # 40% weight on schema compatibility
    integrity: 0.4      # 40% weight on data integrity
    governance: 0.2     # 20% weight on compliance
  thresholds:
    green: 90           # >= 90 = GREEN
    yellow: 70          # >= 70 = YELLOW, < 70 = RED
```

Governance-specific settings:

```yaml
validation:
  governance:
    pii_keywords: [ssn, email, phone, dob, credit_card, account_number]
    naming_regex: "^[a-z0-9_]+$"
    max_null_percent: 10
    required_fields:
      my_table: [id, name, status]
```

---

## 9. Examples

### Example 1: Customer Service — Pre-Migration

```
Structure:
  42 legacy columns
  2 archived (ct_bact, ct_brtn): 2 x 1 = 2 pts penalty
  2 removed (ct_fil1, ct_fil2): 2 x 4 = 8 pts penalty
  0 type mismatches
  Structure score = 100 - 10 = 90

Governance:
  7 PII columns detected: 7 x 5 = 35, capped at 30 pts
  0 naming violations
  0 missing required
  1 null threshold breach: 1 x 3 = 3 pts
  Governance score = 100 - 33 = 67

Confidence (pre-migration):
  = (0.4 x 90) + (0.4 x 100) + (0.2 x 67)
  = 36 + 40 + 13.4
  = 89.4 → YELLOW
```

### Example 2: Customer Service — Post-Migration

```
Row count: 15 legacy, 15 modern = match, 0 penalty
Checksums: all match, 0 penalty
FK integrity: no violations, 0 penalty
Archived leakage: no data in archived columns, 0 penalty

Integrity score = 100

Confidence (post-migration):
  = (0.4 x 90) + (0.4 x 100) + (0.2 x 67)
  = 89.4 → YELLOW
```

### Example 3: Proof Report

```
Pre-score:  89.4
Post-score: 95.0
Proof score = (89.4 + 95.0) / 2 = 92.2 → GREEN
```

### Example 4: Rationalization — Fresh OM Instance

```
Query activity: 50 (no usage data, default)
Downstream: 0 (no lineage registered)
Freshness: 25 (no profiler data)
Completeness: 50 (no profiler data)
Tier: 50 (no tier assigned)

Relevance = (0.35 x 50) + (0.25 x 0) + (0.20 x 25) + (0.10 x 50) + (0.10 x 50)
          = 17.5 + 0 + 5 + 5 + 5
          = 32.5 → ARCHIVE
```

This is why all tables scored 32.5 on a fresh OpenMetadata instance — no usage data, profiling, or lineage to demonstrate the tables are actively used.
