# Pre-Migration Readiness Report

**Generated:** 2026-02-14 01:15:39

**Structure Score:** 62.0/100

---

## Schema Analysis

### ⚠️ Columns Missing in Modern System

- **ssn**
  - *Possibly mapped to 'ssn': Social Security Number - highly sensitive PII field requiring special handling and encryption*
- **customer_name**
  - *Possibly mapped to 'customer_name': Full name of the customer as stored in legacy system*
- **created_date**
  - *Possibly mapped to 'created_date': Date when the customer record was created in the legacy system*

### ⚠️ Type Mismatches

- **phone**: character varying → bigint

## Governance Findings

### 🔒 PII Detected (1 columns)

- ssn

## Recommendations

1. Review schema mappings and create transformation logic
2. Implement PII masking/encryption before migration

---

**Readiness Status:** ❌ NOT READY
