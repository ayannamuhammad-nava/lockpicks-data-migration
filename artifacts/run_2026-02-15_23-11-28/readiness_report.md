# Pre-Migration Readiness Report

**Generated:** 2026-02-15 23:11:31

**Structure Score:** 56.0/100

---

## Schema Analysis

### ⚠️ Columns Missing in Modern System

**customer_name**

```
📘 Why? Possibly mapped to 'last_name': Last name of the individual
```

**created_date**

```
📘 Why? Possibly mapped to 'registered_at': Date/time when registered occurred
```

**ssn**

```
📘 Why? Possibly mapped to 'ssn_hash': Hashed Social Security Number for secure identification
```

### ⚠️ Type Mismatches

**phone**: character varying → bigint

```
📘 Why? No explanation found for phone
```

## Governance Findings

### 🔒 Sensitive Data Detection (3 columns)

- **email**
- **phone**
- **ssn**

⚠️  **Action Required:** These fields contain sensitive information and require encryption or masking.

## Recommendations

1. **Schema Mapping**: Review column transformations and create ETL logic
2. **Data Protection**: Implement PII masking/encryption before migration

---

**Readiness Status:** ❌ REVIEW REQUIRED
