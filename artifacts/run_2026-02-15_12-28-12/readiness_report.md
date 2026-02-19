# Pre-Migration Readiness Report

**Generated:** 2026-02-15 12:28:14

**Structure Score:** 58.0/100

---

## Schema Analysis

### ⚠️ Columns Missing in Modern System

**created_date**

```
📘 Why? Possibly mapped to 'created_date': Timestamp when the record was created
```

**customer_name**

```
📘 Why? Possibly mapped to 'customer_name': Name field for customer
```

**ssn**

```
📘 Why? Possibly mapped to 'ssn': Social Security Number of Customer User, this is highly sensitive.
```

### ⚠️ Type Mismatches

**phone**: character varying → bigint

```
📘 Why? Phone number for contact purposes [PII]
```

## Governance Findings

### 🔒 Sensitive Data Detection (1 columns)

- **ssn**

⚠️  **Action Required:** These fields contain sensitive information and require encryption or masking.

## Recommendations

1. **Schema Mapping**: Review column transformations and create ETL logic
2. **Data Protection**: Implement PII masking/encryption before migration

---

**Readiness Status:** ❌ REVIEW REQUIRED
