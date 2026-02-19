# Pre-Migration Readiness Report

**Generated:** 2026-02-14 23:56:53

**Structure Score:** 62.0/100

---

## Schema Analysis

### ⚠️ Columns Missing in Modern System

**created_date**

```
📘 Why? Possibly mapped to 'created_date': Date when the customer record was created in the legacy system
```

**customer_name**

```
📘 Why? Possibly mapped to 'customer_name': Full name of the customer as stored in legacy system
```

**ssn**

```
📘 Why? Possibly mapped to 'ssn': Social Security Number - highly sensitive PII field requiring special handling and encryption
```

### ⚠️ Type Mismatches

**phone**: character varying → bigint

```
📘 Why? Customer phone number for contact purposes [PII]
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
