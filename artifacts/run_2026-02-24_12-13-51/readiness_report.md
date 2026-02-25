# Pre-Migration Readiness Report

**Generated:** 2026-02-24 12:13:51

**Structure Score:** 90.0/100

---

## Schema Analysis

### ⚠️ Columns Missing in Modern System

**cl_fnam**

```
📘 Why? Maps to 'first_name' (rename): Renamed from cl_fnam to first_name to align with modern naming conventions
```

**cl_adr1**

```
📘 Why? Maps to 'address_line1' (rename): Renamed from cl_adr1 to address_line1 to align with modern naming conventions
```

**cl_city**

```
📘 Why? Maps to 'city' (rename): Renamed from cl_city to city to align with modern naming conventions
```

**cl_zip**

```
📘 Why? Maps to 'zip_code' (rename): Renamed from cl_zip to zip_code to align with modern naming conventions
```

**cl_recid**

```
📘 Why? Maps to 'claimant_id' (rename): Renamed from cl_recid to claimant_id to align with modern naming conventions
```

**cl_dob**

```
📘 Why? Maps to 'date_of_birth' (rename): Renamed from cl_dob to date_of_birth to align with modern naming conventions
```

**cl_emal**

```
📘 Why? Maps to 'email' (rename): Renamed from cl_emal to email to align with modern naming conventions
```

**cl_brtn**

```
📘 Why? ARCHIVED: Bank routing number NOT migrated — PCI-DSS regulated financial identifier. Archive alongside cl_bact to encrypted vault. Routing numbers combined with account numbers constitute full banking credentials — any plaintext exposure is a PCI-DSS Level 1 violation. Do NOT include in ETL pipeline.
```

**cl_phon**

```
📘 Why? Maps to 'phone_number' (rename): Renamed from cl_phon to phone_number to align with modern naming conventions
```

**cl_ssn**

```
📘 Why? Maps to 'ssn_hash' (transform): SHA-256 hash for HIPAA compliance — raw SSN must NEVER be written to modern system; duplicate SSNs indicate identity conflicts requiring manual resolution before migration proceeds
```

**cl_rgdt**

```
📘 Why? Maps to 'registered_at' (rename): Renamed from cl_rgdt to registered_at to align with modern naming conventions
```

**cl_st**

```
📘 Why? Maps to 'state' (rename): Renamed from cl_st to state to align with modern naming conventions
```

**cl_dcsd**

```
📘 Why? Maps to 'is_deceased' (rename): Renamed from cl_dcsd to is_deceased to align with modern naming conventions
```

**cl_stat**

```
📘 Why? Maps to 'claimant_status' (transform): Status codes normalised: 'ACTIVE'→'active', 'INACTIVE'→'inactive', 'SUSPENDED'→'suspended'. Legacy freetext values must be mapped; unmapped values default to 'pending_review' and flagged for manual triage.
```

**cl_fil1**

```
📘 Why? REMOVED: Legacy filler/padding field with no business meaning — safe to drop. Verify no application code reads this field before finalising ETL.
```

**cl_lnam**

```
📘 Why? Maps to 'last_name' (rename): Renamed from cl_lnam to last_name to align with modern naming conventions
```

### ⚠️ Type Mismatches

**cl_bact**: character → character varying

```
📘 Why? Bank account number - sensitive financial information [PII]
```

## Governance Findings

### 🔒 Sensitive Data Detection (8 columns)

- **cl_ssn**
- **cl_dob**
- **cl_phon**
- **cl_emal**
- **cl_adr1**
- **cl_zip**
- **cl_bact**
- **cl_brtn**

⚠️  **Action Required:** These fields contain sensitive information and require encryption or masking.

## ⚠️ Data Quality Anomalies

### 🔴 Deceased claimant with active benefit status — 2 record(s) in sample

**Condition:** `cl_dcsd = 'Y'  AND  cl_stat IN ('ACTIVE', 'ACT')`

**Affected record IDs (sample):** [88, 160]

> ⚠️ **Risk:** Active benefits may be disbursed to deceased claimants — potential fraud exposure and audit finding. Migrating these records as-is carries the anomaly into production.

**Action required:** Close or suspend affected records before migration. Refer to benefits fraud review team.

## Recommendations

1. **Schema Mapping**: Review column transformations and create ETL logic
2. **Data Protection**: Implement PII masking/encryption before migration

---

**Readiness Status:** ✅ READY TO PROCEED
