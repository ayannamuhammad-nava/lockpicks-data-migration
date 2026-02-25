# Pre-Migration Readiness Report

**Generated:** 2026-02-22 15:21:10

**Structure Score:** 94.0/100

---

## Schema Analysis

### ⚠️ Columns Missing in Modern System

**cl_st**

```
📘 Why? Maps to 'state' (rename): Renamed from cl_st to state. ETL must validate against the list of valid US state codes.
```

**cl_city**

```
📘 Why? Maps to 'city' (rename): Renamed from cl_city to city to align with modern naming conventions.
```

**cl_dob**

```
📘 Why? Maps to 'date_of_birth' (rename): Renamed from cl_dob to date_of_birth. Date stored as string in legacy; ETL must parse and store as DATE type in modern schema.
```

**cl_stat**

```
📘 Why? Maps to 'claimant_status' (transform): Status values normalised from legacy codes (e.g. 'ACTIVE', 'INACT', 'PEND') to modern enum ('active', 'inactive', 'pending'). ETL must map all legacy codes and reject unknown values. Business rule: claimant_status='active' is incompatible with is_deceased=true — flag these as data integrity violations before migration.
```

**cl_recid**

```
📘 Why? Maps to 'claimant_id' (rename): Primary key renamed from cl_recid (legacy auto-increment integer) to claimant_id. All foreign key references in claims and benefit_payments tables must be updated to reference the new claimant_id values before migration completes.
```

**cl_rgdt**

```
📘 Why? Maps to 'registered_at' (rename): Renamed from cl_rgdt to registered_at. Date stored as string in legacy; ETL must parse and store as TIMESTAMP in modern schema.
```

**cl_zip**

```
📘 Why? Maps to 'zip_code' (rename): Renamed from cl_zip to zip_code. ETL must validate ZIP code format (5-digit or ZIP+4).
```

**cl_ssn**

```
📘 Why? Maps to 'ssn_hash' (transform): SSN transformed from plaintext to SHA-256 hash for HIPAA compliance — raw SSN must NEVER be written to the modern system; duplicate SSNs indicate identity conflicts requiring manual resolution before migration proceeds. ETL must: 1) hash using SHA-256, 2) verify uniqueness in modern schema, 3) flag duplicates for manual review.
```

**cl_adr1**

```
📘 Why? Maps to 'address_line1' (rename): Renamed from cl_adr1 to address_line1. PII field — part of claimant's residential address used for benefit correspondence.
```

**cl_brtn**

```
📘 Why? ARCHIVED: Bank routing number NOT migrated — PCI-DSS regulated financial identifier. Archive alongside cl_bact to encrypted vault. Routing numbers combined with account numbers constitute full banking credentials — any plaintext exposure is a PCI-DSS Level 1 violation. Do NOT include in ETL pipeline.
```

**cl_fil1**

```
📘 Why? REMOVED: Legacy filler/overflow field with no semantic meaning in the modern schema. Review any non-null values before discarding — they may contain ad-hoc data entered by legacy users.
```

**cl_fnam**

```
📘 Why? Maps to 'first_name' (rename): Renamed from cl_fnam to first_name for readability. PII field — must be protected under HIPAA minimum necessary standard; do not expose in unauthenticated API responses.
```

**cl_bact**

```
📘 Why? ARCHIVED: Bank account number NOT migrated — PCI-DSS regulated financial identifier must NOT be stored in plaintext in the modern system. Archive to encrypted vault (AES-256). Claimants must re-enroll direct deposit through the secure payment portal post-migration. Do NOT include in ETL pipeline.
```

**cl_dcsd**

```
📘 Why? Maps to 'is_deceased' (transform): Renamed from cl_dcsd (Y/N flag) to is_deceased (boolean). CRITICAL business rule: is_deceased=true must NEVER coexist with claimant_status='active' — a deceased claimant drawing active benefits is an audit violation and potential fraud indicator. Pre-migration check must flag all records where cl_dcsd='Y' AND cl_stat='ACTIVE' for manual review before ETL proceeds.
```

**cl_phon**

```
📘 Why? Maps to 'phone_number' (rename): Renamed from cl_phon to phone_number. PII field — phone numbers must be masked in non-production environments.
```

**cl_lnam**

```
📘 Why? Maps to 'last_name' (rename): Renamed from cl_lnam to last_name for readability. PII field — must be protected under HIPAA minimum necessary standard; do not expose in unauthenticated API responses.
```

**cl_emal**

```
📘 Why? Maps to 'email' (rename): Renamed from cl_emal to email. PII field — email addresses must not be exposed in unauthenticated API responses. Modern schema enforces NOT NULL — legacy records with null emails must be resolved before migration.
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

## Recommendations

1. **Schema Mapping**: Review column transformations and create ETL logic
2. **Data Protection**: Implement PII masking/encryption before migration

---

**Readiness Status:** ✅ READY TO PROCEED
