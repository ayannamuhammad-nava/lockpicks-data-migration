# Pre-Migration Readiness Report

**Generated:** 2026-02-22 15:27:37

**Structure Score:** 80.0/100

---

## Schema Analysis

### ⚠️ Columns Missing in Modern System

**cl_rgdt**

```
📘 Why? Maps to 'registered_at' (rename): Renamed from cl_rgdt to registered_at to align with modern naming conventions
```

**cl_bact**

```
📘 Why? REMOVED: Column removed from modern system. May require data archival or migration to alternate storage.
```

**cl_dcsd**

```
📘 Why? Maps to 'is_deceased' (rename): Renamed from cl_dcsd to is_deceased to align with modern naming conventions
```

**cl_ssn**

```
📘 Why? REMOVED: Column removed from modern system. May require data archival or migration to alternate storage.
```

**cl_lnam**

```
📘 Why? Maps to 'last_name' (rename): Renamed from cl_lnam to last_name to align with modern naming conventions
```

**cl_brtn**

```
📘 Why? REMOVED: Column removed from modern system. May require data archival or migration to alternate storage.
```

**cl_city**

```
📘 Why? Maps to 'city' (rename): Renamed from cl_city to city to align with modern naming conventions
```

**cl_dob**

```
📘 Why? Maps to 'date_of_birth' (rename): Renamed from cl_dob to date_of_birth to align with modern naming conventions
```

**cl_st**

```
📘 Why? Maps to 'state' (rename): Renamed from cl_st to state to align with modern naming conventions
```

**cl_emal**

```
📘 Why? Maps to 'email' (rename): Renamed from cl_emal to email to align with modern naming conventions
```

**cl_fil1**

```
📘 Why? REMOVED: Column removed from modern system. May require data archival or migration to alternate storage.
```

**cl_recid**

```
📘 Why? Maps to 'claimant_id' (rename): Renamed from cl_recid to claimant_id to align with modern naming conventions
```

**cl_fnam**

```
📘 Why? Maps to 'first_name' (rename): Renamed from cl_fnam to first_name to align with modern naming conventions
```

**cl_stat**

```
📘 Why? REMOVED: Column removed from modern system. May require data archival or migration to alternate storage.
```

**cl_zip**

```
📘 Why? Maps to 'zip_code' (rename): Renamed from cl_zip to zip_code to align with modern naming conventions
```

**cl_phon**

```
📘 Why? Maps to 'phone_number' (rename): Renamed from cl_phon to phone_number to align with modern naming conventions
```

**cl_adr1**

```
📘 Why? Maps to 'address_line1' (rename): Renamed from cl_adr1 to address_line1 to align with modern naming conventions
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
