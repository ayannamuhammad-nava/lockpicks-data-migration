# Pre-Migration Readiness Report

**Generated:** 2026-02-16 23:23:12

**Structure Score:** 0/100

---

## Schema Analysis

### ⚠️ Columns Missing in Modern System

**cl_phon**

```
📘 Why? Possibly mapped to 'cl_phon': Phone number (cl_phon) for contact purposes
```

**cl_zip**

```
📘 Why? Possibly mapped to 'cl_zip': Text field for cl_zip postal code
```

**cl_lnam**

```
📘 Why? Possibly mapped to 'cl_lnam': Text field for cl_lnam last name
```

**cl_city**

```
📘 Why? Possibly mapped to 'cl_city': Text field for cl_city
```

**cl_bact**

```
📘 Why? Possibly mapped to 'cl_bact': Bank account number (cl_bact) - sensitive financial information
```

**cl_stat**

```
📘 Why? Possibly mapped to 'cl_stat': Current status (cl_stat) of the claimant
```

**cl_rgdt**

```
📘 Why? Possibly mapped to 'cl_rgdt': Date/time value for cl_rgdt registration date
```

**cl_recid**

```
📘 Why? Possibly mapped to 'cl_recid': Unique identifier for cl record
```

**cl_dob**

```
📘 Why? Possibly mapped to 'cl_dob': Date of birth (cl_dob) of the individual
```

**cl_ssn**

```
📘 Why? Possibly mapped to 'cl_ssn': Social Security Number (cl_ssn) - highly sensitive personal identifier
```

**cl_emal**

```
📘 Why? Possibly mapped to 'cl_emal': Email address (cl_emal) for communication and identification
```

**cl_fnam**

```
📘 Why? Possibly mapped to 'cl_fnam': Text field for cl_fnam first name
```

**cl_st**

```
📘 Why? Possibly mapped to 'cl_st': Text field for cl_st state
```

**cl_dcsd**

```
📘 Why? Possibly mapped to 'cl_dcsd': Flag indicating deceased (cl_dcsd)
```

**cl_fil1**

```
📘 Why? Possibly mapped to 'cl_fil1': COBOL FILLER field used for record alignment padding
```

**cl_brtn**

```
📘 Why? Possibly mapped to 'cl_brtn': Bank routing number (cl_brtn) for electronic transfers
```

**cl_adr1**

```
📘 Why? Possibly mapped to 'cl_adr1': Text field for cl_adr1 address
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

**Readiness Status:** ❌ REVIEW REQUIRED
