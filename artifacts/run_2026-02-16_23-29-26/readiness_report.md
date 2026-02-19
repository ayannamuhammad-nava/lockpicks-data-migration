# Pre-Migration Readiness Report

**Generated:** 2026-02-16 23:29:28

**Structure Score:** 0/100

---

## Schema Analysis

### ⚠️ Columns Missing in Modern System

**cl_st**

```
📘 Why? Possibly mapped to 'cl_st': Cl St field
```

**cl_phon**

```
📘 Why? Possibly mapped to 'cl_phon': Cl Phon field
```

**cl_ssn**

```
📘 Why? Possibly mapped to 'cl_ssn': Social Security Number - highly sensitive personal identifier
```

**cl_rgdt**

```
📘 Why? Possibly mapped to 'cl_rgdt': Cl Rgdt field
```

**cl_zip**

```
📘 Why? Possibly mapped to 'cl_zip': Cl Zip field
```

**cl_bact**

```
📘 Why? Possibly mapped to 'cl_phon': Cl Phon field
```

**cl_stat**

```
📘 Why? Possibly mapped to 'cl_stat': Cl Stat field
```

**cl_emal**

```
📘 Why? Possibly mapped to 'cl_emal': Text field for cl emal
```

**cl_lnam**

```
📘 Why? Possibly mapped to 'cl_lnam': Cl Lnam field
```

**cl_city**

```
📘 Why? Possibly mapped to 'cl_city': Cl City field
```

**cl_fil1**

```
📘 Why? Possibly mapped to 'cl_fil1': Cl Fil1 field
```

**cl_dcsd**

```
📘 Why? Possibly mapped to 'cl_dcsd': Cl Dcsd field
```

**cl_dob**

```
📘 Why? Possibly mapped to 'cl_dob': Date of birth of the individual
```

**cl_brtn**

```
📘 Why? Possibly mapped to 'cl_lnam': Cl Lnam field
```

**cl_fnam**

```
📘 Why? Possibly mapped to 'cl_fnam': Cl Fnam field
```

**cl_adr1**

```
📘 Why? Possibly mapped to 'cl_adr1': Text field for cl adr1
```

**cl_recid**

```
📘 Why? Possibly mapped to 'cl_recid': Numeric value for cl recid
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
