# Pre-Migration Readiness Report

**Generated:** 2026-02-15 23:02:10

**Structure Score:** 0/100

---

## Schema Analysis

### ⚠️ Columns Missing in Modern System

**clmt_email**

```
📘 Why? Possibly mapped to 'clmt_email': Email address for communication and identification
```

**clmt_id**

```
📘 Why? Possibly mapped to 'clmt_id': Unique identifier for clmt
```

**clmt_dob**

```
📘 Why? Possibly mapped to 'clmt_phone': Phone number for contact purposes
```

**clmt_phone**

```
📘 Why? Possibly mapped to 'clmt_phone': Phone number for contact purposes
```

**clmt_first_nm**

```
📘 Why? Possibly mapped to 'clmt_first_nm': Text field for clmt first nm
```

**bank_acct_num**

```
📘 Why? Possibly mapped to 'bank_acct_num': Bank account number - sensitive financial information
```

**clmt_addr**

```
📘 Why? Possibly mapped to 'clmt_addr': Text field for clmt addr
```

**clmt_ssn**

```
📘 Why? Possibly mapped to 'clmt_ssn': Social Security Number - highly sensitive personal identifier
```

**clmt_zip**

```
📘 Why? Possibly mapped to 'clmt_zip': Text field for clmt zip
```

**clmt_city**

```
📘 Why? Possibly mapped to 'clmt_city': Text field for clmt city
```

**bank_routing_num**

```
📘 Why? Possibly mapped to 'bank_routing_num': Bank routing number for electronic transfers
```

**clmt_state**

```
📘 Why? Possibly mapped to 'clmt_status': Current status of the clmt
```

**clmt_last_nm**

```
📘 Why? Possibly mapped to 'clmt_last_nm': Text field for clmt last nm
```

**registration_dt**

```
📘 Why? Possibly mapped to 'registration_dt': Date/time value for registration dt
```

**clmt_status**

```
📘 Why? Possibly mapped to 'clmt_status': Current status of the clmt
```

## Governance Findings

### 🔒 Sensitive Data Detection (7 columns)

- **clmt_ssn**
- **clmt_dob**
- **clmt_phone**
- **clmt_email**
- **clmt_zip**
- **bank_acct_num**
- **bank_routing_num**

⚠️  **Action Required:** These fields contain sensitive information and require encryption or masking.

## Recommendations

1. **Schema Mapping**: Review column transformations and create ETL logic
2. **Data Protection**: Implement PII masking/encryption before migration

---

**Readiness Status:** ❌ REVIEW REQUIRED
