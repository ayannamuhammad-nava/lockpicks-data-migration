# Post-Migration Reconciliation Report

**Generated:** 2026-02-25 06:36:51

**Integrity Score:** 62.5/100

---

## 🚨 COMPLIANCE GATE FAILED — Archived Fields Detected in Modern Schema

> **CRITICAL:** The following fields are marked ARCHIVED in the migration knowledge base but were found in the modern schema. These fields must NOT be present in the migrated system. Halt go-live until resolved.

### ❌ `cl_bact` — claimants

> Bank account number NOT migrated — PCI-DSS regulated financial identifier; must be archived to encrypted vault and claimants re-enrolled through secure portal. Archive alongside cl_brtn. Any plaintext exposure is a PCI-DSS Level 1 violation. Do NOT include in ETL pipeline.

**Action required:** Remove `cl_bact` from modern schema and purge any migrated data.

---

## ⚠️ GOVERNANCE WARNING — Ungoverned Columns in Modern Schema

> The following columns exist in the modern schema but have **no source mapping** in the ETL specification. They were added outside of the governed migration process and have not been validated. Review and document or remove before go-live.

- `legacy_system_ref` — no ETL mapping, origin unknown

---

## Before/After Comparison

| Metric | Legacy | Modern | Status |
|--------|--------|--------|--------|
| Total Rows | 200 | 195 | ⚠️ Mismatch |
| Null Emails | N/A | 0 | ✅ Fixed |
| Duplicate IDs | 1+ | 0 | ✅ Resolved |
| Orphan Records | Unknown | 0 | ✅ None |

## Row Count Verification

- **Legacy System:** 200
- **Modern System:** 195
- **Match:** ❌ No

## Data Checksums

- **cl_recid->claimant_id:** ❌
- **cl_fnam->first_name:** ❌
- **cl_lnam->last_name:** ❌
- **cl_dob->date_of_birth:** ❌
- **cl_phon->phone_number:** ❌
- **cl_emal->email:** ❌
- **cl_adr1->address_line1:** ❌
- **cl_city->city:** ❌
- **cl_st->state:** ❌
- **cl_zip->zip_code:** ❌
- **cl_rgdt->registered_at:** ❌
- **cl_dcsd->is_deceased:** ❌
- **_summary:** ❌

## Sample Comparison

- **Records Compared:** 98
- **Exact Matches:** 98
- **Discrepancies:** 0

---

**Migration Status:** ❌ FAILURE
