# Post-Migration Reconciliation Report

**Generated:** 2026-02-22 14:52:40

**Integrity Score:** 32.5/100

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
- **cl_ssn->ssn_hash:** ❌
- **cl_dob->date_of_birth:** ❌
- **cl_phon->phone_number:** ❌
- **cl_emal->email:** ❌
- **cl_adr1->address_line1:** ❌
- **cl_city->city:** ❌
- **cl_st->state:** ❌
- **cl_zip->zip_code:** ❌
- **cl_stat->claimant_status:** ❌
- **cl_rgdt->registered_at:** ❌
- **cl_dcsd->is_deceased:** ❌
- **_summary:** ❌

## Sample Comparison

- **Records Compared:** 98
- **Exact Matches:** 0
- **Discrepancies:** 98

---

**Migration Status:** ❌ FAILURE
