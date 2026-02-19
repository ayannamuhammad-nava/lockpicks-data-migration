# Post-Migration Reconciliation Report

**Generated:** 2026-02-15 23:03:38

**Integrity Score:** 36.48/100

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

- **clmt_id->claimant_id:** ❌
- **clmt_first_nm->first_name:** ❌
- **clmt_last_nm->last_name:** ❌
- **clmt_status->claimant_status:** ❌
- **is_deceased->is_deceased:** ❌
- **_summary:** ❌

## Sample Comparison

- **Records Compared:** 98
- **Exact Matches:** 13
- **Discrepancies:** 85

---

**Migration Status:** ❌ FAILURE
