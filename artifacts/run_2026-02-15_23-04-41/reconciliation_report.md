# Post-Migration Reconciliation Report

**Generated:** 2026-02-15 23:04:41

**Integrity Score:** 71.8/100

---

## Before/After Comparison

| Metric | Legacy | Modern | Status |
|--------|--------|--------|--------|
| Total Rows | 500 | 484 | ⚠️ Mismatch |
| Null Emails | N/A | 0 | ✅ Fixed |
| Duplicate IDs | 1+ | 0 | ✅ Resolved |
| Orphan Records | Unknown | 0 | ✅ None |

## Row Count Verification

- **Legacy System:** 500
- **Modern System:** 484
- **Match:** ❌ No

## Data Checksums

- **pymt_id->payment_id:** ❌
- **pymt_dt->payment_date:** ❌
- **pymt_amt->payment_amount:** ❌
- **pymt_method->payment_method:** ❌
- **week_ending_dt->week_ending_date:** ❌
- **pymt_status->payment_status:** ❌
- **claim_id->claim_id:** ❌
- **check_number->check_number:** ❌
- **_summary:** ❌

## Referential Integrity

- **benefit_payments_claims_fk:** ✅ Pass

## Sample Comparison

- **Records Compared:** 97
- **Exact Matches:** 97
- **Discrepancies:** 0

---

**Migration Status:** ⚠️ REVIEW REQUIRED
