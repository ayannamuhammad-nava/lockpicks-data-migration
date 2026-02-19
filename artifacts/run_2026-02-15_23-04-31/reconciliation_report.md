# Post-Migration Reconciliation Report

**Generated:** 2026-02-15 23:04:31

**Integrity Score:** 45.64/100

---

## Before/After Comparison

| Metric | Legacy | Modern | Status |
|--------|--------|--------|--------|
| Total Rows | 300 | 291 | ⚠️ Mismatch |
| Null Emails | N/A | 0 | ✅ Fixed |
| Duplicate IDs | 1+ | 0 | ✅ Resolved |
| Orphan Records | Unknown | 0 | ✅ None |

## Row Count Verification

- **Legacy System:** 300
- **Modern System:** 291
- **Match:** ❌ No

## Data Checksums

- **clmt_id->claimant_id:** ❌
- **empl_id->employer_id:** ❌
- **bnf_year_start->benefit_year_start:** ❌
- **bnf_year_end->benefit_year_end:** ❌
- **max_bnf_amt->max_benefit_amount:** ❌
- **claim_id->claim_id:** ❌
- **separation_reason->separation_reason:** ❌
- **total_paid->total_paid:** ❌
- **weeks_claimed->weeks_claimed:** ❌
- **claim_status->claim_status:** ❌
- **_summary:** ❌

## Referential Integrity

- **claims_claimants_fk:** ✅ Pass
- **claims_employers_fk:** ✅ Pass

## Sample Comparison

- **Records Compared:** 99
- **Exact Matches:** 12
- **Discrepancies:** 87

---

**Migration Status:** ❌ FAILURE
