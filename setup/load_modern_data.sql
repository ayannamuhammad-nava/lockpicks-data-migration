-- Load demo data into modern_db (migrated data with transformations)
-- Scenario: State Department of Labor - Unemployment Insurance System

\c modern_db

-- Temporarily disable FK constraints to allow orphan records for testing
ALTER TABLE benefit_payments DROP CONSTRAINT IF EXISTS benefit_payments_claim_id_fkey;
ALTER TABLE claims DROP CONSTRAINT IF EXISTS claims_claimant_id_fkey;
ALTER TABLE claims DROP CONSTRAINT IF EXISTS claims_employer_id_fkey;

-- ── Employers (all migrated, clean transformation) ──
-- Transformations: empl_* -> employer_*, phone VARCHAR -> BIGINT, status normalized

INSERT INTO employers (employer_id, employer_name, employer_ein, industry, address_line1, city, state, zip_code, phone_number, employer_status) VALUES
(1, 'Midwest Manufacturing Corp', '12-3456789', 'Manufacturing', '100 Industrial Blvd', 'Springfield', 'IL', '62701', 2175550100, 'active'),
(2, 'Great Lakes Logistics LLC', '23-4567890', 'Transportation', '200 Harbor Way', 'Columbus', 'OH', '43215', 6145550200, 'active'),
(3, 'Heartland Healthcare Systems', '34-5678901', 'Healthcare', '300 Medical Dr', 'Jacksonville', 'FL', '32099', 9045550300, 'active'),
(4, 'Prairie State Energy Co', '45-6789012', 'Energy', '400 Power Ln', 'Indianapolis', 'IN', '46201', 3175550400, 'active'),
(5, 'Central Valley Foods Inc', '56-7890123', 'Food Processing', '500 Harvest Rd', 'Charlotte', 'NC', '28201', 7045550500, 'active');

-- ── Claimants (migrated with transformations) ──
-- Transformations:
--   clmt_* -> modern names (first_name, last_name, etc.)
--   clmt_ssn -> ssn_hash (SHA256 hashed)
--   clmt_dob (VARCHAR) -> date_of_birth (DATE)
--   clmt_phone (VARCHAR) -> phone_number (BIGINT)
--   bank_acct_num, bank_routing_num -> REMOVED (security)
--   status values normalized to lowercase
--   Whitespace in names trimmed
-- NOTE: Duplicate SSN claimant (clmt_id 5) NOT migrated -> row count mismatch
-- NOTE: Deceased claimant status corrected to 'inactive'

-- cl_bact and legacy_system_ref added as demo findings:
--   cl_bact: ARCHIVED field that leaked into modern (PCI-DSS violation — compliance gate catches this)
--   legacy_system_ref: ungoverned column added by ops team with no ETL mapping
-- claimant_status for records 3,4: left un-normalized (ETL transformation bug demo)
INSERT INTO claimants (claimant_id, first_name, last_name, ssn_hash, date_of_birth, phone_number, email, address_line1, city, state, zip_code, claimant_status, registered_at, is_deceased, cl_bact, legacy_system_ref) VALUES
(1, 'James', 'Smith', 'a1b2c3d4e5f6a7b8', '1980-01-15', 2175551001, 'james.smith42@email.com', '123 Main St', 'Springfield', 'IL', '62701', 'active', '2022-03-15', FALSE, NULL, 'LEGACY-0001'),
(2, 'Mary', 'Johnson', 'c3d4e5f6a7b8c9d0', '1975-03-20', 6145551002, 'mary.johnson7@email.com', '456 Oak Ave', 'Columbus', 'OH', '43215', 'active', '2022-06-20', FALSE, NULL, 'LEGACY-0002'),
(3, 'Robert', 'Williams', 'd4e5f6a7b8c9d0e1', '1988-01-10', 9045551003, NULL, '789 Pine St', 'Jacksonville', 'FL', '32099', 'Active', '2023-01-10', FALSE, '1111222233', 'LEGACY-0003'),
-- cl_bact leaked ^^ and status NOT normalized (ETL bug) ^^
(4, 'Patricia', 'Brown', 'e5f6a7b8c9d0e1f2', '1992-07-04', 3175551004, 'patricia.brown15@email.com', '321 Elm St', 'Indianapolis', 'IN', '46201', 'ACT', '2023-04-05', FALSE, '4444555566', 'LEGACY-0004'),
-- cl_bact leaked ^^ and status NOT normalized (ETL bug) ^^
-- NOTE: clmt_id 5 (John Davis) SKIPPED - duplicate SSN with clmt_id 1
(6, 'Jennifer', 'Garcia', 'f6a7b8c9d0e1f2a3', '1985-11-22', 2175551006, 'jennifer.garcia3@email.com', '100 Maple Dr', 'Springfield', 'IL', '62701', 'active', '2021-09-12', FALSE, NULL, 'LEGACY-0006'),
(7, 'Michael', 'Miller', 'a7b8c9d0e1f2a3b4', '1970-08-15', 6145551007, 'michael.miller56@email.com', '200 Lake Rd', 'Columbus', 'OH', '43215', 'inactive', '2020-12-01', FALSE, NULL, 'LEGACY-0007'),
(8, 'Linda', 'Martinez', 'b8c9d0e1f2a3b4c5', '1995-03-30', 9045551008, NULL, '300 Hill St', 'Jacksonville', 'FL', '32099', 'active', '2023-07-18', FALSE, NULL, 'LEGACY-0008'),
-- NULL email persists ^^
(9, 'David', 'Wilson', 'c9d0e1f2a3b4c5d6', '1978-12-25', 3175551009, 'david.wilson22@email.com', '400 River Rd', 'Indianapolis', 'IN', '46201', 'active', '2022-11-05', FALSE, NULL, 'LEGACY-0009'),
-- Name whitespace trimmed ^^
(10, 'Elizabeth', 'Anderson', 'd0e1f2a3b4c5d6e7', '1965-06-14', 7045551010, 'elizabeth.a44@email.com', '500 Spring St', 'Charlotte', 'NC', '28201', 'inactive', '2021-04-22', TRUE, NULL, 'LEGACY-0010');
-- Status corrected to 'inactive' for deceased ^^

-- ── Claims (migrated with transformations) ──
-- Transformations:
--   claim_filing_dt (VARCHAR) -> filing_date (DATE)
--   bnf_year_start/end (VARCHAR) -> benefit_year_start/end (DATE)
--   bnf_wkly_amt -> weekly_benefit_amount
--   max_bnf_amt -> max_benefit_amount
--   last_updated -> updated_at
--   Status values normalized
-- NOTE: Orphan claims (clmt_id 9990, 9991) dropped during migration
-- NOTE: Future filing date claim (claim_id 13) migrated as-is for testing

INSERT INTO claims (claim_id, claimant_id, employer_id, separation_reason, filing_date, benefit_year_start, benefit_year_end, weekly_benefit_amount, max_benefit_amount, total_paid, weeks_claimed, claim_status, updated_at) VALUES
(1, 1, 1, 'Laid off - lack of work', '2023-04-01', '2023-04-01', '2024-04-01', 450.00, 11700.00, 5400.00, 12, 'active', '2023-10-01'),
(2, 2, 2, 'Company closure', '2023-05-15', '2023-05-15', '2024-05-15', 380.00, 9880.00, 3040.00, 8, 'active', '2023-09-15'),
(3, 3, 3, 'Seasonal layoff', '2023-06-01', '2023-06-01', '2024-06-01', 275.00, 7150.00, 0.00, 0, 'pending', '2023-06-01'),
(4, 4, 4, 'Reduction in force', '2023-07-10', '2023-07-10', '2024-07-10', 520.00, 13520.00, 7800.00, 15, 'active', '2023-11-10'),
-- NOTE: claim_id 5 for clmt_id 5 dropped (claimant not migrated due to duplicate SSN)
(6, 6, 1, 'Laid off - position eliminated', '2022-10-01', '2022-10-01', '2023-10-01', 400.00, 10400.00, 12000.00, 30, 'exhausted', '2023-10-01'),
-- Overpayment persists ^^
(7, 7, 2, 'Plant shutdown', '2021-03-15', '2021-03-15', '2022-03-15', 350.00, 9100.00, 9100.00, 26, 'closed', '2022-03-15'),
(8, 8, 3, 'COVID-19 related', '2023-09-01', '2023-09-01', '2024-09-01', 290.00, 7540.00, 1740.00, 6, 'active', '2023-12-01'),
(9, 9, 4, 'Downsizing', '2023-02-01', '2023-02-01', '2024-02-01', 480.00, 12480.00, 4800.00, 10, 'active', '2023-07-01'),
(10, 10, 5, 'Automation displacement', '2022-06-01', '2022-06-01', '2023-06-01', 410.00, 10660.00, 10660.00, 26, 'exhausted', '2023-06-01'),
(11, 1, 3, 'Seasonal layoff', '2024-01-15', '2024-01-15', '2025-01-15', 450.00, 11700.00, 2250.00, 5, 'active', '2024-03-15'),
(12, 2, 4, 'Reduction in force', '2024-02-01', '2024-02-01', '2025-02-01', 395.00, 10270.00, 0.00, 0, 'pending', '2024-02-01'),
(13, 6, 2, 'Laid off - lack of work', '2027-06-01', '2027-06-01', '2028-06-01', 425.00, 11050.00, 0.00, 0, 'pending', '2027-06-01');
-- Future filing date persists ^^
-- NOTE: Orphan claims 14, 15 NOT migrated (clmt_id 9990, 9991 don't exist)

-- ── Benefit Payments (migrated with transformations) ──
-- Transformations:
--   pymt_id -> payment_id, pymt_dt -> payment_date, pymt_amt -> payment_amount
--   pymt_method -> payment_method, week_ending_dt -> week_ending_date
--   pymt_status -> payment_status
--   Dates converted from VARCHAR to DATE
-- NOTE: Orphan payments (claim_id 8888, 9999) dropped during migration
-- NOTE: Negative payment amount migrated as-is

INSERT INTO benefit_payments (payment_id, claim_id, payment_date, payment_amount, payment_method, week_ending_date, payment_status, check_number) VALUES
(1, 1, '2023-04-08', 450.00, 'direct_deposit', '2023-04-07', 'processed', NULL),
(2, 1, '2023-04-15', 450.00, 'direct_deposit', '2023-04-14', 'processed', NULL),
(3, 1, '2023-04-22', 450.00, 'direct_deposit', '2023-04-21', 'processed', NULL),
(4, 2, '2023-05-22', 380.00, 'debit_card', '2023-05-21', 'processed', NULL),
(5, 2, '2023-05-29', 380.00, 'debit_card', '2023-05-28', 'processed', NULL),
(6, 4, '2023-07-17', 520.00, 'direct_deposit', '2023-07-16', 'processed', NULL),
(7, 4, '2023-07-24', 520.00, 'direct_deposit', '2023-07-23', 'processed', NULL),
(8, 4, '2023-07-31', 520.00, 'direct_deposit', '2023-07-30', 'processed', NULL),
(9, 8, '2023-09-08', 290.00, 'debit_card', '2023-09-07', 'processed', NULL),
(10, 8, '2023-09-15', 290.00, 'debit_card', '2023-09-14', 'processed', NULL),
(11, 9, '2023-02-08', 480.00, 'direct_deposit', '2023-02-07', 'processed', NULL),
(12, 9, '2023-02-15', 480.00, 'direct_deposit', '2023-02-14', 'processed', NULL),
(13, 11, '2024-01-22', 450.00, 'direct_deposit', '2024-01-21', 'processed', NULL),
(14, 1, '2023-05-01', -450.00, 'direct_deposit', '2023-04-28', 'reversed', NULL);
-- Negative payment amount persists ^^
-- NOTE: Orphan payments 19, 20 NOT migrated (claim_id 8888, 9999 don't exist)

-- Show counts
SELECT 'Modern Claimants' as table_name, COUNT(*) as row_count FROM claimants
UNION ALL SELECT 'Modern Employers', COUNT(*) FROM employers
UNION ALL SELECT 'Modern Claims', COUNT(*) FROM claims
UNION ALL SELECT 'Modern Benefit Payments', COUNT(*) FROM benefit_payments;

-- Compare to legacy
SELECT 'ROW COUNT COMPARISON:' as note;
SELECT
    'Claimants' as table_name,
    (SELECT COUNT(*) FROM claimants) as modern_count,
    'Should be 9 (1 duplicate SSN removed from legacy 10)' as note
UNION ALL
SELECT
    'Claims',
    (SELECT COUNT(*) FROM claims),
    'Should be 12 (2 orphan claims + 1 for dropped claimant removed from legacy 15)'
UNION ALL
SELECT
    'Benefit Payments',
    (SELECT COUNT(*) FROM benefit_payments),
    'Should be 14 (2 orphan payments removed from legacy 20)';

-- Show referential integrity
SELECT 'REFERENTIAL INTEGRITY CHECK:' as note;
SELECT
    'Orphan claims (claimant_id not in claimants):' as check_name,
    COUNT(*) as orphan_count
FROM claims c
LEFT JOIN claimants cl ON c.claimant_id = cl.claimant_id
WHERE cl.claimant_id IS NULL;
