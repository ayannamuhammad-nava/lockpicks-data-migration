-- Load demo data into legacy_db with INTENTIONAL ISSUES for validation testing
-- Scenario: State Department of Labor - Unemployment Insurance System

\c legacy_db

-- ── Employers (clean data) ──

INSERT INTO employers (er_recid, er_name, er_ein, er_ind, er_adr1, er_city, er_st, er_zip, er_phon, er_stat) VALUES
(1, 'Midwest Manufacturing Corp', '12-3456789', 'Manufacturing', '100 Industrial Blvd', 'Springfield', 'IL', '62701', '217-555-0100', 'active'),
(2, 'Great Lakes Logistics LLC', '23-4567890', 'Transportation', '200 Harbor Way', 'Columbus', 'OH', '43215', '614-555-0200', 'active'),
(3, 'Heartland Healthcare Systems', '34-5678901', 'Healthcare', '300 Medical Dr', 'Jacksonville', 'FL', '32099', '904-555-0300', 'active'),
(4, 'Prairie State Energy Co', '45-6789012', 'Energy', '400 Power Ln', 'Indianapolis', 'IN', '46201', '317-555-0400', 'active'),
(5, 'Central Valley Foods Inc', '56-7890123', 'Food Processing', '500 Harvest Rd', 'Charlotte', 'NC', '28201', '704-555-0500', 'active');

-- ── Claimants with intentional issues ──
-- Issue 1: Duplicate SSNs (cl_recid 5 reuses SSN from cl_recid 1)
-- Issue 2: NULL emails (cl_recid 3, 8)
-- Issue 3: PII in plaintext (cl_ssn, cl_bact, cl_brtn)
-- Issue 4: Mixed date formats in cl_dob
-- Issue 5: Inconsistent phone formats
-- Issue 6: Status inconsistencies (ACTIVE, active, Active, ACT)
-- Issue 7: Whitespace in names (cl_recid 9)
-- Issue 8: Deceased claimant with active status (cl_recid 10)

INSERT INTO claimants (cl_recid, cl_fnam, cl_lnam, cl_ssn, cl_dob, cl_phon, cl_emal, cl_adr1, cl_city, cl_st, cl_zip, cl_bact, cl_brtn, cl_stat, cl_rgdt, cl_dcsd, cl_fil1) VALUES
-- Normal records with various legacy date/phone formats
(1, 'James', 'Smith', '123-45-6789', '1980-01-15', '217-555-1001', 'james.smith42@email.com', '123 Main St', 'Springfield', 'IL', '62701', '1234567890', '111000025', 'ACTIVE', '2022-03-15 00:00:00', 'N', ''),
(2, 'Mary', 'Johnson', '987-65-4321', '03/20/1975', '(614) 555-1002', 'mary.johnson7@email.com', '456 Oak Ave', 'Columbus', 'OH', '43215', '0987654321', '111000026', 'active', '2022-06-20 00:00:00', 'N', ''),
(3, 'Robert', 'Williams', '555-12-3456', 'January 10, 1988', '9045551003', NULL, '789 Pine St', 'Jacksonville', 'FL', '32099', '1111222233', '111000027', 'Active', '2023-01-10 00:00:00', 'N', ''),
(4, 'Patricia', 'Brown', '444-55-6666', '1992-07-04', '317.555.1004', 'patricia.brown15@email.com', '321 Elm St', 'Indianapolis', 'IN', '46201', '4444555566', '111000028', 'ACT', '2023-04-05 00:00:00', 'N', ''),
(5, 'John', 'Davis', '123-45-6789', '90-05-01', '704 555 1005', 'john.davis88@email.com', '654 Cedar Ln', 'Charlotte', 'NC', '28201', '7777888899', '111000029', 'ACTIVE', '2023-05-01 00:00:00', 'N', ''),
-- DUPLICATE SSN ^^ same as cl_recid 1
(6, 'Jennifer', 'Garcia', '222-33-4444', '1985-11-22', '217-555-1006', 'jennifer.garcia3@email.com', '100 Maple Dr', 'Springfield', 'IL', '62701', '2222333344', '111000030', 'ACTIVE', '2021-09-12 00:00:00', 'N', ''),
(7, 'Michael', 'Miller', '333-44-5555', '08/15/1970', '(614) 555-1007', 'michael.miller56@email.com', '200 Lake Rd', 'Columbus', 'OH', '43215', '3333444455', '111000031', 'INACTIVE', '2020-12-01 00:00:00', 'N', ''),
(8, 'Linda', 'Martinez', '666-77-8888', '1995-03-30', '9045551008', NULL, '300 Hill St', 'Jacksonville', 'FL', '32099', '6666777788', '111000032', 'active', '2023-07-18 00:00:00', 'N', ''),
-- NULL email ^^
(9, '  David  ', '  Wilson  ', '777-88-9999', '1978-12-25', '317.555.1009', 'david.wilson22@email.com', '400 River Rd', 'Indianapolis', 'IN', '46201', '8888999900', '111000033', 'ACTIVE', '2022-11-05 00:00:00', 'N', ''),
-- Whitespace in names ^^
(10, 'Elizabeth', 'Anderson', '888-99-0000', '1965-06-14', '704 555 1010', 'elizabeth.a44@email.com', '500 Spring St', 'Charlotte', 'NC', '28201', '9999000011', '111000034', 'ACTIVE', '2021-04-22 00:00:00', 'Y', '');
-- Deceased but ACTIVE status ^^

-- ── Claims with intentional issues ──
-- Issue 9: Orphan claims (cm_recid 14, 15 reference non-existent claimants)
-- Issue 10: Future filing dates (cm_recid 13)
-- Issue 11: Overpayment (cm_recid 6 cm_totpd > cm_mxamt)

INSERT INTO claims (cm_recid, cm_clmnt, cm_emplr, cm_seprs, cm_fildt, cm_bystr, cm_byend, cm_wkamt, cm_mxamt, cm_totpd, cm_wkcnt, cm_stat, cm_lupdt) VALUES
(1, 1, 1, 'Laid off - lack of work', '2023-04-01', '2023-04-01', '2024-04-01', 450.00, 11700.00, 5400.00, 12, 'ACTIVE', '2023-10-01 00:00:00'),
(2, 2, 2, 'Company closure', '2023-05-15', '2023-05-15', '2024-05-15', 380.00, 9880.00, 3040.00, 8, 'active', '2023-09-15 00:00:00'),
(3, 3, 3, 'Seasonal layoff', '2023-06-01', '2023-06-01', '2024-06-01', 275.00, 7150.00, 0.00, 0, 'PENDING', '2023-06-01 00:00:00'),
(4, 4, 4, 'Reduction in force', '2023-07-10', '2023-07-10', '2024-07-10', 520.00, 13520.00, 7800.00, 15, 'ACTIVE', '2023-11-10 00:00:00'),
(5, 5, 5, 'Contract ended', '2023-08-01', '2023-08-01', '2024-08-01', 325.50, 8463.00, 1627.50, 5, 'Active', '2023-10-01 00:00:00'),
(6, 6, 1, 'Laid off - position eliminated', '2022-10-01', '2022-10-01', '2023-10-01', 400.00, 10400.00, 12000.00, 30, 'EXHAUSTED', '2023-10-01 00:00:00'),
-- Overpayment: cm_totpd $12000 > cm_mxamt $10400 ^^
(7, 7, 2, 'Plant shutdown', '2021-03-15', '2021-03-15', '2022-03-15', 350.00, 9100.00, 9100.00, 26, 'CLOSED', '2022-03-15 00:00:00'),
(8, 8, 3, 'COVID-19 related', '2023-09-01', '2023-09-01', '2024-09-01', 290.00, 7540.00, 1740.00, 6, 'active', '2023-12-01 00:00:00'),
(9, 9, 4, 'Downsizing', '2023-02-01', '2023-02-01', '2024-02-01', 480.00, 12480.00, 4800.00, 10, 'ACTIVE', '2023-07-01 00:00:00'),
(10, 10, 5, 'Automation displacement', '2022-06-01', '2022-06-01', '2023-06-01', 410.00, 10660.00, 10660.00, 26, 'EXHAUSTED', '2023-06-01 00:00:00'),
(11, 1, 3, 'Seasonal layoff', '2024-01-15', '2024-01-15', '2025-01-15', 450.00, 11700.00, 2250.00, 5, 'ACTIVE', '2024-03-15 00:00:00'),
(12, 2, 4, 'Reduction in force', '2024-02-01', '2024-02-01', '2025-02-01', 395.00, 10270.00, 0.00, 0, 'PENDING', '2024-02-01 00:00:00'),
(13, 6, 2, 'Laid off - lack of work', '2027-06-01', '2027-06-01', '2028-06-01', 425.00, 11050.00, 0.00, 0, 'PENDING', '2027-06-01 00:00:00'),
-- Future filing date ^^
(14, 9990, 1, 'Company closure', '2023-11-01', '2023-11-01', '2024-11-01', 300.00, 7800.00, 0.00, 0, 'PENDING', '2023-11-01 00:00:00'),
(15, 9991, 2, 'Laid off - lack of work', '2024-01-10', '2024-01-10', '2025-01-10', 350.00, 9100.00, 0.00, 0, 'ACTIVE', '2024-01-10 00:00:00');
-- Orphan claims: cm_clmnt 9990, 9991 don't exist ^^

-- ── Benefit Payments with intentional issues ──
-- Issue 12: Orphan payments (bp_recid 19, 20 reference non-existent claims)
-- Issue 13: Negative payment amount (bp_recid 18)

INSERT INTO benefit_payments (bp_recid, bp_clmid, bp_paydt, bp_payam, bp_methd, bp_wkedt, bp_stat, bp_chkno) VALUES
(1, 1, '2023-04-08', 450.00, 'direct_deposit', '2023-04-07', 'processed', NULL),
(2, 1, '2023-04-15', 450.00, 'direct_deposit', '2023-04-14', 'processed', NULL),
(3, 1, '2023-04-22', 450.00, 'direct_deposit', '2023-04-21', 'processed', NULL),
(4, 2, '2023-05-22', 380.00, 'debit_card', '2023-05-21', 'processed', NULL),
(5, 2, '2023-05-29', 380.00, 'debit_card', '2023-05-28', 'processed', NULL),
(6, 4, '2023-07-17', 520.00, 'direct_deposit', '2023-07-16', 'processed', NULL),
(7, 4, '2023-07-24', 520.00, 'direct_deposit', '2023-07-23', 'processed', NULL),
(8, 4, '2023-07-31', 520.00, 'direct_deposit', '2023-07-30', 'processed', NULL),
(9, 5, '2023-08-08', 325.50, 'check', '2023-08-07', 'processed', 'CHK-10001'),
(10, 5, '2023-08-15', 325.50, 'check', '2023-08-14', 'processed', 'CHK-10002'),
(11, 6, '2022-10-08', 400.00, 'direct_deposit', '2022-10-07', 'processed', NULL),
(12, 6, '2022-10-15', 400.00, 'direct_deposit', '2022-10-14', 'processed', NULL),
(13, 8, '2023-09-08', 290.00, 'debit_card', '2023-09-07', 'processed', NULL),
(14, 8, '2023-09-15', 290.00, 'debit_card', '2023-09-14', 'processed', NULL),
(15, 9, '2023-02-08', 480.00, 'direct_deposit', '2023-02-07', 'processed', NULL),
(16, 9, '2023-02-15', 480.00, 'direct_deposit', '2023-02-14', 'processed', NULL),
(17, 11, '2024-01-22', 450.00, 'direct_deposit', '2024-01-21', 'processed', NULL),
(18, 1, '2023-05-01', -450.00, 'direct_deposit', '2023-04-28', 'reversed', NULL),
-- Negative payment amount (reversal) ^^
(19, 8888, '2023-10-01', 300.00, 'debit_card', '2023-09-30', 'processed', NULL),
(20, 9999, '2023-11-01', 275.00, 'check', '2023-10-31', 'processed', 'CHK-99999');
-- Orphan payments: bp_clmid 8888, 9999 don't exist ^^

-- Show counts
SELECT 'Legacy Claimants' as table_name, COUNT(*) as row_count FROM claimants
UNION ALL SELECT 'Legacy Employers', COUNT(*) FROM employers
UNION ALL SELECT 'Legacy Claims', COUNT(*) FROM claims
UNION ALL SELECT 'Legacy Benefit Payments', COUNT(*) FROM benefit_payments;

-- Show intentional issues summary
SELECT 'INTENTIONAL ISSUES SUMMARY:' as note;
SELECT 'Issue 1: Duplicate SSN (cl_recid 1 and 5)' as issue
UNION ALL SELECT 'Issue 2: NULL emails (cl_recid 3, 8)'
UNION ALL SELECT 'Issue 3: PII in plaintext (cl_ssn, cl_bact, cl_brtn)'
UNION ALL SELECT 'Issue 4: Mixed date formats in cl_dob'
UNION ALL SELECT 'Issue 5: Inconsistent phone formats'
UNION ALL SELECT 'Issue 6: Status inconsistencies (ACTIVE, active, Active, ACT)'
UNION ALL SELECT 'Issue 7: Whitespace in names (cl_recid 9)'
UNION ALL SELECT 'Issue 8: Deceased claimant with active status (cl_recid 10)'
UNION ALL SELECT 'Issue 9: Orphan claims (cm_clmnt 9990, 9991 do not exist)'
UNION ALL SELECT 'Issue 10: Future filing date (cm_recid 13)'
UNION ALL SELECT 'Issue 11: Overpayment (cm_recid 6: cm_totpd > cm_mxamt)'
UNION ALL SELECT 'Issue 12: Orphan payments (bp_clmid 8888, 9999 do not exist)'
UNION ALL SELECT 'Issue 13: Negative payment amount (bp_recid 18)';
