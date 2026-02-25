-- Data Validation Agent - Database Setup
-- State Department of Labor: Unemployment Insurance System Migration
-- Creates legacy_db and modern_db databases with schema structures

-- Create databases (run as postgres user)
-- Note: Run this with: psql -U postgres -f setup/create_databases.sql

DROP DATABASE IF EXISTS legacy_db;
DROP DATABASE IF EXISTS modern_db;

CREATE DATABASE legacy_db;
CREATE DATABASE modern_db;

\c legacy_db

-- ══════════════════════════════════════════════════════════════════════
-- LEGACY MAINFRAME SCHEMA
-- Migrated from IBM DB2 z/OS via Micro Focus COBOL bridge (circa 2009)
-- Original COBOL copybooks preserved as comments
-- Relaxed constraints to allow intentional data quality issues
-- ══════════════════════════════════════════════════════════════════════

-- COPYBOOK: UICL010.CPY  (UI Claimant Master Record)
-- RECORD LENGTH: 1024 BYTES
-- LAST MODIFIED: 2009-03-15 BY BATCH JOB UICL-LOAD
CREATE TABLE claimants (
    cl_recid            INTEGER,            -- PIC 9(8)    CLAIMANT-RECORD-ID
    cl_fnam             CHAR(30),           -- PIC X(30)   CLAIMANT-FIRST-NAME
    cl_lnam             CHAR(30),           -- PIC X(30)   CLAIMANT-LAST-NAME
    cl_ssn              CHAR(11),           -- PIC X(11)   CLAIMANT-SSN
    cl_dob              CHAR(30),           -- PIC X(30)   CLAIMANT-DATE-OF-BIRTH
    cl_phon             CHAR(20),           -- PIC X(20)   CLAIMANT-PHONE-NUM
    cl_emal             VARCHAR(255),       -- PIC X(255)  CLAIMANT-EMAIL-ADDR
    cl_adr1             VARCHAR(500),       -- PIC X(500)  CLAIMANT-STREET-ADDR
    cl_city             CHAR(30),           -- PIC X(30)   CLAIMANT-CITY-NAME
    cl_st               CHAR(2),            -- PIC X(2)    CLAIMANT-STATE-CODE
    cl_zip              CHAR(10),           -- PIC X(10)   CLAIMANT-ZIP-CODE
    cl_bact             CHAR(20),           -- PIC X(20)   CLAIMANT-BANK-ACCT
    cl_brtn             CHAR(20),           -- PIC X(20)   CLAIMANT-BANK-ROUTE
    cl_stat             CHAR(20),           -- PIC X(20)   CLAIMANT-STATUS-CODE
    cl_rgdt             CHAR(26),           -- PIC X(26)   CLAIMANT-REG-DATE
    cl_dcsd             CHAR(1),            -- PIC X(1)    CLAIMANT-DECEASED-FLAG (Y/N)
    cl_fil1             CHAR(10)            -- PIC X(10)   FILLER
);

-- COPYBOOK: UIER020.CPY  (UI Employer Master Record)
-- RECORD LENGTH: 512 BYTES
-- LAST MODIFIED: 2008-11-20 BY BATCH JOB UIER-LOAD
CREATE TABLE employers (
    er_recid            INTEGER PRIMARY KEY, -- PIC 9(8)   EMPLOYER-RECORD-ID
    er_name             VARCHAR(255) NOT NULL, -- PIC X(255) EMPLOYER-CORP-NAME
    er_ein              CHAR(20),           -- PIC X(20)   EMPLOYER-EIN
    er_ind              CHAR(30),           -- PIC X(30)   EMPLOYER-INDUSTRY-CODE
    er_adr1             VARCHAR(500),       -- PIC X(500)  EMPLOYER-STREET-ADDR
    er_city             CHAR(30),           -- PIC X(30)   EMPLOYER-CITY-NAME
    er_st               CHAR(2),            -- PIC X(2)    EMPLOYER-STATE-CODE
    er_zip              CHAR(10),           -- PIC X(10)   EMPLOYER-ZIP-CODE
    er_phon             CHAR(20),           -- PIC X(20)   EMPLOYER-PHONE-NUM
    er_stat             CHAR(20) DEFAULT 'active' -- PIC X(20) EMPLOYER-STATUS-CODE
);

-- COPYBOOK: UICM030.CPY  (UI Claim Detail Record)
-- RECORD LENGTH: 768 BYTES
-- LAST MODIFIED: 2010-06-01 BY BATCH JOB UICM-PROC
CREATE TABLE claims (
    cm_recid            INTEGER,            -- PIC 9(8)    CLAIM-RECORD-ID
    cm_clmnt            INTEGER,            -- PIC 9(8)    CLAIM-CLAIMANT-REF
    cm_emplr            INTEGER,            -- PIC 9(8)    CLAIM-EMPLOYER-REF
    cm_seprs            VARCHAR(255),       -- PIC X(255)  CLAIM-SEPARATION-REASON
    cm_fildt            CHAR(30),           -- PIC X(30)   CLAIM-FILING-DATE
    cm_bystr            CHAR(30),           -- PIC X(30)   CLAIM-BNF-YEAR-START
    cm_byend            CHAR(30),           -- PIC X(30)   CLAIM-BNF-YEAR-END
    cm_wkamt            DECIMAL(10, 2),     -- PIC S9(8)V99 COMP-3  CLAIM-WEEKLY-AMT
    cm_mxamt            DECIMAL(10, 2),     -- PIC S9(8)V99 COMP-3  CLAIM-MAX-BNF-AMT
    cm_totpd            DECIMAL(10, 2) DEFAULT 0, -- PIC S9(8)V99 COMP-3 CLAIM-TOTAL-PAID
    cm_wkcnt            INTEGER DEFAULT 0,  -- PIC 9(4)    CLAIM-WEEKS-COUNT
    cm_stat             CHAR(20),           -- PIC X(20)   CLAIM-STATUS-CODE
    cm_lupdt            CHAR(26)            -- PIC X(26)   CLAIM-LAST-UPD-DATE
);

-- COPYBOOK: UIBP040.CPY  (UI Benefit Payment Transaction)
-- RECORD LENGTH: 256 BYTES
-- LAST MODIFIED: 2010-06-01 BY BATCH JOB UIBP-DISB
CREATE TABLE benefit_payments (
    bp_recid            INTEGER,            -- PIC 9(8)    PAYMENT-RECORD-ID
    bp_clmid            INTEGER,            -- PIC 9(8)    PAYMENT-CLAIM-REF
    bp_paydt            CHAR(30),           -- PIC X(30)   PAYMENT-PROCESS-DATE
    bp_payam            DECIMAL(10, 2),     -- PIC S9(8)V99 COMP-3  PAYMENT-AMOUNT
    bp_methd            CHAR(30),           -- PIC X(30)   PAYMENT-METHOD-CODE
    bp_wkedt            CHAR(30),           -- PIC X(30)   PAYMENT-WEEK-END-DATE
    bp_stat             CHAR(20) DEFAULT 'processed', -- PIC X(20) PAYMENT-STATUS-CODE
    bp_chkno            CHAR(20)            -- PIC X(20)   PAYMENT-CHECK-NUM
);

\c modern_db

-- Modern cloud platform schema (clean naming, proper types, constraints)

CREATE TABLE claimants (
    claimant_id         INTEGER PRIMARY KEY,
    first_name          VARCHAR(100) NOT NULL,
    last_name           VARCHAR(100) NOT NULL,
    ssn_hash            VARCHAR(64),
    date_of_birth       DATE,
    phone_number        BIGINT,
    email               VARCHAR(255),
    address_line1       VARCHAR(500),
    city                VARCHAR(100),
    state               VARCHAR(2),
    zip_code            VARCHAR(10),
    claimant_status     VARCHAR(20) DEFAULT 'active',
    registered_at       TIMESTAMP DEFAULT NOW(),
    is_deceased         BOOLEAN DEFAULT FALSE,
    cl_bact             VARCHAR(20),        -- DEMO: archived field leaked (PCI-DSS violation)
    legacy_system_ref   VARCHAR(50)         -- DEMO: ungoverned column, no ETL mapping
);

CREATE TABLE employers (
    employer_id         INTEGER PRIMARY KEY,
    employer_name       VARCHAR(255) NOT NULL,
    employer_ein        VARCHAR(20),
    industry            VARCHAR(100),
    address_line1       VARCHAR(500),
    city                VARCHAR(100),
    state               VARCHAR(2),
    zip_code            VARCHAR(10),
    phone_number        BIGINT,
    employer_status     VARCHAR(20) DEFAULT 'active'
);

CREATE TABLE claims (
    claim_id            INTEGER PRIMARY KEY,
    claimant_id         INTEGER REFERENCES claimants(claimant_id),
    employer_id         INTEGER REFERENCES employers(employer_id),
    separation_reason   VARCHAR(255),
    filing_date         DATE NOT NULL,
    benefit_year_start  DATE,
    benefit_year_end    DATE,
    weekly_benefit_amount DECIMAL(10, 2),
    max_benefit_amount  DECIMAL(10, 2),
    total_paid          DECIMAL(10, 2) DEFAULT 0,
    weeks_claimed       INTEGER DEFAULT 0,
    claim_status        VARCHAR(20),
    updated_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE benefit_payments (
    payment_id          INTEGER PRIMARY KEY,
    claim_id            INTEGER REFERENCES claims(claim_id),
    payment_date        DATE NOT NULL,
    payment_amount      DECIMAL(10, 2) NOT NULL,
    payment_method      VARCHAR(30),
    week_ending_date    DATE,
    payment_status      VARCHAR(20) DEFAULT 'processed',
    check_number        VARCHAR(20)
);

-- Grant permissions (adjust username as needed)
\c legacy_db
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;

\c modern_db
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
