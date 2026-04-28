-- Contact Management System — Legacy Schema
-- Migrated from IBM COBOL/VSAM via batch bridge (circa 2005)
-- Original copybook: CTMST010.CPY
-- Relaxed constraints to preserve original data quality issues

CREATE TABLE IF NOT EXISTS contacts (
    ct_recid    INTEGER,                -- PIC 9(8)    CONTACT-RECORD-ID
    ct_fnam     CHAR(25),               -- PIC X(25)   CONTACT-FIRST-NAME
    ct_mnam     CHAR(25),               -- PIC X(25)   CONTACT-MIDDLE-NAME
    ct_lnam     CHAR(30),               -- PIC X(30)   CONTACT-LAST-NAME
    ct_sufx     CHAR(5),                -- PIC X(5)    CONTACT-NAME-SUFFIX
    ct_ssn      CHAR(11),               -- PIC X(11)   CONTACT-SSN
    ct_dob      CHAR(12),               -- PIC X(12)   CONTACT-DATE-OF-BIRTH (widened for mixed date formats)
    ct_gndr     CHAR(1),                -- PIC X(1)    CONTACT-GENDER-CODE
    ct_ethn     CHAR(20),               -- PIC X(20)   CONTACT-ETHNICITY
    ct_ptel     CHAR(14),               -- PIC X(14)   CONTACT-PRIMARY-PHONE
    ct_mtel     CHAR(14),               -- PIC X(14)   CONTACT-MOBILE-PHONE
    ct_wtel     CHAR(14),               -- PIC X(14)   CONTACT-WORK-PHONE
    ct_emal     VARCHAR(60),            -- PIC X(60)   CONTACT-EMAIL-ADDR
    ct_adr1     VARCHAR(40),            -- PIC X(40)   CONTACT-STREET-ADDR-1
    ct_adr2     VARCHAR(40),            -- PIC X(40)   CONTACT-STREET-ADDR-2
    ct_city     CHAR(30),               -- PIC X(30)   CONTACT-CITY-NAME
    ct_st       CHAR(2),                -- PIC X(2)    CONTACT-STATE-CODE
    ct_zip      CHAR(10),               -- PIC X(10)   CONTACT-ZIP-CODE
    ct_adtyp    CHAR(10),               -- PIC X(10)   CONTACT-ADDR-TYPE (HOME/WORK/BUSNS)
    ct_madr1    VARCHAR(40),            -- PIC X(40)   CONTACT-MAIL-ADDR-1
    ct_madr2    VARCHAR(40),            -- PIC X(40)   CONTACT-MAIL-ADDR-2
    ct_mcity    CHAR(30),               -- PIC X(30)   CONTACT-MAIL-CITY
    ct_mst      CHAR(2),                -- PIC X(2)    CONTACT-MAIL-STATE
    ct_mzip     CHAR(10),               -- PIC X(10)   CONTACT-MAIL-ZIP
    ct_emrg     VARCHAR(50),            -- PIC X(50)   CONTACT-EMERGENCY-NAME
    ct_etel     CHAR(14),               -- PIC X(14)   CONTACT-EMERGENCY-PHONE
    ct_erel     CHAR(20),               -- PIC X(20)   CONTACT-EMERGENCY-RELATION
    ct_dln      CHAR(20),               -- PIC X(20)   CONTACT-DRIVERS-LICENSE-NUM
    ct_dlst     CHAR(2),                -- PIC X(2)    CONTACT-DL-STATE
    ct_bact     CHAR(20),               -- PIC X(20)   CONTACT-BANK-ACCT
    ct_brtn     CHAR(20),               -- PIC X(20)   CONTACT-BANK-ROUTE
    ct_mstat    CHAR(10),               -- PIC X(10)   CONTACT-MARITAL-STATUS
    ct_dpnds    INTEGER,                -- PIC 9(2)    CONTACT-DEPENDENTS-COUNT
    ct_lang     CHAR(10),               -- PIC X(10)   CONTACT-LANGUAGE-PREF
    ct_vetf     CHAR(1),                -- PIC X(1)    CONTACT-VETERAN-FLAG (Y/N)
    ct_disf     CHAR(1),                -- PIC X(1)    CONTACT-DISABILITY-FLAG (Y/N)
    ct_stat     CHAR(10),               -- PIC X(10)   CONTACT-STATUS-CODE
    ct_crtdt    CHAR(26),               -- PIC X(26)   CONTACT-CREATED-DATE
    ct_upddt    CHAR(26),               -- PIC X(26)   CONTACT-UPDATED-DATE
    ct_srccd    CHAR(10),               -- PIC X(10)   CONTACT-SOURCE-CODE
    ct_fil1     CHAR(50),               -- PIC X(50)   FILLER-1
    ct_fil2     CHAR(30)                -- PIC X(30)   FILLER-2
);
