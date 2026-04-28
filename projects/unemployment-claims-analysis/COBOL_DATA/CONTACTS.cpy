      *================================================================*
      * COPYBOOK: CTMST010.CPY                                        *
      * SYSTEM:   CONTACT MANAGEMENT SYSTEM (CMS)                     *
      * RECORD:   CONTACT MASTER RECORD                               *
      * LENGTH:   1200 BYTES                                          *
      * CREATED:  2005-08-12 BY BATCH JOB CMS-INIT                   *
      * MODIFIED: 2011-03-22 BY BATCH JOB CMS-UPDT                   *
      *================================================================*
       01  CT-MASTER-REC.
           05  CT-RECID            PIC 9(8).
           05  CT-FNAM             PIC X(25).
           05  CT-MNAM             PIC X(25).
           05  CT-LNAM             PIC X(30).
           05  CT-SUFX             PIC X(5).
           05  CT-SSN              PIC X(11).
           05  CT-DOB              PIC X(10).
           05  CT-GNDR             PIC X(1).
           05  CT-ETHN             PIC X(20).
           05  CT-PTEL             PIC X(14).
           05  CT-MTEL             PIC X(14).
           05  CT-WTEL             PIC X(14).
           05  CT-EMAL             PIC X(60).
           05  CT-ADR1             PIC X(40).
           05  CT-ADR2             PIC X(40).
           05  CT-CITY             PIC X(30).
           05  CT-ST               PIC X(2).
           05  CT-ZIP              PIC X(10).
           05  CT-ADTYP            PIC X(10).
           05  CT-MADR1            PIC X(40).
           05  CT-MADR2            PIC X(40).
           05  CT-MCITY            PIC X(30).
           05  CT-MST              PIC X(2).
           05  CT-MZIP             PIC X(10).
           05  CT-EMRG             PIC X(50).
           05  CT-ETEL             PIC X(14).
           05  CT-EREL             PIC X(20).
           05  CT-DLN              PIC X(20).
           05  CT-DLST             PIC X(2).
           05  CT-BACT             PIC X(20).
           05  CT-BRTN             PIC X(20).
           05  CT-MSTAT            PIC X(10).
           05  CT-DPNDS            PIC 9(2).
           05  CT-LANG             PIC X(10).
           05  CT-VETF             PIC X(1).
           05  CT-DISF             PIC X(1).
           05  CT-STAT             PIC X(10).
           05  CT-CRTDT            PIC X(26).
           05  CT-UPDDT            PIC X(26).
           05  CT-SRCCD            PIC X(10).
           05  CT-FIL1             PIC X(50).
           05  CT-FIL2             PIC X(30).
