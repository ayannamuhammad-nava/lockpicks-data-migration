#!/usr/bin/env python3
"""
Setup PostgreSQL databases for the Data Validation Agent demo.

Scenario: State Department of Labor - Unemployment Insurance System Migration
Migrating from a 15-year-old legacy mainframe database to a modern cloud platform.

Creates:
  - legacy_db: Legacy mainframe-style schema with intentional data quality issues
  - modern_db: Modern cloud schema with migrated (but not perfectly clean) data
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import random
import sys
from datetime import datetime, timedelta

# Fixed seed for reproducible demo data
random.seed(42)

# ── Data Generation Helpers ──

FIRST_NAMES = [
    'James', 'Mary', 'Robert', 'Patricia', 'John', 'Jennifer', 'Michael', 'Linda',
    'David', 'Elizabeth', 'William', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica',
    'Thomas', 'Sarah', 'Charles', 'Karen', 'Christopher', 'Lisa', 'Daniel', 'Nancy',
    'Matthew', 'Betty', 'Anthony', 'Margaret', 'Mark', 'Sandra', 'Donald', 'Ashley',
    'Steven', 'Dorothy', 'Paul', 'Kimberly', 'Andrew', 'Emily', 'Joshua', 'Donna',
    'Kenneth', 'Michelle', 'Kevin', 'Carol', 'Brian', 'Amanda', 'George', 'Melissa',
    'Timothy', 'Deborah', 'Ronald', 'Stephanie', 'Edward', 'Rebecca', 'Jason', 'Sharon',
    'Jeffrey', 'Laura', 'Ryan', 'Cynthia', 'Jacob', 'Kathleen', 'Gary', 'Amy',
    'Nicholas', 'Angela', 'Eric', 'Shirley', 'Jonathan', 'Anna', 'Stephen', 'Brenda',
    'Larry', 'Pamela', 'Justin', 'Emma', 'Scott', 'Nicole', 'Brandon', 'Helen',
    'Benjamin', 'Samantha', 'Samuel', 'Katherine', 'Raymond', 'Christine', 'Gregory', 'Debra',
    'Frank', 'Rachel', 'Alexander', 'Carolyn', 'Patrick', 'Janet', 'Jack', 'Catherine',
]

LAST_NAMES = [
    'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
    'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
    'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
    'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker',
    'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill',
    'Flores', 'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell',
    'Mitchell', 'Carter', 'Roberts', 'Gomez', 'Phillips', 'Evans', 'Turner', 'Diaz',
    'Parker', 'Cruz', 'Edwards', 'Collins', 'Reyes', 'Stewart', 'Morris', 'Morales',
]

STREETS = [
    'Main St', 'Oak Ave', 'Maple Dr', 'Cedar Ln', 'Pine St', 'Elm St', 'Washington Blvd',
    'Park Ave', 'Lake Rd', 'Hill St', 'River Rd', 'Spring St', 'Church St', 'Market St',
    'Union Ave', 'Forest Dr', 'Sunset Blvd', 'Highland Ave', 'Lincoln Way', 'Franklin St',
]

CITIES = [
    ('Springfield', 'IL'), ('Columbus', 'OH'), ('Jacksonville', 'FL'), ('Indianapolis', 'IN'),
    ('Charlotte', 'NC'), ('Detroit', 'MI'), ('Memphis', 'TN'), ('Baltimore', 'MD'),
    ('Milwaukee', 'WI'), ('Albuquerque', 'NM'), ('Tucson', 'AZ'), ('Nashville', 'TN'),
    ('Portland', 'OR'), ('Sacramento', 'CA'), ('Kansas City', 'MO'), ('Mesa', 'AZ'),
    ('Atlanta', 'GA'), ('Omaha', 'NE'), ('Raleigh', 'NC'), ('Cleveland', 'OH'),
]

EMPLOYERS = [
    ('Midwest Manufacturing Corp', 'Manufacturing'), ('Great Lakes Logistics LLC', 'Transportation'),
    ('Heartland Healthcare Systems', 'Healthcare'), ('Prairie State Energy Co', 'Energy'),
    ('Central Valley Foods Inc', 'Food Processing'), ('Metro Transit Authority', 'Government'),
    ('Keystone Construction Group', 'Construction'), ('Liberty Financial Services', 'Finance'),
    ('Pinnacle Retail Group', 'Retail'), ('Summit Technology Solutions', 'Technology'),
    ('Atlas Automotive Parts', 'Automotive'), ('Guardian Insurance Corp', 'Insurance'),
    ('Horizon Telecommunications', 'Telecom'), ('Riverdale Hospitality Group', 'Hospitality'),
    ('Cascade Paper Products', 'Manufacturing'), ('Northern Steel Works', 'Manufacturing'),
    ('Silverline Medical Center', 'Healthcare'), ('Crossroads Shipping Inc', 'Logistics'),
    ('Cornerstone Education Services', 'Education'), ('Valley View Agriculture', 'Agriculture'),
    ('Bayshore Seafood Processing', 'Food Processing'), ('Mountain View Mining Co', 'Mining'),
    ('Lakeside Resort Properties', 'Hospitality'), ('Eastgate Distribution Center', 'Warehouse'),
    ('Westfield Shopping Centers', 'Retail'), ('Northpoint Engineering', 'Engineering'),
    ('Southgate Auto Dealership', 'Automotive'), ('Bridgeport Marine Services', 'Maritime'),
    ('Oakwood Senior Living', 'Healthcare'), ('Parkside Community College', 'Education'),
    ('Stonebridge Legal Associates', 'Legal'), ('Greenfield Organic Farms', 'Agriculture'),
    ('Ironworks Industrial Supply', 'Industrial'), ('Clearwater Environmental', 'Environmental'),
    ('Redwood Software Systems', 'Technology'), ('Blueridge Consulting Group', 'Consulting'),
    ('Capitol City Bank', 'Banking'), ('Heritage Home Builders', 'Construction'),
    ('Pacific Coast Airlines', 'Aviation'), ('Diamond State Utilities', 'Utilities'),
]

SEPARATION_REASONS = [
    'Laid off - lack of work', 'Laid off - position eliminated',
    'Company closure', 'Seasonal layoff', 'Reduction in force',
    'Contract ended', 'Plant shutdown', 'Downsizing',
    'COVID-19 related', 'Automation displacement',
]

CLAIM_STATUSES_LEGACY = ['ACTIVE', 'active', 'Active', 'ACT', 'EXHAUSTED', 'DENIED', 'PENDING', 'SUSPENDED', 'CLOSED']
CLAIM_STATUSES_MODERN = ['active', 'exhausted', 'denied', 'pending', 'suspended', 'closed']

PAYMENT_METHODS = ['direct_deposit', 'debit_card', 'check']


def gen_ssn():
    return f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"


def gen_phone_legacy():
    """Generate phone in various inconsistent legacy formats."""
    area = random.randint(200, 999)
    prefix = random.randint(200, 999)
    line = random.randint(1000, 9999)
    fmt = random.choice(['dash', 'paren', 'plain', 'dot', 'space'])
    if fmt == 'dash':
        return f"{area}-{prefix}-{line}"
    elif fmt == 'paren':
        return f"({area}) {prefix}-{line}"
    elif fmt == 'plain':
        return f"{area}{prefix}{line}"
    elif fmt == 'dot':
        return f"{area}.{prefix}.{line}"
    else:
        return f"{area} {prefix} {line}"


def gen_dob_legacy(age_min=22, age_max=65):
    """Generate DOB in various inconsistent legacy formats."""
    age = random.randint(age_min, age_max)
    base = datetime(2024, 6, 1) - timedelta(days=age * 365 + random.randint(0, 364))
    fmt = random.choice(['iso', 'us', 'text', 'us_short'])
    if fmt == 'iso':
        return base.strftime('%Y-%m-%d')
    elif fmt == 'us':
        return base.strftime('%m/%d/%Y')
    elif fmt == 'text':
        return base.strftime('%B %d, %Y')
    else:
        return base.strftime('%m/%d/%y')


def gen_address():
    num = random.randint(100, 9999)
    street = random.choice(STREETS)
    city, state = random.choice(CITIES)
    zipcode = f"{random.randint(10000, 99999)}"
    return f"{num} {street}", city, state, zipcode


def gen_bank_account():
    routing = f"{random.randint(100000000, 999999999)}"
    account = f"{random.randint(1000000000, 9999999999)}"
    return routing, account


def gen_weekly_benefit():
    return round(random.uniform(150.00, 823.00), 2)


# ── Main Setup ──

def main():
    print("=" * 70)
    print("STATE DEPARTMENT OF LABOR")
    print("Unemployment Insurance System - Database Setup")
    print("=" * 70)
    print()
    print("Scenario: Migrating legacy mainframe UI system to modern cloud platform")
    print()

    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host='localhost', port=5432,
            user='postgres', password='postgres', database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        print("Connected to PostgreSQL")
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        print("\nEnsure PostgreSQL is running: brew services start postgresql@15")
        sys.exit(1)

    cursor = conn.cursor()

    # Drop and recreate databases
    for db in ['legacy_db', 'modern_db']:
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
        if cursor.fetchone():
            # Terminate existing connections
            cursor.execute(f"""
                SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
            """, (db,))
            cursor.execute(f"DROP DATABASE {db}")
        cursor.execute(f"CREATE DATABASE {db}")
        print(f"  Created {db}")

    cursor.close()
    conn.close()

    # ── Create Legacy Schema ──
    print("\nCreating legacy database schema...")
    legacy_conn = psycopg2.connect(
        host='localhost', port=5432,
        user='postgres', password='postgres', database='legacy_db'
    )
    cur = legacy_conn.cursor()
    cur.execute("""
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
    """)
    legacy_conn.commit()

    # ── Generate Legacy Data ──
    print("Generating legacy demo data with intentional issues...")

    # Generate employers
    employers = []
    for i, (name, industry) in enumerate(EMPLOYERS, start=1):
        addr, city, state, zipcode = gen_address()
        phone = gen_phone_legacy()
        ein = f"{random.randint(10,99)}-{random.randint(1000000,9999999)}"
        employers.append((i, name, ein, industry, addr, city, state, zipcode, phone, 'active'))

    cur.executemany("""
        INSERT INTO employers VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, employers)

    # Generate claimants (200 rows with issues)
    claimants = []
    used_ssns = []

    for i in range(1, 201):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        ssn = gen_ssn()
        dob = gen_dob_legacy()
        phone = gen_phone_legacy()
        email = f"{first.lower()}.{last.lower()}{random.randint(1,99)}@email.com"
        addr, city, state, zipcode = gen_address()
        routing, account = gen_bank_account()
        status = random.choice(['ACTIVE', 'active', 'Active', 'ACT', 'INACTIVE', 'CLOSED'])
        reg_dt = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))
        deceased = 'N'

        # ── Plant intentional issues ──

        # Issue 1: Duplicate SSNs (5 claimants reuse an earlier SSN)
        if i in [45, 89, 133, 167, 195]:
            ssn = used_ssns[random.randint(0, min(len(used_ssns)-1, 30))]

        # Issue 2: NULL emails (15 claimants)
        if i in [12, 27, 38, 51, 66, 78, 94, 108, 121, 139, 144, 156, 170, 183, 197]:
            email = None

        # Issue 3: NULL DOBs (8 claimants)
        if i in [19, 55, 77, 99, 115, 148, 172, 190]:
            dob = None

        # Issue 4: Whitespace issues in names (10 claimants)
        if i in [8, 33, 58, 82, 107, 132, 155, 179, 16, 42]:
            first = f"  {first}  "
            last = f" {last} "

        # Issue 5: Status inconsistencies (already mixed in choices above)

        # Issue 6: Deceased claimant still active (2 cases)
        if i in [88, 160]:
            deceased = 'Y'
            status = 'ACTIVE'  # Should not be active if deceased

        used_ssns.append(ssn)
        claimants.append((
            i, first, last, ssn, dob, phone, email, addr, city, state, zipcode,
            account, routing, status, reg_dt.strftime('%Y-%m-%d %H:%M:%S'), deceased, ''
        ))

    cur.executemany("""
        INSERT INTO claimants VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, claimants)

    # Generate claims (300 rows with issues)
    claims = []
    for i in range(1, 301):
        clmt_id = random.randint(1, 200)
        empl_id = random.randint(1, len(EMPLOYERS))
        reason = random.choice(SEPARATION_REASONS)
        filing_dt = (datetime(2020, 3, 1) + timedelta(days=random.randint(0, 1400))).strftime('%Y-%m-%d')
        year_start = filing_dt
        year_end = (datetime.strptime(filing_dt, '%Y-%m-%d') + timedelta(days=365)).strftime('%Y-%m-%d')
        weekly = gen_weekly_benefit()
        max_bnf = round(weekly * 26, 2)
        weeks = random.randint(0, 26)
        total_paid = round(weekly * weeks, 2)
        status = random.choice(CLAIM_STATUSES_LEGACY)
        last_updated = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 400))

        # Issue 7: Orphan claims (3 claims reference non-existent claimants)
        if i in [250, 275, 298]:
            clmt_id = 9000 + i  # Non-existent claimant

        # Issue 8: Future filing dates (2 claims)
        if i in [180, 260]:
            filing_dt = '2027-06-15'
            year_start = filing_dt
            year_end = '2028-06-15'

        # Issue 9: Overpayments (3 claims where total_paid > max_benefit)
        if i in [50, 150, 220]:
            total_paid = round(max_bnf * 1.3, 2)  # 30% over max
            weeks = 34  # More than 26 weeks

        claims.append((
            i, clmt_id, empl_id, reason, filing_dt, year_start, year_end,
            weekly, max_bnf, total_paid, weeks, status, last_updated.strftime('%Y-%m-%d %H:%M:%S')
        ))

    cur.executemany("""
        INSERT INTO claims VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, claims)

    # Generate benefit payments (500 rows with issues)
    payments = []
    for i in range(1, 501):
        claim_id = random.randint(1, 300)
        pymt_dt = (datetime(2020, 4, 1) + timedelta(days=random.randint(0, 1500))).strftime('%Y-%m-%d')
        # Find the claim's weekly amount (approximate)
        claim_idx = min(claim_id - 1, len(claims) - 1)
        weekly_amt = claims[claim_idx][7]  # bnf_wkly_amt
        pymt_amt = round(weekly_amt + random.uniform(-5, 5), 2)
        method = random.choice(PAYMENT_METHODS)
        week_ending = (datetime.strptime(pymt_dt, '%Y-%m-%d') + timedelta(days=random.randint(0, 6))).strftime('%Y-%m-%d')
        check_num = f"CHK{random.randint(100000, 999999)}" if method == 'check' else None

        # Issue 10: Orphan payments (2 payments reference non-existent claims)
        if i in [333, 444]:
            claim_id = 8000 + i

        # Issue 11: Negative payment amounts (2 cases - data entry errors)
        if i in [200, 400]:
            pymt_amt = round(-random.uniform(100, 500), 2)

        # Issue 12: Overpayments exceeding state max ($823/week)
        if i in [100, 250, 375]:
            pymt_amt = round(random.uniform(900, 1500), 2)

        payments.append((
            i, claim_id, pymt_dt, pymt_amt, method, week_ending, 'processed', check_num
        ))

    cur.executemany("""
        INSERT INTO benefit_payments VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, payments)

    legacy_conn.commit()

    # Get counts
    for table in ['claimants', 'employers', 'claims', 'benefit_payments']:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"  {table}: {count} rows")

    cur.close()
    legacy_conn.close()

    # ── Create Modern Schema ──
    print("\nCreating modern database schema...")
    modern_conn = psycopg2.connect(
        host='localhost', port=5432,
        user='postgres', password='postgres', database='modern_db'
    )
    cur = modern_conn.cursor()
    cur.execute("""
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
            is_deceased         BOOLEAN DEFAULT FALSE
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
    """)
    modern_conn.commit()

    # ── Generate Modern Data (migrated with some cleanup) ──
    print("Generating modern (migrated) data...")

    # Migrate employers (clean migration, all records)
    modern_employers = []
    for emp in employers:
        eid, name, ein, industry, addr, city, state, zipcode, phone, status = emp
        # Clean phone to bigint
        phone_clean = int(''.join(c for c in phone if c.isdigit())[:10] or '0')
        modern_employers.append((
            eid, name, ein, industry, addr, city, state, zipcode, phone_clean, status.lower()
        ))

    cur.executemany("""
        INSERT INTO employers VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, modern_employers)

    # Migrate claimants (195 of 200 - 5 duplicates removed)
    migrated_ssns = set()
    modern_claimants = []
    skipped = 0

    for clmt in claimants:
        cid, first, last, ssn, dob, phone, email, addr, city, state, zipcode, \
            account, routing, status, reg_dt, deceased, _ = clmt

        # Skip duplicate SSNs (keep first occurrence)
        if ssn in migrated_ssns:
            skipped += 1
            continue
        migrated_ssns.add(ssn)

        # Clean data during migration
        first_clean = first.strip()
        last_clean = last.strip()

        # Hash SSN (security improvement)
        import hashlib
        ssn_hash = hashlib.sha256(ssn.encode()).hexdigest()[:16] if ssn else None

        # Parse DOB to proper date
        dob_clean = None
        if dob:
            for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%B %d, %Y', '%m/%d/%y']:
                try:
                    dob_clean = datetime.strptime(dob, fmt).date()
                    break
                except ValueError:
                    continue

        # Clean phone to bigint
        phone_clean = int(''.join(c for c in (phone or '').replace(' ', '') if c.isdigit())[:10] or '0')

        # Normalize status
        status_map = {'ACTIVE': 'active', 'Active': 'active', 'ACT': 'active',
                      'active': 'active', 'INACTIVE': 'inactive', 'CLOSED': 'closed'}
        status_clean = status_map.get(status, status.lower())

        modern_claimants.append((
            cid, first_clean, last_clean, ssn_hash, dob_clean, phone_clean,
            email, addr, city, state, zipcode, status_clean, reg_dt, deceased
        ))

    cur.executemany("""
        INSERT INTO claimants VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, modern_claimants)

    print(f"  Claimants: {len(modern_claimants)} migrated ({skipped} duplicates removed)")

    # Migrate claims (skip orphans that reference non-existent claimants)
    modern_claimant_ids = {c[0] for c in modern_claimants}
    modern_claims = []
    claims_skipped = 0

    for claim in claims:
        cid, clmt_id, empl_id, reason, filing_dt, yr_start, yr_end, \
            weekly, max_bnf, total_paid, weeks, status, last_updated = claim

        # Skip orphan claims
        if clmt_id not in modern_claimant_ids:
            claims_skipped += 1
            continue

        # Parse dates
        try:
            filing_date = datetime.strptime(filing_dt, '%Y-%m-%d').date()
            yr_start_date = datetime.strptime(yr_start, '%Y-%m-%d').date()
            yr_end_date = datetime.strptime(yr_end, '%Y-%m-%d').date()
        except ValueError:
            filing_date = datetime.now().date()
            yr_start_date = filing_date
            yr_end_date = filing_date + timedelta(days=365)

        # Normalize status
        status_clean = status.lower()
        if status_clean == 'act':
            status_clean = 'active'

        modern_claims.append((
            cid, clmt_id, empl_id, reason, filing_date, yr_start_date, yr_end_date,
            weekly, max_bnf, total_paid, weeks, status_clean, last_updated
        ))

    cur.executemany("""
        INSERT INTO claims VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, modern_claims)

    print(f"  Claims: {len(modern_claims)} migrated ({claims_skipped} orphans skipped)")

    # Migrate benefit payments (skip orphans)
    modern_claim_ids = {c[0] for c in modern_claims}
    modern_payments = []
    payments_skipped = 0

    for pmt in payments:
        pid, claim_id, pymt_dt, pymt_amt, method, week_ending, pstatus, check_num = pmt

        # Skip orphan payments
        if claim_id not in modern_claim_ids:
            payments_skipped += 1
            continue

        try:
            payment_date = datetime.strptime(pymt_dt, '%Y-%m-%d').date()
            week_ending_date = datetime.strptime(week_ending, '%Y-%m-%d').date()
        except ValueError:
            payment_date = datetime.now().date()
            week_ending_date = payment_date

        modern_payments.append((
            pid, claim_id, payment_date, pymt_amt, method, week_ending_date, pstatus, check_num
        ))

    cur.executemany("""
        INSERT INTO benefit_payments VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, modern_payments)

    print(f"  Benefit payments: {len(modern_payments)} migrated ({payments_skipped} orphans skipped)")

    # Migrate employers count
    print(f"  Employers: {len(modern_employers)} migrated")

    modern_conn.commit()
    cur.close()
    modern_conn.close()

    # ── Print Summary ──
    print()
    print("=" * 70)
    print("DATABASE SETUP COMPLETE")
    print("=" * 70)
    print()
    print("Legacy Database (legacy_db) - 15-year-old mainframe system:")
    print(f"  claimants:        200 rows (with 15 data quality issues)")
    print(f"  employers:        {len(EMPLOYERS)} rows")
    print(f"  claims:           300 rows (with orphans, future dates, overpayments)")
    print(f"  benefit_payments: 500 rows (with orphans, negatives, over-max)")
    print()
    print("Modern Database (modern_db) - Cloud platform (post-migration):")
    print(f"  claimants:        {len(modern_claimants)} rows (duplicates removed)")
    print(f"  employers:        {len(modern_employers)} rows")
    print(f"  claims:           {len(modern_claims)} rows (orphans dropped)")
    print(f"  benefit_payments: {len(modern_payments)} rows (orphans dropped)")
    print()
    print("Planted Issues for Validation Demo:")
    print("  1. Duplicate SSNs (5 claimants with reused SSN)")
    print("  2. NULL emails (15 claimants missing email)")
    print("  3. NULL dates of birth (8 claimants)")
    print("  4. SSN in plaintext (PII violation - all legacy records)")
    print("  5. Bank account numbers in legacy (PII - routing + account)")
    print("  6. Mixed date formats in DOB (ISO, US, text, 2-digit year)")
    print("  7. Inconsistent phone formats (dashes, parens, dots, plain)")
    print("  8. Status inconsistencies (ACTIVE, active, Active, ACT)")
    print("  9. Orphan claims (3 referencing non-existent claimants)")
    print(" 10. Orphan payments (2 referencing non-existent claims)")
    print(" 11. Overpayments exceeding max weekly benefit ($823)")
    print(" 12. Future filing dates (2 claims dated 2027)")
    print(" 13. Negative payment amounts (2 data entry errors)")
    print(" 14. Whitespace in names ('  John  ' instead of 'John')")
    print(" 15. Deceased claimants with active status (2 cases)")
    print()
    print("Row count mismatches (legacy vs modern):")
    print(f"  claimants:        200 vs {len(modern_claimants)} ({200 - len(modern_claimants)} removed)")
    print(f"  claims:           300 vs {len(modern_claims)} ({300 - len(modern_claims)} orphans dropped)")
    print(f"  benefit_payments: 500 vs {len(modern_payments)} ({500 - len(modern_payments)} orphans dropped)")
    print()
    print("Next steps:")
    print("  1. Generate metadata: python3 main.py --generate-metadata --no-interactive")
    print("  2. Run validation:    python3 main.py --phase pre --dataset claimants")
    print()


if __name__ == '__main__':
    main()
