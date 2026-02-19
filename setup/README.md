# Database Setup Instructions

This directory contains SQL scripts to set up demo databases for the Data Validation Agent.

## Overview

The setup creates two PostgreSQL databases simulating a **State Department of Labor Unemployment Insurance System** migration:
- `legacy_db` - Legacy mainframe-style schema with intentional data quality issues
- `modern_db` - Modern cloud platform schema with migrated (but not perfectly clean) data

## Tables

| Table | Legacy Rows | Modern Rows | Description |
|-------|------------|-------------|-------------|
| claimants | 10 | 9 | Unemployed individuals filing for benefits |
| employers | 5 | 5 | Employers where claimants were separated from |
| claims | 15 | 12 | Individual unemployment claims |
| benefit_payments | 20 | 14 | Weekly benefit payments processed |

## Intentional Issues for Testing

The demo data includes these intentional issues to showcase the agent's detection capabilities:

### Legacy Database Issues:
1. **Duplicate SSNs**: cl_recid 1 and 5 share the same SSN
2. **Null required fields**: cl_recid 3 and 8 have NULL emails
3. **PII columns**: `cl_ssn`, `cl_bact`, `cl_brtn` contain plaintext sensitive data
4. **Mixed date formats**: `cl_dob` uses ISO, US, text, and 2-digit year formats
5. **Inconsistent phone formats**: dashes, parentheses, dots, spaces, plain digits
6. **Status inconsistencies**: ACTIVE, active, Active, ACT
7. **Whitespace in names**: cl_recid 9 has leading/trailing spaces
8. **Deceased with active status**: cl_recid 10 is deceased but cl_stat is ACTIVE
9. **Orphan claims**: claim_id 14, 15 reference non-existent claimants (9990, 9991)
10. **Future filing dates**: claim_id 13 dated 2027
11. **Overpayment**: claim_id 6 total_paid exceeds max_bnf_amt
12. **Orphan payments**: bp_recid 19, 20 reference non-existent claims (8888, 9999)
13. **Negative payment amount**: bp_recid 18

### Schema Differences (Legacy -> Modern):
1. **Column renames**: `cl_recid` -> `claimant_id`, `cl_fnam` -> `first_name`, `cl_ssn` -> `ssn_hash`, etc.
2. **Type changes**: `cl_phon` VARCHAR -> `phone_number` BIGINT, `cl_dob` VARCHAR -> `date_of_birth` DATE
3. **Removed columns**: `cl_bact`, `cl_brtn` removed for security
4. **Added constraints**: PRIMARY KEYs, FOREIGN KEYs, NOT NULL constraints in modern schema

### Modern Database Issues (Post-Migration):
1. **Row count mismatch**: Duplicate SSN claimant not migrated (10 -> 9)
2. **Orphan claims dropped**: 2 orphan claims removed during migration (15 -> 12)
3. **NULL emails persist**: Not fixed during migration
4. **Overpayment persists**: Not corrected during migration
5. **Future filing date persists**: Not caught during migration

## Prerequisites

- PostgreSQL 12+ installed and running
- Python 3.10+ with `psycopg2-binary` installed
- Sufficient privileges to create databases

## Setup Steps

### Quick Setup (Recommended)

```bash
# Navigate to project root
cd /path/to/lockpick-data-migration-agent

# Install dependencies
pip install -r requirements.txt

# Run the automated setup script
python3 setup_databases.py
```

**What it does:**
- Creates `legacy_db` and `modern_db` databases
- Creates table schemas (claimants, employers, claims, benefit_payments)
- Loads legacy data with intentional issues (200 claimants, 44 employers, 300 claims, 500 payments)
- Loads modern data with migrated records (195 claimants, 44 employers, ~297 claims, ~498 payments)
- Handles all database operations via Python (no `psql` CLI needed)

### Auto-Generate RAG Metadata

After database setup, generate metadata for intelligent explanations:

```bash
python3 main.py --generate-metadata --no-interactive
```

### Alternative: Manual SQL Setup

If you prefer using `psql` directly (smaller dataset for quick testing):

```bash
psql -U postgres -f setup/create_databases.sql
psql -U postgres -d legacy_db -f setup/load_legacy_data.sql
psql -U postgres -d modern_db -f setup/load_modern_data.sql
```

## Database Connection Details

Update `config.yaml` if your PostgreSQL setup differs:

```yaml
database:
  legacy:
    host: ${DB_LEGACY_HOST:localhost}
    port: 5432
    database: ${DB_LEGACY_NAME:legacy_db}
    user: ${DB_LEGACY_USER:postgres}
    password: ${DB_LEGACY_PASSWORD:postgres}
  modern:
    host: ${DB_MODERN_HOST:localhost}
    port: 5432
    database: ${DB_MODERN_NAME:modern_db}
    user: ${DB_MODERN_USER:postgres}
    password: ${DB_MODERN_PASSWORD:postgres}
```

Values use `${VAR:default}` syntax — defaults work out of the box for local development, and can be overridden via environment variables for deployment.

## Cleanup

To remove the demo databases, re-run the setup script (it drops and recreates) or use Python:

```bash
python3 -c "
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
conn = psycopg2.connect(host='localhost', port=5432, user='postgres', password='postgres', database='postgres')
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()
cursor.execute('DROP DATABASE IF EXISTS legacy_db')
cursor.execute('DROP DATABASE IF EXISTS modern_db')
print('Databases dropped.')
"
```

## Troubleshooting

### Permission Denied
```bash
sudo -u postgres psql -c "ALTER USER your_username WITH SUPERUSER;"
```

### Connection Refused
```bash
pg_isready
brew services start postgresql@15
```

## Next Steps

After database setup:

```bash
# Auto-generate RAG metadata (one time)
python3 main.py --generate-metadata --no-interactive

# Pre-migration check
python3 main.py --phase pre --dataset claimants

# Post-migration proof
python3 main.py --phase post --dataset claimants
```
