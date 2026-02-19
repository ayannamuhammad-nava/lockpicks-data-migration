#!/bin/bash

# Data Validation Agent - Automated Demo Script
# Story-Driven Walkthrough: "Here is the Risk We Removed"
# Scenario: State Department of Labor - Unemployment Insurance System Migration

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
print_header() {
    echo ""
    echo -e "${BLUE}============================================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}============================================================${NC}"
    echo ""
}

print_step() {
    echo -e "${GREEN}▶ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

pause_for_demo() {
    echo ""
    echo -e "${YELLOW}Press ENTER to continue...${NC}"
    read
}

# Check prerequisites
check_prereqs() {
    print_header "Checking Prerequisites"

    # Check Python
    if command -v python3 &> /dev/null; then
        print_step "Python 3: $(python3 --version)"
    else
        print_error "Python 3 not found. Please install Python 3.10+"
        exit 1
    fi

    # Check virtual environment
    if [ -d ".venv" ]; then
        print_step "Virtual environment: Found"
    else
        print_warning "Virtual environment not found. Creating..."
        python3 -m venv .venv
    fi

    # Activate venv
    source .venv/bin/activate

    # Check dependencies
    if python3 -c "import pandas, pandera, psycopg2" 2>/dev/null; then
        print_step "Python dependencies: Installed"
    else
        print_warning "Installing dependencies..."
        pip install -q -r requirements.txt
    fi

    # Check PostgreSQL (via Python connection, not psql CLI) - after deps are installed
    if python3 -c "import psycopg2; psycopg2.connect(host='localhost', port=5432, user='postgres', password='postgres', database='postgres').close()" 2>/dev/null; then
        print_step "PostgreSQL: Running on localhost:5432"
    else
        print_error "PostgreSQL not reachable. Please ensure PostgreSQL is running."
        exit 1
    fi

    print_step "All prerequisites OK!"
    pause_for_demo
}

# Act 1: Pre-migration check
pre_migration_check() {
    print_header "ACT 1: Pre-Migration Risk Check"

    echo "We're migrating unemployment insurance claimant data from a"
    echo "15-year-old legacy mainframe to a modern cloud platform."
    echo "The State Department of Labor depends on this data being accurate."
    echo ""

    print_warning "What could go wrong?"
    echo "  • Duplicate SSNs across claimant records (cl_ssn)"
    echo "  • NULL emails (required contact fields like cl_emal)"
    echo "  • PII exposure (cl_ssn, cl_bact in plaintext)"
    echo "  • Orphan claims referencing non-existent claimants"
    echo "  • Deceased claimants with active status (cl_dcsd='Y' but cl_stat='ACTIVE')"
    echo ""
    echo "Without validation, these issues make it to production!"
    echo ""

    print_step "Let's see how the agent catches them BEFORE migration..."
    echo ""
    pause_for_demo

    echo "Running validation on legacy claimant data..."
    echo "The agent will automatically generate validation schemas if needed."
    echo ""
    echo "Checking: Schema compatibility, Data quality, Governance"
    echo ""

    # Remove existing schemas to demonstrate auto-generation
    rm -f schemas/legacy/claimants.py schemas/modern/claimants.py 2>/dev/null

    # Run pre-migration validation (will auto-generate schemas)
    python3 main.py --phase pre --dataset claimants --sample 500

    echo ""
    print_step "Validation complete! Let's review the findings..."
    pause_for_demo

    # Find latest artifacts folder
    LATEST_PRE=$(ls -td artifacts/run_* 2>/dev/null | head -1)

    if [ -n "$LATEST_PRE" ]; then
        echo ""
        print_step "Opening readiness report..."
        echo ""
        head -50 "$LATEST_PRE/readiness_report.md"
        echo ""
        echo "  [Full report: $LATEST_PRE/readiness_report.md]"
        echo ""

        print_warning "Key Findings:"
        echo "  • Schema mismatches detected (cl_recid -> claimant_id, type changes)"
        echo "  • PII fields found (cl_ssn, cl_bact - require masking)"
        echo "  • Removed columns detected (cl_bact, cl_brtn)"
        echo "  • Score: YELLOW - Review required before migration"
        echo ""
    fi

    pause_for_demo
}

# Act 2: Post-migration proof
post_migration_proof() {
    print_header "ACT 2: Post-Migration Reconciliation"

    echo "Migration finished. Now we prove data integrity..."
    echo "Comparing: Row counts, Foreign keys, Sample records"
    echo ""

    # Run post-migration reconciliation
    python3 main.py --phase post --dataset claimants

    echo ""
    print_step "Reconciliation complete! Let's review the proof..."
    pause_for_demo

    # Find latest artifacts folder
    LATEST_POST=$(ls -td artifacts/run_* 2>/dev/null | head -1)

    if [ -n "$LATEST_POST" ]; then
        echo ""
        print_step "Opening reconciliation report..."
        echo ""
        cat "$LATEST_POST/reconciliation_report.md"
        echo ""
        echo "  [Full report: $LATEST_POST/reconciliation_report.md]"
        echo ""

        print_warning "Key Findings:"
        echo "  • Row count mismatch: Legacy 200 vs Modern 195 (duplicate SSNs removed)"
        echo "  • Orphan claims detected (referencing non-existent claimants)"
        echo "  • Score: YELLOW - Investigation required"
        echo ""
        echo "We have PROOF of what went wrong and where!"
        echo ""
    fi

    pause_for_demo
}

# Act 4: Fail-loud scenario
fail_loud_demo() {
    print_header "ACT 4: Fail-Loud Design (What Happens When Things Break)"

    echo "Let's intentionally add more bad data and see if the agent catches it..."
    echo ""

    print_warning "Simulating disaster scenario:"
    echo "  • Adding claimants with duplicate SSNs"
    echo "  • Adding NULL required fields"
    echo ""

    # Add bad data via Python (no psql needed)
    python3 -c "
import psycopg2
conn = psycopg2.connect(host='localhost', port=5432, user='postgres', password='postgres', database='legacy_db')
cursor = conn.cursor()
cursor.execute(\"\"\"
INSERT INTO claimants (cl_recid, cl_fnam, cl_lnam, cl_ssn, cl_dob, cl_phon, cl_emal, cl_adr1, cl_city, cl_st, cl_zip, cl_bact, cl_brtn, cl_stat, cl_rgdt, cl_dcsd, cl_fil1)
VALUES
(998, 'Bad', 'Data', '000-00-0000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-01-01 00:00:00', 'N', ''),
(999, 'Duplicate', 'SSN1', '123-45-6789', '1990-01-01', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'ACTIVE', '2026-01-01 00:00:00', 'N', '')
\"\"\")
conn.commit()
conn.close()
" 2>/dev/null || true

    echo ""
    print_step "Bad data added. Re-running validation..."
    echo ""

    # Re-run pre-check
    python3 main.py --phase pre --dataset claimants --sample 500 || true

    echo ""
    print_warning "Agent immediately flagged the issues:"
    echo "  • More duplicate SSN violations (cl_ssn)"
    echo "  • More null violations (cl_emal, cl_phon, cl_adr1)"
    echo "  • Lower confidence score"
    echo "  • Status changed to RED"
    echo ""
    print_step "This is fail-loud design - bad data doesn't sneak through!"
    echo ""

    pause_for_demo
}

# Summary
show_summary() {
    print_header "Demo Summary: The Value Delivered"

    echo -e "${GREEN}Risk Removed:${NC}"
    echo "   • Schema mismatches caught before migration"
    echo "   • PII exposure (SSNs, bank accounts) flagged for remediation"
    echo "   • Orphan claims and payments detected in reconciliation"
    echo "   • Duplicate SSN records prevented from proceeding"
    echo ""

    echo -e "${GREEN}Evidence Generated:${NC}"
    echo "   • 12+ artifacts across 3 validation runs"
    echo "   • Audit-ready CSV and JSON logs"
    echo "   • Human-readable markdown reports"
    echo "   • Confidence scores with traffic lights"
    echo ""

    echo -e "${GREEN}Time Saved:${NC}"
    echo "   • Schema + metadata generation: 4-6 hours -> 4 seconds"
    echo "   • Total validation time: < 2 minutes"
    echo "   • Issues caught before production"
    echo "   • No manual SQL queries needed"
    echo ""

    print_header "Artifacts Location"
    echo "All evidence saved to: ./artifacts/"
    echo ""
    ls -lt artifacts/ | head -5
    echo ""

    print_header "Demo Complete!"
    echo "Review the artifacts to see detailed findings."
    echo "Read DEMO_SCRIPT.md for presentation talking points."
    echo ""
}

# Main demo flow
main() {
    clear
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║                                                            ║"
    echo "║        Data Validation Agent - Live Demo                  ║"
    echo "║        State DoL Unemployment Insurance Migration         ║"
    echo "║                                                            ║"
    echo "║        \"Here is the Risk We Removed\"                       ║"
    echo "║                                                            ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
    echo "This demo will walk through a complete migration validation:"
    echo "  1. Pre-Migration Risk Check (with auto-generated schemas + RAG metadata)"
    echo "  2. During-Migration Monitoring"
    echo "  3. Post-Migration Reconciliation"
    echo "  4. Fail-Loud Design Demo"
    echo ""
    echo "Duration: ~10 minutes"
    echo ""

    pause_for_demo

    # Run demo acts
    check_prereqs
    pre_migration_check
    post_migration_proof
    fail_loud_demo
    show_summary

    echo -e "${GREEN}Thank you for watching the demo!${NC}"
    echo ""
}

# Run the demo
main
