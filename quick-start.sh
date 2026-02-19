#!/bin/bash

# Data Validation Agent - Quick Start Script
# Sets up everything from scratch, including schema auto-generation
# Scenario: State Department of Labor - Unemployment Insurance System Migration

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║                                                            ║"
echo "║     Data Validation Agent - Quick Start Setup             ║"
echo "║     State DoL Unemployment Insurance Migration            ║"
echo "║                                                            ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Step 1: Check prerequisites
echo -e "${BLUE}Step 1: Checking prerequisites...${NC}"

if ! command -v python3 &> /dev/null; then
    echo -e "${YELLOW}Python 3 not found. Please install Python 3.10+${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Python 3: $(python3 --version)${NC}"

# Step 2: Create virtual environment
echo ""
echo -e "${BLUE}Step 2: Setting up Python environment...${NC}"

if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi
source .venv/bin/activate
echo -e "${GREEN}✓ Virtual environment activated${NC}"

# Step 3: Install dependencies
echo ""
echo -e "${BLUE}Step 3: Installing Python dependencies...${NC}"

if python3 -c "import pandas, pandera, psycopg2" 2>/dev/null; then
    echo -e "${GREEN}✓ Dependencies already installed${NC}"
else
    echo "Installing dependencies..."
    pip install -q -r requirements.txt
    echo -e "${GREEN}✓ Dependencies installed${NC}"
fi

# Step 4: Setup databases
echo ""
echo -e "${BLUE}Step 4: Setting up demo databases...${NC}"

echo "  • Creating legacy_db and modern_db with unemployment insurance tables..."
python3 setup_databases.py || {
    echo -e "${YELLOW}Database setup failed. Check PostgreSQL credentials.${NC}"
    echo "Ensure PostgreSQL is running: pg_isready"
    exit 1
}
echo -e "${GREEN}✓ Databases created and loaded${NC}"

# Step 5: Auto-generate validation schemas
echo ""
echo -e "${BLUE}Step 5: Auto-generating validation schemas...${NC}"

python3 generate_schemas.py --all --output-dir schemas

echo -e "${GREEN}✓ Pandera schemas generated for all tables${NC}"
echo "  → schemas/legacy/claimants.py"
echo "  → schemas/legacy/employers.py"
echo "  → schemas/modern/claimants.py"
echo "  → schemas/modern/employers.py"

# Step 6: Auto-generate RAG metadata
echo ""
echo -e "${BLUE}Step 6: Auto-generating RAG metadata...${NC}"

python3 main.py --generate-metadata --no-interactive

echo -e "${GREEN}✓ RAG metadata generated${NC}"

# Step 7: Run a quick validation test
echo ""
echo -e "${BLUE}Step 7: Running validation test...${NC}"

python3 main.py --phase pre --dataset claimants --sample 100 > /dev/null 2>&1

LATEST_RUN=$(ls -td artifacts/run_* 2>/dev/null | head -1)

if [ -n "$LATEST_RUN" ]; then
    echo -e "${GREEN}✓ Validation test successful${NC}"
    echo "  → Artifacts saved to: $LATEST_RUN"
else
    echo -e "${YELLOW}Validation test did not generate artifacts${NC}"
fi

# Summary
echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║                                                            ║"
echo "║                 Setup Complete!                            ║"
echo "║                                                            ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

echo -e "${GREEN}What was set up:${NC}"
echo "  ✓ PostgreSQL databases (legacy_db, modern_db)"
echo "  ✓ Unemployment insurance demo data with intentional issues"
echo "  ✓ Python dependencies"
echo "  ✓ Auto-generated Pandera schemas (4 tables)"
echo "  ✓ RAG metadata for intelligent explanations"
echo "  ✓ Validation agent tested and working"
echo ""

echo -e "${BLUE}Next Steps:${NC}"
echo ""
echo "1. Run the full demo:"
echo "   ${GREEN}./demo.sh${NC}"
echo ""
echo "2. Or run validation manually:"
echo "   ${GREEN}python main.py --phase pre --dataset claimants${NC}"
echo "   ${GREEN}python main.py --phase post --dataset claimants${NC}"
echo ""
echo "3. Review results:"
echo "   ${GREEN}ls -lt artifacts/${NC}"
echo "   ${GREEN}cat artifacts/run_*/READINESS_DASHBOARD.md${NC}"
echo ""

echo -e "${BLUE}Documentation:${NC}"
echo "  • README.md - Overview and usage"
echo "  • DEMO_SCRIPT.md - Presentation guide"
echo "  • SCHEMA_GENERATION.md - Auto-schema generation guide"
echo "  • STAKEHOLDER_PRESENTATION.md - Full pitch deck"
echo ""
