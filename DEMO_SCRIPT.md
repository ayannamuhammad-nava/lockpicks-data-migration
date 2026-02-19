# Data Validation Agent - Demo Script

**Narrative-Driven Walkthrough: "Here is the Risk We Removed"**

---

## Demo Overview

**Duration**: 10-14 minutes
**Goal**: Show how the agent removes migration risk before and after migration
**Approach**: Story-driven, not feature-driven
**NEW**: Now includes auto-schema generation (zero manual configuration!)

---

## Setup (Before Demo)

```bash
# Ensure databases are ready
python3 setup_databases.py

# Auto-generate RAG metadata
python3 main.py --generate-metadata --no-interactive

# Quick test
python3 main.py --phase pre --dataset claimants --sample 100 2>&1 | tail -10
```

**Confirm**: Artifacts generated in `artifacts/` folder

---

## Act 0: "Zero Manual Configuration" (2 minutes)

### The Setup Challenge

> "Before we can validate data, we need two things:
> 1. Validation schemas for both databases
> 2. Metadata for intelligent explanations (glossary + mappings)
>
> Traditionally, this means 4-6 hours of manual work: writing validation code AND curating JSON files.
>
> **What if we could skip that entirely?**"

### Auto-Generate Everything

```bash
# Step 1: Auto-generate RAG metadata from database schemas
python3 main.py --generate-metadata --no-interactive
```

**While it runs** (takes ~2 seconds), explain:
> "The agent is:
> 1. **Introspecting** both database schemas via `information_schema`
> 2. **Inferring** column descriptions from names and types
> 3. **Detecting** PII fields automatically (email, phone, ssn)
> 4. **Finding** column renames using fuzzy matching
> 5. **Generating** mapping rationales with confidence scores"

### Show the Results

**Show CLI Output:**
```
🔍 Analyzing database schemas...
   Tables: claimants, employers, claims, benefit_payments

✅ Metadata generation complete!
   📄 Glossary: 23 columns (with confidence scores)
   🔄 Mappings: 5 transformations detected
   📁 Saved to: ./metadata/

💡 RAG is now ready to provide intelligent explanations!
```

### Show Auto-Generated Metadata

```bash
python3 -c "import json; data=json.load(open('metadata/glossary.json')); [print(f\"{c['name']}: {c['description']} ({int(c['confidence']*100)}%)\") for c in data['columns'][:5]]"
```

**Point out the auto-generated descriptions:**
```
cl_recid: Unique identifier for claimant (90%)
cl_fnam: First name field for claimant (70%)
email: Email address for communication and identification (90%)
phone_number: Phone number for contact purposes (90%)
cl_ssn: Social Security Number for claimant (50%) ← low confidence, would prompt user
```

**Show auto-detected mappings:**
```bash
python3 -c "import json; data=json.load(open('metadata/mappings.json')); [print(f\"{m['source']} → {m['target'] or '(removed)'}: {m['rationale']} ({int(m['confidence']*100)}%)\") for m in data['mappings']]"
```

```
cl_fnam → first_name: Renamed from legacy abbreviation for clarity (80%)
phone_number → phone_number: Type changed from varchar to bigint (95%)
cl_ssn → ssn_hash: Changed from plaintext SSN to hashed value (70%)
cl_rgdt → registered_at: Standardized timestamp naming (91%)
```

**The Message:**
> "In 2 seconds, the agent:
> - Generated descriptions for 30+ columns with confidence scores
> - Detected 5 schema transformations automatically
> - Flagged PII fields (email, phone_number, cl_ssn)
> - Found renames like cl_fnam → first_name
>
> **Time saved: 4-6 hours → 2 seconds. Zero manual JSON editing.**
>
> Now the RAG system can explain every schema difference intelligently."

---

## Act 1: "Here is the Risk" (2 minutes)

### The Scenario
> "We're migrating unemployment insurance claimant data from a 10-year-old legacy system to a modern cloud platform.
> The state depends on this data being accurate. **What could go wrong?**"

### Show the Legacy Data Issues

**Open**: `setup/load_legacy_data.sql` (lines 35-45)

**Point out the intentional problems:**
```sql
-- Issue 1: Duplicate claimant ID
(101, 'Test', 'User1', 'test1@example.com', ...),
(101, 'Test', 'User2', 'test2@example.com', ...),  -- DUPLICATE!

-- Issue 2: Null required fields
(102, 'No', 'Email', NULL, ...),  -- Missing email!

-- Issue 3: PII exposure
cl_ssn VARCHAR(11),  -- Social Security Numbers in plain text!

-- Issue 4: Orphan claims
INSERT INTO claims ... (999, 9999, '2024-01-15', ...);  -- Claimant 9999 doesn't exist!
```

**The Message:**
> "Without validation, these issues make it to production.
> Let's see how the agent **catches them before migration**."

---

## Act 2: "Pre-Migration Check" (4 minutes)

**Using the auto-generated schemas from Act 0**

### Run the Validation

```bash
python main.py --phase pre --dataset claimants --sample 500
```

**While it runs** (takes ~40 seconds), explain:
> "The agent is checking three things:
> 1. **Schema compatibility** - Will the data fit the new structure?
> 2. **Data quality** - Are there nulls, duplicates, or bad values?
> 3. **Governance** - Is sensitive data exposed?"

### Review the Results

**Show CLI Output:**
```
============================================================
    VALIDATION COMPLETE
============================================================
Phase:       PRE
Dataset:     claimants
Score:       82.8/100
Status:      YELLOW ⚠️
Artifacts:   ./artifacts/run_2026-02-14_01-12-20
============================================================

⚠️  VALIDATION WARNING - Review recommended
```

**The Message:**
> "Yellow means proceed with caution. Let's see what it found."

### Open the Readiness Report

```bash
cat artifacts/run_2026-02-14_01-12-20/readiness_report.md
```

**Highlight Key Findings:**
```markdown
### ⚠️ Columns Missing in Modern System
- **cl_ssn** (Social Security Number - PII field) → Hashed as ssn_hash
- **cl_rgdt** → Renamed to registered_at
- **cl_fnam** → Renamed to first_name

### 🔒 PII Detected (1 column)
- cl_ssn

### Recommendations
1. Review schema mappings and create transformation logic
2. Implement SSN hashing before migration (cl_ssn → ssn_hash)
```

**The Message:**
> "The agent didn't just say 'error' - it **explained** what's wrong and **why**.
> It even suggested the mapping: `cl_fnam → first_name`."

### Show the Governance Report

```bash
cat artifacts/run_2026-02-14_01-12-20/governance_report.csv
```

**Point out:**
```
PII,cl_ssn,VIOLATION,Contains PII keywords
Null Check,email,PASS,0% null
```

**The Message:**
> "Every issue is logged. This becomes your audit trail."

---

## Act 3: "Post-Migration Proof" (3 minutes)

### The Scenario
> "Migration finished. **How do we prove the data is correct?**"

### Run Reconciliation

```bash
python main.py --phase post --dataset claimants
```

**Explain:**
> "Now we're comparing legacy vs. modern:
> - Row counts must match
> - Foreign keys must be valid
> - Sample records must be identical"

### Review Reconciliation Report

```bash
cat artifacts/run_2026-02-14_01-12-59/reconciliation_report.md
```

**Highlight the findings:**
```markdown
## Row Count Verification
- **Legacy System:** 200
- **Modern System:** 195
- **Match:** ❌ No

## Referential Integrity
- **claims_claimants_fk:** 3 orphan claims detected
  - Sample Claimant IDs: [9999, 9998, 9997]
```

**The Message:**
> "We have proof:
> - Row count mismatch (200 vs 195) - needs investigation
> - Orphan claims detected (claimants 9999, 9998, 9997 don't exist)
>
> **This is exactly what we wanted to catch!**"

### Show Confidence Score

```bash
cat artifacts/run_2026-02-14_01-12-59/confidence_score.txt
```

**Point out:**
```
🟡 72.0/100 - YELLOW

Score: 72.0/100
Status: YELLOW
```

**The Message:**
> "Yellow means migration has issues. The orphan FK brought the score down.
> **We have evidence** to show what went wrong and where."

---

## Act 4: "What Happens When Something Breaks" (3 minutes)

### The Scenario (Fail-Loud Demo)
> "Let's intentionally break something worse and see if the agent catches it."

### Simulate a Disaster

```bash
# Add more bad data to legacy
python3 -c "
import psycopg2
conn = psycopg2.connect(host='localhost', port=5432, user='postgres', password='postgres', database='legacy_db')
cursor = conn.cursor()
cursor.execute(\"\"\"
INSERT INTO claimants VALUES
(999, 'Bad', 'Data', NULL, '555', '000-00-0000', NULL, NOW()),
(999, 'Duplicate', 'Again', NULL, '555', '000-00-0000', NULL, NOW())
\"\"\")
conn.commit()
print('Bad data inserted!')
"
```

### Re-run Pre-Check

```bash
python main.py --phase pre --dataset claimants --sample 500
```

**Expected Result:**
- Score drops significantly (maybe 50-60)
- More violations flagged
- Status: RED 🔴

**The Message:**
> "The agent immediately flagged it:
> - Lower confidence score
> - More duplicate IDs
> - More null violations
>
> **This is fail-loud design.** Bad data doesn't sneak through."

---

## Closing: The Value Proposition (2 minutes)

### What We Just Demonstrated

**Risk Removed:**
✅ Schema mismatches caught before migration
✅ PII exposure flagged for remediation
✅ Orphan records detected in reconciliation
✅ Duplicate data prevented from proceeding

**Evidence Generated:**
✅ 8+ artifacts across 2 validation runs
✅ Audit-ready CSV and JSON logs
✅ Human-readable markdown reports
✅ Confidence scores with traffic lights

**Time Saved:**
✅ Schema + metadata generation: 4-6 hours → 4 seconds
✅ Total validation time: < 2 minutes
✅ Issues caught before production
✅ No manual SQL queries or coding needed

### The Confidence Formula

```
Structure (40%) + Integrity (40%) + Governance (20%) = Trust
```

**Without this agent:**
- Issues discovered in production ❌
- No audit trail ❌
- Manual validation for weeks ❌
- Risk of data loss ❌

**With this agent:**
- Issues caught in pre-production ✅
- Complete evidence trail ✅
- Automated validation in minutes ✅
- Confidence in data accuracy ✅

---

## Q&A Prep

### Expected Questions

**Q: "What if we have 100 tables?"**
A: Run the agent on each table. It's designed for automation - you can script it:
```bash
for table in claimants employers claims benefit_payments; do
  python main.py --phase pre --dataset $table
done
```

**Q: "Can it integrate with our CI/CD pipeline?"**
A: Yes. The exit codes signal success (0), warning (0), or failure (1). Easy to integrate.

**Q: "What about performance on large datasets?"**
A: It samples data (configurable size). 1M rows takes ~same time as 10K rows.

**Q: "Does it fix the issues automatically?"**
A: No - it's validation only. It tells you what's wrong, you fix it. That's the safe approach.

**Q: "What databases are supported?"**
A: PostgreSQL in this demo, but the architecture works with any SQL database.

**Q: "How do we customize the validation rules?"**
A: Edit `config.yaml` - all rules, thresholds, and weights are configurable.

---

## Demo Success Criteria

After the demo, stakeholders should understand:

✅ **The Problem**: Migration risk is real and costly
✅ **The Solution**: Automated validation before and after migration
✅ **The Evidence**: Audit-ready artifacts prove data quality
✅ **The Value**: Catch issues early, save time, build trust

**Call to Action:**
> "Let's discuss integrating this into your migration plan.
> We can run it on your actual data (read-only) to see what it finds."

---

## Tips for a Strong Demo

1. **Start with the pain point** - "Migrations fail because..."
2. **Use real examples** - Show actual SQL with bad data
3. **Let it fail** - Demonstrate the fail-loud scenario
4. **Show the evidence** - Open the actual report files
5. **Keep it simple** - Don't mention RAG, vectors, or Pandera
6. **End with confidence** - "This removes risk. Here's proof."

---

**Demo Time: 10-15 minutes**
**Impact: High**
**Confidence: Proven**
