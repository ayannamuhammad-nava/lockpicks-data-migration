# Data Validation Agent - Architecture

**Simple. Modular. Trust-Building.**

---

## One-Slide Architecture

```mermaid
graph TB
    subgraph "Data Validation Agent"
        subgraph Phases
            BEFORE[BEFORE<br/>Check]
            AFTER[AFTER<br/>Prove]
        end
        subgraph Capabilities
            SCHEMA[Schema Rules]
            METADATA[Metadata KB]
            QUALITY[Data Quality]
            GOV[Governance]
            EVIDENCE[Evidence]
            ARTIFACTS[Artifacts]
        end
    end
    BEFORE -.-> AFTER
```

---

## Core Components

### 1. **Validation Phases** (What Users See)

```mermaid
graph LR
    PRE["PRE-CHECK<br/>• Schema<br/>• Governance<br/>• Readiness"]
    POST["PROOF<br/>• Row Counts<br/>• Checksums<br/>• Integrity"]

    PRE --> POST
    PRE --> RISK[Risk Report]
    POST --> PROOF_RPT[Proof Report]

    style PRE fill:#e1f5ff
    style POST fill:#e8f5e9
```

### 2. **Validation Engine** (What Happens Inside)

```mermaid
flowchart TD
    INPUT[Input: Legacy & Modern Data]
    INPUT --> SCHEMA[1. Schema Validation]
    INPUT --> QUALITY[2. Data Quality Checks]
    INPUT --> GOV[3. Governance Rules]
    INPUT --> INTEGRITY[4. Integrity Proofs]

    SCHEMA --> |Compare structures| OUTPUT
    QUALITY --> |Detect nulls, duplicates| OUTPUT
    GOV --> |PII, standards| OUTPUT
    INTEGRITY --> |Reconciliation| OUTPUT

    OUTPUT[Output: Confidence Score + Evidence]

    style INPUT fill:#e1f5ff
    style OUTPUT fill:#e8f5e9
```

### 3. **Intelligent Reasoning** (Auto-Generated Knowledge Base)

```mermaid
flowchart TD
    DB["Database Schemas<br/>Legacy & Modern"]
    DB --> AUTOGEN["Auto-Generate Metadata<br/>tools/metadata_generator.py"]
    AUTOGEN --> METADATA["Metadata Store<br/>• Column Glossary (with confidence)<br/>• Mappings (with rationales)<br/>• PII Detection"]
    METADATA --> RAG[RAG Explainer]
    RAG --> ANSWER["Why did cl_fnam → first_name?<br/>→ Renamed from legacy abbreviation for clarity (80% confidence)"]

    style DB fill:#f5f5f5
    style AUTOGEN fill:#e1f5ff
    style METADATA fill:#fff4e1
    style RAG fill:#e1f5ff
    style ANSWER fill:#e8f5e9
```

Metadata is **automatically generated** from database schemas - no manual JSON curation needed.
See [RAG_METADATA_ANALYSIS.md](RAG_METADATA_ANALYSIS.md) for details.

### 4. **Evidence Generation** (Audit Trail)

```mermaid
flowchart TD
    RUN[Every Validation Run]
    RUN --> FOLDER[Timestamped Folder]

    FOLDER --> MD[Markdown Reports] --> |Human-readable| COMPLIANCE
    FOLDER --> CSV[CSV Exports] --> |Spreadsheet-friendly| COMPLIANCE
    FOLDER --> JSON[JSON Logs] --> |Machine-readable| COMPLIANCE
    FOLDER --> SCORE[Confidence Scores] --> |Executive dashboard| COMPLIANCE

    COMPLIANCE[Compliance-Ready Artifacts]

    style RUN fill:#e1f5ff
    style FOLDER fill:#fff4e1
    style COMPLIANCE fill:#e8f5e9
```

---

## Data Flow

### Pre-Migration Check

```mermaid
flowchart LR
    LEGACY[Legacy DB] --> SAMPLE[Sample]
    SAMPLE --> SCHEMA[Schema Validator] --> DIFF[Diff Report]
    SAMPLE --> GOV[Governance Engine] --> PII[PII/Standards Check]
    SAMPLE --> RAG[RAG Tool] --> EXPLAIN[Explanations]

    DIFF --> SCORE[Structure Score<br/>0-100]
    PII --> SCORE
    EXPLAIN --> SCORE

    style LEGACY fill:#ffebee
    style SCORE fill:#e8f5e9
```

### Post-Migration Proof

```mermaid
flowchart LR
    LEGACY[Legacy DB] --> RECON[Reconciliation Engine]
    MODERN[Modern DB] --> RECON

    RECON --> ROWS[Row Counts]
    RECON --> CHECKS[Checksums]
    RECON --> FK[FK Integrity]

    ROWS --> SCORE[Integrity Score<br/>0-100]
    CHECKS --> SCORE
    FK --> SCORE

    style LEGACY fill:#ffebee
    style MODERN fill:#e1f5ff
    style SCORE fill:#e8f5e9
```

---

## Confidence Scoring Formula

```mermaid
graph TD
    STRUCTURE[Structure 40%<br/>Schema compatibility]
    INTEGRITY[Integrity 40%<br/>Data accuracy]
    GOV[Governance 20%<br/>Compliance]

    STRUCTURE --> SCORE[Final Score]
    INTEGRITY --> SCORE
    GOV --> SCORE

    SCORE --> GREEN{90-100}
    SCORE --> YELLOW{70-89}
    SCORE --> RED{0-69}

    GREEN --> |🟢 GREEN| ACTION1[Safe to proceed]
    YELLOW --> |🟡 YELLOW| ACTION2[Review recommended]
    RED --> |🔴 RED| ACTION3[Fix issues first]

    style GREEN fill:#e8f5e9
    style YELLOW fill:#fff9c4
    style RED fill:#ffebee
```

---

## Key Design Principles

### 1. **Fail-Loud, Not Silent**
```mermaid
flowchart LR
    BAD[Bad Data] --> LOW[Low Score] --> EVIDENCE[Clear Evidence] --> REVIEW[Forced Review]
    style BAD fill:#ffebee
    style REVIEW fill:#fff4e1
```
No false confidence. Issues are surfaced immediately.

### 2. **Explainable, Not Black Box**
```mermaid
flowchart LR
    ISSUE[Issue Detected] --> RAG[RAG Explanation] --> HUMAN[Human Understanding]
    style ISSUE fill:#ffebee
    style HUMAN fill:#e8f5e9
```
Every finding includes "why" and "what to do."

### 3. **Evidence-First**
```mermaid
flowchart LR
    RUN[Every Run] --> ARTIFACTS[Artifacts] --> AUDIT[Audit Trail] --> COMPLIANCE[Compliance]
    style RUN fill:#e1f5ff
    style COMPLIANCE fill:#e8f5e9
```
Trust through transparency.

### 4. **Modular & Extensible**
```mermaid
flowchart LR
    CORE[Core Engine] --> PLUG[Pluggable Validators] --> CUSTOM[Custom Rules]
    style CORE fill:#e1f5ff
    style CUSTOM fill:#e8f5e9
```
Easy to adapt for different databases and rules.

---

## Technology Mapping (For Technical Audiences)

| Stakeholder Term | Technical Implementation |
|-----------------|-------------------------|
| Schema Validation Rules | Pandera DataFrameSchemas |
| Metadata Knowledge Base | Auto-generated JSON + sentence-transformers embeddings |
| Data Quality Engine | pandas + custom validation logic |
| Governance Checks | Regex patterns + keyword matching |
| Reconciliation Engine | SQL queries + hash comparisons |
| Evidence Artifacts | Markdown + CSV + JSON generators |

---

## Integration Points

### Input
- Legacy database connection (PostgreSQL, MySQL, etc.)
- Modern database connection
- Configuration file (YAML)
- Validation schemas (auto-generated via Pandera)
- RAG metadata (auto-generated from database schemas)

### Output
- Markdown reports (human-readable)
- CSV exports (spreadsheet-compatible)
- JSON logs (API-compatible)
- Confidence scores (dashboards)

### Extension Points
- Custom validation rules (add to governance.py)
- Additional data sources (extend db_utils.py)
- Custom report formats (add to reporter.py)
- New scoring weights (modify config.yaml)

---

## Deployment Options

### Option 1: CLI (Current)
```bash
python main.py --phase pre --dataset claimants
```
**Use Case**: Manual validation, ad-hoc checks

### Option 2: CI/CD Pipeline
```yaml
# .github/workflows/migration-check.yml
- name: Validate Migration
  run: |
    python main.py --phase pre --dataset claimants
    if [ $? -ne 0 ]; then exit 1; fi
```
**Use Case**: Automated pre-deployment checks

### Option 3: API Wrapper (Future)
```python
# api.py
@app.post("/validate/{phase}")
def validate(phase: str, dataset: str):
    result = run_agent(phase, dataset)
    return result
```
**Use Case**: Integration with dashboards

---

## Performance Characteristics

- **Pre-Check**: ~30 seconds (500-1000 sample size)
- **Post-Check**: ~45 seconds
- **Total E2E**: < 2 minutes

**Scalability**: Sampling-based approach means performance is independent of total data size.

---

## Security Considerations

1. **Read-Only Operations**: Agent only reads data, never writes
2. **PII Detection**: Flags sensitive data for masking
3. **Audit Trail**: Complete log of all checks performed
4. **Credential Management**: Database credentials via config with `${VAR:default}` environment variable support

---

## Future Enhancements

### Real-Time Drift Monitoring (TODO)

The current agent validates **before** and **after** migration. A future enhancement would add **during-migration** monitoring for live drift detection. Realistic production approaches include:

- **CDC-based**: Hook into Debezium/Kafka Connect change streams to monitor rows as they are written to the modern system. Compare each batch against baseline quality metrics (null rates, duplicate rates, value distributions).
- **Polling-based**: Periodic row-count and checksum snapshots on a cron schedule (e.g., every 5 minutes). Detect sudden spikes in nulls, duplicates, or row-count divergence.
- **ETL-integrated**: Callbacks from Airflow/dbt after each batch completes. The validation agent runs a quick quality check on the latest batch and reports drift scores.
- **Alerting**: Slack/PagerDuty webhooks when drift score drops below a configurable threshold, enabling immediate human intervention.

### Other Enhancements

1. **Web Dashboard**: Visual interface for results
2. **Real-Time Alerts**: Slack/email notifications
3. **Historical Trends**: Track confidence scores over time
4. **ML-Based Anomaly Detection**: Learn normal patterns
5. **Multi-Database Support**: Oracle, SQL Server, Snowflake

---

**Architecture Philosophy:**
Keep the core simple. Make it explainable. Trust through evidence.
