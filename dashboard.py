"""
Lockpicks Data Migration — Streamlit Dashboard

Reads artifacts produced by DM and provides:
  - Confidence score gauge with traffic-light status
  - Schema diff with RAG-powered field mapping table
  - Governance / PII findings table (color-coded)
  - Reconciliation report for post-migration runs
  - RAG chat interface: ask any question about schema fields or mappings

Usage:
  streamlit run dashboard.py -- --project projects/loops-nj
"""
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ── Resolve project directory ────────────────────────────────────────────────
# Accept --project <path> via CLI args (after the Streamlit `--` separator)
_argv = sys.argv[1:]
_project_dir = Path(".")
for i, arg in enumerate(_argv):
    if arg == "--project" and i + 1 < len(_argv):
        _project_dir = Path(_argv[i + 1])
        break

# Also accept STREAMLIT_PROJECT env var as fallback
if _project_dir == Path(".") and os.environ.get("DM_PROJECT"):
    _project_dir = Path(os.environ["DM_PROJECT"])

PROJECT_DIR = _project_dir.resolve()

# ── Constants ─────────────────────────────────────────────────────────────────
ARTIFACTS_DIR = PROJECT_DIR / "artifacts"
METADATA_DIR = PROJECT_DIR / "metadata"

# Load project name from project.yaml
_project_yaml = PROJECT_DIR / "project.yaml"
if _project_yaml.exists():
    import yaml as _yaml
    _proj_config = _yaml.safe_load(_project_yaml.read_text()) or {}
    PROJECT_NAME = _proj_config.get("project", {}).get("name", PROJECT_DIR.name)
else:
    PROJECT_NAME = PROJECT_DIR.name

# Data Migration Lifecycle phases
LIFECYCLE_PHASES = [
    ("Discovery", "discover"),
    ("Modeling", "generate-schema"),
    ("Governance", "validate --phase pre"),
    ("Transformation", "convert"),
    ("Compliance", "validate --phase pre"),
    ("Quality", "validate --phase post"),
]

STATUS_EMOJI = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴"}
STATUS_COLOR = {"GREEN": "#2e7d32", "YELLOW": "#f57f17", "RED": "#c62828"}
STATUS_BG    = {"GREEN": "#e8f5e9", "YELLOW": "#fff9c4", "RED": "#ffebee"}

MAPPING_TYPE_ICON = {
    "rename":    "→",
    "transform": "⚙️",
    "removed":   "🗑️",
    "archived":  "🔒",
}

RAG_SUGGESTIONS = [
    "What is cl_bact?",
    "Why did cl_ssn become ssn_hash?",
    "What is the risk with cl_dcsd?",
    "Why was cl_brtn not migrated?",
    "What does cm_mxamt mean?",
    "Why was cl_stat renamed?",
]

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Data-Migration Tool",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .status-badge {
        display: inline-block;
        padding: 4px 14px;
        border-radius: 20px;
        font-weight: 700;
        font-size: 0.95rem;
        letter-spacing: 0.05em;
    }
    .metric-card {
        background: #f8f9fa;
        border-radius: 10px;
        padding: 16px 20px;
        text-align: center;
    }
    .metric-label { font-size: 0.8rem; color: #666; text-transform: uppercase; letter-spacing: 0.08em; }
    .metric-value { font-size: 2rem; font-weight: 700; }
    .section-header { font-size: 1.1rem; font-weight: 600; margin-bottom: 6px; }
    div[data-testid="stChatMessage"] { border-radius: 12px; }
    .pii-alert {
        background: #ffebee;
        border-left: 4px solid #c62828;
        border-radius: 4px;
        padding: 10px 16px;
        margin: 8px 0;
    }
</style>
""", unsafe_allow_html=True)


# ── Helper functions ───────────────────────────────────────────────────────────

def get_runs() -> list[str]:
    if not ARTIFACTS_DIR.exists():
        return []
    runs = [
        d.name for d in ARTIFACTS_DIR.iterdir()
        if d.is_dir() and d.name.startswith("run_")
    ]
    return sorted(runs, reverse=True)


def load_json(run_name: str, filename: str) -> dict | None:
    path = ARTIFACTS_DIR / run_name / filename
    if path.exists():
        return json.loads(path.read_text())
    return None


def load_text(run_name: str, filename: str) -> str | None:
    path = ARTIFACTS_DIR / run_name / filename
    return path.read_text() if path.exists() else None


def load_csv(run_name: str, filename: str) -> pd.DataFrame | None:
    path = ARTIFACTS_DIR / run_name / filename
    return pd.read_csv(path) if path.exists() else None


def load_mappings(table: str | None = None) -> list[dict]:
    path = METADATA_DIR / "mappings.json"
    if not path.exists():
        return []
    data = json.loads(path.read_text())
    mappings = data.get("mappings", [])
    if table:
        mappings = [m for m in mappings if m.get("table") == table]
    return mappings


def build_confidence_gauge(score: float, status: str) -> go.Figure:
    color = STATUS_COLOR.get(status, "#999")
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        domain={"x": [0, 1], "y": [0, 1]},
        title={"text": "Migration Confidence", "font": {"size": 16}},
        number={"font": {"size": 44, "color": color}, "suffix": "/100"},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1, "tickcolor": "#ccc"},
            "bar": {"color": color, "thickness": 0.25},
            "bgcolor": "white",
            "borderwidth": 0,
            "steps": [
                {"range": [0, 70],  "color": "#ffebee"},
                {"range": [70, 90], "color": "#fff9c4"},
                {"range": [90, 100],"color": "#e8f5e9"},
            ],
        },
    ))
    fig.update_layout(
        height=260,
        margin=dict(l=20, r=20, t=50, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def parse_schema_diff(md_text: str) -> tuple[list[dict], list[dict]]:
    missing, added = [], []
    section = None
    for line in md_text.splitlines():
        if "Missing in Modern" in line:
            section = "missing"
        elif "New Columns in Modern" in line:
            section = "added"
        elif line.startswith("- **") and section:
            name = line.split("**")[1]
            col_type = line.split("(")[-1].rstrip(")") if "(" in line else ""
            entry = {"Column": name, "Type": col_type}
            (missing if section == "missing" else added).append(entry)
    return missing, added


def color_gov_status(val: str) -> str:
    if val == "VIOLATION":
        return "background-color: #ffebee; color: #c62828; font-weight: bold"
    if val == "PASS":
        return "background-color: #e8f5e9; color: #2e7d32"
    if val == "WARNING":
        return "background-color: #fff9c4; color: #e65100"
    return ""


@st.cache_resource(show_spinner=False)
def get_rag_tool_cached(glossary_mtime: float):
    """Load RAG tool, keyed on glossary mtime so cache busts when metadata changes."""
    from dm.kb.rag import RAGTool
    # Delete stale embeddings cache so build_embeddings always regenerates from current glossary
    cache_file = METADATA_DIR / ".embeddings_cache.npz"
    if cache_file.exists():
        cache_file.unlink()
    rag = RAGTool(metadata_dir=str(METADATA_DIR))
    rag.load_metadata()
    rag.build_embeddings()
    return rag


def _get_rag() -> object:
    """Return the RAG tool, busting cache if glossary.json has changed."""
    glossary_path = METADATA_DIR / "glossary.json"
    mtime = glossary_path.stat().st_mtime if glossary_path.exists() else 0.0
    return get_rag_tool_cached(mtime)


def answer_rag_query(query: str) -> str:
    """Route query to explain_column or explain_mapping and return formatted answer."""
    try:
        rag = _get_rag()
    except Exception as e:
        return f"⚠️ Could not load RAG knowledge base: {e}"

    # Step 1 — Explicit mapping query: "why did X become Y", "X → Y", "X -> Y"
    mapping_match = re.search(
        r"([\w_]+)\s*(?:→|->|become|became)\s*([\w_]+)",
        query, re.IGNORECASE
    )
    if mapping_match:
        src, tgt = mapping_match.group(1), mapping_match.group(2)
        rationale = rag.explain_mapping(src, tgt)
        return f"**Mapping: `{src}` → `{tgt}`**\n\n{rationale}"

    # Step 2 — "not migrated / removed / archived" pattern: extract column and return mapping rationale
    not_migrated_match = re.search(
        r"(?:why\s+(?:was|is|did)|reason\s+(?:for|why))\s+([\w_]+)\s+(?:not\s+migrated|removed|archived|not\s+in\s+modern|missing)",
        query, re.IGNORECASE
    )
    if not_migrated_match:
        col_name = not_migrated_match.group(1)
        mappings = load_mappings()
        for m in mappings:
            if m.get("source", "").lower() == col_name.lower():
                tgt_str = f"`{m['target']}`" if m.get("target") else "*(not migrated)*"
                explanation = rag.explain_column(col_name)
                return (
                    f"**`{col_name}`** — {m.get('type', 'removed').upper()}\n\n"
                    f"{explanation}\n\n"
                    f"---\n**Migration mapping:** `{m['source']}` → {tgt_str}  "
                    f"*(type: `{m['type']}`, confidence: {int(m['confidence'] * 100)}%)*"
                    f"\n\n**Rationale:** {m['rationale']}"
                )
        # Column exists in glossary but has no mapping entry — still explain it
        explanation = rag.explain_column(col_name)
        return f"**`{col_name}`**\n\n{explanation}\n\n*No migration mapping found for this field.*"

    # Step 3 — Detect legacy column pattern anywhere in the query: e.g. cl_bact, cm_mxamt
    col_pattern_match = re.search(r"\b([a-z]{2}_[a-z0-9_]+)\b", query)
    if col_pattern_match:
        col_name = col_pattern_match.group(1)
        explanation = rag.explain_column(col_name)
        response = f"**`{col_name}`**\n\n{explanation}"
        mappings = load_mappings()
        for m in mappings:
            if m.get("source", "").lower() == col_name.lower():
                tgt_str = f"`{m['target']}`" if m.get("target") else "*(not migrated)*"
                response += (
                    f"\n\n---\n**Migration mapping:** `{m['source']}` → {tgt_str}  "
                    f"*(type: `{m['type']}`, confidence: {int(m['confidence'] * 100)}%)*"
                    f"\n\n**Rationale:** {m['rationale']}"
                )
                break
        return response

    # Step 4 — Keyword fallback with stop-word skip ("what is the X" → captures X, not "the")
    col_match = re.search(
        r"(?:what\s+is|explain|tell\s+me\s+about|describe|what\s+does|risk\s+(?:with|of)|meaning\s+of)\s+(?:the\s+|a\s+|an\s+)?([\w_]+)",
        query, re.IGNORECASE
    )
    col_name = col_match.group(1) if col_match else query.strip().split()[-1].rstrip("?")

    explanation = rag.explain_column(col_name)
    response = f"**`{col_name}`**\n\n{explanation}"

    # Attach mapping info if it exists
    mappings = load_mappings()
    for m in mappings:
        if m.get("source", "").lower() == col_name.lower():
            tgt_str = f"`{m['target']}`" if m.get("target") else "*(not migrated)*"
            response += (
                f"\n\n---\n**Migration mapping:** `{m['source']}` → {tgt_str}  "
                f"*(type: `{m['type']}`, confidence: {int(m['confidence'] * 100)}%)*"
                f"\n\n**Rationale:** {m['rationale']}"
            )
            break

    return response


def get_lifecycle_status() -> dict:
    """Compute overall lifecycle status from the most recent date's run scores."""
    runs = get_runs()
    phase_map = {"pre": set(), "post": set(), "prove": set()}

    # Group runs by date and collect scores
    runs_by_date = {}
    for run_name in runs:
        meta = load_json(run_name, "run_metadata.json") or {}
        phase = meta.get("phase", "")
        dataset = meta.get("dataset", "")
        if phase in phase_map:
            phase_map[phase].add(dataset)

        # Extract date from run name: run_YYYY-MM-DD_HH-MM-SS -> YYYY-MM-DD
        run_date = run_name.replace("run_", "")[:10]
        if run_date not in runs_by_date:
            runs_by_date[run_date] = []
        s = meta.get("confidence_score")
        if s is not None and float(s) > 0:
            runs_by_date[run_date].append(float(s))

    # Use the most recent date only
    latest_date = ""
    latest_scores = []
    if runs_by_date:
        latest_date = sorted(runs_by_date.keys(), reverse=True)[0]
        latest_scores = runs_by_date[latest_date]

    avg = round(sum(latest_scores) / len(latest_scores), 1) if latest_scores else 0
    if avg >= 90:
        status, color, bg = "GREEN", "#2e7d32", "#e8f5e9"
    elif avg >= 70:
        status, color, bg = "YELLOW", "#f57f17", "#fff9c4"
    else:
        status, color, bg = "RED", "#c62828", "#ffebee"

    # Determine current lifecycle phase based on what's been completed
    current_phase = 0  # Discovery
    if METADATA_DIR.exists() and (METADATA_DIR / "glossary.json").exists():
        current_phase = 1  # Modeling
    if (ARTIFACTS_DIR / "generated_schema").exists():
        current_phase = 2  # Governance
    if (ARTIFACTS_DIR / "converted").exists():
        current_phase = 3  # Transformation
    if phase_map["pre"]:
        current_phase = 4  # Compliance
    if phase_map["post"]:
        current_phase = 5  # Quality

    return {
        "avg_score": avg,
        "avg_date": latest_date,
        "status": status,
        "color": color,
        "bg": bg,
        "run_count": len(runs),
        "score_count": len(latest_scores),
        "current_phase": current_phase,
    }


def render_lifecycle_bar(lifecycle: dict, phase: str = "", dataset: str = ""):
    """Render the Data Migration Lifecycle status bar at the top of the page."""
    avg = lifecycle["avg_score"]
    avg_date = lifecycle.get("avg_date", "")
    color = lifecycle["color"]
    bg = lifecycle["bg"]
    lc_status = lifecycle["status"]
    current = lifecycle["current_phase"]
    emoji = STATUS_EMOJI.get(lc_status, "⚪")

    # Context line — project name only
    context = f"<strong>{PROJECT_NAME}</strong>"

    st.markdown(f"""
    <div style="background:{bg};border-left:5px solid {color};border-radius:8px;padding:12px 20px;margin-bottom:8px">
        <div style="display:flex;justify-content:space-between;align-items:center">
            <div style="font-size:1.1rem;font-weight:700;color:{color}">{emoji} {context}</div>
            <div style="text-align:right">
                <div style="font-size:0.9rem;color:{color};font-weight:600">Avg Score: {avg}/100 &middot; {lc_status}</div>
                <div style="font-size:0.7rem;color:#888">{avg_date}</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Phase buttons — clickable
    cols = st.columns([0.12] + [1] * len(LIFECYCLE_PHASES))
    with cols[0]:
        st.markdown("<div style='padding-top:6px;font-size:0.7rem;color:#666;font-weight:600'>LIFECYCLE:</div>", unsafe_allow_html=True)

    for i, (label, _cmd) in enumerate(LIFECYCLE_PHASES):
        with cols[i + 1]:
            if i < current:
                btn_type = "primary"
                lbl = f"✓ {label}"
            elif i == current:
                btn_type = "secondary"
                lbl = f"● {label}"
            else:
                btn_type = "secondary"
                lbl = label
            if st.button(lbl, key=f"lc_phase_{i}", use_container_width=True, type=btn_type, disabled=(i > current)):
                st.session_state["lifecycle_view"] = label
                st.rerun()


def render_discovery_page():
    """Render the Discovery detail page showing tables, columns, glossary, mappings, and abbreviations."""
    st.markdown("## 🔎 Discovery — Catalog Overview")
    st.caption(f"Project: {PROJECT_NAME}")

    # Load all metadata
    glossary_path = METADATA_DIR / "glossary.json"
    mappings_path = METADATA_DIR / "mappings.json"
    abbrev_path = METADATA_DIR / "abbreviations.yaml"
    scope_path = METADATA_DIR / "migration_scope.yaml"
    rationalization_path = METADATA_DIR / "rationalization_report.json"

    if not glossary_path.exists():
        st.warning("No discovery data found. Run `dm discover --enrich` first.")
        return

    glossary = json.loads(glossary_path.read_text())
    columns = glossary.get("columns", [])
    mappings_data = json.loads(mappings_path.read_text()) if mappings_path.exists() else {"mappings": []}
    mappings = mappings_data.get("mappings", [])

    # Tables summary
    tables = sorted(set(c.get("table", "") for c in columns if c.get("system") == "legacy"))
    legacy_cols = [c for c in columns if c.get("system") == "legacy"]
    modern_cols = [c for c in columns if c.get("system") == "modern"]
    pii_cols = [c for c in legacy_cols if c.get("pii")]

    st.markdown("### Summary")
    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("Legacy Tables", len(tables))
    mc2.metric("Legacy Columns", len(legacy_cols))
    mc3.metric("Modern Columns", len(modern_cols))
    mc4.metric("PII Fields", len(pii_cols))

    st.divider()

    # Tabs for different views
    tab_tables, tab_sample, tab_glossary, tab_mappings, tab_pii, tab_abbrev, tab_scope = st.tabs([
        "📋 Tables & Columns",
        "🗂️ Sample Data",
        "📖 Glossary",
        "🔄 Field Mappings",
        "🔒 PII Detection",
        "🔤 Abbreviations",
        "📊 Rationalization",
    ])

    # ── Tables & Columns tab
    with tab_tables:
        for table in tables:
            table_cols = [c for c in legacy_cols if c.get("table") == table]
            st.markdown(f"#### 📁 `{table}` — {len(table_cols)} columns")

            rows = []
            for c in table_cols:
                pii_flag = "🔴 PII" if c.get("pii") else ""
                rows.append({
                    "Column": c["name"],
                    "COBOL Description": c.get("description", ""),
                    "Data Type": c.get("data_type_display", c.get("data_type", "")),
                    "Nullable": c.get("is_nullable", ""),
                    "PII": pii_flag,
                    "Confidence": c.get("confidence", 0),
                })
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True, hide_index=True)

    # ── Sample Data tab
    with tab_sample:
        st.markdown("#### Sample Data — Legacy Tables")
        st.caption("Live data from the legacy database. Showing up to 25 rows per table.")

        # Load project config for DB connection
        import yaml as _yaml_sample
        _proj_yaml = PROJECT_DIR / "project.yaml"
        _sample_config = {}
        if _proj_yaml.exists():
            _sample_config = _yaml_sample.safe_load(_proj_yaml.read_text()) or {}

        legacy_conn_cfg = _sample_config.get("connections", {}).get("legacy", {})
        if legacy_conn_cfg:
            try:
                import psycopg2

                # Resolve env var syntax ${VAR:default}
                def _resolve(val):
                    if isinstance(val, str) and val.startswith("${"):
                        import re
                        m = re.match(r'\$\{([^:}]+):?(.*)\}', val)
                        if m:
                            return os.environ.get(m.group(1), m.group(2))
                    return val

                conn = psycopg2.connect(
                    host=_resolve(legacy_conn_cfg.get("host", "localhost")),
                    port=int(_resolve(legacy_conn_cfg.get("port", 5432))),
                    database=_resolve(legacy_conn_cfg.get("database", "legacy_db")),
                    user=_resolve(legacy_conn_cfg.get("user", "postgres")),
                    password=_resolve(legacy_conn_cfg.get("password", "postgres")),
                )

                for table in tables:
                    st.markdown(f"##### 📁 `{table}`")
                    try:
                        query = f"SELECT * FROM {table} LIMIT 25"
                        df_sample = pd.read_sql(query, conn)
                        row_count_query = f"SELECT COUNT(*) FROM {table}"
                        total_rows = pd.read_sql(row_count_query, conn).iloc[0, 0]
                        st.caption(f"Showing {len(df_sample)} of {total_rows} total rows")
                        st.dataframe(df_sample, use_container_width=True, hide_index=True)
                    except Exception as e:
                        st.error(f"Could not load data for `{table}`: {e}")

                conn.close()
            except ImportError:
                st.error("psycopg2 is required for sample data. Install with: `uv pip install psycopg2-binary`")
            except Exception as e:
                st.error(f"Could not connect to legacy database: {e}")
        else:
            st.warning("No legacy connection configured in project.yaml.")

    # ── Glossary tab
    with tab_glossary:
        st.markdown("#### Business Glossary")
        st.caption("Column-level metadata with COBOL copybook descriptions, data types, and confidence scores.")
        rows = []
        for c in legacy_cols:
            rows.append({
                "Table": c.get("table", ""),
                "Column": c["name"],
                "Description": c.get("description", ""),
                "Data Type": c.get("data_type_display", c.get("data_type", "")),
                "PII": "Yes" if c.get("pii") else "No",
                "Confidence": f"{c.get('confidence', 0):.0%}",
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True, column_config={
            "Description": st.column_config.TextColumn(width="large"),
        })

    # ── Field Mappings tab
    with tab_mappings:
        st.markdown("#### Legacy → Modern Field Mappings")
        st.caption("Auto-resolved by the COBOL-aware matcher using abbreviation dictionary + copybook descriptions.")

        if mappings:
            rows = []
            for m in mappings:
                icon = MAPPING_TYPE_ICON.get(m.get("type", ""), "→")
                target = m.get("target") or "*(not migrated)*"
                rows.append({
                    "Table": m.get("table", ""),
                    "Legacy Field": m["source"],
                    " ": icon,
                    "Modern Field": target,
                    "Type": m.get("type", ""),
                    "Conf.": f"{int(m.get('confidence', 0) * 100)}%",
                    "Rationale": m.get("rationale", ""),
                })
            df = pd.DataFrame(rows)

            def highlight_mapping(row):
                if row["Type"] == "archived":
                    return ["background-color: #ffebee; color: #000000"] * len(row)
                if row["Type"] == "transform":
                    return ["background-color: #fff9c4; color: #000000"] * len(row)
                if row["Type"] == "removed":
                    return ["background-color: #f5f5f5; color: #888"] * len(row)
                return [""] * len(row)

            st.dataframe(
                df.style.apply(highlight_mapping, axis=1),
                use_container_width=True, hide_index=True,
                column_config={"Rationale": st.column_config.TextColumn(width="large")},
            )
            st.markdown(
                "🔒 **archived** — PCI/HIPAA: not migrated &nbsp;&nbsp; "
                "⚙️ **transform** — type/format change &nbsp;&nbsp; "
                "→ **rename** — column renamed &nbsp;&nbsp; "
                "🗑️ **removed** — no equivalent",
            )
        else:
            st.info("No mappings found. Run `dm enrich` first.")

    # ── PII Detection tab
    with tab_pii:
        st.markdown("#### PII / Sensitive Data Detection")
        if pii_cols:
            rows = []
            for c in pii_cols:
                tags = ", ".join(c.get("pii_tags", [])) if c.get("pii_tags") else "keyword match"
                rows.append({
                    "Table": c.get("table", ""),
                    "Column": c["name"],
                    "Description": c.get("description", ""),
                    "Detection Method": tags,
                })
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True, hide_index=True)

            st.markdown(f"""
            <div class="pii-alert">
                ⚠️ <strong>{len(pii_cols)} PII field(s) detected in legacy data.</strong><br>
                <small>These fields require hashing, masking, or archival before migration.</small>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.success("No PII fields detected.")

    # ── Abbreviations tab
    with tab_abbrev:
        st.markdown("#### COBOL Abbreviation Mappings")
        st.caption("Auto-generated from copybook descriptions. Project-specific overrides take priority over the built-in dictionary.")

        if abbrev_path.exists():
            import yaml
            abbrev_data = yaml.safe_load(abbrev_path.read_text()) or {}
            abbrevs = abbrev_data.get("abbreviations", {})
            generated_from = abbrev_data.get("_generated_from", "")

            if generated_from:
                st.caption(f"Source: {generated_from}")

            if abbrevs:
                rows = [{"COBOL Suffix": k, "Modern Name": v} for k, v in sorted(abbrevs.items())]
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            else:
                st.info("No project-specific abbreviations generated.")

            st.markdown(f"**Built-in dictionary:** {len(LIFECYCLE_PHASES)} lifecycle phases tracked. "
                        f"The built-in COBOL dictionary contains 90+ common patterns.")
        else:
            st.info("No abbreviations.yaml found. Run `dm discover --enrich` to auto-generate.")

    # ── Rationalization tab
    with tab_scope:
        st.markdown("#### Migration Scope Rationalization")

        if rationalization_path.exists():
            rat_data = json.loads(rationalization_path.read_text())
            summary = rat_data.get("summary", {})

            rc1, rc2, rc3, rc4 = st.columns(4)
            rc1.metric("Total Tables", summary.get("total_tables", 0))
            rc2.metric("Migrate", summary.get("migrate_count", 0))
            rc3.metric("Review", summary.get("review_count", 0))
            rc4.metric("Archive", summary.get("archive_count", 0))

            st.divider()

            table_details = rat_data.get("tables", [])
            if table_details:
                rows = []
                for t in table_details:
                    b = t.get("breakdown", {})
                    rows.append({
                        "Table": t.get("table", ""),
                        "Score": t.get("score", 0),
                        "Recommendation": t.get("recommendation", "").upper(),
                        "Query Activity": b.get("query_activity", 0),
                        "Downstream": b.get("downstream", 0),
                        "Freshness": b.get("freshness", 0),
                        "Completeness": b.get("completeness", 0),
                        "Tier": b.get("tier", 0),
                    })
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

            # Show rationale
            for t in table_details:
                st.markdown(f"**{t.get('table', '')}:** {t.get('rationale', '')}")
        else:
            st.info("No rationalization data. Run `dm rationalize` first.")


# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🔍 Data-Migration <span style='color:#e63946'>Validation</span> Tool", unsafe_allow_html=True)
    st.markdown("*Data quality · Governance · Proof*")
    st.divider()

    runs = get_runs()
    if runs:
        # Format label: show phase + dataset from metadata if available
        def run_label(run_name: str) -> str:
            meta = load_json(run_name, "run_metadata.json") or {}
            phase = meta.get("phase", "?").upper()
            dataset = meta.get("dataset", "?")
            score = meta.get("confidence_score", "?")
            status = meta.get("status", "")
            emoji = STATUS_EMOJI.get(status, "⚪")
            ts = run_name.replace("run_", "")
            return f"{emoji} {phase} · {dataset} · {score}/100 ({ts})"

        selected_run = st.selectbox(
            "Select Run",
            runs,
            format_func=run_label,
        )
    else:
        st.warning("No runs found. Run a validation first.")
        selected_run = None

    st.divider()

    with st.expander("▶ Run New Validation"):
        phase_sel = st.selectbox("Phase", ["pre", "post"], key="new_phase")
        dataset_sel = st.selectbox(
            "Dataset",
            ["claimants", "employers", "claims", "benefit_payments"],
            key="new_dataset",
        )
        sample_sel = st.slider("Sample size", 100, 2000, 500, 100) if phase_sel == "pre" else None

        if st.button("Run Validation", type="primary", use_container_width=True):
            cmd = [
                sys.executable, "-m", "dm.cli",
                "validate",
                "--phase", phase_sel,
                "--dataset", dataset_sel,
                "--project", str(PROJECT_DIR),
            ]
            if sample_sel:
                cmd += ["--sample", str(sample_sel)]
            with st.spinner(f"Running {phase_sel.upper()} check on '{dataset_sel}'…"):
                result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode in (0, 1):
                st.success("Done! Refresh the run selector.")
                st.cache_resource.clear()
                st.rerun()
            else:
                st.error(result.stderr[:600] or "Unknown error")

    st.divider()
    st.caption(f"Project: `{PROJECT_NAME}`")
    st.caption("CLI: `dm validate --help`")


# ── Main area ──────────────────────────────────────────────────────────────────

lifecycle = get_lifecycle_status()

# Check if a lifecycle phase was clicked
lifecycle_view = st.session_state.get("lifecycle_view", None)

if lifecycle_view:
    render_lifecycle_bar(lifecycle)
    # Back button
    if st.button("← Back to Run View", key="back_from_lifecycle"):
        del st.session_state["lifecycle_view"]
        st.rerun()

    if lifecycle_view == "Discovery":
        render_discovery_page()
    else:
        st.markdown(f"## {lifecycle_view}")
        st.info(f"Detail page for **{lifecycle_view}** phase coming soon.")
    st.stop()

if not selected_run:
    render_lifecycle_bar(lifecycle)
    st.markdown("# Data-Migration <span style='color:#e63946'>Validation</span> Tool", unsafe_allow_html=True)
    st.info("No run selected. Use the sidebar to pick an existing run or trigger a new validation.")
    st.stop()

meta = load_json(selected_run, "run_metadata.json")
if not meta:
    st.error(f"Could not load metadata for `{selected_run}`.")
    st.stop()

phase       = meta.get("phase", "unknown").upper()
dataset     = meta.get("dataset", "unknown")
score       = float(meta.get("confidence_score", 0))
status      = meta.get("status", "UNKNOWN")
struct_score = meta.get("structure_score")
gov_score    = meta.get("governance_score")
generated_at = meta.get("generated_at", "")[:16].replace("T", " ")

emoji = STATUS_EMOJI.get(status, "⚪")
color = STATUS_COLOR.get(status, "#999")
bg    = STATUS_BG.get(status, "#f5f5f5")

# ── Lifecycle Status Bar ───────────────────────────────────────────────────────
render_lifecycle_bar(lifecycle, phase=phase, dataset=dataset)

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown(
    f"## {emoji} {phase}-Migration Validation — **{dataset.title()}**"
)
st.caption(f"Run: `{selected_run}`  ·  Generated: {generated_at}")

# ── Score row ──────────────────────────────────────────────────────────────────
col_gauge, col_struct, col_gov, col_status = st.columns([2.5, 1, 1, 1])

with col_gauge:
    st.plotly_chart(build_confidence_gauge(score, status), use_container_width=True)

with col_struct:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown('<div class="metric-label">Structure</div>', unsafe_allow_html=True)
    val = f"{struct_score}/100" if struct_score is not None else "N/A"
    st.markdown(f'<div class="metric-value">{val}</div>', unsafe_allow_html=True)
    st.markdown("<div style='font-size:0.75rem;color:#888'>Schema compat · 40% weight</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

with col_gov:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown('<div class="metric-label">Governance</div>', unsafe_allow_html=True)
    val = f"{gov_score}/100" if gov_score is not None else "N/A"
    st.markdown(f'<div class="metric-value">{val}</div>', unsafe_allow_html=True)
    st.markdown("<div style='font-size:0.75rem;color:#888'>PII & compliance · 20% weight</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

with col_status:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown('<div class="metric-label">Status</div>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="metric-value" style="color:{color}">{emoji} {status}</div>',
        unsafe_allow_html=True,
    )
    threshold_hint = "Safe to proceed" if status == "GREEN" else \
                     "Review recommended" if status == "YELLOW" else "Fix issues first"
    st.markdown(f"<div style='font-size:0.75rem;color:#888'>{threshold_hint}</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

st.divider()

# ── Tabs ───────────────────────────────────────────────────────────────────────
if phase == "PRE":
    tabs = st.tabs(["📋 Readiness Report", "🔍 Schema Diff & Mappings", "🔒 Governance", "💬 Ask the Agent"])
    tab_readiness, tab_schema, tab_gov, tab_rag = tabs
else:
    tabs = st.tabs(["📋 Reconciliation Report", "💬 Ask the Agent"])
    tab_recon, tab_rag = tabs


# ── PRE: Readiness Report ──────────────────────────────────────────────────────
if phase == "PRE":
    with tab_readiness:
        report_md = load_text(selected_run, "readiness_report.md")
        if report_md:
            st.markdown(report_md)
        else:
            st.info("No readiness report found for this run.")

    # ── PRE: Schema Diff & Mappings ────────────────────────────────────────────
    with tab_schema:
        schema_md = load_text(selected_run, "schema_diff.md")
        if schema_md:
            missing_cols, added_cols = parse_schema_diff(schema_md)

            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f"#### ❌ {len(missing_cols)} Legacy columns not in modern schema")
                if missing_cols:
                    st.dataframe(
                        pd.DataFrame(missing_cols),
                        use_container_width=True,
                        hide_index=True,
                    )
            with c2:
                st.markdown(f"#### ✅ {len(added_cols)} New columns in modern schema")
                if added_cols:
                    st.dataframe(
                        pd.DataFrame(added_cols),
                        use_container_width=True,
                        hide_index=True,
                    )

            st.divider()

            # Mappings from knowledge base
            mappings = load_mappings(table=dataset)
            if mappings:
                st.markdown("#### 🔄 Field Mappings — RAG Knowledge Base")
                st.caption(
                    "Auto-generated from live database schemas. "
                    "Each rationale explains *why* the transformation exists, not just *what* it is."
                )

                # Build display rows
                rows = []
                for m in mappings:
                    icon = MAPPING_TYPE_ICON.get(m.get("type", ""), "→")
                    target = m.get("target") or "*(not migrated)*"
                    conf = f"{int(m['confidence'] * 100)}%"
                    rationale = m.get("rationale", "")
                    rows.append({
                        "Legacy Field": m["source"],
                        " ": icon,
                        "Modern Field": target,
                        "Type": m.get("type", ""),
                        "Conf.": conf,
                        "Rationale": rationale,
                    })

                df_map = pd.DataFrame(rows)

                # Highlight transform/archived rows
                def highlight_mapping_type(row):
                    if row["Type"] == "archived":
                        return ["background-color: #ffebee; color: #000000"] * len(row)
                    if row["Type"] == "transform":
                        return ["background-color: #fff9c4; color: #000000"] * len(row)
                    return [""] * len(row)

                st.dataframe(
                    df_map.style.apply(highlight_mapping_type, axis=1),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Rationale": st.column_config.TextColumn(width="large"),
                    },
                )

                # Legend
                st.markdown(
                    "🔒 **archived** — PCI/HIPAA: field not migrated, stored in encrypted vault  "
                    "&nbsp;&nbsp; ⚙️ **transform** — ETL logic required before insert  "
                    "&nbsp;&nbsp; → **rename** — column renamed, direct copy  "
                    "&nbsp;&nbsp; 🗑️ **removed** — no equivalent in modern schema",
                    unsafe_allow_html=True,
                )
            else:
                st.info("No field mappings found. Run `--generate-metadata` to build the knowledge base.")
        else:
            st.info("No schema diff found for this run.")

    # ── PRE: Governance ────────────────────────────────────────────────────────
    with tab_gov:
        gov_df = load_csv(selected_run, "governance_report.csv")
        if gov_df is not None:
            # Summary metrics
            violations = int((gov_df["status"] == "VIOLATION").sum())
            warnings   = int((gov_df["status"] == "WARNING").sum())
            passes     = int((gov_df["status"] == "PASS").sum())

            mc1, mc2, mc3, mc4 = st.columns(4)
            mc1.metric("Violations", violations)
            mc2.metric("Warnings",   warnings)
            mc3.metric("Passing",    passes)
            mc4.metric("Total Checks", len(gov_df))

            # PII callout banner
            pii_violations = gov_df[
                (gov_df["category"] == "PII") & (gov_df["status"] == "VIOLATION")
            ]
            if not pii_violations.empty:
                fields = ", ".join(f"`{f}`" for f in pii_violations["item"].tolist())
                st.markdown(
                    f'<div class="pii-alert">⚠️ <strong>PII / Financial data in plaintext:</strong> '
                    f'{fields}<br>'
                    f'<small>These fields must be hashed, masked, or archived before migration proceeds.</small></div>',
                    unsafe_allow_html=True,
                )

            st.divider()

            # Color-coded table
            styled = gov_df.style.map(color_gov_status, subset=["status"])
            st.dataframe(styled, use_container_width=True, hide_index=True)
        else:
            st.info("No governance report found for this run.")


# ── POST: Reconciliation Report ────────────────────────────────────────────────
if phase == "POST":
    with tab_recon:
        recon_md = load_text(selected_run, "reconciliation_report.md")
        if recon_md:
            # Try to surface the before/after table prominently
            # The MD already has a nice table; render it directly
            st.markdown(recon_md)
        else:
            st.info("No reconciliation report found for this run.")


# ── RAG Chat — shared for both phases ─────────────────────────────────────────
with tab_rag:
    st.markdown("#### 💬 Ask the Agent")
    st.caption(
        "Query the knowledge base for schema explanations, mapping rationales, and migration guidance. "
        "Try a suggested question or type your own."
    )

    # Suggested question pills
    st.markdown("**Suggested questions:**")
    pill_cols = st.columns(3)
    for idx, suggestion in enumerate(RAG_SUGGESTIONS):
        if pill_cols[idx % 3].button(suggestion, key=f"pill_{idx}", use_container_width=True):
            st.session_state["pending_rag_query"] = suggestion
            st.rerun()

    st.divider()

    # Initialise chat history
    if "rag_history" not in st.session_state:
        st.session_state.rag_history = []

    # Process any pending query from suggestion pills (fires on next rerun)
    if pending := st.session_state.pop("pending_rag_query", None):
        st.session_state.rag_history.append({"role": "user", "content": pending})
        with st.spinner("Searching knowledge base…"):
            answer = answer_rag_query(pending)
        st.session_state.rag_history.append({"role": "assistant", "content": answer})

    # Render chat history
    for msg in st.session_state.rag_history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Chat input for typed queries
    if typed_query := st.chat_input("Ask about any schema field or mapping…"):
        st.session_state.rag_history.append({"role": "user", "content": typed_query})
        with st.chat_message("user"):
            st.markdown(typed_query)
        with st.chat_message("assistant"):
            with st.spinner("Searching knowledge base…"):
                answer = answer_rag_query(typed_query)
            st.markdown(answer)
        st.session_state.rag_history.append({"role": "assistant", "content": answer})

    # Clear chat button
    if st.session_state.rag_history:
        if st.button("Clear chat", key="clear_rag"):
            st.session_state.rag_history = []
            st.rerun()
