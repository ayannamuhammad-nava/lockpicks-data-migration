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

    # Check if Quality has been signed off
    signoff_path = ARTIFACTS_DIR / "signoff.json"
    has_signoff = False
    if signoff_path.exists():
        try:
            _signoff_data = json.loads(signoff_path.read_text())
            has_signoff = bool(_signoff_data) and len(_signoff_data) > 0
        except Exception:
            pass

    # Phase buttons — clickable
    cols = st.columns(len(LIFECYCLE_PHASES))

    for i, (label, _cmd) in enumerate(LIFECYCLE_PHASES):
        with cols[i]:
            # Quality button turns green when signed off
            if label == "Quality" and has_signoff:
                st.markdown(
                    f'<div style="background:#2e7d32;color:#fff;padding:3px 0;border-radius:6px;'
                    f'text-align:center;font-weight:700;font-size:0.75rem">SIGNED OFF</div>',
                    unsafe_allow_html=True,
                )
                if st.button(f"✓ {label}", key=f"lc_phase_{i}", use_container_width=True, type="primary"):
                    st.session_state["lifecycle_view"] = label
                    st.rerun()
            elif i < current:
                btn_type = "primary"
                lbl = f"✓ {label}"
                if st.button(lbl, key=f"lc_phase_{i}", use_container_width=True, type=btn_type):
                    st.session_state["lifecycle_view"] = label
                    st.rerun()
            elif i == current:
                btn_type = "secondary"
                lbl = f"● {label}"
                if st.button(lbl, key=f"lc_phase_{i}", use_container_width=True, type=btn_type):
                    st.session_state["lifecycle_view"] = label
                    st.rerun()
            else:
                lbl = label
                st.button(lbl, key=f"lc_phase_{i}", use_container_width=True, type="secondary", disabled=True)


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


def render_modeling_page():
    """Render the Modeling detail page showing generated modern schema tables, columns, and types."""
    st.markdown("## 🏗️ Modeling — Generated Modern Schema")
    st.caption(f"Project: {PROJECT_NAME}")

    schema_dir = ARTIFACTS_DIR / "generated_schema"
    diff_path = schema_dir / "diff_report.json"
    norm_path = METADATA_DIR / "normalization_plan.json"
    full_schema_path = schema_dir / "full_schema.sql"

    if not schema_dir.exists():
        st.warning("No generated schema found. Run `dm generate-schema --all` first.")
        return

    # Load diff report for structured column data
    diff_data = {}
    if diff_path.exists():
        diff_data = json.loads(diff_path.read_text())

    # Load normalization plan
    norm_plan = {}
    if norm_path.exists():
        norm_plan = json.loads(norm_path.read_text())

    # Parse tables from SQL files
    sql_files = sorted([f for f in schema_dir.iterdir() if f.suffix == ".sql" and f.name != "full_schema.sql" and "_transforms" not in f.name])
    transform_files = sorted([f for f in schema_dir.iterdir() if "_transforms" in f.name])

    # Summary metrics
    legacy_col_count = diff_data.get("legacy_column_count", 0)
    modern_table_count = diff_data.get("modern_table_count", len(sql_files))

    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("Legacy Columns", legacy_col_count)
    mc2.metric("Modern Tables", modern_table_count)
    mc3.metric("Schema Files", len(sql_files))
    mc4.metric("Transform Files", len(transform_files))

    st.divider()

    tabs = st.tabs(["📐 Table Schemas", "🔀 Column Mapping", "📐 Normalization Plan", "📄 Full DDL"])
    tab_schemas, tab_col_map, tab_norm, tab_ddl = tabs

    # ── Table Schemas tab
    with tab_schemas:
        st.markdown("#### Generated Modern Tables")
        st.caption("New table structures with expanded column names, optimized types, and constraints.")

        for sql_file in sql_files:
            table_name = sql_file.stem
            sql_content = sql_file.read_text()

            # Parse columns from CREATE TABLE statement
            import re
            col_rows = []
            in_create = False
            for line in sql_content.splitlines():
                if "CREATE TABLE" in line:
                    in_create = True
                    continue
                if in_create and line.strip().startswith(")"):
                    break
                if in_create and line.strip() and not line.strip().startswith("CONSTRAINT") and not line.strip().startswith("CREATE"):
                    # Parse: column_name TYPE -- comment
                    col_match = re.match(r'\s+([\w_]+)\s+(\S+(?:\([^)]*\))?(?:\s+\S+)*?)\s*(?:--\s*(.*))?[,]?\s*$', line)
                    if col_match:
                        col_name = col_match.group(1)
                        col_type = col_match.group(2).rstrip(",")
                        comment = col_match.group(3) or ""
                        # Extract source field from comment
                        source = ""
                        source_match = re.search(r'Source:\s*([\w_]+)', comment)
                        if source_match:
                            source = source_match.group(1)
                        col_rows.append({
                            "Column": col_name,
                            "Type": col_type,
                            "Source (Legacy)": source,
                            "Notes": comment.strip(),
                        })

            # Determine table role from SQL comment
            role = "primary"
            if "child" in sql_content.lower():
                role = "child"
            elif "lookup" in sql_content.lower():
                role = "lookup"

            role_icon = {"primary": "🟢", "child": "🔵", "lookup": "🟡"}.get(role, "⚪")

            st.markdown(f"##### {role_icon} `{table_name}` — {len(col_rows)} columns ({role})")

            if col_rows:
                df = pd.DataFrame(col_rows)
                st.dataframe(df, use_container_width=True, hide_index=True, column_config={
                    "Notes": st.column_config.TextColumn(width="large"),
                })
            else:
                st.code(sql_content, language="sql")

    # ── Column Mapping tab
    with tab_col_map:
        st.markdown("#### Legacy → Modern Column Mapping")
        st.caption("How each legacy column maps to the new table structure.")

        columns_data = diff_data.get("columns", {})
        if columns_data:
            for category in ["renamed", "transformed", "archived", "removed"]:
                items = columns_data.get(category, {})
                if items:
                    cat_icon = {"renamed": "→", "transformed": "⚙️", "archived": "🔒", "removed": "🗑️"}.get(category, "")
                    st.markdown(f"##### {cat_icon} {category.title()} ({len(items)} columns)")
                    rows = []
                    for src_col, info in items.items():
                        rows.append({
                            "Legacy Column": src_col,
                            "Target Table": info.get("target_table", ""),
                            "Target Column": info.get("target_column", ""),
                            "Transform": info.get("transform", "") or "",
                            "PII Action": info.get("pii_action", "") or "",
                        })
                    df = pd.DataFrame(rows)

                    if category == "archived":
                        styled = df.style.apply(lambda r: ["background-color: #ffebee; color: #000"] * len(r), axis=1)
                        st.dataframe(styled, use_container_width=True, hide_index=True)
                    elif category == "transformed":
                        styled = df.style.apply(lambda r: ["background-color: #fff9c4; color: #000"] * len(r), axis=1)
                        st.dataframe(styled, use_container_width=True, hide_index=True)
                    else:
                        st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No diff report found. Run `dm generate-schema --all` to generate.")

    # ── Normalization Plan tab
    with tab_norm:
        st.markdown("#### Normalization Plan")
        st.caption("How the legacy table was decomposed into normalized modern entities.")

        if norm_plan:
            entities = norm_plan.get("entities", norm_plan.get("tables", []))
            if isinstance(entities, list):
                for entity in entities:
                    role = entity.get("role", "primary")
                    name = entity.get("name", entity.get("table", ""))
                    confidence = entity.get("confidence", 0)
                    columns = entity.get("columns", [])
                    role_icon = {"primary": "🟢", "child": "🔵", "lookup": "🟡"}.get(role, "⚪")
                    st.markdown(f"**{role_icon} {name}** — {role} (confidence: {confidence:.0%})")
                    if columns:
                        st.markdown(f"  Columns: `{'`, `'.join(columns)}`")
                    rels = entity.get("relationships", [])
                    if rels:
                        for rel in rels:
                            st.markdown(f"  FK: `{rel.get('column', '')}` → `{rel.get('references', '')}`")
                    st.markdown("")
            elif isinstance(entities, dict):
                st.json(entities)
            else:
                st.json(norm_plan)
        else:
            st.info("No normalization plan found. Run `dm generate-schema --all` to generate.")

    # ── Full DDL tab
    with tab_ddl:
        st.markdown("#### Full Generated DDL")
        st.caption("Complete SQL schema ready for deployment to the modern database.")

        if full_schema_path.exists():
            sql_text = full_schema_path.read_text()
            st.code(sql_text, language="sql", line_numbers=True)

            st.download_button(
                "Download full_schema.sql",
                sql_text,
                file_name="full_schema.sql",
                mime="text/sql",
            )
        else:
            st.info("No full_schema.sql found.")

    # Legend
    st.divider()
    st.markdown(
        "**Legend:** &nbsp;&nbsp; "
        "🟢 **Primary** — Main entity table containing core fields &nbsp;&nbsp; "
        "🔵 **Child** — Normalized out from the primary table (address, contact info groups) with FK back to primary &nbsp;&nbsp; "
        "🟡 **Lookup** — Low-cardinality reference table extracted for status codes, types, etc.",
        unsafe_allow_html=True,
    )


def render_governance_page():
    """Render the Governance detail page showing PII inventory, compliance, naming, nulls, and audit trail."""
    st.markdown("## 🔒 Governance — Compliance & Data Controls")
    st.caption(f"Project: {PROJECT_NAME}")

    # Load data sources
    glossary_path = METADATA_DIR / "glossary.json"
    mappings_path = METADATA_DIR / "mappings.json"

    if not glossary_path.exists():
        st.warning("No discovery data found. Run `dm discover --enrich` first.")
        return

    glossary = json.loads(glossary_path.read_text())
    columns = glossary.get("columns", [])
    legacy_cols = [c for c in columns if c.get("system") == "legacy"]
    pii_cols = [c for c in legacy_cols if c.get("pii")]

    mappings_data = json.loads(mappings_path.read_text()) if mappings_path.exists() else {"mappings": []}
    mappings = mappings_data.get("mappings", [])
    mappings_by_source = {m["source"]: m for m in mappings}

    # Load project governance config
    import yaml as _yaml_gov
    _proj_yaml = PROJECT_DIR / "project.yaml"
    _gov_config = {}
    if _proj_yaml.exists():
        _pc = _yaml_gov.safe_load(_proj_yaml.read_text()) or {}
        _gov_config = _pc.get("validation", {}).get("governance", {})

    pii_keywords = _gov_config.get("pii_keywords", [])
    naming_regex = _gov_config.get("naming_regex", r"^[a-z0-9_]+$")
    max_null_pct = _gov_config.get("max_null_percent", 10)

    # Load governance reports from validation runs
    gov_reports = []
    runs = get_runs()
    for run_name in runs:
        gov_csv = ARTIFACTS_DIR / run_name / "governance_report.csv"
        if gov_csv.exists():
            meta = load_json(run_name, "run_metadata.json") or {}
            gov_reports.append({
                "run": run_name,
                "meta": meta,
                "csv": gov_csv,
            })

    # ── Compliance Summary
    archived_count = sum(1 for m in mappings if m.get("type") == "archived")
    removed_count = sum(1 for m in mappings if m.get("type") == "removed")
    pii_handled = sum(1 for c in pii_cols if mappings_by_source.get(c["name"], {}).get("type") in ("archived", "transform"))
    pii_unhandled = len(pii_cols) - pii_handled

    st.markdown("### Compliance Summary")
    mc1, mc2, mc3, mc4, mc5 = st.columns(5)
    mc1.metric("PII Fields Detected", len(pii_cols))
    mc2.metric("PII Properly Handled", pii_handled)
    mc3.metric("PII Unhandled", pii_unhandled, delta=f"-{pii_unhandled}" if pii_unhandled else "0", delta_color="inverse")
    mc4.metric("Fields Archived", archived_count)
    mc5.metric("Fields Removed", removed_count)

    if pii_unhandled > 0:
        st.markdown(f"""
        <div class="pii-alert">
            ⚠️ <strong>{pii_unhandled} PII field(s) may not be properly handled.</strong><br>
            <small>Review the PII Inventory tab to ensure all sensitive data is hashed, masked, or archived before migration.</small>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.success("All detected PII fields have been properly handled (archived or transformed).")

    st.divider()

    tabs = st.tabs([
        "🔐 PII Inventory",
        "📝 Data Modification Controls",
        "✅ Naming Compliance",
        "📊 Null Threshold Report",
        "📜 Audit Trail",
    ])
    tab_pii, tab_mods, tab_naming, tab_nulls, tab_audit = tabs

    # ── PII Inventory
    with tab_pii:
        st.markdown("#### PII / Sensitive Data Inventory")
        st.caption("All fields identified as containing personally identifiable information, with the action taken for each.")

        if pii_cols:
            rows = []
            for c in pii_cols:
                col_name = c["name"]
                mapping = mappings_by_source.get(col_name, {})
                action = mapping.get("type", "unknown")
                target = mapping.get("target") or "—"
                rationale = mapping.get("rationale", "")

                # Infer regulation
                desc_lower = c.get("description", "").lower()
                col_lower = col_name.lower()
                if any(k in col_lower for k in ("ssn", "social")):
                    regulation = "HIPAA / PII"
                elif any(k in col_lower for k in ("bact", "brtn", "bank", "acct", "routing", "credit")):
                    regulation = "PCI-DSS"
                elif any(k in col_lower for k in ("dln", "driver", "license")):
                    regulation = "PII"
                elif any(k in col_lower for k in ("dob", "birth")):
                    regulation = "HIPAA"
                elif any(k in col_lower for k in ("email", "emal", "phone", "phon", "tel")):
                    regulation = "PII"
                elif any(k in col_lower for k in ("zip", "addr", "adr")):
                    regulation = "PII"
                else:
                    regulation = "PII"

                action_display = {
                    "archived": "🔒 Archived (not migrated)",
                    "transform": "⚙️ Transformed (hashed/masked)",
                    "rename": "→ Renamed (review needed)",
                    "removed": "🗑️ Removed",
                    "pending": "⚠️ Pending",
                }.get(action, f"⚠️ {action}")

                rows.append({
                    "Table": c.get("table", ""),
                    "Legacy Field": col_name,
                    "Description": c.get("description", ""),
                    "Regulation": regulation,
                    "Action": action_display,
                    "Modern Field": target,
                    "Rationale": rationale,
                })

            df = pd.DataFrame(rows)

            def highlight_pii_action(row):
                action = row["Action"]
                if "Archived" in action:
                    return ["background-color: #e8f5e9; color: #000"] * len(row)
                elif "Transformed" in action:
                    return ["background-color: #e8f5e9; color: #000"] * len(row)
                elif "Pending" in action or "review" in action.lower():
                    return ["background-color: #ffebee; color: #000"] * len(row)
                return [""] * len(row)

            st.dataframe(
                df.style.apply(highlight_pii_action, axis=1),
                use_container_width=True, hide_index=True,
                column_config={"Rationale": st.column_config.TextColumn(width="large")},
            )
        else:
            st.success("No PII fields detected in the legacy data.")

    # ── Data Modification Controls
    with tab_mods:
        st.markdown("#### Data Modification Controls")
        st.caption("Complete audit of every field change during migration — what changed, why, and the mapping type.")

        if mappings:
            rows = []
            for m in mappings:
                target = m.get("target") or "—"
                mtype = m.get("type", "")
                icon = {"rename": "→", "transform": "⚙️", "archived": "🔒", "removed": "🗑️"}.get(mtype, "?")
                rows.append({
                    "Table": m.get("table", ""),
                    "Legacy Field": m["source"],
                    "Action": f"{icon} {mtype}",
                    "Modern Field": target,
                    "Confidence": f"{int(m.get('confidence', 0) * 100)}%",
                    "Rationale": m.get("rationale", ""),
                })
            df = pd.DataFrame(rows)

            # Summary counts
            type_counts = {}
            for m in mappings:
                t = m.get("type", "unknown")
                type_counts[t] = type_counts.get(t, 0) + 1

            cols_summary = st.columns(len(type_counts))
            for i, (t, count) in enumerate(sorted(type_counts.items())):
                icon = {"rename": "→", "transform": "⚙️", "archived": "🔒", "removed": "🗑️"}.get(t, "")
                cols_summary[i].metric(f"{icon} {t.title()}", count)

            st.divider()

            def highlight_mod(row):
                action = row["Action"]
                if "archived" in action:
                    return ["background-color: #ffebee; color: #000"] * len(row)
                if "transform" in action:
                    return ["background-color: #fff9c4; color: #000"] * len(row)
                if "removed" in action:
                    return ["background-color: #f5f5f5; color: #888"] * len(row)
                return [""] * len(row)

            st.dataframe(
                df.style.apply(highlight_mod, axis=1),
                use_container_width=True, hide_index=True,
                column_config={"Rationale": st.column_config.TextColumn(width="large")},
            )
        else:
            st.info("No mappings found. Run `dm enrich` first.")

    # ── Naming Compliance
    with tab_naming:
        st.markdown("#### Naming Convention Compliance")
        st.caption(f"Checks modern column names against the configured regex: `{naming_regex}`")

        import re as _re_naming
        modern_fields = [m for m in mappings if m.get("target") and m.get("type") not in ("archived", "removed")]
        if modern_fields:
            pass_count = 0
            fail_rows = []
            for m in modern_fields:
                target = m.get("target", "")
                if _re_naming.match(naming_regex, target):
                    pass_count += 1
                else:
                    fail_rows.append({
                        "Legacy Field": m["source"],
                        "Modern Field": target,
                        "Issue": f"Does not match `{naming_regex}`",
                    })

            nc1, nc2 = st.columns(2)
            nc1.metric("Passing", pass_count)
            nc2.metric("Violations", len(fail_rows), delta=f"-{len(fail_rows)}" if fail_rows else "0", delta_color="inverse")

            if fail_rows:
                st.dataframe(pd.DataFrame(fail_rows), use_container_width=True, hide_index=True)
            else:
                st.success(f"All {pass_count} modern column names comply with naming convention.")
        else:
            st.info("No modern field mappings to check.")

    # ── Null Threshold Report
    with tab_nulls:
        st.markdown("#### Null Threshold Report")
        st.caption(f"Fields exceeding the configured maximum null percentage: **{max_null_pct}%**")

        # Try to get null data from legacy DB
        import yaml as _yaml_null
        _proj_yaml_null = PROJECT_DIR / "project.yaml"
        _null_config = _yaml_null.safe_load(_proj_yaml_null.read_text()) or {} if _proj_yaml_null.exists() else {}
        legacy_conn_cfg = _null_config.get("connections", {}).get("legacy", {})

        if legacy_conn_cfg:
            try:
                import psycopg2

                def _resolve_null(val):
                    if isinstance(val, str) and val.startswith("${"):
                        import re as _re_null
                        m = _re_null.match(r'\$\{([^:}]+):?(.*)\}', val)
                        if m:
                            return os.environ.get(m.group(1), m.group(2))
                    return val

                conn = psycopg2.connect(
                    host=_resolve_null(legacy_conn_cfg.get("host", "localhost")),
                    port=int(_resolve_null(legacy_conn_cfg.get("port", 5432))),
                    database=_resolve_null(legacy_conn_cfg.get("database", "legacy_db")),
                    user=_resolve_null(legacy_conn_cfg.get("user", "postgres")),
                    password=_resolve_null(legacy_conn_cfg.get("password", "postgres")),
                )

                tables = sorted(set(c.get("table", "") for c in legacy_cols))
                all_null_rows = []
                for table in tables:
                    total_query = f"SELECT COUNT(*) FROM {table}"
                    total = pd.read_sql(total_query, conn).iloc[0, 0]
                    if total == 0:
                        continue

                    table_cols_list = [c["name"] for c in legacy_cols if c.get("table") == table]
                    for col_name in table_cols_list:
                        null_query = f"SELECT COUNT(*) FROM {table} WHERE {col_name} IS NULL OR TRIM({col_name}::text) = ''"
                        try:
                            null_count = pd.read_sql(null_query, conn).iloc[0, 0]
                            null_pct = round((null_count / total) * 100, 1)
                            status = "VIOLATION" if null_pct > max_null_pct else "PASS"
                            all_null_rows.append({
                                "Table": table,
                                "Column": col_name,
                                "Null/Empty": null_count,
                                "Total Rows": total,
                                "Null %": null_pct,
                                "Status": status,
                            })
                        except Exception:
                            pass

                conn.close()

                if all_null_rows:
                    df_null = pd.DataFrame(all_null_rows)
                    violations = df_null[df_null["Status"] == "VIOLATION"]
                    passing = df_null[df_null["Status"] == "PASS"]

                    nc1, nc2 = st.columns(2)
                    nc1.metric("Violations", len(violations))
                    nc2.metric("Passing", len(passing))

                    if not violations.empty:
                        st.markdown("##### Fields Exceeding Null Threshold")
                        styled = violations.style.map(
                            lambda v: "background-color: #ffebee; color: #c62828; font-weight: bold" if v == "VIOLATION" else "",
                            subset=["Status"]
                        )
                        st.dataframe(styled, use_container_width=True, hide_index=True)

                    with st.expander("Show all columns"):
                        st.dataframe(df_null, use_container_width=True, hide_index=True)
                else:
                    st.success("No null data found.")

            except Exception as e:
                st.error(f"Could not connect to legacy database: {e}")
        else:
            st.warning("No legacy connection configured in project.yaml.")

    # ── Audit Trail
    with tab_audit:
        st.markdown("#### Validation Audit Trail")
        st.caption("Timestamped history of all validation runs with governance scores.")

        if runs:
            rows = []
            for run_name in sorted(runs, reverse=True):
                meta = load_json(run_name, "run_metadata.json") or {}
                phase = meta.get("phase", "?")
                dataset = meta.get("dataset", "?")
                score = meta.get("confidence_score", "?")
                gov_score = meta.get("governance_score", "—")
                run_status = meta.get("status", "?")
                ts = run_name.replace("run_", "")
                emoji = STATUS_EMOJI.get(run_status, "⚪")

                has_gov = (ARTIFACTS_DIR / run_name / "governance_report.csv").exists()

                rows.append({
                    "Timestamp": ts,
                    "Phase": phase.upper(),
                    "Dataset": dataset,
                    "Score": score,
                    "Gov Score": gov_score,
                    "Status": f"{emoji} {run_status}",
                    "Gov Report": "Yes" if has_gov else "—",
                })

            df_audit = pd.DataFrame(rows)
            st.dataframe(df_audit, use_container_width=True, hide_index=True)
        else:
            st.info("No validation runs found.")


def render_transformation_page():
    """Render the Transformation detail page showing ETL transform scripts, converted SQL, and warnings."""
    st.markdown("## ⚙️ Transformation — ETL Logic & Converted SQL")
    st.caption(f"Project: {PROJECT_NAME}")

    schema_dir = ARTIFACTS_DIR / "generated_schema"
    converted_dir = ARTIFACTS_DIR / "converted"

    if not schema_dir.exists():
        st.warning("No transformation artifacts found. Run `dm generate-schema --all` and `dm convert` first.")
        return

    # Collect transform scripts and converted files
    transform_files = sorted([f for f in schema_dir.iterdir() if "_transforms" in f.name])
    converted_files = []
    if converted_dir.exists():
        for dialect_dir in sorted(converted_dir.iterdir()):
            if dialect_dir.is_dir():
                for f in sorted(dialect_dir.iterdir()):
                    converted_files.append(f)

    # Summary
    mc1, mc2, mc3 = st.columns(3)
    mc1.metric("Transform Scripts", len(transform_files))
    mc2.metric("Converted Files", len(converted_files))
    target_dialect = converted_files[0].parent.name if converted_files else "—"
    mc3.metric("Target Platform", target_dialect.title())

    st.divider()

    tabs = st.tabs([
        "🔀 Transform Scripts",
        "🎯 Converted SQL",
        "⚖️ Before / After",
        "⚠️ Warnings & TODOs",
    ])
    tab_transforms, tab_converted, tab_compare, tab_warnings = tabs

    # ── Transform Scripts
    with tab_transforms:
        st.markdown("#### ETL Transform Scripts")
        st.caption("INSERT...SELECT statements that define how each legacy column maps to the modern table. "
                   "Includes type conversions, PII hashing, and format transformations.")

        if transform_files:
            for tf in transform_files:
                table_name = tf.stem.replace("_transforms", "")
                sql = tf.read_text()

                # Count transforms (look for function calls in SELECT)
                import re
                transform_patterns = re.findall(r'(encode\(sha256|CASE\s+WHEN|CAST\(|TO_DATE|TO_TIMESTAMP|COALESCE|TRIM|UPPER|LOWER|NOW\(\))', sql, re.IGNORECASE)
                transform_count = len(transform_patterns)

                st.markdown(f"##### `{table_name}` — {transform_count} transformation(s)")
                st.code(sql, language="sql", line_numbers=True)

                # Highlight key transforms
                if "sha256" in sql.lower():
                    st.markdown("🔐 **PII Hashing:** SSN field hashed via SHA-256")
                if "now()" in sql.lower():
                    st.markdown("🕐 **Auto-timestamps:** `created_at`/`updated_at` set to current time")

                st.divider()
        else:
            st.info("No transform scripts found. Run `dm generate-schema --all` to generate.")

    # ── Converted SQL
    with tab_converted:
        st.markdown("#### Platform-Converted SQL")
        st.caption("Final SQL after dialect translation. Ready for deployment to the target database.")

        if converted_files:
            for cf in converted_files:
                dialect = cf.parent.name
                sql = cf.read_text()

                st.markdown(f"##### `{cf.name}` — Target: {dialect.title()}")
                st.code(sql, language="sql", line_numbers=True)

                st.download_button(
                    f"Download {cf.name}",
                    sql,
                    file_name=cf.name,
                    mime="text/sql",
                    key=f"dl_{cf.name}",
                )
                st.divider()
        else:
            st.info("No converted SQL found. Run `dm convert` to generate.")

    # ── Before / After Comparison
    with tab_compare:
        st.markdown("#### Before / After SQL Comparison")
        st.caption("Side-by-side view of the generated schema vs the platform-converted output.")

        source_schema = schema_dir / "full_schema.sql"
        if source_schema.exists() and converted_files:
            source_sql = source_schema.read_text()
            converted_sql = converted_files[0].read_text()

            col_before, col_after = st.columns(2)
            with col_before:
                st.markdown("##### Generated Schema (Before)")
                st.code(source_sql, language="sql")

            with col_after:
                st.markdown(f"##### Converted ({target_dialect.title()}) (After)")
                st.code(converted_sql, language="sql")

            # Diff summary
            source_lines = set(source_sql.strip().splitlines())
            converted_lines = set(converted_sql.strip().splitlines())
            only_source = source_lines - converted_lines
            only_converted = converted_lines - source_lines

            if only_source or only_converted:
                st.markdown(f"**Diff:** {len(only_source)} lines removed, {len(only_converted)} lines added during conversion")
            else:
                st.success("No differences — source and target dialects are identical.")
        elif not source_schema.exists():
            st.info("No generated schema found. Run `dm generate-schema --all` first.")
        else:
            st.info("No converted SQL found. Run `dm convert` first.")

    # ── Warnings & TODOs
    with tab_warnings:
        st.markdown("#### Conversion Warnings & Manual TODOs")
        st.caption("Statements that could not be fully translated and require manual review.")

        has_warnings = False
        if converted_files:
            for cf in converted_files:
                sql = cf.read_text()
                # Find TODO comments
                import re
                todos = re.findall(r'-- TODO:.*', sql)
                # Find commented-out CREATE TABLE blocks (failed conversions)
                commented_creates = re.findall(r'-- CREATE TABLE \w+', sql)

                if todos or commented_creates:
                    has_warnings = True
                    st.markdown(f"##### `{cf.name}`")

                    if todos:
                        st.markdown(f"**{len(todos)} TODO(s) requiring manual review:**")
                        for todo in todos:
                            st.markdown(f"""
                            <div style="background:#fff9c4;border-left:4px solid #f57f17;padding:8px 12px;margin:4px 0;border-radius:4px;color:#000">
                                {todo.replace('-- ', '')}
                            </div>
                            """, unsafe_allow_html=True)

                    if commented_creates:
                        st.markdown(f"**{len(commented_creates)} CREATE TABLE statement(s) commented out** (sqlglot could not parse due to inline comments in column definitions):")
                        for cc in commented_creates:
                            table_name = cc.replace("-- CREATE TABLE ", "")
                            st.markdown(f"- `{table_name}` — needs manual DDL review")

                    st.divider()

        if not has_warnings:
            st.success("No warnings or TODOs. All SQL was successfully converted.")


def render_compliance_page():
    """Render the Compliance detail page showing pre-migration evidence package."""
    st.markdown("## ✅ Compliance — Pre-Migration Evidence")
    st.caption(f"Project: {PROJECT_NAME}")

    # Find the latest pre-validation run
    runs = get_runs()
    pre_runs = []
    for run_name in runs:
        meta = load_json(run_name, "run_metadata.json") or {}
        if meta.get("phase") == "pre":
            pre_runs.append((run_name, meta))

    if not pre_runs:
        st.warning("No pre-migration validation runs found. Run `dm validate --phase pre` first.")
        return

    # Use most recent pre-run
    latest_run, latest_meta = pre_runs[-1]
    score = float(latest_meta.get("confidence_score", 0))
    run_status = latest_meta.get("status", "UNKNOWN")
    struct_score = latest_meta.get("structure_score")
    gov_score = latest_meta.get("governance_score")
    dataset = latest_meta.get("dataset", "?")
    emoji = STATUS_EMOJI.get(run_status, "⚪")
    color = STATUS_COLOR.get(run_status, "#999")

    # ── Compliance Checklist Summary
    st.markdown("### Compliance Checklist")

    # Load governance report
    gov_csv_path = ARTIFACTS_DIR / latest_run / "governance_report.csv"
    gov_df = None
    if gov_csv_path.exists():
        gov_df = pd.read_csv(gov_csv_path)

    # Build checklist
    checks = []

    # PII check
    glossary_path = METADATA_DIR / "glossary.json"
    mappings_path = METADATA_DIR / "mappings.json"
    if glossary_path.exists() and mappings_path.exists():
        glossary = json.loads(glossary_path.read_text())
        mappings_data = json.loads(mappings_path.read_text())
        legacy_cols = [c for c in glossary.get("columns", []) if c.get("system") == "legacy"]
        pii_cols = [c for c in legacy_cols if c.get("pii")]
        mappings_by_src = {m["source"]: m for m in mappings_data.get("mappings", [])}
        pii_handled = all(
            mappings_by_src.get(c["name"], {}).get("type") in ("archived", "transform")
            for c in pii_cols
        )
        checks.append(("PII fields properly handled (hashed/archived)", pii_handled, f"{len(pii_cols)} PII fields detected"))
    else:
        checks.append(("PII fields properly handled", False, "No glossary data"))

    # Naming convention
    if mappings_path.exists():
        import re as _re_comp
        naming_regex = r"^[a-z0-9_]+$"
        mappings_list = json.loads(mappings_path.read_text()).get("mappings", [])
        modern_names = [m["target"] for m in mappings_list if m.get("target") and m.get("type") not in ("archived", "removed")]
        naming_pass = all(_re_comp.match(naming_regex, n) for n in modern_names)
        checks.append(("Modern column names follow naming convention", naming_pass, f"{len(modern_names)} columns checked"))

    # Governance violations
    if gov_df is not None:
        violations = int((gov_df["status"] == "VIOLATION").sum())
        checks.append(("No governance violations", violations == 0, f"{violations} violation(s)" if violations else "All clear"))

    # Confidence threshold
    checks.append(("Confidence score >= 70 (YELLOW+)", score >= 70, f"Score: {score}"))
    checks.append(("Confidence score >= 90 (GREEN)", score >= 90, f"Score: {score}"))

    # Schema generated
    schema_exists = (ARTIFACTS_DIR / "generated_schema" / "full_schema.sql").exists()
    checks.append(("Modern schema generated", schema_exists, ""))

    # Render checklist
    all_pass = True
    for label, passed, detail in checks:
        icon = "✅" if passed else "❌"
        if not passed:
            all_pass = False
        detail_str = f" — {detail}" if detail else ""
        st.markdown(f"{icon} **{label}**{detail_str}")

    st.divider()

    if all_pass:
        st.markdown(f"""
        <div style="background:#e8f5e9;border-left:5px solid #2e7d32;padding:12px 20px;border-radius:8px;margin-bottom:16px">
            <div style="font-size:1.1rem;font-weight:700;color:#2e7d32">🟢 COMPLIANT — Ready to proceed with migration</div>
            <div style="font-size:0.85rem;color:#444">All compliance checks passed. Score: {score}/100</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div style="background:#ffebee;border-left:5px solid #c62828;padding:12px 20px;border-radius:8px;margin-bottom:16px">
            <div style="font-size:1.1rem;font-weight:700;color:#c62828">🔴 NOT COMPLIANT — Issues must be resolved</div>
            <div style="font-size:0.85rem;color:#444">One or more compliance checks failed. Score: {score}/100</div>
        </div>
        """, unsafe_allow_html=True)

    # ── Tabs
    tabs = st.tabs([
        "📋 Readiness Report",
        "🔒 Governance Report",
        "🔍 Schema Diff",
        "📊 Risk Assessment",
    ])
    tab_readiness, tab_gov_report, tab_schema_diff, tab_risk = tabs

    # ── Readiness Report
    with tab_readiness:
        report_md = load_text(latest_run, "readiness_report.md")
        if report_md:
            st.markdown(report_md)
        else:
            st.info("No readiness report found for this run.")

    # ── Governance Report
    with tab_gov_report:
        if gov_df is not None:
            violations = int((gov_df["status"] == "VIOLATION").sum())
            warnings = int((gov_df["status"] == "WARNING").sum())
            passes = int((gov_df["status"] == "PASS").sum())

            gc1, gc2, gc3, gc4 = st.columns(4)
            gc1.metric("Violations", violations)
            gc2.metric("Warnings", warnings)
            gc3.metric("Passing", passes)
            gc4.metric("Total Checks", len(gov_df))

            styled = gov_df.style.map(color_gov_status, subset=["status"])
            st.dataframe(styled, use_container_width=True, hide_index=True)
        else:
            st.info("No governance report found for this run.")

    # ── Schema Diff
    with tab_schema_diff:
        schema_md = load_text(latest_run, "schema_diff.md")
        if schema_md:
            st.markdown(schema_md)
            st.info("**Note:** Columns listed as 'missing in modern' are renamed, not lost. "
                    "COBOL abbreviated names (ct_fnam, bp_payam) were expanded to descriptive "
                    "modern names (first_name, payment_amount). See the Field Mappings in Discovery for details.")
        else:
            st.info("No schema diff found for this run.")

    # ── Risk Assessment
    with tab_risk:
        st.markdown("#### Confidence Score Breakdown")

        st.markdown(f"**Overall Score:** {emoji} **{score}/100** — {run_status}")
        st.markdown(f"**Dataset:** {dataset} &nbsp;&nbsp; **Run:** `{latest_run}`")

        st.divider()

        rc1, rc2, rc3 = st.columns(3)
        with rc1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown('<div class="metric-label">Structure (40%)</div>', unsafe_allow_html=True)
            val = f"{struct_score}/100" if struct_score is not None else "N/A"
            st.markdown(f'<div class="metric-value">{val}</div>', unsafe_allow_html=True)
            st.markdown("<div style='font-size:0.75rem;color:#888'>Schema compatibility</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
        with rc2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown('<div class="metric-label">Integrity (40%)</div>', unsafe_allow_html=True)
            st.markdown('<div class="metric-value">N/A</div>', unsafe_allow_html=True)
            st.markdown("<div style='font-size:0.75rem;color:#888'>Post-migration only</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
        with rc3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown('<div class="metric-label">Governance (20%)</div>', unsafe_allow_html=True)
            val = f"{gov_score}/100" if gov_score is not None else "N/A"
            st.markdown(f'<div class="metric-value">{val}</div>', unsafe_allow_html=True)
            st.markdown("<div style='font-size:0.75rem;color:#888'>PII & compliance</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        st.divider()

        st.markdown("**Scoring formula:** `confidence = (0.4 × structure) + (0.4 × integrity) + (0.2 × governance)`")
        st.markdown(
            "**Thresholds:** &nbsp; "
            "🟢 GREEN >= 90 (safe to proceed) &nbsp;&nbsp; "
            "🟡 YELLOW 70-89 (review recommended) &nbsp;&nbsp; "
            "🔴 RED < 70 (fix issues first)"
        )


def render_quality_page():
    """Render the Quality detail page showing post-migration results and sign-off."""
    st.markdown("## 📊 Quality — Post-Migration Validation & Sign-Off")
    st.caption(f"Project: {PROJECT_NAME}")

    # Collect all runs
    runs = get_runs()
    pre_runs = []
    post_runs = []
    prove_runs = []
    for run_name in runs:
        meta = load_json(run_name, "run_metadata.json") or {}
        phase = meta.get("phase", "")
        if phase == "pre":
            pre_runs.append((run_name, meta))
        elif phase == "post":
            post_runs.append((run_name, meta))
        elif phase == "prove":
            prove_runs.append((run_name, meta))

    # Compute overall scores
    all_scores = []
    for _, meta in pre_runs + post_runs:
        s = meta.get("confidence_score")
        if s is not None and float(s) > 0:
            all_scores.append(float(s))

    avg_score = round(sum(all_scores) / len(all_scores), 1) if all_scores else 0
    if avg_score >= 90:
        overall_status = "GREEN"
    elif avg_score >= 70:
        overall_status = "YELLOW"
    else:
        overall_status = "RED"
    overall_emoji = STATUS_EMOJI.get(overall_status, "⚪")
    overall_color = STATUS_COLOR.get(overall_status, "#999")

    # Load sign-offs
    signoff_path = ARTIFACTS_DIR / "signoff.json"
    signoffs = []
    if signoff_path.exists():
        signoffs = json.loads(signoff_path.read_text())

    # ── Overall Quality Summary
    st.markdown("### Overall Quality Status")

    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("Avg Score", f"{avg_score}/100")
    mc2.metric("Pre Runs", len(pre_runs))
    mc3.metric("Post Runs", len(post_runs))
    mc4.metric("Proof Reports", len(prove_runs))

    # Sign-off status banner
    if signoffs:
        latest_signoff = signoffs[-1]
        st.markdown(f"""
        <div style="background:#e8f5e9;border-left:5px solid #2e7d32;padding:12px 20px;border-radius:8px;margin:12px 0">
            <div style="font-size:1rem;font-weight:700;color:#2e7d32">✅ SIGNED OFF</div>
            <div style="font-size:0.85rem;color:#444">
                Last sign-off by <strong>{latest_signoff.get('name', '')}</strong>
                ({latest_signoff.get('role', '')})
                on {latest_signoff.get('date', '')} at {latest_signoff.get('time', '')}
                — Score: {latest_signoff.get('score', '')}/100
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div style="background:#ffebee;border-left:5px solid #c62828;padding:12px 20px;border-radius:8px;margin:12px 0">
            <div style="font-size:1rem;font-weight:700;color:#c62828">🔴 NOT SIGNED OFF</div>
            <div style="font-size:0.85rem;color:#444">No sign-offs recorded. Review the results below and sign off when satisfied.</div>
        </div>
        """, unsafe_allow_html=True)

    st.divider()

    # ── Tabs
    tabs = st.tabs([
        "📋 Reconciliation",
        "📊 Proof Reports",
        "📈 Score Summary",
        "✍️ Sign-Off",
    ])
    tab_recon, tab_proof, tab_scores, tab_signoff = tabs

    # ── Reconciliation
    with tab_recon:
        st.markdown("#### Post-Migration Reconciliation Reports")

        if post_runs:
            for run_name, meta in sorted(post_runs, reverse=True):
                dataset = meta.get("dataset", "?")
                score = meta.get("confidence_score", "?")
                run_status = meta.get("status", "?")
                emoji = STATUS_EMOJI.get(run_status, "⚪")

                st.markdown(f"##### {emoji} {dataset.title()} — {score}/100 ({run_status})")

                recon_md = load_text(run_name, "reconciliation_report.md")
                if recon_md:
                    with st.expander("View Report", expanded=False):
                        st.markdown(recon_md)
                st.divider()
        else:
            st.info("No post-migration runs found. Run `dm validate --phase post` first.")

    # ── Proof Reports
    with tab_proof:
        st.markdown("#### Migration Proof Reports")
        st.caption("Combined pre + post validation results for audit.")

        if prove_runs:
            for run_name, meta in sorted(prove_runs, reverse=True):
                dataset = meta.get("dataset", "?")
                pre_score = meta.get("pre_score", "?")
                post_score = meta.get("post_score", "?")
                final = meta.get("confidence_score", meta.get("final_score", "?"))
                run_status = meta.get("status", "?")
                emoji = STATUS_EMOJI.get(run_status, "⚪")

                st.markdown(f"##### {emoji} {dataset.title()} — Final: {final}/100 ({run_status})")
                st.markdown(f"Pre: {pre_score} &nbsp;&nbsp; Post: {post_score} &nbsp;&nbsp; Final: {final}")

                proof_md = load_text(run_name, "proof_report.md")
                if proof_md:
                    with st.expander("View Proof Report", expanded=False):
                        st.markdown(proof_md)
                st.divider()
        else:
            st.info("No proof reports found. Run `dm prove` first.")

    # ── Score Summary
    with tab_scores:
        st.markdown("#### All Validation Scores")

        all_run_rows = []
        for run_name in sorted(runs, reverse=True):
            meta = load_json(run_name, "run_metadata.json") or {}
            phase = meta.get("phase", "?")
            dataset = meta.get("dataset", "?")
            score = meta.get("confidence_score", "?")
            run_status = meta.get("status", "?")
            emoji = STATUS_EMOJI.get(run_status, "⚪")
            ts = run_name.replace("run_", "")
            all_run_rows.append({
                "Timestamp": ts,
                "Phase": phase.upper(),
                "Dataset": dataset,
                "Score": score,
                "Status": f"{emoji} {run_status}",
            })

        if all_run_rows:
            st.dataframe(pd.DataFrame(all_run_rows), use_container_width=True, hide_index=True)
        else:
            st.info("No runs found.")

    # ── Sign-Off
    with tab_signoff:
        st.markdown("#### Migration Sign-Off")
        st.caption("Record formal approval of the migration results. Each sign-off captures the signer's name, role, timestamp, and the current confidence score.")

        st.markdown(f"**Current Overall Score:** {overall_emoji} **{avg_score}/100** — {overall_status}")

        st.divider()

        # Sign-off form
        st.markdown("##### Add Sign-Off")
        so_col1, so_col2 = st.columns(2)
        with so_col1:
            signoff_name = st.text_input("Full Name", key="signoff_name", placeholder="e.g., Ayanna Muhammad")
        with so_col2:
            signoff_role = st.selectbox("Role", [
                "Data Migration Lead",
                "Technical Lead",
                "Compliance Officer",
                "Program Manager",
                "Data Steward",
                "QA Lead",
                "Other",
            ], key="signoff_role")

        if st.button("✍️ Sign Off", type="primary", use_container_width=True):
            if not signoff_name:
                st.error("Please enter your full name before signing off.")
            else:
                st.session_state["pending_signoff"] = {
                    "name": signoff_name,
                    "role": signoff_role,
                    "score": avg_score,
                    "status": overall_status,
                }
                st.rerun()

        # Confirmation dialog
        if "pending_signoff" in st.session_state:
            pending = st.session_state["pending_signoff"]
            st.warning(
                f"**Are you sure?** Your information will be saved as signing off on these changes.\n\n"
                f"**Name:** {pending['name']}  \n"
                f"**Role:** {pending['role']}  \n"
                f"**Score:** {pending['score']}/100 ({pending['status']})"
            )
            confirm_col, cancel_col = st.columns(2)
            with confirm_col:
                if st.button("✅ Confirm Sign-Off", type="primary", use_container_width=True):
                    from datetime import datetime
                    now = datetime.now()
                    new_signoff = {
                        "name": pending["name"],
                        "role": pending["role"],
                        "date": now.strftime("%Y-%m-%d"),
                        "time": now.strftime("%H:%M:%S"),
                        "score": pending["score"],
                        "status": pending["status"],
                        "project": PROJECT_NAME,
                    }
                    signoffs.append(new_signoff)
                    signoff_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(signoff_path, "w") as f:
                        json.dump(signoffs, f, indent=2)
                    del st.session_state["pending_signoff"]
                    st.success(f"Signed off by **{pending['name']}** ({pending['role']}) at score {pending['score']}/100")
                    st.rerun()
            with cancel_col:
                if st.button("❌ Cancel", use_container_width=True):
                    del st.session_state["pending_signoff"]
                    st.rerun()

        st.divider()

        # Display existing sign-offs
        st.markdown("##### Sign-Off History")

        if signoffs:
            for i, so in enumerate(reversed(signoffs)):
                so_status = so.get("status", "")
                so_color = STATUS_COLOR.get(so_status, "#999")
                so_emoji = STATUS_EMOJI.get(so_status, "⚪")
                st.markdown(f"""
                <div style="background:#f8f9fa;border-left:4px solid {so_color};padding:10px 16px;margin:6px 0;border-radius:4px">
                    <div style="font-weight:700;color:#333">{so_emoji} {so.get('name', '')} — {so.get('role', '')}</div>
                    <div style="font-size:0.85rem;color:#666">
                        {so.get('date', '')} at {so.get('time', '')} &nbsp;&nbsp;
                        Score: <strong>{so.get('score', '')}/100</strong> ({so_status}) &nbsp;&nbsp;
                        Project: {so.get('project', '')}
                    </div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown("*No sign-offs recorded yet.*")


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

        # Load datasets from project.yaml
        _proj_yaml_sidebar = PROJECT_DIR / "project.yaml"
        _dataset_names = ["my_table"]
        if _proj_yaml_sidebar.exists():
            import yaml as _yaml_sidebar
            _sidebar_config = _yaml_sidebar.safe_load(_proj_yaml_sidebar.read_text()) or {}
            _datasets = _sidebar_config.get("datasets", [])
            if _datasets:
                _dataset_names = [d.get("name", d) if isinstance(d, dict) else d for d in _datasets]

        dataset_sel = st.selectbox(
            "Dataset",
            _dataset_names,
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
    elif lifecycle_view == "Modeling":
        render_modeling_page()
    elif lifecycle_view == "Governance":
        render_governance_page()
    elif lifecycle_view == "Transformation":
        render_transformation_page()
    elif lifecycle_view == "Compliance":
        render_compliance_page()
    elif lifecycle_view == "Quality":
        render_quality_page()
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
