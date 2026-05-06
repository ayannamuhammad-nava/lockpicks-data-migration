"""
Flat File Pipeline — runs the full discovery-to-schema pipeline for
copybook and flat file sources without requiring OpenMetadata or a database.

Produces the same artifacts as the OM-backed pipeline:
  - metadata/glossary.json
  - metadata/mappings.json
  - metadata/profiling_stats.json
  - metadata/normalization_plan.json
  - metadata/rationalization_report.json
  - metadata/rationalization_report.md
  - metadata/migration_scope.yaml
  - metadata/abbreviations.yaml
  - artifacts/generated_schema/{target}/ (DDL for all 4 targets)
  - artifacts/generated_schema/*_transforms.sql
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import yaml

from dm.config import (
    get_connection_config,
    get_datasets,
    get_metadata_path,
    load_project_config,
)

logger = logging.getLogger(__name__)


def _get_ai_client(config: Dict):
    """Initialize AI client if configured. Returns None if not available."""
    ai_config = config.get("ai", {})
    if not ai_config.get("api_key") and not ai_config.get("provider"):
        return None
    try:
        from dm.ai.client import AIClient
        client = AIClient(ai_config)
        if client.is_available():
            logger.info("AI client available — will enhance analysis")
            return client
        return None
    except Exception:
        return None


def run_flatfile_pipeline(project_dir: str) -> Dict:
    """Run the full pre-migration pipeline for flat file / copybook sources.

    When an AI configuration is present in project.yaml (ai.provider + ai.api_key),
    the pipeline enhances its analysis with three AI-assisted operations:
    1. Column name understanding — maps COBOL abbreviations to modern names
    2. Normalization review — validates and refines the rule-based plan
    3. Data quality assessment — finds issues rules can't catch

    AI is always optional. The rule engine produces complete output first;
    AI refines but never blocks.

    Args:
        project_dir: Path to the project directory.

    Returns:
        Summary dict with counts and scores.
    """
    config = load_project_config(project_dir)
    metadata_path = get_metadata_path(config)
    metadata_path.mkdir(parents=True, exist_ok=True)
    schema_dir = Path(config.get("_project_dir", ".")) / "artifacts" / "generated_schema"
    schema_dir.mkdir(parents=True, exist_ok=True)

    datasets = get_datasets(config)

    # ── Step 1: Connect to all sources and profile ────────────────
    logger.info("Step 1: Profiling all sources")
    all_profiles = {}
    all_schemas = {}  # {table_name: [col_dicts]}
    all_dataframes = {}

    from dm.connectors.flatfile import FlatFileConnector

    for ds in datasets:
        name = ds["name"] if isinstance(ds, dict) else ds
        source = ds.get("source", name) if isinstance(ds, dict) else name

        try:
            conn_config = get_connection_config(config, source)
            conn = FlatFileConnector(conn_config)
            conn.connect()

            schema = conn.get_table_schema(name)
            df = conn.execute_query(f"SELECT * FROM {name}")
            row_count = len(df)

            col_stats = {}
            for col in schema:
                cn = col["column_name"]
                if cn in df.columns:
                    null_count = int(df[cn].isnull().sum() + (df[cn] == "").sum())
                    null_pct = round(null_count / row_count * 100, 2) if row_count else 0
                    distinct = int(df[cn].nunique())
                    max_len = int(df[cn].astype(str).str.len().max()) if not df[cn].empty else 0

                    # Value frequencies
                    freqs = []
                    vc = df[cn][df[cn] != ""].value_counts().head(10)
                    for val, count in vc.items():
                        freqs.append({"value": str(val), "count": int(count)})

                    col_stats[cn] = {
                        "null_count": null_count,
                        "null_percent": null_pct,
                        "distinct_count": distinct,
                        "unique_percent": round(distinct / row_count * 100, 2) if row_count else 0,
                        "max_length": max_len,
                        "min_value": str(df[cn].min()) if not df[cn].empty else None,
                        "max_value": str(df[cn].max()) if not df[cn].empty else None,
                        "value_frequencies": freqs,
                        "row_count": row_count,
                    }

            all_profiles[name] = {
                "row_count": row_count,
                "column_count": len(schema),
                "columns": col_stats,
            }
            all_schemas[name] = schema
            all_dataframes[name] = df

            logger.info(f"  {name}: {row_count} rows, {len(schema)} columns")
            conn.close()

        except Exception as e:
            logger.warning(f"  {name}: {e}")

    # Save profiling stats
    with open(metadata_path / "profiling_stats.json", "w") as f:
        json.dump(all_profiles, f, indent=2, default=str)

    # ── Initialize AI client (optional) ─────────────────────────
    ai_client = _get_ai_client(config)

    # ── Step 2: Generate glossary + mappings ──────────────────────
    logger.info("Step 2: Generating glossary and mappings")

    pii_keywords = config.get("validation", {}).get("governance", {}).get(
        "pii_keywords", ["ssn", "phone", "addr", "zip", "dob", "credit", "account"]
    )

    glossary_entries = []
    all_mappings = []
    abbreviations = {}

    # AI-assisted column understanding (if available)
    ai_column_map = {}  # {table: {source_col: {modern_name, description, data_type_suggestion}}}
    if ai_client:
        for name, schema in all_schemas.items():
            fields = [{"name": col["column_name"], "pic": col.get("pic", ""), "sql_type": col["data_type"]} for col in schema]
            logger.info(f"  AI: Understanding column names for {name}...")
            ai_results = ai_client.understand_columns(
                fields=fields,
                context=name,
                domain=config.get("project", {}).get("description", "government services"),
            )
            if ai_results:
                ai_column_map[name] = {r["source"]: r for r in ai_results}
                logger.info(f"  AI: Mapped {len(ai_results)} columns for {name}")

    for name, schema in all_schemas.items():
        ai_cols = ai_column_map.get(name, {})

        for col in schema:
            cn = col["column_name"]

            # Use AI mapping if available, otherwise fall back to rule-based
            if cn in ai_cols:
                ai_info = ai_cols[cn]
                modern_name = ai_info.get("modern_name", cn.lower().replace("-", "_"))
                description = ai_info.get("description", col.get("pic", col["data_type"]))
                confidence = 0.95  # AI-assisted
            else:
                modern_name = cn.lower().replace("-", "_")
                description = col.get("pic", col["data_type"])
                confidence = 0.9  # Rule-based

            is_pii = any(kw in cn.lower() for kw in pii_keywords)

            glossary_entries.append({
                "name": cn,
                "table": name,
                "system": "legacy",
                "description": description,
                "data_type": col["data_type"],
                "pii": is_pii,
                "confidence": confidence,
            })

            mapping_type = "rename"
            if is_pii and any(kw in cn.lower() for kw in ["ssn"]):
                mapping_type = "transform"
                modern_name = modern_name + "_hash" if not modern_name.endswith("_hash") else modern_name
            elif is_pii and any(kw in cn.lower() for kw in ["eft", "bank"]):
                mapping_type = "archived"

            rationale = f"AI: {description}" if cn in ai_cols else f"From copybook: {col.get('pic', col['data_type'])}"
            all_mappings.append({
                "source": cn,
                "target": modern_name,
                "table": name,
                "type": mapping_type,
                "confidence": confidence,
                "rationale": rationale,
            })

            # Track abbreviation if name differs
            if cn != modern_name:
                abbreviations[cn] = modern_name

    with open(metadata_path / "glossary.json", "w") as f:
        json.dump({"columns": glossary_entries}, f, indent=2)
    with open(metadata_path / "mappings.json", "w") as f:
        json.dump({"mappings": all_mappings}, f, indent=2)
    with open(metadata_path / "abbreviations.yaml", "w") as f:
        yaml.dump({"abbreviations": abbreviations, "_generated_from": "copybook fields"}, f)

    # ── Step 3: Normalization plan ────────────────────────────────
    logger.info("Step 3: Building normalization plan")

    norm_plan = {}
    for name, schema in all_schemas.items():
        columns = [col["column_name"].lower().replace("-", "_") for col in schema]
        filler_cols = [c for c in columns if "filler" in c.lower()]
        data_cols = [c for c in columns if "filler" not in c.lower()]

        # ── Rule 1: Detect repeated column groups → normalize to rows ──
        # When the same structure appears multiple times (physical + mailing
        # address, personal + mobile + work phone), propose a single child
        # table with a type discriminator instead of separate tables.

        ROW_PATTERNS = {
            "addresses": {
                "groups": [
                    {"type_value": "physical", "keywords": ["adr1", "adr2", "city", "st", "zip", "adtyp"],
                     "exclude": ["madr", "maddr", "mcity", "mst", "mzip"]},
                    {"type_value": "mailing", "keywords": ["madr1", "madr2", "maddr", "mcity", "mst", "mzip"]},
                ],
                "type_column": "address_type",
                "standard_columns": ["line_1", "line_2", "city", "state", "zip_code"],
                "min_fields": 3,
            },
            "phones": {
                "groups": [
                    {"type_value": "personal", "keywords": ["ptel", "phon"]},
                    {"type_value": "mobile", "keywords": ["mtel"]},
                    {"type_value": "work", "keywords": ["wtel"]},
                ],
                "type_column": "phone_type",
                "standard_columns": ["phone_number"],
                "min_fields": 1,
            },
        }

        # Non-row-based child entity patterns
        CHILD_PATTERNS = {
            "emergency_contacts": {
                "keywords": ["emrg", "etel", "erel", "emergency"],
                "min_fields": 2,
            },
        }

        child_entities = []
        child_cols_used = set()
        profile = all_profiles.get(name, {})

        # Process row-based patterns
        for entity_name, rp in ROW_PATTERNS.items():
            all_matched = []
            group_count = 0
            for group in rp["groups"]:
                matched = []
                for c in data_cols:
                    c_lower = c.lower()
                    if any(kw in c_lower for kw in group["keywords"]):
                        if "exclude" in group and any(ex in c_lower for ex in group["exclude"]):
                            continue
                        matched.append(c)
                if len(matched) >= rp.get("min_fields", 1):
                    group_count += 1
                    all_matched.extend(matched)

            if group_count >= 1 and all_matched:
                child_entities.append({
                    "name": f"{name}_{entity_name}",
                    "role": "child",
                    "columns": rp["standard_columns"] + [rp["type_column"]],
                    "source_columns": all_matched,
                    "confidence": 0.9,
                    "rationale": (
                        f"{entity_name.replace('_', ' ').title()} — {group_count} groups normalized to rows "
                        f"with {rp['type_column']} discriminator "
                        f"({', '.join(g['type_value'] for g in rp['groups'] if any(any(kw in c.lower() for kw in g['keywords']) for c in all_matched))})"
                    ),
                    "relationships": [{"column": f"{name}_id", "references": f"{name}({data_cols[0]})"}],
                })
                child_cols_used.update(all_matched)

        # Process non-row-based child patterns
        for entity_name, pattern in CHILD_PATTERNS.items():
            matched = [c for c in data_cols if any(kw in c.lower() for kw in pattern["keywords"])]
            if len(matched) >= pattern.get("min_fields", 2):
                child_entities.append({
                    "name": f"{name}_{entity_name}",
                    "role": "child",
                    "columns": matched,
                    "confidence": 0.85,
                    "rationale": f"{entity_name.replace('_', ' ').title()} sub-entity ({len(matched)} fields)",
                    "relationships": [{"column": f"{name}_id", "references": f"{name}({data_cols[0]})"}],
                })
                child_cols_used.update(matched)

        # ── Rule 2: Compliance grouping — isolate PII/financial fields ──
        pii_keywords = config.get("validation", {}).get("governance", {}).get(
            "pii_keywords", ["ssn", "bank", "bact", "brtn", "dln", "eft", "credit"]
        )
        compliance_cols = []
        for c in data_cols:
            if c in child_cols_used:
                continue
            c_lower = c.lower()
            if any(kw in c_lower for kw in ["ssn", "bact", "brtn", "dln", "dlst", "eft", "bank", "routing", "credit", "govt"]):
                compliance_cols.append(c)

        if len(compliance_cols) >= 2:
            child_entities.append({
                "name": f"{name}_identification",
                "role": "child",
                "columns": compliance_cols,
                "confidence": 0.9,
                "rationale": f"Compliance isolation — {len(compliance_cols)} PII/financial fields separated for restricted access (PCI-DSS, HIPAA)",
                "relationships": [{"column": f"{name}_id", "references": f"{name}({data_cols[0]})"}],
            })
            child_cols_used.update(compliance_cols)

        # Primary entity gets remaining non-child, non-filler columns
        primary_cols = [c for c in data_cols if c not in child_cols_used]

        # ── Rule 3: Type inference from profiling ──
        # Annotate columns with inferred types based on profiling data
        type_inferences = {}
        for c in data_cols:
            orig_name = next((col["column_name"] for col in schema if col["column_name"].lower().replace("-", "_") == c), "")
            col_stats = profile.get("columns", {}).get(orig_name, {})
            orig_type = next((col["data_type"] for col in schema if col["column_name"].lower().replace("-", "_") == c), "VARCHAR")
            distinct = col_stats.get("distinct_count", 0)
            freqs = col_stats.get("value_frequencies", [])
            max_len = col_stats.get("max_length", 0)

            # Boolean detection: exactly 2 values that look like Y/N, T/F, etc.
            if distinct == 2 and freqs:
                vals = {str(f.get("value", f) if isinstance(f, dict) else f).upper() for f in freqs}
                if vals in [{"Y", "N"}, {"YES", "NO"}, {"T", "F"}, {"TRUE", "FALSE"}, {"1", "0"}]:
                    type_inferences[c] = {"inferred_type": "BOOLEAN", "reason": f"2 values: {vals}"}
                    continue

            # Date detection: column name contains date keywords AND values match date pattern
            if any(kw in c.lower() for kw in ["_dt", "_dob", "date", "crtdt", "upddt"]):
                if freqs:
                    sample_val = str(freqs[0].get("value", freqs[0]) if isinstance(freqs[0], dict) else freqs[0])
                    if len(sample_val) >= 8 and (sample_val[:4].isdigit() or sample_val[6:10].isdigit()):
                        if max_len and max_len > 12:
                            type_inferences[c] = {"inferred_type": "TIMESTAMPTZ", "reason": f"Date/time pattern in values"}
                        else:
                            type_inferences[c] = {"inferred_type": "DATE", "reason": f"Date pattern in values"}
                        continue

            # Integer detection: all values are numeric
            if orig_type.startswith("VARCHAR") and freqs:
                all_numeric = all(
                    str(f.get("value", f) if isinstance(f, dict) else f).replace("-", "").replace(".", "").isdigit()
                    for f in freqs if str(f.get("value", f) if isinstance(f, dict) else f).strip()
                )
                if all_numeric and distinct > 2:
                    type_inferences[c] = {"inferred_type": "INTEGER", "reason": "All sampled values are numeric"}

        entities = [{
            "name": name,
            "role": "primary",
            "columns": primary_cols,
            "confidence": 0.9,
            "type_inferences": {c: ti for c, ti in type_inferences.items() if c in primary_cols},
            "rationale": (
                f"Primary entity ({len(primary_cols)} core fields, "
                f"{len(child_cols_used)} normalized into {len(child_entities)} child tables, "
                f"{len(filler_cols)} filler removed, "
                f"{len(type_inferences)} type inferences from profiling)"
            ),
        }]
        entities.extend(child_entities)

        # Detect lookup candidates from primary columns
        for c in primary_cols:
            orig_name = next((col["column_name"] for col in schema if col["column_name"].lower().replace("-", "_") == c), "")
            col_stats = profile.get("columns", {}).get(orig_name, {})
            distinct = col_stats.get("distinct_count", 0)
            row_count = profile.get("row_count", 0)
            if 2 < distinct <= 15 and row_count > 0 and (distinct / row_count) < 0.5:
                # Name the lookup meaningfully
                clean_name = c.lower()
                import re as _re_norm
                clean_name = _re_norm.sub(r'^[a-z]{2,3}_', '', clean_name)
                entities.append({
                    "name": f"{clean_name}_lookup",
                    "role": "lookup",
                    "columns": [c],
                    "confidence": 0.7,
                    "rationale": f"Lookup candidate — {distinct} distinct values in {row_count} rows",
                })

        norm_plan[name] = {"entities": entities, "relationships": [], "type_inferences": type_inferences}

    # AI normalization review (if available)
    if ai_client:
        for name, plan_data in norm_plan.items():
            columns = [col["column_name"].lower().replace("-", "_") for col in all_schemas.get(name, [])]
            logger.info(f"  AI: Reviewing normalization for {name}...")
            review = ai_client.review_normalization(
                table_name=name,
                columns=columns,
                proposed_plan=plan_data,
                profiling=all_profiles.get(name, {}),
            )
            if review:
                plan_data["ai_review"] = review
                if review.get("approved"):
                    logger.info(f"  AI: Normalization approved — {review.get('rationale', '')[:100]}")
                else:
                    logger.info(f"  AI: Suggestions — {len(review.get('changes', []))} changes proposed")

    with open(metadata_path / "normalization_plan.json", "w") as f:
        json.dump(norm_plan, f, indent=2)

    # AI data quality assessment (if available)
    ai_quality_findings = {}
    if ai_client:
        for name in all_schemas:
            df = all_dataframes.get(name)
            if df is not None and not df.empty:
                logger.info(f"  AI: Assessing data quality for {name}...")
                sample_str = df.head(5).to_string()
                findings = ai_client.assess_data_quality(
                    table_name=name,
                    profiling_stats=all_profiles.get(name, {}),
                    sample_data=sample_str,
                )
                if findings:
                    ai_quality_findings[name] = findings
                    logger.info(f"  AI: Found {len(findings)} quality issue(s) in {name}")

        if ai_quality_findings:
            with open(metadata_path / "ai_quality_findings.json", "w") as f:
                json.dump(ai_quality_findings, f, indent=2)

    # ── Step 4: Rationalization ───────────────────────────────────
    logger.info("Step 4: Rationalization")

    rat_tables = []
    for name, profile in all_profiles.items():
        cols = profile.get("columns", {})
        avg_null = sum(c.get("null_percent", 0) for c in cols.values()) / len(cols) if cols else 50
        completeness = max(0, 100 - avg_null)
        score = round(completeness * 0.5 + 50, 1)
        rec = "migrate" if score >= 70 else "review" if score >= 40 else "archive"
        rat_tables.append({
            "table": name, "score": score, "recommendation": rec,
            "breakdown": {"completeness": completeness, "query_activity": 0, "downstream": 0, "freshness": 100, "tier": 50},
            "rationale": f"Completeness: {completeness:.0f}/100. Score: {score}/100.",
        })

    migrate = sum(1 for t in rat_tables if t["recommendation"] == "migrate")
    review = sum(1 for t in rat_tables if t["recommendation"] == "review")
    archive = sum(1 for t in rat_tables if t["recommendation"] == "archive")

    rat_report = {
        "tables": rat_tables,
        "summary": {"migrate_count": migrate, "review_count": review, "archive_count": archive},
    }
    with open(metadata_path / "rationalization_report.json", "w") as f:
        json.dump(rat_report, f, indent=2)

    # Markdown report
    md_lines = ["# Migration Scope Rationalization\n"]
    md_lines.append(f"Tables: {len(rat_tables)} | Migrate: {migrate} | Review: {review} | Archive: {archive}\n")
    for t in rat_tables:
        emoji = {"migrate": "🟢", "review": "🟡", "archive": "🔴"}.get(t["recommendation"], "⚪")
        md_lines.append(f"- {emoji} **{t['table']}**: {t['score']}/100 ({t['recommendation']}) — {t['rationale']}")
    with open(metadata_path / "rationalization_report.md", "w") as f:
        f.write("\n".join(md_lines))

    # Migration scope YAML
    scope = {"migrate": [], "review": [], "archive": []}
    for t in rat_tables:
        scope[t["recommendation"]].append(t["table"])
    with open(metadata_path / "migration_scope.yaml", "w") as f:
        yaml.dump(scope, f)

    # ── Step 5: Schema generation for all targets ─────────────────
    logger.info("Step 5: Generating schemas for all target platforms")

    from dm.targets.postgres import get_available_targets, get_target_adapter

    for target_key, display_name in get_available_targets().items():
        adapter = get_target_adapter(target_key)
        target_dir = schema_dir / target_key
        target_dir.mkdir(exist_ok=True)

        full_ddl_parts = [
            f"-- ============================================================",
            f"-- DM Generated Schema — {display_name}",
            f"-- Source: {config.get('project', {}).get('name', 'Mainframe')}",
            f"-- Target: {display_name}",
            f"-- Tables: {len(all_schemas)}",
            f"-- Generated: {datetime.now(timezone.utc).isoformat()}",
            f"-- ============================================================\n",
        ]

        for name, schema in all_schemas.items():
            columns = []
            pk = ""
            for i, col in enumerate(schema):
                cn = col["column_name"].lower().replace("-", "_")
                dt = adapter.map_type(col["data_type"])

                # Use profiling to optimize types
                stats = all_profiles.get(name, {}).get("columns", {}).get(col["column_name"], {})
                if stats:
                    distinct = stats.get("distinct_count", 0)
                    freqs = stats.get("value_frequencies", [])
                    row_count = stats.get("row_count", 0)
                    if distinct == 2 and freqs and row_count > 0:
                        vals = {str(f["value"]).upper() for f in freqs}
                        if vals in [{"Y", "N"}, {"YES", "NO"}, {"T", "F"}, {"TRUE", "FALSE"}, {"1", "0"}]:
                            dt = adapter.map_type("boolean")

                constraints = []
                if i == 0:
                    constraints.append("PRIMARY KEY")
                    pk = cn
                nullable = col.get("is_nullable", "YES") == "YES"
                if not nullable and "PRIMARY KEY" not in constraints:
                    constraints.append("NOT NULL")

                columns.append({
                    "name": cn, "data_type": dt, "nullable": nullable,
                    "constraints": constraints,
                    "comment": f"Source: {col['column_name']} {col.get('pic', '')}".strip(),
                })

            # Render DDL
            ddl = adapter.render_create_table(name, columns, pk)
            (target_dir / f"{name}.sql").write_text(ddl)
            full_ddl_parts.append(ddl)

            # Render transform SQL
            transform_lines = [
                f"-- Transform script: {name}",
                f"-- Target: {adapter.dialect_name()}",
                f"-- Review and customize before execution\n",
                f"INSERT INTO {name} (",
            ]
            col_names = [c["name"] for c in columns]
            transform_lines.append("    " + ",\n    ".join(col_names))
            transform_lines.append(")")
            transform_lines.append("SELECT")
            source_exprs = []
            for col in columns:
                src = col["comment"].replace("Source: ", "").split(" ")[0] if col.get("comment") else col["name"]
                source_exprs.append(src.lower().replace("-", "_"))
            transform_lines.append("    " + ",\n    ".join(source_exprs))
            transform_lines.append(f"FROM {name}_legacy;\n")
            (target_dir / f"{name}_transforms.sql").write_text("\n".join(transform_lines))

        (target_dir / "full_schema.sql").write_text("\n".join(full_ddl_parts))

    # Also write root-level files (postgres default)
    pg_dir = schema_dir / "postgres"
    for f in pg_dir.iterdir():
        target_f = schema_dir / f.name
        if not target_f.exists() or f.name == "full_schema.sql":
            target_f.write_text(f.read_text())

    # Diff report
    diff = {
        "legacy_column_count": sum(len(s) for s in all_schemas.values()),
        "modern_table_count": len(all_schemas),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "columns": {
            "renamed": {
                m["source"]: {"target_table": m["table"], "target_column": m["target"]}
                for m in all_mappings if m["source"] != m["target"]
            },
            "transformed": {
                m["source"]: {"target_table": m["table"], "target_column": m["target"], "transform": "SHA-256"}
                for m in all_mappings if m["type"] == "transform"
            },
            "archived": [m["source"] for m in all_mappings if m["type"] == "archived"],
            "unmapped": [],
        },
    }
    with open(schema_dir / "diff_report.json", "w") as f:
        json.dump(diff, f, indent=2)
    for target_dir in schema_dir.iterdir():
        if target_dir.is_dir():
            with open(target_dir / "diff_report.json", "w") as f:
                json.dump(diff, f, indent=2)

    # Updated datasets and mappings
    updated_datasets = [{"name": name, "legacy_table": name, "generated": True} for name in all_schemas]
    with open(schema_dir / "updated_datasets.yaml", "w") as f:
        yaml.dump({"datasets": updated_datasets}, f)
    with open(schema_dir / "updated_mappings.json", "w") as f:
        json.dump({"mappings": all_mappings}, f, indent=2)

    logger.info("Pipeline complete")

    return {
        "tables": len(all_schemas),
        "columns": sum(len(s) for s in all_schemas.values()),
        "rows": sum(p["row_count"] for p in all_profiles.values()),
        "mappings": len(all_mappings),
        "targets": list(get_available_targets().keys()),
        "migrate": migrate,
        "review": review,
        "archive": archive,
    }
