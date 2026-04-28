"""
DM Pipeline — phase orchestration.

Replaces agents/orchestrator.py with a plugin-aware pipeline that
discovers, instantiates, and runs validators.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pluggy

from dm.config import (
    get_artifacts_path,
    get_metadata_path,
    get_plugin_specs,
    get_scoring_config,
    load_project_config,
)
from dm.connectors.postgres import get_connector
from dm.discovery.pandera_generator import ensure_schemas_exist
from dm.kb.rag import RAGTool
from dm.plugin_manager import get_plugin_manager
from dm.scoring import calculate_confidence, get_traffic_light, score_from_penalties
from dm.validators.base import ValidatorResult
from dm.validators.pre import BUILTIN_PRE_VALIDATORS
from dm.validators.pre.data_quality import DataQualityValidator
from dm.validators.post import BUILTIN_POST_VALIDATORS

logger = logging.getLogger(__name__)


def _collect_plugin_connectors(pm: pluggy.PluginManager) -> Dict:
    """Merge connector registrations from all plugins."""
    connectors = {}
    results = pm.hook.dm_register_connectors()
    for result in results:
        if result:
            connectors.update(result)
    return connectors


def _collect_data_quality_rules(pm: pluggy.PluginManager, dataset: str) -> List[Dict]:
    """Collect cross-field data quality rules from plugins."""
    rules = []
    results = pm.hook.dm_data_quality_rules(dataset=dataset)
    for result in results:
        if result:
            rules.extend(result)
    return rules


def run_validation(
    phase: str,
    dataset: str,
    sample_size: int = 1000,
    project_dir: str = ".",
) -> Dict:
    """Main entry point — run pre or post validation for a dataset.

    Args:
        phase: 'pre' or 'post'.
        dataset: Table/dataset name.
        sample_size: Rows to sample (pre-phase).
        project_dir: Path to the project directory.

    Returns:
        Dict with score, status, artifact_path, and details.
    """
    config = load_project_config(project_dir)

    # Set up plugin manager
    plugin_specs = get_plugin_specs(config)
    pm = get_plugin_manager(plugin_specs, project_dir=project_dir)

    # Get connectors (with plugin extensions)
    plugin_connectors = _collect_plugin_connectors(pm)

    legacy_conn = get_connector(config["connections"]["legacy"], plugin_connectors)
    modern_conn = get_connector(config["connections"]["modern"], plugin_connectors)

    try:
        legacy_conn.connect()
        modern_conn.connect()

        # Create artifact folder
        from dm.reporting.reporter import create_artifact_folder
        artifacts_base = get_artifacts_path(config)
        artifact_folder = create_artifact_folder(artifacts_base)

        # Auto-generate schemas if missing
        ensure_schemas_exist(legacy_conn, modern_conn, dataset, config)

        if phase == "pre":
            result = _run_pre_phase(
                legacy_conn, modern_conn, dataset, sample_size, config, pm, artifact_folder,
            )
        elif phase == "post":
            result = _run_post_phase(
                legacy_conn, modern_conn, dataset, config, pm, artifact_folder,
            )
        else:
            raise ValueError(f"Invalid phase: {phase}. Must be 'pre' or 'post'")

        return result

    finally:
        legacy_conn.close()
        modern_conn.close()


def _run_pre_phase(
    legacy_conn, modern_conn, dataset, sample_size, config, pm, artifact_folder,
) -> Dict:
    """Run all pre-migration validators and generate reports."""

    # Sample data from legacy
    query = f"SELECT * FROM {dataset} ORDER BY RANDOM() LIMIT {sample_size}"
    sample_df = legacy_conn.execute_query(query)

    # Instantiate built-in validators
    validators = [cls() for cls in BUILTIN_PRE_VALIDATORS if cls != DataQualityValidator]

    # Add data quality validator with plugin rules
    plugin_rules = _collect_data_quality_rules(pm, dataset)
    validators.append(DataQualityValidator(plugin_rules=plugin_rules))

    # Add plugin-provided validators
    plugin_validators = pm.hook.dm_pre_validators()
    for result in plugin_validators:
        if result:
            validators.extend(result)

    # Run all validators
    results: List[ValidatorResult] = []
    for validator in validators:
        logger.info(f"Running pre-validator: {validator.name}")
        result = validator.run(legacy_conn, modern_conn, dataset, sample_df, config)
        results.append(result)

    # Calculate scores
    structure_penalty = sum(r.score_penalty for r in results)
    structure_score = score_from_penalties(structure_penalty)

    # Extract governance score (the governance validator reports it in details)
    gov_score = 100
    for r in results:
        if r.name == "governance":
            gov_score = r.details.get("governance_score", 100)

    confidence = calculate_confidence(
        structure_score=structure_score,
        integrity_score=100,  # Not applicable in pre-phase
        governance_score=gov_score,
        config=config,
    )

    # Allow plugins to adjust score
    adjusted_scores = pm.hook.dm_adjust_score(
        phase="pre", base_score=confidence["score"],
        results={r.name: r.details for r in results},
    )
    for adj in adjusted_scores:
        if adj is not None:
            confidence["score"] = adj
            confidence["status"] = get_traffic_light(adj, config)

    # Generate reports
    _generate_pre_reports(results, confidence, dataset, sample_size, config, pm, artifact_folder,
                          structure_score=structure_score, gov_score=gov_score)

    return {
        "phase": "pre",
        "score": confidence["score"],
        "status": confidence["status"],
        "artifact_path": artifact_folder,
        "details": {r.name: r.details for r in results},
        "validators": [
            {"name": r.name, "status": r.status, "penalty": r.score_penalty, "severity": r.severity}
            for r in results
        ],
    }


def _run_post_phase(legacy_conn, modern_conn, dataset, config, pm, artifact_folder) -> Dict:
    """Run all post-migration validators and generate reports."""

    # Instantiate built-in validators
    validators = [cls() for cls in BUILTIN_POST_VALIDATORS]

    # Add plugin-provided validators
    plugin_validators = pm.hook.dm_post_validators()
    for result in plugin_validators:
        if result:
            validators.extend(result)

    # Run all validators
    results: List[ValidatorResult] = []
    for validator in validators:
        logger.info(f"Running post-validator: {validator.name}")
        result = validator.run(legacy_conn, modern_conn, dataset, config)
        results.append(result)

    # Calculate integrity score from penalties
    total_penalty = sum(r.score_penalty for r in results)
    integrity_score = score_from_penalties(total_penalty)

    confidence = {
        "score": round(integrity_score, 2),
        "status": get_traffic_light(integrity_score, config),
    }

    # Allow plugins to adjust score
    adjusted_scores = pm.hook.dm_adjust_score(
        phase="post", base_score=confidence["score"],
        results={r.name: r.details for r in results},
    )
    for adj in adjusted_scores:
        if adj is not None:
            confidence["score"] = adj
            confidence["status"] = get_traffic_light(adj, config)

    # Generate reports
    _generate_post_reports(results, confidence, dataset, config, pm, artifact_folder)

    return {
        "phase": "post",
        "score": confidence["score"],
        "status": confidence["status"],
        "artifact_path": artifact_folder,
        "details": {r.name: r.details for r in results},
        "validators": [
            {"name": r.name, "status": r.status, "penalty": r.score_penalty, "severity": r.severity}
            for r in results
        ],
    }


def _generate_pre_reports(results, confidence, dataset, sample_size, config, pm, artifact_folder,
                          structure_score=None, gov_score=None):
    """Generate pre-phase artifact reports."""
    from dm.reporting.reporter import (
        save_markdown_report,
        save_confidence_score,
        save_run_metadata,
    )
    import os

    # Build a simple readiness report from validator results
    report_lines = [f"# Pre-Migration Readiness Report: {dataset}\n"]
    for r in results:
        icon = {"PASS": "PASS", "WARN": "WARN", "FAIL": "FAIL", "SKIP": "SKIP"}.get(r.status, r.status)
        report_lines.append(f"\n## {r.name} — {icon}\n")
        report_lines.append(f"Severity: {r.severity} | Penalty: {r.score_penalty}\n")
        if r.details:
            for key, val in r.details.items():
                if not isinstance(val, (dict, list)) or len(str(val)) < 200:
                    report_lines.append(f"- **{key}**: {val}\n")

    # Add plugin extra sections
    extra_sections = pm.hook.dm_extra_report_sections(
        phase="pre", results={r.name: r.details for r in results},
    )
    for sections in extra_sections:
        if sections:
            report_lines.extend(sections)

    save_markdown_report("\n".join(report_lines), os.path.join(artifact_folder, "readiness_report.md"))
    save_confidence_score(confidence["score"], confidence["status"], os.path.join(artifact_folder, "confidence_score.txt"))

    # Generate schema_diff.md from schema_diff validator results
    for r in results:
        if r.name == "schema_diff" and r.details.get("schema_diff"):
            from dm.discovery.schema_introspector import generate_schema_diff_report
            diff_md = generate_schema_diff_report(
                r.details.get("legacy_schema", {}),
                r.details.get("modern_schema", {}),
                dataset,
            )
            save_markdown_report(diff_md, os.path.join(artifact_folder, "schema_diff.md"))
            break

    # Generate governance_report.csv from governance validator results
    for r in results:
        if r.name == "governance" and r.details:
            import csv
            gov_rows = []
            for col in r.details.get("pii_columns", []):
                gov_rows.append({"category": "PII", "item": col, "status": "VIOLATION", "detail": "Plaintext PII detected"})
            for col in r.details.get("naming_violations", []):
                gov_rows.append({"category": "Naming", "item": col, "status": "WARNING", "detail": "Does not match naming convention"})
            for col in r.details.get("missing_required", []):
                gov_rows.append({"category": "Required", "item": col, "status": "VIOLATION", "detail": "Required field missing"})
            for item in r.details.get("null_violations", []):
                col_name = item if isinstance(item, str) else item.get("column", str(item))
                gov_rows.append({"category": "Null", "item": col_name, "status": "WARNING", "detail": "Exceeds null threshold"})
            # Add passing checks for columns without issues
            if not gov_rows:
                gov_rows.append({"category": "Overall", "item": "all_checks", "status": "PASS", "detail": "No governance issues"})
            gov_path = os.path.join(artifact_folder, "governance_report.csv")
            with open(gov_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["category", "item", "status", "detail"])
                writer.writeheader()
                writer.writerows(gov_rows)
            break

    save_run_metadata(
        {"phase": "pre", "dataset": dataset, "sample_size": sample_size,
         "confidence_score": confidence["score"], "status": confidence["status"],
         "structure_score": structure_score,
         "governance_score": gov_score},
        os.path.join(artifact_folder, "run_metadata.json"),
    )


def _generate_post_reports(results, confidence, dataset, config, pm, artifact_folder):
    """Generate post-phase artifact reports."""
    from dm.reporting.reporter import (
        save_markdown_report,
        save_confidence_score,
        save_run_metadata,
    )
    import os

    report_lines = [f"# Post-Migration Reconciliation Report: {dataset}\n"]
    for r in results:
        icon = {"PASS": "PASS", "WARN": "WARN", "FAIL": "FAIL", "SKIP": "SKIP"}.get(r.status, r.status)
        report_lines.append(f"\n## {r.name} — {icon}\n")
        report_lines.append(f"Severity: {r.severity} | Penalty: {r.score_penalty}\n")
        if r.details:
            for key, val in r.details.items():
                if not isinstance(val, (dict, list)) or len(str(val)) < 200:
                    report_lines.append(f"- **{key}**: {val}\n")

    extra_sections = pm.hook.dm_extra_report_sections(
        phase="post", results={r.name: r.details for r in results},
    )
    for sections in extra_sections:
        if sections:
            report_lines.extend(sections)

    save_markdown_report("\n".join(report_lines), os.path.join(artifact_folder, "reconciliation_report.md"))
    save_confidence_score(confidence["score"], confidence["status"], os.path.join(artifact_folder, "confidence_score.txt"))
    save_run_metadata(
        {"phase": "post", "dataset": dataset,
         "confidence_score": confidence["score"], "status": confidence["status"]},
        os.path.join(artifact_folder, "run_metadata.json"),
    )


# ── New Pipeline Functions ────────────────────────────────────────────


def run_enrichment(
    tables: list,
    project_dir: str = ".",
) -> Dict:
    """Enrich metadata using OpenMetadata profiling, lineage, and glossary.

    Pulls OM catalog data and generates enriched glossary.json + mappings.json.
    """
    from dm.config import get_metadata_path, get_openmetadata_config
    from dm.discovery.metadata_generator import generate_metadata_from_om
    from dm.discovery.om_plugin import OpenMetadataPlugin
    from dm.discovery.openmetadata_enricher import OpenMetadataEnricher

    config = load_project_config(project_dir)
    pm = get_plugin_manager(get_plugin_specs(config), project_dir=project_dir)

    om_config = get_openmetadata_config(config)
    om = OpenMetadataEnricher(om_config)
    om.connect()

    om_plugin = OpenMetadataPlugin(om)
    pm.register(om_plugin, name="openmetadata")

    # Try to connect to modern DB for COBOL-aware column matching
    modern_conn = None
    try:
        from dm.pipeline import get_connector
        plugin_connectors = {}
        results = pm.hook.dm_register_connectors()
        for r in results:
            if r:
                plugin_connectors.update(r)
        modern_conn = get_connector(config["connections"]["modern"], plugin_connectors)
        modern_conn.connect()
    except Exception:
        logger.info("Modern DB not available — using COBOL abbreviation expansion for mappings")

    try:
        if not tables:
            tables = om.get_tables()

        metadata_path = get_metadata_path(config)
        glossary, mappings = generate_metadata_from_om(
            om_enricher=om,
            tables=list(tables),
            output_dir=str(metadata_path),
            plugin_manager=pm,
            modern_conn=modern_conn,
        )

        return {
            "glossary_count": len(glossary.get("columns", [])),
            "mappings_count": len(mappings.get("mappings", [])),
            "metadata_path": str(metadata_path),
            "tables": list(tables),
        }
    finally:
        om.close()
        if modern_conn:
            try:
                modern_conn.close()
            except Exception:
                pass


def run_schema_generation(
    tables: list,
    project_dir: str = ".",
    normalize: bool = True,
    dry_run: bool = False,
) -> Dict:
    """Generate normalized PostgreSQL schema from OM-enriched legacy metadata.

    Full pipeline: OM discovery → enrich → normalize → generate DDL.
    """
    import json

    from dm.config import (
        get_generated_schema_path,
        get_metadata_path,
        get_openmetadata_config,
        get_schema_generation_config,
    )
    from dm.discovery.normalization_analyzer import NormalizationAnalyzer
    from dm.discovery.om_plugin import OpenMetadataPlugin
    from dm.discovery.openmetadata_enricher import OpenMetadataEnricher
    from dm.discovery.schema_gen import SchemaGenerator

    config = load_project_config(project_dir)
    pm = get_plugin_manager(get_plugin_specs(config), project_dir=project_dir)
    gen_config = get_schema_generation_config(config)

    om_config = get_openmetadata_config(config)
    om = OpenMetadataEnricher(om_config)
    om.connect()

    om_plugin = OpenMetadataPlugin(om)
    pm.register(om_plugin, name="openmetadata")

    try:
        # Resolve tables
        if not tables:
            datasets = config.get("datasets", [])
            tables = [
                d["name"] if isinstance(d, dict) else d
                for d in datasets
            ]
        if not tables:
            tables = om.get_tables()

        metadata_path = get_metadata_path(config)
        output_path = get_generated_schema_path(config)

        # Step 1: Ensure enriched metadata exists
        glossary_file = metadata_path / "glossary.json"
        mappings_file = metadata_path / "mappings.json"
        if not glossary_file.exists():
            logger.info("No glossary found — running enrichment first")
            from dm.discovery.metadata_generator import generate_metadata_from_om
            generate_metadata_from_om(
                om_enricher=om, tables=tables,
                output_dir=str(metadata_path), plugin_manager=pm,
            )

        with open(glossary_file) as f:
            glossary = json.load(f)
        with open(mappings_file) as f:
            mappings = json.load(f)

        all_results = []

        for table in tables:
            logger.info(f"Generating schema for: {table}")

            # Step 2: Get legacy schema + profiling from OM
            legacy_schema = om.get_table_schema(table)
            try:
                om_profile = om.get_table_profile(table)
            except Exception:
                om_profile = {"columns": {}}
            om_stats = om_profile.get("columns", {})

            # Step 3: Normalization analysis
            if normalize and gen_config.get("normalization", {}).get("enabled", True):
                analyzer = NormalizationAnalyzer(om, pm, gen_config)
                plan = analyzer.analyze_table(table)
                # Save normalization plan
                if not dry_run:
                    analyzer.save_plan({table: plan}, metadata_path)
            else:
                # No normalization — treat as single entity
                from dm.discovery.normalization_analyzer import (
                    NormalizationPlan,
                    ProposedEntity,
                )
                cols = [
                    {"source_col": c["column_name"], "data_type": c.get("data_type", "VARCHAR"),
                     "nullable": c.get("is_nullable", "YES") == "YES", "transform": None}
                    for c in legacy_schema
                ]
                plan = NormalizationPlan(
                    source_table=table,
                    entities=[ProposedEntity(
                        name=table, columns=cols,
                        primary_key=f"{table.rstrip('s')}_id",
                        source_table=table, role="primary",
                        rationale="Single-table migration (normalization disabled)",
                    )],
                    relationships=[], lookup_tables=[],
                    confidence=0.9, rationale="No normalization applied",
                )

            # Step 4: Generate schema
            generator = SchemaGenerator(config, pm, om)
            result = generator.generate(
                normalization_plan=plan,
                legacy_schema=legacy_schema,
                glossary=glossary,
                mappings=mappings,
                om_profiles=om_stats,
            )

            # Step 5: Save artifacts
            if not dry_run:
                generator.save_artifacts(result, output_path)

            all_results.append(result)

        # Combine results
        total_tables = sum(len(r.tables) for r in all_results)
        avg_confidence = (
            sum(r.confidence for r in all_results) / len(all_results)
            if all_results else 0.0
        )
        full_ddl = "\n\n".join(r.full_ddl for r in all_results)

        return {
            "table_count": total_tables,
            "confidence": round(avg_confidence, 2),
            "output_path": str(output_path),
            "full_ddl": full_ddl,
            "results": all_results,
        }
    finally:
        om.close()


def run_prove(
    dataset: str,
    project_dir: str = ".",
) -> Dict:
    """Combine pre+post validation artifacts into a migration proof report."""
    import json
    import os

    from dm.reporting.reporter import (
        create_artifact_folder,
        save_markdown_report,
        save_run_metadata,
    )

    config = load_project_config(project_dir)
    artifacts_base = get_artifacts_path(config)

    # Find latest pre and post runs for this dataset
    pre_run = None
    post_run = None

    if Path(artifacts_base).exists():
        for run_dir in sorted(Path(artifacts_base).iterdir(), reverse=True):
            meta_file = run_dir / "run_metadata.json"
            if not meta_file.exists():
                continue
            meta = json.loads(meta_file.read_text())
            if meta.get("dataset") != dataset:
                continue
            if meta.get("phase") == "pre" and not pre_run:
                pre_run = meta
                pre_run["_dir"] = str(run_dir)
            elif meta.get("phase") == "post" and not post_run:
                post_run = meta
                post_run["_dir"] = str(run_dir)
            if pre_run and post_run:
                break

    pre_score = pre_run.get("confidence_score", "N/A") if pre_run else "N/A"
    post_score = post_run.get("confidence_score", "N/A") if post_run else "N/A"

    # Calculate final score
    final_score = "N/A"
    status = "INCOMPLETE"
    if isinstance(pre_score, (int, float)) and isinstance(post_score, (int, float)):
        final_score = round((pre_score + post_score) / 2, 2)
        from dm.scoring import get_traffic_light
        status = get_traffic_light(final_score, config)

    # Generate proof report
    proof_folder = create_artifact_folder(artifacts_base)
    report_lines = [
        f"# Migration Proof Report: {dataset}\n",
        f"## Pre-Migration Assessment\n",
        f"- Score: {pre_score}\n",
        f"- Status: {pre_run.get('status', 'N/A') if pre_run else 'Not run'}\n",
        f"- Run: {pre_run.get('_dir', 'N/A') if pre_run else 'N/A'}\n",
        f"\n## Post-Migration Verification\n",
        f"- Score: {post_score}\n",
        f"- Status: {post_run.get('status', 'N/A') if post_run else 'Not run'}\n",
        f"- Run: {post_run.get('_dir', 'N/A') if post_run else 'N/A'}\n",
        f"\n## Final Assessment\n",
        f"- Combined Score: {final_score}\n",
        f"- Status: {status}\n",
    ]

    save_markdown_report(
        "\n".join(report_lines),
        os.path.join(proof_folder, "proof_report.md"),
    )
    save_run_metadata(
        {
            "phase": "prove",
            "dataset": dataset,
            "pre_score": pre_score,
            "post_score": post_score,
            "confidence_score": final_score if isinstance(final_score, (int, float)) else 0,
            "status": status,
        },
        os.path.join(proof_folder, "run_metadata.json"),
    )

    return {
        "pre_score": pre_score,
        "post_score": post_score,
        "final_score": final_score,
        "status": status,
        "report_path": proof_folder,
    }


def run_rationalization(
    tables: list,
    project_dir: str = ".",
) -> Dict:
    """Rationalize migration scope using OM catalog analysis (L-Discoverer)."""
    from dm.config import get_openmetadata_config
    from dm.discovery.openmetadata_enricher import OpenMetadataEnricher
    from dm.rationalization.discoverer import MigrationRationalizer

    config = load_project_config(project_dir)
    pm = get_plugin_manager(get_plugin_specs(config), project_dir=project_dir)

    om_config = get_openmetadata_config(config)
    om = OpenMetadataEnricher(om_config)
    om.connect()

    try:
        if not tables:
            tables = om.get_tables()

        rationalizer = MigrationRationalizer(om, pm)
        report = rationalizer.rationalize(list(tables))

        metadata_path = Path(config.get("_project_dir", ".")) / config.get(
            "metadata", {}
        ).get("path", "./metadata")
        rationalizer.save_report(report, metadata_path)

        return {
            "total": len(report.tables),
            "migrate_count": report.migrate_count,
            "review_count": report.review_count,
            "archive_count": report.archive_count,
            "scope_reduction_pct": report.scope_reduction_pct,
            "report_path": str(metadata_path),
        }
    finally:
        om.close()


def run_conversion(
    source_path: str,
    target: str = "postgres",
    ai_refine: bool = False,
    dry_run: bool = False,
    project_dir: str = ".",
) -> Dict:
    """Convert legacy SQL/ETL to modern target platform code (L-Converter)."""
    from dm.conversion.converter import CodeConverter

    config = load_project_config(project_dir)
    pm = get_plugin_manager(get_plugin_specs(config), project_dir=project_dir)

    converter = CodeConverter(config, pm)
    result = converter.convert(
        source_path=source_path,
        target=target,
        ai_refine=ai_refine,
        dry_run=dry_run,
    )

    return {
        "source": result.source_path,
        "target": result.target_dialect,
        "output_path": getattr(result, "output_path", result.source_path),
        "warning_count": len(result.warnings),
        "ai_suggestions": result.ai_suggestions,
        "prompt_file": result.prompt_file_path,
    }


def run_ingestion(
    project_dir: str = ".",
    plan_only: bool = False,
    dataset: str = None,
    resume: bool = False,
) -> Dict:
    """Orchestrate data migration with dependency ordering (L-Ingestor)."""
    from dm.ingestion.executor import MigrationExecutor
    from dm.ingestion.planner import MigrationPlanner

    config = load_project_config(project_dir)
    pm = get_plugin_manager(get_plugin_specs(config), project_dir=project_dir)

    planner = MigrationPlanner(config, pm)

    if dataset:
        tables = [dataset]
    else:
        tables = [ds["name"] for ds in config.get("datasets", [])]
    plan = planner.generate_plan(tables)

    if plan_only:
        return {
            "plan": [
                {
                    "table": step.table,
                    "strategy": step.strategy,
                    "depends_on": step.depends_on,
                }
                for step in plan.strategies.values()
            ],
        }

    executor = MigrationExecutor(config, pm)
    result = executor.execute(plan, resume=resume)
    return result


def run_observation(
    project_dir: str = ".",
    once: bool = False,
    set_baseline: bool = False,
    show_history: bool = False,
    interval: str = "6h",
) -> Dict:
    """Monitor pipeline health and detect drift (L-Observer)."""
    from dm.config import get_openmetadata_config
    from dm.connectors.postgres import get_connector
    from dm.observer.observer import PipelineObserver

    config = load_project_config(project_dir)
    pm = get_plugin_manager(get_plugin_specs(config), project_dir=project_dir)

    observer = PipelineObserver(config, pm)

    if set_baseline:
        plugin_connectors = {}
        results = pm.hook.dm_register_connectors()
        for r in results:
            if r:
                plugin_connectors.update(r)
        modern_conn = get_connector(config["connections"]["modern"], plugin_connectors)
        modern_conn.connect()
        try:
            observer.set_baseline(modern_conn)
            return {"baseline_path": str(observer.baseline_manager.baseline_path)}
        finally:
            modern_conn.close()

    if show_history:
        history_fn = getattr(observer, "get_history", None)
        if history_fn:
            return {"history": history_fn()}
        return {"history": [], "message": "Observation history not yet implemented"}

    # Run checks once
    plugin_connectors = {}
    results = pm.hook.dm_register_connectors()
    for r in results:
        if r:
            plugin_connectors.update(r)
    modern_conn = get_connector(config["connections"]["modern"], plugin_connectors)
    modern_conn.connect()
    try:
        check_results = observer.run_once(modern_conn)
        drifts = [r for r in check_results if r.get("drifted") or r.get("anomaly") or r.get("stale") or r.get("violations", 0) > 0]
        return {
            "checks_run": len(check_results),
            "drift_count": len(drifts),
            "drifts": [
                {"check": d.get("check", ""), "severity": "WARN", "detail": str(d)}
                for d in drifts
            ],
        }
    finally:
        modern_conn.close()
