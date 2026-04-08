"""
DM CLI — Click-based command-line interface.

Commands:
    dm init <name>              Scaffold a new migration project
    dm discover                 Introspect databases and generate metadata
    dm validate --phase pre|post   Run validation
    dm dashboard                Launch Streamlit dashboard
    dm status                   Show latest run scores
"""

import logging
import os
import shutil
import sys
from pathlib import Path

import click

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


@click.group()
@click.version_option(version="0.1.0", prog_name="dm")
def cli():
    """DM — Lockpicks Data Migration.

    Validate legacy-to-modern data migrations with pluggable checks,
    confidence scoring, and audit-ready reports.
    """


@cli.command()
@click.argument("name")
@click.option("--template", default=None, help="Path to template directory")
def init(name, template):
    """Scaffold a new migration project.

    Creates a project directory with project.yaml, metadata/, plugins/,
    and schemas/ subdirectories.
    """
    project_dir = Path("projects") / name
    if project_dir.exists():
        click.echo(f"Project directory already exists: {project_dir}", err=True)
        sys.exit(1)

    # Create structure
    (project_dir / "metadata").mkdir(parents=True)
    (project_dir / "plugins").mkdir(parents=True)
    (project_dir / "schemas" / "legacy").mkdir(parents=True)
    (project_dir / "schemas" / "modern").mkdir(parents=True)
    (project_dir / "artifacts").mkdir(parents=True)

    # Write template project.yaml
    template_yaml = _get_project_template(name)
    (project_dir / "project.yaml").write_text(template_yaml)

    # Write plugin template
    (project_dir / "plugins" / "__init__.py").write_text("")
    (project_dir / "plugins" / "my_plugin.py").write_text(_get_plugin_template(name))

    click.echo(f"Created project: {project_dir}")
    click.echo(f"  Edit {project_dir / 'project.yaml'} to configure connections")
    click.echo(f"  Edit {project_dir / 'plugins' / 'my_plugin.py'} to add domain rules")


@cli.command()
@click.option("--project", "-p", default=".", help="Project directory")
@click.option("--tables", "-t", multiple=True, help="Specific tables (default: all from config)")
@click.option("--no-interactive", is_flag=True, help="Skip interactive prompts")
@click.option("--enrich", is_flag=True, help="Chain into OM enrichment after discovery")
def discover(project, tables, no_interactive, enrich):
    """Introspect databases and generate metadata (glossary + mappings)."""
    from dm.config import get_metadata_path, get_openmetadata_config, get_plugin_specs, load_project_config
    from dm.discovery.metadata_generator import generate_metadata, generate_metadata_from_om
    from dm.plugin_manager import get_plugin_manager

    config = load_project_config(project)
    pm = get_plugin_manager(get_plugin_specs(config), project_dir=project)

    # Resolve tables
    if not tables:
        datasets = config.get("datasets", [])
        tables = [
            d["name"] if isinstance(d, dict) else d
            for d in datasets
        ]

    metadata_path = get_metadata_path(config)

    # Check if OM enrichment mode (no modern DB required)
    if enrich:
        from dm.discovery.openmetadata_enricher import OpenMetadataEnricher
        from dm.discovery.om_plugin import OpenMetadataPlugin

        om_config = get_openmetadata_config(config)
        om = OpenMetadataEnricher(om_config)
        om.connect()

        # Register OM plugin
        om_plugin = OpenMetadataPlugin(om)
        pm.register(om_plugin, name="openmetadata")

        # If no tables specified, discover from OM
        if not tables:
            tables = om.get_tables()

        try:
            generate_metadata_from_om(
                om_enricher=om,
                tables=list(tables),
                output_dir=str(metadata_path),
                plugin_manager=pm,
            )
            click.echo(f"OM-enriched metadata generated: {metadata_path}")
        finally:
            om.close()
    else:
        from dm.connectors.postgres import get_connector

        legacy_conn = get_connector(config["connections"]["legacy"])
        modern_conn = get_connector(config["connections"]["modern"])
        try:
            legacy_conn.connect()
            modern_conn.connect()
            generate_metadata(
                legacy_conn, modern_conn,
                tables=list(tables),
                output_dir=str(metadata_path),
                plugin_manager=pm,
                interactive=not no_interactive,
            )
            click.echo(f"Metadata generated: {metadata_path}")
        finally:
            legacy_conn.close()
            modern_conn.close()


@cli.command()
@click.option("--phase", "-ph", required=True, type=click.Choice(["pre", "post"]))
@click.option("--project", "-p", default=".", help="Project directory")
@click.option("--dataset", "-d", required=True, help="Dataset/table to validate")
@click.option("--sample", "-s", default=1000, type=int, help="Sample size (pre-phase)")
def validate(phase, project, dataset, sample):
    """Run pre- or post-migration validation."""
    from dm.pipeline import run_validation

    result = run_validation(
        phase=phase,
        dataset=dataset,
        sample_size=sample,
        project_dir=project,
    )

    # Print summary
    click.echo("")
    click.echo("=" * 60)
    click.echo("           VALIDATION COMPLETE")
    click.echo("=" * 60)
    click.echo(f"\nPhase:       {phase.upper()}")
    click.echo(f"Dataset:     {dataset}")
    click.echo("")
    click.echo("=" * 60)
    click.echo(f"  MIGRATION CONFIDENCE: {result['score']}/100")

    status_emoji = {"GREEN": "G", "YELLOW": "Y", "RED": "R"}.get(result["status"], "?")
    click.echo(f"  STATUS: [{status_emoji}] {result['status']}")
    click.echo("=" * 60)
    click.echo(f"\nArtifacts:   {result['artifact_path']}")
    click.echo("=" * 60)

    if result["status"] == "RED":
        click.echo(f"\n[{status_emoji}] VALIDATION FAILED")
        click.echo("   Action Required: Review artifacts and fix issues")
        sys.exit(1)
    elif result["status"] == "YELLOW":
        click.echo(f"\n[{status_emoji}] VALIDATION WARNING")
        click.echo("   Recommendation: Review findings and address warnings")
    else:
        click.echo(f"\n[{status_emoji}] VALIDATION PASSED")
        click.echo("   Status: Safe to proceed with migration")


@cli.command("enrich")
@click.option("--project", "-p", default=".", help="Project directory")
@click.option("--tables", "-t", multiple=True, help="Specific tables (default: all)")
def enrich(project, tables):
    """Enrich metadata using OpenMetadata profiling, lineage, and glossary."""
    from dm.pipeline import run_enrichment

    result = run_enrichment(tables=list(tables), project_dir=project)

    click.echo("")
    click.echo(f"Enrichment complete: {result['glossary_count']} glossary entries")
    click.echo(f"Artifacts: {result.get('metadata_path', '')}")


@cli.command("generate-schema")
@click.option("--project", "-p", default=".", help="Project directory")
@click.option("--dataset", "-d", multiple=True, help="Specific datasets")
@click.option("--all", "all_datasets", is_flag=True, help="Process all datasets")
@click.option("--normalize/--no-normalize", default=True, help="Run normalization analysis")
@click.option("--dry-run", is_flag=True, help="Preview without writing files")
@click.option("--output", "-o", default=None, help="Output directory override")
def generate_schema(project, dataset, all_datasets, normalize, dry_run, output):
    """Generate normalized PostgreSQL schema from OM-enriched legacy metadata."""
    from dm.pipeline import run_schema_generation

    tables = list(dataset)
    result = run_schema_generation(
        tables=tables,
        project_dir=project,
        normalize=normalize,
        dry_run=dry_run,
    )

    click.echo("")
    click.echo("=" * 60)
    click.echo("       SCHEMA GENERATION COMPLETE")
    click.echo("=" * 60)
    click.echo(f"\nTables generated: {result.get('table_count', 0)}")
    click.echo(f"Confidence:       {result.get('confidence', 0)}/1.00")

    if dry_run:
        click.echo("\n[DRY RUN] No files written.")
        click.echo("\nGenerated DDL preview:")
        click.echo("-" * 60)
        click.echo(result.get("full_ddl", "")[:2000])
        if len(result.get("full_ddl", "")) > 2000:
            click.echo("... (truncated)")
    else:
        click.echo(f"\nArtifacts: {result.get('output_path', '')}")

    click.echo("=" * 60)


@cli.command("prove")
@click.option("--project", "-p", default=".", help="Project directory")
@click.option("--dataset", "-d", required=True, help="Dataset to prove")
def prove(project, dataset):
    """Generate migration proof report combining pre+post validation artifacts."""
    from dm.pipeline import run_prove

    result = run_prove(dataset=dataset, project_dir=project)

    click.echo("")
    click.echo("=" * 60)
    click.echo("       MIGRATION PROOF REPORT")
    click.echo("=" * 60)
    click.echo(f"\nDataset:     {dataset}")
    click.echo(f"Pre-score:   {result.get('pre_score', 'N/A')}")
    click.echo(f"Post-score:  {result.get('post_score', 'N/A')}")
    click.echo(f"Final:       {result.get('final_score', 'N/A')}")
    click.echo(f"Status:      {result.get('status', 'N/A')}")
    click.echo(f"\nReport: {result.get('report_path', '')}")
    click.echo("=" * 60)


@cli.command("rationalize")
@click.option("--project", "-p", default=".", help="Project directory")
@click.option("--tables", "-t", multiple=True, help="Specific tables (default: all from OM)")
def rationalize(project, tables):
    """Analyze legacy catalog and recommend migration scope (L-Discoverer)."""
    from dm.pipeline import run_rationalization

    result = run_rationalization(tables=list(tables), project_dir=project)

    click.echo("")
    click.echo("=" * 60)
    click.echo("       MIGRATION SCOPE RATIONALIZATION")
    click.echo("=" * 60)
    click.echo(f"\nTables analyzed: {result.get('total', 0)}")
    click.echo(f"  Migrate:      {result.get('migrate_count', 0)}")
    click.echo(f"  Review:       {result.get('review_count', 0)}")
    click.echo(f"  Archive:      {result.get('archive_count', 0)}")
    reduction = result.get("scope_reduction_pct", 0)
    if reduction > 0:
        click.echo(f"\nScope reduction: {reduction:.0f}%")
    click.echo(f"\nReport: {result.get('report_path', '')}")
    click.echo("=" * 60)


@cli.command("convert")
@click.option("--project", "-p", default=".", help="Project directory")
@click.option("--source", "-s", required=True, help="Source SQL file or directory")
@click.option("--target", "-t", default="postgres", help="Target platform (postgres, snowflake, bigquery)")
@click.option("--ai-refine", is_flag=True, help="Use Claude AI for refinement (requires API key)")
@click.option("--dry-run", is_flag=True, help="Preview without writing files")
def convert(project, source, target, ai_refine, dry_run):
    """Translate legacy SQL/ETL to modern target platform code (L-Converter)."""
    from dm.pipeline import run_conversion

    result = run_conversion(
        source_path=source,
        target=target,
        ai_refine=ai_refine,
        dry_run=dry_run,
        project_dir=project,
    )

    click.echo("")
    click.echo("=" * 60)
    click.echo("       CODE CONVERSION COMPLETE")
    click.echo("=" * 60)
    click.echo(f"\nSource:    {source}")
    click.echo(f"Target:    {target}")
    click.echo(f"Warnings:  {result.get('warning_count', 0)}")
    if ai_refine and result.get("ai_suggestions"):
        click.echo(f"AI suggestions: {len(result['ai_suggestions'])}")
    if result.get("prompt_file"):
        click.echo(f"\nAI prompt file: {result['prompt_file']}")
    if dry_run:
        click.echo("\n[DRY RUN] No files written.")
    else:
        click.echo(f"\nOutput: {result.get('output_path', '')}")
    click.echo("=" * 60)


@cli.command("ingest")
@click.option("--project", "-p", default=".", help="Project directory")
@click.option("--plan", "plan_only", is_flag=True, help="Generate migration plan only")
@click.option("--execute", "execute", is_flag=True, help="Execute migration plan")
@click.option("--dataset", "-d", default=None, help="Specific dataset to ingest")
@click.option("--resume", is_flag=True, help="Resume from last checkpoint")
def ingest(project, plan_only, execute, dataset, resume):
    """Orchestrate data migration with dependency ordering (L-Ingestor)."""
    from dm.pipeline import run_ingestion

    if not plan_only and not execute:
        click.echo("Specify --plan or --execute", err=True)
        sys.exit(1)

    result = run_ingestion(
        project_dir=project,
        plan_only=plan_only,
        dataset=dataset,
        resume=resume,
    )

    click.echo("")
    click.echo("=" * 60)
    if plan_only:
        click.echo("       MIGRATION PLAN")
        click.echo("=" * 60)
        for step in result.get("plan", []):
            deps = ", ".join(step.get("depends_on", [])) or "none"
            click.echo(f"  {step['table']:<25} strategy: {step['strategy']:<15} deps: {deps}")
    else:
        click.echo("       MIGRATION EXECUTION")
        click.echo("=" * 60)
        click.echo(f"\nTables migrated:  {result.get('completed', 0)}")
        click.echo(f"Tables failed:    {result.get('failed', 0)}")
        click.echo(f"Tables pending:   {result.get('pending', 0)}")
        total_rows = result.get("total_rows", 0)
        click.echo(f"Total rows:       {total_rows}")
        click.echo(f"\nState: {result.get('state_path', '')}")
    click.echo("=" * 60)


@cli.command("observe")
@click.option("--project", "-p", default=".", help="Project directory")
@click.option("--once", is_flag=True, help="Run checks once and exit")
@click.option("--set-baseline", "set_baseline", is_flag=True, help="Capture current state as baseline")
@click.option("--history", is_flag=True, help="Show observation history")
@click.option("--interval", default="6h", help="Check interval (e.g., 6h, 30m)")
def observe(project, once, set_baseline, history, interval):
    """Monitor post-migration pipeline health and detect drift (L-Observer)."""
    from dm.pipeline import run_observation

    result = run_observation(
        project_dir=project,
        once=once,
        set_baseline=set_baseline,
        show_history=history,
        interval=interval,
    )

    if set_baseline:
        click.echo(f"Baseline captured: {result.get('baseline_path', '')}")
        return

    if history:
        click.echo("\nObservation History:")
        click.echo("-" * 60)
        for entry in result.get("history", []):
            click.echo(f"  {entry.get('timestamp', '')}  checks: {entry.get('checks_run', 0)}  drift: {entry.get('drift_count', 0)}")
        return

    click.echo("")
    click.echo("=" * 60)
    click.echo("       PIPELINE OBSERVATION")
    click.echo("=" * 60)
    drift_count = result.get("drift_count", 0)
    click.echo(f"\nChecks run:    {result.get('checks_run', 0)}")
    click.echo(f"Drift detected: {drift_count}")
    if drift_count > 0:
        for d in result.get("drifts", [])[:5]:
            click.echo(f"  [{d.get('severity', 'WARN')}] {d.get('check', '')}: {d.get('detail', '')}")
    else:
        click.echo("  No drift detected. Pipeline healthy.")
    click.echo("=" * 60)


@cli.command()
@click.option("--project", "-p", default=".", help="Project directory")
def status(project):
    """Show latest run scores across all datasets."""
    import json
    from dm.config import get_artifacts_path, load_project_config

    config = load_project_config(project)
    artifacts_base = get_artifacts_path(config)

    if not Path(artifacts_base).exists():
        click.echo("No runs found.")
        return

    runs = sorted(Path(artifacts_base).iterdir(), reverse=True)
    click.echo(f"\nLatest runs ({artifacts_base}):\n")
    click.echo(f"{'Run':<35} {'Phase':<8} {'Dataset':<15} {'Score':<10} {'Status'}")
    click.echo("-" * 80)

    for run_dir in runs[:10]:
        metadata_file = run_dir / "run_metadata.json"
        if metadata_file.exists():
            meta = json.loads(metadata_file.read_text())
            click.echo(
                f"{run_dir.name:<35} "
                f"{meta.get('phase', '?'):<8} "
                f"{meta.get('dataset', '?'):<15} "
                f"{meta.get('confidence_score', '?'):<10} "
                f"{meta.get('status', '?')}"
            )


@cli.command()
@click.option("--project", "-p", default=".", help="Project directory")
def dashboard(project):
    """Launch the Streamlit interactive dashboard."""
    import subprocess

    # Look for dashboard.py in repo root (next to dm/ package)
    repo_root = Path(__file__).parent.parent
    dashboard_path = repo_root / "dashboard.py"
    if not dashboard_path.exists():
        # Fallback: dm/reporting/dashboard.py or project-level
        dashboard_path = Path(__file__).parent / "reporting" / "dashboard.py"
    if not dashboard_path.exists():
        dashboard_path = Path(project) / "dashboard.py"
    if not dashboard_path.exists():
        click.echo("Dashboard not found.", err=True)
        sys.exit(1)

    project_abs = str(Path(project).resolve())
    subprocess.run(["streamlit", "run", str(dashboard_path), "--", "--project", project_abs])


def _get_project_template(name: str) -> str:
    """Generate a template project.yaml."""
    return f"""# DM Project Configuration
# Generated by: dm init {name}

project:
  name: "{name}"
  description: "Data migration validation project"
  version: "1.0"

connections:
  legacy:
    type: postgres
    host: ${{DB_LEGACY_HOST:localhost}}
    port: 5432
    database: ${{DB_LEGACY_NAME:legacy_db}}
    user: ${{DB_LEGACY_USER:postgres}}
    password: ${{DB_LEGACY_PASSWORD:postgres}}
  modern:
    type: postgres
    host: ${{DB_MODERN_HOST:localhost}}
    port: 5432
    database: ${{DB_MODERN_NAME:modern_db}}
    user: ${{DB_MODERN_USER:postgres}}
    password: ${{DB_MODERN_PASSWORD:postgres}}

datasets:
  - name: my_table
    legacy_table: my_table
    modern_table: my_table

validation:
  sample_size: 1000
  governance:
    pii_keywords: [ssn, email, phone, dob, credit_card, account_number]
    naming_regex: "^[a-z0-9_]+$"
    max_null_percent: 10
    required_fields:
      my_table: []

  aggregates:
    my_table: []

  referential_integrity: {{}}

scoring:
  weights:
    structure: 0.4
    integrity: 0.4
    governance: 0.2
  thresholds:
    green: 90
    yellow: 70

metadata:
  path: ./metadata
  rag:
    explain_threshold: 0.5
    mapping_threshold: 0.3

plugins: []
# Uncomment to load your domain plugin:
# plugins:
#   - plugins.my_plugin.MyPlugin

artifacts:
  base_path: ./artifacts

# OpenMetadata integration
openmetadata:
  host: ${{OM_HOST:http://localhost:8585}}
  auth_token: ${{OM_AUTH_TOKEN:}}
  legacy_service: "my_legacy_service"
  legacy_database: "my_database"
  legacy_schema: "public"

# Schema generation settings
schema_generation:
  target: postgres
  naming_convention: snake_case
  abbreviation_expansion: true
  type_optimization: true
  pii_handling:
    default_action: hash
  normalization:
    enabled: true
    min_group_size: 3
    prefix_detection: true
    lookup_threshold: 20
  constraints:
    infer_not_null: true
    infer_unique: true
    infer_check: true
  defaults:
    add_created_at: true
    add_updated_at: true
    id_strategy: serial
"""


def _get_plugin_template(name: str) -> str:
    """Generate a template plugin file."""
    class_name = "".join(word.capitalize() for word in name.replace("-", "_").split("_")) + "Plugin"
    return f'''"""
Domain-specific plugin for {name} migration.

Implements DM hooks to provide custom validation rules,
column mapping overrides, and business logic checks.
"""

from dm.hookspecs import hookimpl


class {class_name}:
    """Custom validation rules for {name} migration."""

    @hookimpl
    def dm_get_column_overrides(self, table):
        """Return curated column mapping overrides.

        Example:
            if table == "my_table":
                return {{
                    "old_column": {{
                        "target": "new_column",
                        "type": "rename",
                        "rationale": "Renamed for clarity",
                        "confidence": 1.0,
                    }},
                }}
        """
        return {{}}

    @hookimpl
    def dm_data_quality_rules(self, dataset):
        """Return cross-field data quality rules.

        Example:
            if dataset == "my_table":
                return [{{
                    "name": "my_rule",
                    "severity": "HIGH",
                    "description": "Description of the check",
                    "check_fn": self._check_my_rule,
                }}]
        """
        return []

    # def _check_my_rule(self, df):
    #     \"\"\"Example cross-field check. Return anomaly dict or None.\"\"\"
    #     bad_rows = df[df["status"] == "INVALID"]
    #     if not bad_rows.empty:
    #         return {{
    #             "count": len(bad_rows),
    #             "record_ids": bad_rows.iloc[:5].index.tolist(),
    #             "risk": "Description of the risk",
    #             "action": "What to do about it",
    #         }}
    #     return None
'''


if __name__ == "__main__":
    cli()
