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
@click.option("--repo", default=None, help="Git repo URL containing mainframe artifacts (.cpy, .dat, .csv, .sql)")
@click.option("--data", default=None, help="Local directory containing mainframe artifacts")
@click.option("--target", default="postgres", help="Target platform (postgres, snowflake, oracle, redshift)")
def init(name, template, repo, data, target):
    """Scaffold a new migration project.

    Creates a project directory with project.yaml, metadata/, plugins/,
    and schemas/ subdirectories.

    With --repo: clones a git repository and auto-detects copybooks, data files,
    and legacy SQL to build the project config.

    With --data: scans a local directory for the same artifacts.
    """
    project_dir = Path("projects") / name

    # If --repo or --data provided, use the repo loader
    if repo or data:
        from dm.repo_loader import clone_repo, generate_project_from_repo

        if repo:
            click.echo(f"Cloning {repo}...")
            repo_path = clone_repo(repo, target_dir=str(project_dir / "_source_repo"))
        else:
            repo_path = data

        click.echo(f"Scanning for mainframe artifacts...")
        summary = generate_project_from_repo(
            project_name=name,
            repo_path=repo_path,
            project_dir=str(project_dir),
            target_type=target,
        )

        click.echo("")
        click.echo(f"Project created: {project_dir}")
        click.echo(f"  Copybooks:  {summary['copybooks']}")
        click.echo(f"  Data files: {summary['datafiles']}")
        click.echo(f"  CSV files:  {summary['csv_files']}")
        click.echo(f"  SQL files:  {summary['sql_files']}")
        click.echo(f"  Datasets:   {summary['datasets']}")
        click.echo(f"  Target:     {target}")
        click.echo("")
        click.echo(f"Next steps:")
        click.echo(f"  dm discover --project {project_dir}")
        click.echo(f"  dm generate-schema --all --project {project_dir}")
        click.echo(f"  dm validate --phase pre --dataset <name> --project {project_dir}")
        return

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
@click.argument("name")
@click.option("--repo", default=None, help="Git repo URL containing mainframe artifacts")
@click.option("--data", default=None, help="Local directory containing mainframe artifacts")
@click.option("--target", default="postgres", help="Target platform (postgres, snowflake, oracle, redshift)")
@click.option("--skip-validate", is_flag=True, help="Skip PRE validation step")
def bootstrap(name, repo, data, target, skip_validate):
    """One-command setup: init + discover + rationalize + generate-schema + validate.

    Creates a project from a git repo or local directory and runs the full
    pre-migration pipeline automatically.

    Examples:
        dm bootstrap my-project --repo https://github.com/org/mainframe-data.git
        dm bootstrap my-project --data /path/to/files --target snowflake
    """
    import subprocess as _sp

    project_dir = Path("projects") / name

    # Step 1: Init
    click.echo("============================================================")
    click.echo(f"  Bootstrapping project: {name}")
    click.echo("============================================================")
    click.echo("")

    init_cmd = [sys.executable, "-m", "dm.cli", "init", name]
    if repo:
        init_cmd += ["--repo", repo]
    elif data:
        init_cmd += ["--data", data]
    init_cmd += ["--target", target]

    click.echo("[1/5] Initializing project...")
    result = _sp.run(init_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        click.echo(result.stderr, err=True)
        sys.exit(1)
    click.echo(result.stdout)

    # Check project was created
    if not (project_dir / "project.yaml").exists():
        click.echo(f"ERROR: project.yaml not found at {project_dir}", err=True)
        sys.exit(1)

    project_path = str(project_dir)

    # Step 2: Profile
    click.echo("[2/6] Profiling legacy data...")
    result = _sp.run(
        [sys.executable, "-m", "dm.cli", "profile", "--project", project_path],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        click.echo(f"  Profiling failed (non-fatal): {result.stderr[:200]}")
    else:
        click.echo("  Profiling complete.")
    click.echo("")

    # Step 3: Discover
    click.echo("[3/6] Running discovery + enrichment...")
    result = _sp.run(
        [sys.executable, "-m", "dm.cli", "discover", "--enrich", "--project", project_path],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        click.echo(f"  Discovery failed (non-fatal): {result.stderr[:200]}")
        click.echo("  Continuing — discovery can be re-run later.")
    else:
        click.echo("  Discovery complete.")
    click.echo("")

    # Step 4: Rationalize
    click.echo("[4/6] Running rationalization...")
    result = _sp.run(
        [sys.executable, "-m", "dm.cli", "rationalize", "--project", project_path],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        click.echo(f"  Rationalization failed (non-fatal): {result.stderr[:200]}")
    else:
        click.echo("  Rationalization complete.")
    click.echo("")

    # Step 5: Generate schema
    click.echo("[5/6] Generating schemas for all target platforms...")
    result = _sp.run(
        [sys.executable, "-m", "dm.cli", "generate-schema", "--all", "--project", project_path],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        click.echo(f"  Schema generation failed (non-fatal): {result.stderr[:200]}")
    else:
        click.echo("  Schemas generated for PostgreSQL, Snowflake, Oracle, AWS Redshift.")
    click.echo("")

    # Step 6: PRE validation
    if not skip_validate:
        click.echo("[6/6] Running PRE validation...")
        from dm.config import load_project_config, get_datasets
        try:
            config = load_project_config(project_path)
            datasets = get_datasets(config)
            ds_names = [d.get("name", d) if isinstance(d, dict) else d for d in datasets]

            for ds in ds_names:
                click.echo(f"  Validating: {ds}")
                result = _sp.run(
                    [sys.executable, "-m", "dm.cli", "validate", "--phase", "pre",
                     "--dataset", ds, "--project", project_path],
                    capture_output=True, text=True,
                )
                # Extract score from output
                for line in result.stdout.splitlines():
                    if "CONFIDENCE" in line or "STATUS" in line:
                        click.echo(f"    {line.strip()}")
        except Exception as e:
            click.echo(f"  Validation failed: {e}")
    else:
        click.echo("[5/5] Skipping PRE validation (--skip-validate)")
    click.echo("")

    click.echo("============================================================")
    click.echo("  Bootstrap Complete!")
    click.echo("============================================================")
    click.echo("")
    click.echo(f"  Project:    {project_dir}")
    click.echo(f"  Target:     {target}")
    click.echo("")
    click.echo("  Launch the dashboard:")
    click.echo(f"    streamlit run dashboard.py -- --project {project_dir}")
    click.echo("")
    click.echo("  Or continue from the CLI:")
    click.echo(f"    dm validate --phase pre --dataset <name> --project {project_dir}")
    click.echo("============================================================")


@cli.command()
@click.option("--project", "-p", default=".", help="Project directory")
def profile(project):
    """Profile legacy tables and save column-level statistics locally.

    Connects to each source database and computes null %, distinct count,
    max length, min/max values, and value frequencies for every column.
    Saves results to metadata/profiling_stats.json.

    This runs automatically during bootstrap but can be re-run independently.
    Used as a fallback when OpenMetadata profiler data is unavailable.
    """
    import json
    from dm.config import (
        get_all_sources, get_connection_config, get_datasets,
        get_metadata_path, load_project_config,
    )
    from dm.connectors.postgres import get_connector

    config = load_project_config(project)
    metadata_path = get_metadata_path(config)
    metadata_path.mkdir(parents=True, exist_ok=True)

    datasets = get_datasets(config)
    table_names = [d.get("name", d) if isinstance(d, dict) else d for d in datasets]

    all_profiles = {}

    for table in table_names:
        # Resolve which source connection this table uses
        from dm.config import get_dataset_source
        source_name = get_dataset_source(config, table)

        try:
            conn_config = get_connection_config(config, source_name)
            conn = get_connector(conn_config)
            conn.connect()
        except Exception as e:
            click.echo(f"  {table}: could not connect to {source_name} ({e})")
            continue

        try:
            row_count = conn.get_row_count(table)
            schema = conn.get_table_schema(table)
            col_stats = {}

            for col in schema:
                cn = col["column_name"]
                try:
                    null_pct = conn.get_null_percentage(table, cn)
                    dup_count = conn.get_duplicate_count(table, cn)

                    # Execute scalar queries for distinct count, min, max, max_length
                    distinct = conn.execute_scalar(
                        f"SELECT COUNT(DISTINCT {cn}) FROM {table}"
                    )
                    stats_row = conn.execute_query(
                        f"SELECT MIN({cn}::text) as mn, MAX({cn}::text) as mx, "
                        f"MAX(LENGTH({cn}::text)) as ml FROM {table}"
                    )
                    mn = stats_row.iloc[0]["mn"] if not stats_row.empty else None
                    mx = stats_row.iloc[0]["mx"] if not stats_row.empty else None
                    ml = stats_row.iloc[0]["ml"] if not stats_row.empty else 0

                    # Value frequencies (top 10)
                    freq_df = conn.execute_query(
                        f"SELECT {cn}::text as value, COUNT(*) as count "
                        f"FROM {table} WHERE {cn} IS NOT NULL "
                        f"GROUP BY {cn} ORDER BY count DESC LIMIT 10"
                    )
                    freqs = [
                        {"value": str(r["value"]), "count": int(r["count"])}
                        for _, r in freq_df.iterrows()
                    ] if not freq_df.empty else []

                    col_stats[cn] = {
                        "null_count": int(round(null_pct * row_count / 100)) if row_count else 0,
                        "null_percent": round(null_pct, 2),
                        "distinct_count": int(distinct) if distinct else 0,
                        "unique_percent": round((int(distinct) / row_count * 100), 2) if row_count and distinct else 0,
                        "max_length": int(ml) if ml else 0,
                        "min_value": str(mn) if mn is not None else None,
                        "max_value": str(mx) if mx is not None else None,
                        "value_frequencies": freqs,
                        "row_count": row_count,
                    }
                except Exception:
                    pass

            all_profiles[table] = {
                "row_count": row_count,
                "column_count": len(schema),
                "columns": col_stats,
            }
            click.echo(f"  {table}: {row_count} rows, {len(col_stats)} columns profiled")

        except Exception as e:
            click.echo(f"  {table}: profiling failed ({e})")
        finally:
            conn.close()

    out_path = metadata_path / "profiling_stats.json"
    with open(out_path, "w") as f:
        json.dump(all_profiles, f, indent=2, default=str)

    click.echo("")
    click.echo(f"Profiling complete: {len(all_profiles)} tables")
    click.echo(f"Saved to: {out_path}")


@cli.command()
@click.option("--project", "-p", default=".", help="Project directory")
@click.option("--tables", "-t", multiple=True, help="Specific tables (default: all from config)")
@click.option("--no-interactive", is_flag=True, help="Skip interactive prompts")
@click.option("--enrich", is_flag=True, help="Chain into OM enrichment after discovery")
def discover(project, tables, no_interactive, enrich):
    """Introspect databases and generate metadata (glossary + mappings).

    Auto-detects flat file / copybook sources and runs the appropriate pipeline.
    """
    from dm.config import get_metadata_path, get_plugin_specs, load_project_config
    from dm.plugin_manager import get_plugin_manager

    config = load_project_config(project)

    # Auto-detect if this is a flat file project (no database connections)
    connections = config.get("connections", {})
    flatfile_types = {"copybook", "flatfile", "csv"}
    source_types = set()
    for conn_name, conn_cfg in connections.items():
        if conn_name in ("modern",):
            continue
        source_types.add(conn_cfg.get("type", "").lower())

    if source_types and source_types.issubset(flatfile_types):
        # All sources are flat files — use the flat file pipeline
        click.echo("Detected flat file / copybook sources — running flat file pipeline")
        from dm.pipeline_flatfile import run_flatfile_pipeline
        result = run_flatfile_pipeline(project)
        click.echo("")
        click.echo(f"Pipeline complete:")
        click.echo(f"  Tables:   {result['tables']}")
        click.echo(f"  Columns:  {result['columns']}")
        click.echo(f"  Rows:     {result['rows']}")
        click.echo(f"  Mappings: {result['mappings']}")
        click.echo(f"  Scope:    {result['migrate']} migrate, {result['review']} review, {result['archive']} archive")
        click.echo(f"  Targets:  {', '.join(result['targets'])}")
        return

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
        try:
            from dm.config import get_openmetadata_config
            om_config = get_openmetadata_config(config)
        except KeyError:
            click.echo("  No OpenMetadata configured — falling back to database-only discovery")
            enrich = False

    if enrich:
        from dm.discovery.openmetadata_enricher import OpenMetadataEnricher
        from dm.discovery.om_plugin import OpenMetadataPlugin

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
        from dm.config import get_connection_config, get_all_sources

        # Connect to the first source (required) and target (optional)
        first_source = get_all_sources(config)[0]
        legacy_conn = get_connector(get_connection_config(config, first_source))

        modern_conn = None
        try:
            modern_conn_cfg = get_connection_config(config, "modern")
            modern_conn = get_connector(modern_conn_cfg)
            modern_conn.connect()
        except (KeyError, Exception) as e:
            click.echo(f"  Modern DB not available ({e}) — using COBOL abbreviation expansion only")

        try:
            legacy_conn.connect()
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
            if modern_conn:
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
