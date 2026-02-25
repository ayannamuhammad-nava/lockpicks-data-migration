"""
Orchestrator Agent
Coordinates validation agents and generates artifacts.
"""
import logging
import os
import re
import yaml
from typing import Dict
from datetime import datetime

from tools import db_utils, reporter, governance, schema_loader
from agents import pre_agent, post_agent
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


_ENV_VAR_PATTERN = re.compile(r'\$\{(\w+)(?::([^}]*))?\}')


def _resolve_env_vars(value):
    """
    Recursively resolve ${VAR:default} patterns in config values.

    Supports:
        ${DB_PASSWORD}         -> os.environ['DB_PASSWORD'] (KeyError if unset)
        ${DB_PASSWORD:postgres} -> os.environ.get('DB_PASSWORD', 'postgres')
    """
    if isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_vars(item) for item in value]
    if isinstance(value, str):
        def _replace(match):
            var_name = match.group(1)
            default = match.group(2)
            if default is not None:
                return os.environ.get(var_name, default)
            return os.environ[var_name]
        return _ENV_VAR_PATTERN.sub(_replace, value)
    return value


def load_config(config_path: str = 'config.yaml') -> Dict:
    """Load configuration from YAML file, resolving ${VAR:default} env vars."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    config = _resolve_env_vars(config)
    logger.info(f"Loaded configuration from {config_path}")
    return config


def ensure_schemas_exist(legacy_conn, modern_conn, dataset: str, config: Dict) -> None:
    """
    Auto-generate Pandera schemas if they don't exist.

    This is transparent to the user - schemas are generated automatically
    from database structure when needed.

    Args:
        legacy_conn: Legacy database connection
        modern_conn: Modern database connection
        dataset: Dataset name (e.g., 'claimants')
        config: Configuration dict
    """
    schema_dir = Path('schemas')
    legacy_schema_file = schema_dir / 'legacy' / f'{dataset}.py'
    modern_schema_file = schema_dir / 'modern' / f'{dataset}.py'

    schemas_missing = not legacy_schema_file.exists() or not modern_schema_file.exists()

    if schemas_missing:
        logger.info("=" * 60)
        logger.info("📋 Validation schemas not found - auto-generating...")
        logger.info("=" * 60)

        # Generate for legacy system if missing
        if not legacy_schema_file.exists():
            logger.info(f"Generating schema for legacy.{dataset}...")
            try:
                schema_loader.generate_pandera_schema(
                    legacy_conn,
                    dataset,
                    'legacy',
                    output_path=str(legacy_schema_file)
                )
                logger.info(f"✓ Generated schemas/legacy/{dataset}.py")
            except Exception as e:
                logger.warning(f"Could not generate legacy schema: {e}")

        # Generate for modern system if missing
        if not modern_schema_file.exists():
            logger.info(f"Generating schema for modern.{dataset}...")
            try:
                schema_loader.generate_pandera_schema(
                    modern_conn,
                    dataset,
                    'modern',
                    output_path=str(modern_schema_file)
                )
                logger.info(f"✓ Generated schemas/modern/{dataset}.py")
            except Exception as e:
                logger.warning(f"Could not generate modern schema: {e}")

        logger.info("=" * 60)
        logger.info("✓ Schema generation complete")
        logger.info("=" * 60)
    else:
        logger.debug(f"Schemas exist for {dataset}, skipping auto-generation")


def run_agent(
    phase: str,
    dataset: str,
    sample_size: int,
    config_path: str = 'config.yaml'
) -> Dict:
    """
    Main orchestration function - routes to appropriate agent based on phase.

    Args:
        phase: 'pre' or 'post'
        dataset: Dataset name (e.g., 'claimants')
        sample_size: Sample size for validation
        config_path: Path to config file

    Returns:
        Dict with validation results, confidence score, and artifact path
    """
    logger.info(f"=" * 60)
    logger.info(f"Data Validation Agent - {phase.upper()} Phase")
    logger.info(f"Dataset: {dataset}, Sample Size: {sample_size}")
    logger.info(f"=" * 60)

    # Load configuration
    config = load_config(config_path)

    # Create artifact folder
    artifact_folder = reporter.create_artifact_folder(config['artifacts']['base_path'])

    # Connect to databases
    legacy_conn = None
    modern_conn = None

    try:
        legacy_conn = db_utils.get_connection(config['database']['legacy'])
        modern_conn = db_utils.get_connection(config['database']['modern'])

        # Auto-generate schemas if they don't exist (transparent to user)
        ensure_schemas_exist(legacy_conn, modern_conn, dataset, config)

        # Route to appropriate agent
        if phase == 'pre':
            results = run_pre_phase(
                legacy_conn,
                modern_conn,
                dataset,
                sample_size,
                config,
                artifact_folder
            )

        elif phase == 'post':
            results = run_post_phase(
                legacy_conn,
                modern_conn,
                dataset,
                config,
                artifact_folder
            )

        else:
            raise ValueError(f"Invalid phase: {phase}. Must be 'pre' or 'post'")

        # Close connections
        if legacy_conn:
            legacy_conn.close()
        if modern_conn:
            modern_conn.close()

        logger.info(f"=" * 60)
        logger.info(f"Validation complete!")
        logger.info(f"Artifacts saved to: {artifact_folder}")
        logger.info(f"=" * 60)

        return results

    except Exception as e:
        logger.error(f"Orchestrator error: {e}", exc_info=True)

        # Close connections on error
        if legacy_conn:
            legacy_conn.close()
        if modern_conn:
            modern_conn.close()

        raise


def run_pre_phase(
    legacy_conn,
    modern_conn,
    dataset: str,
    sample_size: int,
    config: Dict,
    artifact_folder: str
) -> Dict:
    """Run pre-migration validation and generate artifacts."""

    # Run pre-migration agent
    pre_results = pre_agent.run_pre_migration_validation(
        legacy_conn,
        modern_conn,
        dataset,
        sample_size,
        config
    )

    # Generate reports
    schema_diff_report = pre_results.get('schema_diff_report', '')
    reporter.save_markdown_report(
        schema_diff_report,
        os.path.join(artifact_folder, 'schema_diff.md')
    )

    readiness_report = reporter.generate_readiness_report(
        pre_results.get('schema_diff', {}),
        pre_results.get('governance', {}),
        pre_results.get('rag_explanations', {}),
        pre_results.get('structure_score', 0),
        data_anomalies=pre_results.get('data_anomalies', [])
    )
    reporter.save_markdown_report(
        readiness_report,
        os.path.join(artifact_folder, 'readiness_report.md')
    )

    # Save governance CSV
    gov_csv = governance.generate_governance_report_csv(
        pre_results.get('governance', {}),
        None  # Would pass df if available
    )
    reporter.save_csv_report(
        gov_csv,
        os.path.join(artifact_folder, 'governance_report.csv')
    )

    # Calculate final confidence score (structure-weighted for pre phase)
    structure_score = pre_results.get('structure_score', 0)
    gov_score = pre_results.get('governance', {}).get('governance_score', 100)

    confidence_score = calculate_confidence(
        structure_score=structure_score,
        integrity_score=100,  # Not applicable in pre-phase
        governance_score=gov_score,
        config=config
    )

    # Generate readiness dashboard (NEW!)
    dashboard = reporter.generate_readiness_dashboard(
        pre_results.get('schema_diff', {}),
        pre_results.get('governance', {}),
        structure_score,
        gov_score,
        confidence_score['score']
    )
    reporter.save_markdown_report(
        dashboard,
        os.path.join(artifact_folder, 'READINESS_DASHBOARD.md')
    )

    # Save confidence score
    reporter.save_confidence_score(
        confidence_score['score'],
        confidence_score['status'],
        os.path.join(artifact_folder, 'confidence_score.txt')
    )

    # Save run metadata
    metadata = {
        'phase': 'pre',
        'dataset': dataset,
        'sample_size': sample_size,
        'structure_score': structure_score,
        'governance_score': gov_score,
        'confidence_score': confidence_score['score'],
        'status': confidence_score['status']
    }
    reporter.save_run_metadata(metadata, os.path.join(artifact_folder, 'run_metadata.json'))

    return {
        'phase': 'pre',
        'score': confidence_score['score'],
        'status': confidence_score['status'],
        'artifact_path': artifact_folder,
        'details': pre_results
    }


def run_post_phase(
    legacy_conn,
    modern_conn,
    dataset: str,
    config: Dict,
    artifact_folder: str
) -> Dict:
    """Run post-migration reconciliation and generate artifacts."""

    # Run post-migration agent
    post_results = post_agent.run_post_migration_reconciliation(
        legacy_conn,
        modern_conn,
        dataset,
        config
    )

    # Generate reconciliation report
    reconciliation_report = reporter.generate_reconciliation_report(
        post_results.get('row_count_check', {}),
        post_results.get('checksum_results', {}),
        post_results.get('integrity_results', {}),
        post_results.get('sample_comparison', {}),
        post_results.get('integrity_score', 0),
        archived_leakage=post_results.get('archived_leakage', {}),
        unmapped_columns=post_results.get('unmapped_columns', {})
    )
    reporter.save_markdown_report(
        reconciliation_report,
        os.path.join(artifact_folder, 'reconciliation_report.md')
    )

    # POST confidence IS the integrity score — structure and governance checks are
    # not applicable post-migration, so we don't dilute the score with dummy 100s.
    integrity_score = post_results.get('integrity_score', 0)

    confidence_score = {
        'score': round(integrity_score, 2),
        'status': get_traffic_light(integrity_score, config)
    }

    # Save confidence score
    reporter.save_confidence_score(
        confidence_score['score'],
        confidence_score['status'],
        os.path.join(artifact_folder, 'confidence_score.txt')
    )

    # Save metadata
    metadata = {
        'phase': 'post',
        'dataset': dataset,
        'integrity_score': integrity_score,
        'confidence_score': confidence_score['score'],
        'status': confidence_score['status']
    }
    reporter.save_run_metadata(metadata, os.path.join(artifact_folder, 'run_metadata.json'))

    return {
        'phase': 'post',
        'score': confidence_score['score'],
        'status': confidence_score['status'],
        'artifact_path': artifact_folder,
        'details': post_results
    }


def calculate_confidence(
    structure_score: float,
    integrity_score: float,
    governance_score: float,
    config: Dict
) -> Dict:
    """
    Calculate weighted confidence score.

    Args:
        structure_score: 0-100
        integrity_score: 0-100
        governance_score: 0-100
        config: Configuration with weights and thresholds

    Returns:
        Dict with score and status (GREEN/YELLOW/RED)
    """
    weights = config.get('confidence', {}).get('weights', {
        'structure': 0.4,
        'integrity': 0.4,
        'governance': 0.2
    })

    final_score = (
        weights['structure'] * structure_score +
        weights['integrity'] * integrity_score +
        weights['governance'] * governance_score
    )

    status = get_traffic_light(final_score, config)

    return {
        'score': round(final_score, 2),
        'status': status
    }


def get_traffic_light(score: float, config: Dict) -> str:
    """Get traffic light status based on score."""
    thresholds = config.get('confidence', {}).get('thresholds', {
        'green': 90,
        'yellow': 70
    })

    if score >= thresholds['green']:
        return 'GREEN'
    elif score >= thresholds['yellow']:
        return 'YELLOW'
    else:
        return 'RED'


