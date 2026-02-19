"""
Pre-Migration Validation Agent
Detects structural and governance risks before migration begins.
"""
import logging
from typing import Dict
from tools import db_utils, sampler, schema_loader, governance, rag_tool
import pandera as pa

logger = logging.getLogger(__name__)


def run_pre_migration_validation(
    legacy_conn,
    modern_conn,
    dataset: str,
    sample_size: int,
    config: Dict
) -> Dict:
    """
    Run pre-migration validation checks.

    Args:
        legacy_conn: Legacy database connection
        modern_conn: Modern database connection
        dataset: Dataset name (e.g., 'claimants')
        sample_size: Number of rows to sample for validation
        config: Configuration dict

    Returns:
        Dict with validation results and structure score
    """
    logger.info(f"Starting pre-migration validation for {dataset}")

    results = {
        'dataset': dataset,
        'sample_size': sample_size
    }

    # 1. Schema comparison
    logger.info("Comparing schemas...")
    legacy_schema_db = schema_loader.introspect_database_schema(legacy_conn, dataset)
    modern_schema_db = schema_loader.introspect_database_schema(modern_conn, dataset)

    schema_diff = schema_loader.compare_schemas(legacy_schema_db, modern_schema_db)
    results['schema_diff'] = schema_diff

    # 2. Generate schema diff report
    schema_diff_report = schema_loader.generate_schema_diff_report(
        legacy_schema_db,
        modern_schema_db,
        dataset
    )
    results['schema_diff_report'] = schema_diff_report

    # 3. Sample data from legacy system
    logger.info(f"Sampling {sample_size} rows from legacy {dataset}...")
    sample_df = sampler.sample_table(legacy_conn, dataset, sample_size)
    results['sample_row_count'] = len(sample_df)

    # 4. Validate with Pandera schema (if available)
    logger.info("Validating with Pandera schema...")
    try:
        legacy_pandera_schema = schema_loader.load_pandera_schema('legacy', dataset)

        if legacy_pandera_schema:
            try:
                legacy_pandera_schema.validate(sample_df, lazy=True)
                results['pandera_validation'] = 'PASS'
                results['pandera_errors'] = []
            except pa.errors.SchemaErrors as e:
                results['pandera_validation'] = 'FAIL'
                results['pandera_errors'] = [str(err) for err in e.failure_cases.to_dict('records')]
                logger.warning(f"Pandera validation failed: {len(e.failure_cases)} errors")
        else:
            results['pandera_validation'] = 'SKIP'
            results['pandera_errors'] = []
    except Exception as e:
        logger.error(f"Pandera validation error: {e}")
        results['pandera_validation'] = 'ERROR'
        results['pandera_errors'] = [str(e)]

    # 5. Governance checks
    logger.info("Running governance checks...")
    gov_config = config.get('validation', {}).get('governance', {})

    # Get required fields for this dataset
    dataset_required_fields = gov_config.get('required_fields', {}).get(dataset, [])
    gov_config_for_dataset = {
        **gov_config,
        'required_fields': dataset_required_fields
    }

    gov_results = governance.run_governance_checks(sample_df, gov_config_for_dataset)
    results['governance'] = gov_results

    # 6. RAG explanations for schema differences
    logger.info("Generating RAG explanations...")
    rag_instance = rag_tool.RAGTool()
    rag_explanations = rag_instance.enrich_schema_diff(schema_diff)
    results['rag_explanations'] = rag_explanations

    # 7. Calculate structure score
    structure_score = calculate_structure_score(schema_diff, gov_results, results)
    results['structure_score'] = structure_score

    logger.info(f"Pre-migration validation complete. Structure score: {structure_score}/100")

    return results


def calculate_structure_score(schema_diff: Dict, gov_results: Dict, validation_results: Dict) -> float:
    """
    Calculate structure/readiness score (0-100).

    Args:
        schema_diff: Schema comparison results
        gov_results: Governance check results
        validation_results: All validation results

    Returns:
        Score from 0-100
    """
    penalties = 0.0

    # Schema penalties
    missing_in_modern = len(schema_diff.get('missing_in_modern', []))
    type_mismatches = len(schema_diff.get('type_mismatches', []))

    penalties += missing_in_modern * 10  # -10 points per missing column
    penalties += type_mismatches * 5     # -5 points per type mismatch

    # Pandera validation penalties
    if validation_results.get('pandera_validation') == 'FAIL':
        error_count = len(validation_results.get('pandera_errors', []))
        penalties += min(error_count, 20)  # Max 20 points penalty

    # Governance score is already 0-100, convert to penalty
    gov_score = gov_results.get('governance_score', 100)
    penalties += (100 - gov_score) * 0.3  # 30% weight on governance

    # Calculate final score
    score = max(0, 100 - penalties)

    return round(score, 2)
