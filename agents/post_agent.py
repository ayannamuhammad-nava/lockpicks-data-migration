"""
Post-Migration Reconciliation Agent
Proves data integrity and equivalence after migration completes.
"""
import json
import logging
import re
from datetime import datetime, date as date_type
from typing import Dict, List, Optional
from tools import db_utils, sampler
import pandas as pd

logger = logging.getLogger(__name__)


def _load_column_mappings(mappings_path: str = './metadata/mappings.json') -> list:
    """Load mappings.json for column name resolution."""
    try:
        with open(mappings_path, 'r') as f:
            data = json.load(f)
        return data.get('mappings', [])
    except FileNotFoundError:
        logger.warning("mappings.json not found, using direct column matching")
        return []


def check_unmapped_columns(
    modern_conn,
    dataset: str,
    mappings_path: str = './metadata/mappings.json'
) -> Dict:
    """
    Find columns present in the modern schema that have no source mapping in
    mappings.json — ungoverned columns that were added outside the ETL spec.

    Args:
        modern_conn: Modern database connection
        dataset: Table name to check
        mappings_path: Path to mappings.json

    Returns:
        Dict with ungoverned_columns list, count, and status
    """
    mappings = _load_column_mappings(mappings_path)

    # All target column names for this table (what the ETL spec says should be in modern)
    mapped_targets = {
        m['target'].lower()
        for m in mappings
        if m.get('table', '').lower() == dataset.lower() and m.get('target')
    }

    try:
        modern_schema = db_utils.get_table_schema(modern_conn, dataset)
        modern_cols = {col['column_name'].lower() for col in modern_schema}
    except Exception as e:
        logger.error(f"Could not introspect modern schema for unmapped column check: {e}")
        return {'ungoverned_columns': [], 'count': 0, 'status': 'ERROR', 'error': str(e)}

    # Exclude columns that are "archived" in mappings — those are already caught by the
    # compliance gate check and reported there with the full PCI/HIPAA rationale.
    # The ungoverned check is specifically for columns with NO mapping at all.
    archived_source_cols = {
        m['source'].lower()
        for m in mappings
        if m.get('table', '').lower() == dataset.lower() and m.get('type') == 'archived'
    }
    ungoverned = sorted((modern_cols - mapped_targets) - archived_source_cols)

    if ungoverned:
        for col in ungoverned:
            logger.warning(
                f"Ungoverned column '{col}' found in modern {dataset} — "
                f"no ETL mapping exists for this column"
            )

    return {
        'ungoverned_columns': ungoverned,
        'count': len(ungoverned),
        'status': 'WARN' if ungoverned else 'PASS',
    }


def check_archived_field_leakage(
    modern_conn,
    dataset: str,
    mappings_path: str = './metadata/mappings.json'
) -> Dict:
    """
    Check whether any fields marked 'archived' in mappings.json have leaked
    into the modern schema. An archived field in modern is a compliance violation
    (e.g. PCI-DSS bank account numbers, HIPAA identifiers that must not be migrated).

    Args:
        modern_conn: Modern database connection
        dataset: Table name to check
        mappings_path: Path to mappings.json

    Returns:
        Dict with violations list, violation_count, and status
    """
    violations = []

    # Load archived columns for this table from mappings.json
    mappings = _load_column_mappings(mappings_path)
    archived_cols = {
        m['source'].lower(): m
        for m in mappings
        if m.get('table', '').lower() == dataset.lower()
        and m.get('type') == 'archived'
    }

    if not archived_cols:
        return {'violations': [], 'violation_count': 0, 'status': 'PASS'}

    # Get actual modern schema columns
    try:
        modern_schema = db_utils.get_table_schema(modern_conn, dataset)
        modern_cols = {col['column_name'].lower() for col in modern_schema}
    except Exception as e:
        logger.error(f"Could not introspect modern schema for archived leakage check: {e}")
        return {'violations': [], 'violation_count': 0, 'status': 'ERROR', 'error': str(e)}

    # Flag any archived column that appears in modern
    for col_lower, mapping in archived_cols.items():
        if col_lower in modern_cols:
            violations.append({
                'column': mapping['source'],
                'table': dataset,
                'rationale': mapping.get('rationale', ''),
                'severity': 'CRITICAL',
            })
            logger.warning(
                f"COMPLIANCE VIOLATION: Archived column '{mapping['source']}' "
                f"found in modern {dataset} schema — {mapping.get('rationale', '')}"
            )

    status = 'FAIL' if violations else 'PASS'
    return {
        'violations': violations,
        'violation_count': len(violations),
        'status': status,
    }


def _build_column_pairs(legacy_cols, modern_cols, mappings, table) -> list:
    """
    Build (legacy_col, modern_col) pairs for field-level comparison.

    Only includes columns with type 'rename' — columns where the value is
    expected to be directly comparable after simple type coercion.
    Transform and archived columns are excluded: transforms produce intentionally
    different values (e.g. cl_ssn → ssn_hash), and archived columns are not
    expected to exist in modern at all.
    """
    SKIP_TYPES = {'removed', 'transform', 'archived'}
    pairs = []
    mapped_legacy = set()

    # Track ALL sources for this table so the fallback never re-adds
    # a column that has any explicit mapping (even archived/removed ones)
    all_mapped_sources = {
        m['source'] for m in mappings if m.get('table') == table
    }

    for m in mappings:
        if (m.get('table') == table
                and m.get('target')
                and m.get('type') not in SKIP_TYPES):
            if m['source'] in legacy_cols and m['target'] in modern_cols:
                pairs.append((m['source'], m['target']))
                mapped_legacy.add(m['source'])

    for col in legacy_cols:
        if col not in all_mapped_sources and col in modern_cols:
            pairs.append((col, col))

    return pairs


_DATE_FORMATS = [
    '%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y', '%B %d, %Y',
    '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S',
]

_BOOL_MAP = {'y': True, 'n': False, 'true': True, 'false': False, '1': True, '0': False}


def _parse_date_safe(val):
    """Return a date object from val, or None if unparseable."""
    if isinstance(val, (datetime, date_type)):
        return val.date() if isinstance(val, datetime) else val
    s = str(val).strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except (ValueError, AttributeError):
            continue
    return None


def _values_equivalent(val1, val2) -> bool:
    """
    Compare values with format-aware tolerance.

    Handles: NaN, Y/N booleans, phone digits, dates in various string formats,
    numeric comparison, and case-insensitive string comparison.
    """
    if pd.isna(val1) and pd.isna(val2):
        return True
    if pd.isna(val1) or pd.isna(val2):
        return False

    # Boolean Y/N normalization (legacy stores 'Y'/'N', modern stores True/False)
    s1 = str(val1).strip().lower()
    s2 = str(val2).strip().lower()
    b1 = val1 if isinstance(val1, bool) else _BOOL_MAP.get(s1)
    b2 = val2 if isinstance(val2, bool) else _BOOL_MAP.get(s2)
    if b1 is not None and b2 is not None:
        return b1 == b2

    # Date normalization — handles legacy text/US/ISO formats vs modern date types
    d1 = _parse_date_safe(val1)
    d2 = _parse_date_safe(val2)
    if d1 is not None and d2 is not None:
        return d1 == d2

    # Phone/digit normalization — strips formatting, compares digit sequences
    digits1 = re.sub(r'\D', '', str(val1))
    digits2 = re.sub(r'\D', '', str(val2))
    if digits1 and digits2 and len(digits1) >= 7 and digits1 == digits2:
        return True

    # Numeric comparison
    try:
        return abs(float(val1) - float(val2)) < 0.001
    except (ValueError, TypeError):
        pass

    # String comparison: strip whitespace, case-insensitive
    return s1 == s2


def run_post_migration_reconciliation(
    legacy_conn,
    modern_conn,
    dataset: str,
    config: Dict
) -> Dict:
    """
    Run post-migration reconciliation checks.

    Args:
        legacy_conn: Legacy database connection
        modern_conn: Modern database connection
        dataset: Dataset name
        config: Configuration dict

    Returns:
        Dict with reconciliation results and integrity score
    """
    logger.info(f"Starting post-migration reconciliation for {dataset}")

    results = {
        'dataset': dataset
    }

    # 1. Row count verification
    logger.info("Verifying row counts...")
    row_count_check = verify_row_counts(legacy_conn, modern_conn, dataset)
    results['row_count_check'] = row_count_check

    # 2. Column checksums
    logger.info("Computing checksums...")
    checksum_results = compute_checksums(legacy_conn, modern_conn, dataset)
    results['checksum_results'] = checksum_results

    # 3. Referential integrity checks (config-driven)
    logger.info("Checking referential integrity...")
    integrity_results = check_referential_integrity(modern_conn, dataset, config)
    results['integrity_results'] = integrity_results

    # 4. Random sample comparison
    logger.info("Comparing random samples...")
    sample_comparison = compare_random_samples(
        legacy_conn,
        modern_conn,
        dataset,
        sample_size=100
    )
    results['sample_comparison'] = sample_comparison

    # 5. Business aggregate validation (config-driven)
    logger.info("Validating business aggregates...")
    aggregate_validation = validate_business_aggregates(legacy_conn, modern_conn, dataset, config)
    results['aggregate_validation'] = aggregate_validation

    # 6. Archived field leakage check — compliance gate
    logger.info("Checking for archived field leakage in modern schema...")
    archived_leakage = check_archived_field_leakage(modern_conn, dataset)
    results['archived_leakage'] = archived_leakage
    if archived_leakage['violation_count'] > 0:
        logger.error(
            f"COMPLIANCE GATE FAILED: {archived_leakage['violation_count']} archived "
            f"field(s) detected in modern {dataset} schema"
        )

    # 7. Unmapped column check — governance warning
    logger.info("Checking for ungoverned columns in modern schema...")
    unmapped_columns = check_unmapped_columns(modern_conn, dataset)
    results['unmapped_columns'] = unmapped_columns
    if unmapped_columns['count'] > 0:
        logger.warning(
            f"GOVERNANCE WARNING: {unmapped_columns['count']} ungoverned column(s) "
            f"in modern {dataset} with no ETL mapping: {unmapped_columns['ungoverned_columns']}"
        )

    # 8. Calculate integrity score
    integrity_score = calculate_integrity_score(results)
    results['integrity_score'] = integrity_score

    logger.info(f"Post-migration reconciliation complete. Integrity score: {integrity_score}/100")

    return results


def verify_row_counts(legacy_conn, modern_conn, dataset: str) -> Dict:
    """Verify row counts match between legacy and modern."""
    legacy_count = db_utils.get_row_count(legacy_conn, dataset)
    modern_count = db_utils.get_row_count(modern_conn, dataset)

    match = legacy_count == modern_count

    return {
        'legacy_count': legacy_count,
        'modern_count': modern_count,
        'match': match,
        'difference': abs(legacy_count - modern_count)
    }


def compute_checksums(legacy_conn, modern_conn, dataset: str) -> Dict:
    """
    Compute and compare checksums for common columns between legacy and modern.
    Uses column mappings from metadata to resolve renames.
    """
    results = {}
    mappings = _load_column_mappings()

    try:
        legacy_schema = db_utils.get_table_schema(legacy_conn, dataset)
        modern_schema = db_utils.get_table_schema(modern_conn, dataset)

        legacy_cols = [col['column_name'] for col in legacy_schema]
        modern_cols = [col['column_name'] for col in modern_schema]

        col_pairs = _build_column_pairs(legacy_cols, modern_cols, mappings, dataset)

        matches = 0
        mismatches = 0
        for legacy_col, modern_col in col_pairs:
            try:
                legacy_hash = db_utils.get_column_hash(legacy_conn, dataset, legacy_col)
                modern_hash = db_utils.get_column_hash(modern_conn, dataset, modern_col)

                is_match = legacy_hash == modern_hash
                if is_match:
                    matches += 1
                else:
                    mismatches += 1

                results[f"{legacy_col}->{modern_col}"] = {
                    'legacy_checksum': legacy_hash[:12] + '...' if legacy_hash else 'N/A',
                    'modern_checksum': modern_hash[:12] + '...' if modern_hash else 'N/A',
                    'match': is_match
                }
            except Exception as e:
                logger.warning(f"Checksum failed for {legacy_col}->{modern_col}: {e}")
                # Rollback to clear aborted transaction state
                try:
                    legacy_conn.rollback()
                    modern_conn.rollback()
                except Exception:
                    pass
                results[f"{legacy_col}->{modern_col}"] = {
                    'match': False,
                    'error': str(e)
                }

        results['_summary'] = {
            'total_columns': len(col_pairs),
            'matches': matches,
            'mismatches': mismatches,
            'match_rate': (matches / len(col_pairs) * 100) if col_pairs else 0
        }

    except Exception as e:
        logger.error(f"Checksum computation failed: {e}")
        results['error'] = str(e)

    return results


def check_referential_integrity(modern_conn, dataset: str, config: Dict = None) -> Dict:
    """Check referential integrity in modern system using config-defined FK relationships."""
    results = {}

    # Get FK checks from config
    fk_config = (config or {}).get('referential_integrity', {}).get(dataset, [])

    if fk_config:
        for fk in fk_config:
            check_name = f"{fk['child_table']}_{fk['parent_table']}_fk"
            try:
                fk_result = db_utils.check_referential_integrity(
                    modern_conn,
                    child_table=fk['child_table'],
                    parent_table=fk['parent_table'],
                    foreign_key_column=fk['fk_column'],
                    parent_key_column=fk.get('pk_column', fk['fk_column'])
                )
                results[check_name] = fk_result
            except Exception as e:
                logger.error(f"FK check '{check_name}' failed: {e}")
                try:
                    modern_conn.rollback()
                except Exception:
                    pass
                results[check_name] = {'orphan_count': -1, 'error': str(e)}
    else:
        logger.info(f"No referential integrity checks configured for {dataset}")

    return results


def compare_random_samples(
    legacy_conn,
    modern_conn,
    dataset: str,
    sample_size: int = 100
) -> Dict:
    """
    Compare random samples from legacy and modern systems field by field.
    Uses column mappings to resolve key column and field renames between systems.
    """
    try:
        # Load mappings to resolve key column between systems
        mappings = _load_column_mappings()

        legacy_schema = db_utils.get_table_schema(legacy_conn, dataset)
        modern_schema = db_utils.get_table_schema(modern_conn, dataset)
        if not legacy_schema or not modern_schema:
            return {'error': f'No schema found for {dataset}', 'match_rate': 0}

        legacy_key = legacy_schema[0]['column_name']
        modern_cols = [col['column_name'] for col in modern_schema]

        # Resolve modern key column via mappings
        modern_key = legacy_key  # default
        for m in mappings:
            if m.get('table') == dataset and m.get('source') == legacy_key and m.get('target'):
                modern_key = m['target']
                break
        if modern_key not in modern_cols and legacy_key in modern_cols:
            modern_key = legacy_key

        # Sample IDs from legacy, then fetch matching records from each system
        legacy_sample = sampler.sample_table(legacy_conn, dataset, sample_size)
        if legacy_sample.empty:
            return {'sample_size': 0, 'exact_matches': 0, 'discrepancies': 0,
                    'match_rate': 0, 'note': 'No records in legacy'}

        sampled_ids = legacy_sample[legacy_key].tolist()

        # Fetch matching modern records using the correct key column
        from psycopg2 import sql as psql
        from psycopg2.extras import RealDictCursor
        placeholders = ', '.join(['%s'] * len(sampled_ids))
        modern_query = psql.SQL(
            "SELECT * FROM {tbl} WHERE {col} IN (" + placeholders + ")"
        ).format(tbl=psql.Identifier(dataset), col=psql.Identifier(modern_key))

        with modern_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(modern_query, sampled_ids)
            modern_rows = cur.fetchall()
        modern_df = pd.DataFrame(modern_rows) if modern_rows else pd.DataFrame()

        if modern_df.empty:
            return {'sample_size': 0, 'exact_matches': 0, 'discrepancies': 0,
                    'match_rate': 0, 'note': 'No matching records in modern'}

        # Build column pairs for comparison
        col_pairs = _build_column_pairs(
            legacy_sample.columns.tolist(), modern_df.columns.tolist(),
            mappings, dataset
        )

        # Match records by key and compare fields
        modern_indexed = {row[modern_key]: row for _, row in modern_df.iterrows()}
        exact_matches = 0
        discrepancies = []

        for _, legacy_row in legacy_sample.iterrows():
            record_id = legacy_row[legacy_key]
            if record_id not in modern_indexed:
                continue

            modern_row = modern_indexed[record_id]
            row_differences = []
            for legacy_col, modern_col in col_pairs:
                if legacy_col == legacy_key and modern_col == modern_key:
                    continue  # Skip key column comparison
                legacy_val = legacy_row.get(legacy_col)
                modern_val = modern_row.get(modern_col)

                if not _values_equivalent(legacy_val, modern_val):
                    row_differences.append({
                        'legacy_column': legacy_col,
                        'modern_column': modern_col,
                        'legacy_value': str(legacy_val)[:50],
                        'modern_value': str(modern_val)[:50]
                    })

            if not row_differences:
                exact_matches += 1
            else:
                discrepancies.append({
                    'id': record_id,
                    'differences': row_differences[:5]
                })

        total = len(modern_indexed)
        return {
            'sample_size': total,
            'exact_matches': exact_matches,
            'discrepancies': len(discrepancies),
            'match_rate': (exact_matches / total * 100) if total else 0,
            'sample_discrepancies': discrepancies[:10]
        }

    except Exception as e:
        logger.error(f"Sample comparison failed: {e}")
        modern_conn.rollback()
        return {
            'sample_size': 0, 'exact_matches': 0,
            'discrepancies': 0, 'match_rate': 0,
            'error': str(e)
        }


def validate_business_aggregates(legacy_conn, modern_conn, dataset: str, config: Dict = None) -> Dict:
    """
    Validate business aggregates match between systems.
    Uses config-defined aggregate queries for each dataset.
    """
    results = {}

    agg_config = (config or {}).get('aggregates', {}).get(dataset, [])

    if not agg_config:
        logger.info(f"No aggregate checks configured for {dataset}")
        return results

    for agg in agg_config:
        name = agg['name']
        legacy_query = agg.get('legacy_query', '')
        modern_query = agg.get('modern_query', '')
        comparison = agg.get('comparison', 'exact')
        tolerance = agg.get('tolerance', 0.01)

        try:
            legacy_result = db_utils.execute_query(legacy_conn, legacy_query)
            modern_result = db_utils.execute_query(modern_conn, modern_query)

            if comparison == 'exact':
                match = legacy_result.equals(modern_result)
            else:
                match = _compare_with_tolerance(legacy_result, modern_result, tolerance)

            results[name] = {
                'legacy': legacy_result.to_dict('records'),
                'modern': modern_result.to_dict('records'),
                'match': match
            }
        except Exception as e:
            logger.error(f"Aggregate check '{name}' failed: {e}")
            try:
                legacy_conn.rollback()
                modern_conn.rollback()
            except Exception:
                pass
            results[name] = {'error': str(e), 'match': False}

    return results


def _compare_with_tolerance(df1: pd.DataFrame, df2: pd.DataFrame, tolerance: float) -> bool:
    """Compare two DataFrames with numeric tolerance."""
    if df1.shape != df2.shape:
        return False

    for col in df1.columns:
        if pd.api.types.is_numeric_dtype(df1[col]):
            if not all(abs(df1[col].fillna(0) - df2[col].fillna(0)) <= tolerance):
                return False
        else:
            if not df1[col].equals(df2[col]):
                return False
    return True


def calculate_integrity_score(results: Dict) -> float:
    """Calculate integrity score (0-100) based on reconciliation results."""
    penalties = 0.0

    # Row count penalty
    row_count_check = results.get('row_count_check', {})
    if not row_count_check.get('match', False):
        difference = row_count_check.get('difference', 0)
        total = row_count_check.get('legacy_count', 1)
        diff_pct = (difference / total * 100) if total > 0 else 100
        penalties += min(diff_pct, 30)

    # Referential integrity penalty
    integrity_results = results.get('integrity_results', {})
    for check_name, check_result in integrity_results.items():
        orphan_count = check_result.get('orphan_count', 0)
        if orphan_count > 0:
            penalties += min(orphan_count, 20)

    # Sample comparison penalty
    sample_comparison = results.get('sample_comparison', {})
    match_rate = sample_comparison.get('match_rate', 100)
    penalties += (100 - match_rate) * 0.3

    # Checksums are not penalised here — legacy uses fixed-width CHAR types
    # (e.g. CHAR(30)) whose hash will always differ from modern VARCHAR values even
    # when the underlying data is identical after trimming. Checksum results are
    # preserved in the report for investigative use but do not affect the score.

    # Business aggregate penalty
    aggregate_validation = results.get('aggregate_validation', {})
    for agg_name, agg_result in aggregate_validation.items():
        if isinstance(agg_result, dict) and not agg_result.get('match', True):
            penalties += 10

    # Archived field leakage — compliance violation, 20 pts per leaked field, cap 40
    archived_leakage = results.get('archived_leakage', {})
    violation_count = archived_leakage.get('violation_count', 0)
    if violation_count > 0:
        penalties += min(violation_count * 20, 40)

    # Ungoverned columns — governance warning, 5 pts per column, cap 15
    unmapped_columns = results.get('unmapped_columns', {})
    ungoverned_count = unmapped_columns.get('count', 0)
    if ungoverned_count > 0:
        penalties += min(ungoverned_count * 5, 15)

    score = max(0, 100 - penalties)
    return round(score, 2)
