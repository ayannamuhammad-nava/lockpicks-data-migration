"""
Data governance checks including PII detection, naming conventions, and data quality rules.
"""
import pandas as pd
import re
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


def detect_pii_columns(
    df: pd.DataFrame,
    pii_keywords: List[str]
) -> List[str]:
    """
    Detect columns that may contain PII based on keyword matching.

    Args:
        df: pandas DataFrame
        pii_keywords: List of PII-related keywords (e.g., ['ssn', 'passport', 'credit_card'])

    Returns:
        List of column names that may contain PII
    """
    pii_columns = []

    for col in df.columns:
        col_lower = col.lower()
        for keyword in pii_keywords:
            if keyword.lower() in col_lower:
                pii_columns.append(col)
                logger.warning(f"PII detected in column: {col}")
                break

    return pii_columns


def check_naming_conventions(
    columns: List[str],
    regex_pattern: str = r"^[a-z0-9_]+$"
) -> Dict[str, List[str]]:
    """
    Check if column names follow naming conventions.

    Args:
        columns: List of column names
        regex_pattern: Regex pattern for valid names (default: snake_case)

    Returns:
        Dict with 'valid' and 'invalid' column name lists
    """
    pattern = re.compile(regex_pattern)

    valid_columns = []
    invalid_columns = []

    for col in columns:
        if pattern.match(col):
            valid_columns.append(col)
        else:
            invalid_columns.append(col)
            logger.warning(f"Invalid column name: {col}")

    return {
        'valid': valid_columns,
        'invalid': invalid_columns
    }


def check_required_fields(
    df: pd.DataFrame,
    required_fields: List[str]
) -> Dict[str, bool]:
    """
    Check if required fields are present in the DataFrame.

    Args:
        df: pandas DataFrame
        required_fields: List of required column names

    Returns:
        Dict mapping field_name -> is_present (bool)
    """
    presence = {}

    for field in required_fields:
        is_present = field in df.columns
        presence[field] = is_present

        if not is_present:
            logger.error(f"Required field missing: {field}")

    return presence


def check_null_thresholds(
    df: pd.DataFrame,
    max_null_percent: float = 20.0
) -> Dict[str, Dict]:
    """
    Check null percentages against threshold.

    Args:
        df: pandas DataFrame
        max_null_percent: Maximum acceptable null percentage

    Returns:
        Dict mapping column_name -> {null_pct, exceeds_threshold}
    """
    results = {}

    for col in df.columns:
        null_count = df[col].isna().sum()
        total_count = len(df)
        null_pct = (null_count / total_count * 100) if total_count > 0 else 0

        exceeds_threshold = bool(null_pct > max_null_percent)

        results[col] = {
            'null_pct': round(null_pct, 2),
            'exceeds_threshold': exceeds_threshold
        }

        if exceeds_threshold:
            logger.warning(f"Column '{col}' exceeds null threshold: {null_pct:.2f}%")

    return results


def run_governance_checks(
    df: pd.DataFrame,
    config: Dict
) -> Dict:
    """
    Run comprehensive governance checks on a DataFrame.

    Args:
        df: pandas DataFrame
        config: Governance configuration dict with keys:
            - pii_keywords: List of PII keywords
            - naming_regex: Regex pattern for naming conventions
            - max_null_percent: Max acceptable null percentage
            - required_fields: List of required field names

    Returns:
        Dict with all governance check results
    """
    logger.info(f"Running governance checks on {len(df)} rows, {len(df.columns)} columns")

    results = {
        'pii_columns': detect_pii_columns(df, config.get('pii_keywords', [])),
        'naming_check': check_naming_conventions(
            df.columns.tolist(),
            config.get('naming_regex', r"^[a-z0-9_]+$")
        ),
        'required_fields': check_required_fields(
            df,
            config.get('required_fields', [])
        ),
        'null_checks': check_null_thresholds(
            df,
            config.get('max_null_percent', 20.0)
        )
    }

    # Calculate overall governance score (0-100)
    score = calculate_governance_score(results, df)
    results['governance_score'] = score

    logger.info(f"Governance score: {score}/100")

    return results


def calculate_governance_score(results: Dict, df: pd.DataFrame) -> float:
    """
    Calculate an overall governance score (0-100).

    Args:
        results: Governance check results
        df: pandas DataFrame

    Returns:
        Score from 0-100
    """
    penalties = 0.0

    # PII penalty: -10 points per PII column found
    pii_count = len(results.get('pii_columns', []))
    penalties += pii_count * 10

    # Naming penalty: -5 points per invalid column name
    invalid_names = len(results.get('naming_check', {}).get('invalid', []))
    penalties += invalid_names * 5

    # Required fields penalty: -20 points per missing required field
    missing_required = sum(1 for present in results.get('required_fields', {}).values() if not present)
    penalties += missing_required * 20

    # Null threshold penalty: -5 points per column exceeding threshold
    null_violations = sum(1 for data in results.get('null_checks', {}).values() if data.get('exceeds_threshold', False))
    penalties += null_violations * 5

    # Calculate final score (minimum 0)
    score = max(0, 100 - penalties)

    return round(score, 2)


def generate_governance_report_csv(results: Dict, df: pd.DataFrame) -> str:
    """
    Generate CSV content for governance report.

    Args:
        results: Governance check results
        df: pandas DataFrame

    Returns:
        CSV formatted string
    """
    csv_lines = ["category,item,status,details\n"]

    # PII checks
    for col in results.get('pii_columns', []):
        csv_lines.append(f"PII,{col},VIOLATION,Contains PII keywords\n")

    # Naming checks
    for col in results.get('naming_check', {}).get('invalid', []):
        csv_lines.append(f"Naming,{col},VIOLATION,Invalid naming convention\n")

    # Required fields
    for field, present in results.get('required_fields', {}).items():
        status = "PASS" if present else "VIOLATION"
        details = "Present" if present else "Missing"
        csv_lines.append(f"Required Field,{field},{status},{details}\n")

    # Null checks
    for col, data in results.get('null_checks', {}).items():
        status = "VIOLATION" if data.get('exceeds_threshold') else "PASS"
        details = f"{data.get('null_pct', 0)}% null"
        csv_lines.append(f"Null Check,{col},{status},{details}\n")

    return ''.join(csv_lines)
