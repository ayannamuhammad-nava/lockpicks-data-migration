"""
Automatic metadata generation for RAG explanations.

Generates glossary.json and mappings.json from database schemas with
confidence scoring and optional user refinement.
"""
import json
import os
import logging
from typing import Dict, List, Tuple, Optional
from difflib import SequenceMatcher
import psycopg2.extensions

from tools.db_utils import get_table_schema

logger = logging.getLogger(__name__)


# PII keywords for detection
PII_KEYWORDS = [
    'email', 'phone', 'ssn', 'social_security', 'passport', 'license',
    'credit_card', 'card_number', 'dob', 'date_of_birth', 'address',
    'zip', 'postal', 'salary', 'income', 'tax',
    # COBOL abbreviated field patterns
    'emal', 'phon', 'bact', 'brtn', 'adr1', 'ein',
]


def detect_pii(column_name: str) -> bool:
    """
    Detect if column name suggests PII data.

    Args:
        column_name: Name of the column

    Returns:
        True if likely contains PII
    """
    name_lower = column_name.lower()
    return any(keyword in name_lower for keyword in PII_KEYWORDS)


def infer_description(column_name: str, data_type: str, is_nullable: bool) -> Tuple[str, float]:
    """
    Infer column description from name and type.

    Args:
        column_name: Name of the column
        data_type: SQL data type
        is_nullable: Whether column allows NULL

    Returns:
        Tuple of (description, confidence_score)
    """
    name = column_name.lower()
    confidence = 0.6  # Default medium confidence

    # Domain-specific PII patterns (high confidence, avoids circular descriptions)
    if name in ['ssn', 'clmt_ssn', 'cl_ssn', 'social_security', 'social_security_number']:
        return "Social Security Number - highly sensitive personal identifier", 0.95
    if name in ['ssn_hash']:
        return "Hashed Social Security Number for secure identification", 0.95
    if name in ['dob', 'date_of_birth', 'birth_date', 'clmt_dob', 'cl_dob']:
        return "Date of birth of the individual", 0.9
    if name in ['passport', 'passport_number']:
        return "Government-issued passport number", 0.9
    if 'bank_acct' in name or 'account_number' in name or name == 'bank_acct_num' or name == 'cl_bact':
        return "Bank account number - sensitive financial information", 0.95
    if 'bank_routing' in name or 'routing_number' in name or name == 'bank_routing_num' or name == 'cl_brtn':
        return "Bank routing number for electronic transfers", 0.95
    if name in ['ein', 'empl_ein', 'employer_ein', 'er_ein']:
        return "Employer Identification Number (federal tax ID)", 0.9

    # High confidence patterns
    if name.endswith('_id'):
        entity = name.replace('_id', '').replace('_', ' ')
        desc = f"Unique identifier for {entity}"
        confidence = 0.9
        return desc, confidence

    if name in ['id', 'pk']:
        desc = "Primary key identifier"
        confidence = 0.95
        return desc, confidence

    if name.startswith('created_') or name == 'created_at':
        desc = "Timestamp when the record was created"
        confidence = 0.9
        return desc, confidence

    if name.startswith('updated_') or name == 'updated_at':
        desc = "Timestamp when the record was last updated"
        confidence = 0.9
        return desc, confidence

    if name.endswith('_date') or name.endswith('_time') or name.endswith('_at'):
        event = name.replace('_date', '').replace('_time', '').replace('_at', '').replace('_', ' ')
        desc = f"Date/time when {event} occurred"
        confidence = 0.85
        return desc, confidence

    if 'email' in name:
        desc = "Email address for communication and identification"
        confidence = 0.9
        return desc, confidence

    if 'phone' in name or 'tel' in name:
        desc = "Phone number for contact purposes"
        confidence = 0.9
        return desc, confidence

    if 'name' in name:
        if 'first' in name:
            desc = "First name of the individual"
            confidence = 0.85
        elif 'last' in name:
            desc = "Last name of the individual"
            confidence = 0.85
        elif 'full' in name:
            desc = "Complete name of the individual or entity"
            confidence = 0.85
        else:
            desc = f"Name field for {name.replace('_name', '').replace('_', ' ')}"
            confidence = 0.7
        return desc, confidence

    if 'status' in name:
        desc = f"Current status of the {name.replace('_status', '').replace('status', 'record')}"
        confidence = 0.75
        return desc, confidence

    if 'total' in name or 'amount' in name or 'price' in name:
        desc = f"Monetary amount for {name.replace('_', ' ')}"
        confidence = 0.8
        return desc, confidence

    if 'count' in name or 'quantity' in name or 'qty' in name:
        desc = f"Numerical count of {name.replace('_', ' ')}"
        confidence = 0.75
        return desc, confidence

    # Medium confidence - type-based inference
    if data_type in ['integer', 'bigint', 'smallint']:
        desc = f"Numeric value for {name.replace('_', ' ')}"
        confidence = 0.5
    elif data_type in ['numeric', 'decimal', 'double precision', 'real']:
        desc = f"Decimal value for {name.replace('_', ' ')}"
        confidence = 0.5
    elif data_type in ['character varying', 'varchar', 'text', 'char']:
        desc = f"Text field for {name.replace('_', ' ')}"
        confidence = 0.5
    elif data_type == 'boolean':
        desc = f"Boolean flag indicating {name.replace('_', ' ').replace('is ', '')}"
        confidence = 0.6
    elif 'timestamp' in data_type or 'date' in data_type:
        desc = f"Date/time value for {name.replace('_', ' ')}"
        confidence = 0.6
    else:
        # Low confidence - generic description
        desc = f"{name.replace('_', ' ').title()} field"
        confidence = 0.3

    return desc, confidence


def _semantic_types_compatible(source_col: str, target_col: str) -> bool:
    """Check if two columns have compatible semantic types based on their names."""
    date_indicators = {'_date', '_time', '_at', 'timestamp', 'created', 'updated', '_dt',
                       '_rgdt', '_lupdt', '_fildt', '_paydt', '_wkedt', '_bystr', '_byend'}
    id_indicators = {'_id', '_key', '_pk', '_fk', '_ref',
                     'recid', '_clmnt', '_emplr', '_clmid'}
    name_indicators = {'_name', '_nm', 'first_', 'last_', 'full_',
                       '_fnam', '_lnam'}
    amount_indicators = {'_amt', '_amount', '_total', '_price', '_sum',
                         '_payam', '_wkamt', '_mxamt', '_totpd'}
    status_indicators = {'_status', 'status_', '_stat'}
    location_indicators = {'_state', '_city', '_zip', '_addr', '_address', 'address_', 'city', 'state', 'zip_code',
                           '_st', '_adr', 'cl_city', 'er_city', 'cl_zip', 'er_zip'}

    def get_type(col):
        col_lower = col.lower()
        # Check more specific types first
        if col_lower.endswith('_status') or col_lower.startswith('status_'):
            return 'status'
        if any(ind in col_lower for ind in location_indicators):
            return 'location'
        if any(ind in col_lower for ind in date_indicators):
            return 'temporal'
        if any(ind in col_lower for ind in id_indicators):
            return 'identifier'
        if any(ind in col_lower for ind in name_indicators):
            return 'name'
        if any(ind in col_lower for ind in amount_indicators):
            return 'amount'
        return 'generic'

    src_type = get_type(source_col)
    tgt_type = get_type(target_col)

    if src_type == 'generic' or tgt_type == 'generic':
        return True
    return src_type == tgt_type


def find_column_mapping(source_col: str, target_columns: List[str], threshold: float = 0.75) -> Optional[Tuple[str, float]]:
    """
    Find potential mapping for a source column in target schema.

    Uses fuzzy string matching with semantic type validation to find renamed columns.

    Args:
        source_col: Column name in source schema
        target_columns: List of column names in target schema
        threshold: Minimum similarity score (0.0 to 1.0)

    Returns:
        Tuple of (matched_column, confidence) or None
    """
    best_match = None
    best_score = 0.0

    source_lower = source_col.lower()

    for target_col in target_columns:
        target_lower = target_col.lower()

        # Skip incompatible semantic types (e.g., date -> id)
        if not _semantic_types_compatible(source_col, target_col):
            continue

        # Calculate similarity
        similarity = SequenceMatcher(None, source_lower, target_lower).ratio()

        # Boost score for common transformations
        if source_lower == target_lower:
            similarity = 1.0
        elif source_lower.replace('_', '') == target_lower.replace('_', ''):
            similarity = max(similarity, 0.95)
        elif source_lower.endswith('_date') and target_lower.endswith('_at'):
            if source_lower.replace('_date', '') == target_lower.replace('_at', ''):
                similarity = max(similarity, 0.9)
        elif source_lower.endswith('_dt') and target_lower.endswith('_date'):
            if source_lower.replace('_dt', '') == target_lower.replace('_date', ''):
                similarity = max(similarity, 0.9)
        elif 'name' in source_lower and 'name' in target_lower:
            similarity = max(similarity, 0.8)
        # Handle legacy abbreviation patterns
        elif source_lower.endswith('_nm') and target_lower.endswith('_name'):
            # e.g., clmt_first_nm → first_name
            src_stem = source_lower.replace('_nm', '')
            tgt_stem = target_lower.replace('_name', '')
            stem_sim = SequenceMatcher(None, src_stem, tgt_stem).ratio()
            if stem_sim > 0.5:
                similarity = max(similarity, 0.85)

        # Handle prefix abbreviation patterns (e.g., clmt_id → claimant_id, pymt_amt → payment_amount)
        # Common government/mainframe abbreviations
        abbrev_map = {
            # Generic abbreviation expansions
            'clmt': 'claimant', 'empl': 'employer', 'bnf': 'benefit',
            'pymt': 'payment', 'dept': 'department', 'addr': 'address',
            'acct': 'account', 'amt': 'amount', 'num': 'number',
            'nm': 'name', 'dt': 'date', 'yr': 'year', 'wkly': 'weekly',
            # COBOL whole-name → modern expansions for fuzzy matching
            'cl_recid': 'claimant_id', 'cl_fnam': 'first_name',
            'cl_lnam': 'last_name', 'cl_ssn': 'ssn',
            'cl_dob': 'date_of_birth', 'cl_phon': 'phone_number',
            'cl_emal': 'email', 'cl_adr1': 'address_line1',
            'cl_city': 'city', 'cl_st': 'state', 'cl_zip': 'zip_code',
            'cl_bact': 'bank_account', 'cl_brtn': 'bank_routing',
            'cl_stat': 'claimant_status', 'cl_rgdt': 'registered_at',
            'cl_dcsd': 'is_deceased',
            'er_recid': 'employer_id', 'er_name': 'employer_name',
            'er_ein': 'employer_ein', 'er_ind': 'industry',
            'er_adr1': 'address_line1', 'er_city': 'city',
            'er_st': 'state', 'er_zip': 'zip_code',
            'er_phon': 'phone_number', 'er_stat': 'employer_status',
            'cm_recid': 'claim_id', 'cm_clmnt': 'claimant_id',
            'cm_emplr': 'employer_id', 'cm_seprs': 'separation_reason',
            'cm_fildt': 'filing_date', 'cm_bystr': 'benefit_year_start',
            'cm_byend': 'benefit_year_end', 'cm_wkamt': 'weekly_benefit_amount',
            'cm_mxamt': 'max_benefit_amount', 'cm_totpd': 'total_paid',
            'cm_wkcnt': 'weeks_claimed', 'cm_stat': 'claim_status',
            'cm_lupdt': 'updated_at',
            'bp_recid': 'payment_id', 'bp_clmid': 'claim_id',
            'bp_paydt': 'payment_date', 'bp_payam': 'payment_amount',
            'bp_methd': 'payment_method', 'bp_wkedt': 'week_ending_date',
            'bp_stat': 'payment_status', 'bp_chkno': 'check_number',
        }
        # Fully expand all abbreviations in source, then compare
        expanded = source_lower
        for a, f in sorted(abbrev_map.items(), key=lambda x: -len(x[0])):
            expanded = expanded.replace(a, f)
        if expanded != source_lower:
            expanded_sim = SequenceMatcher(None, expanded, target_lower).ratio()
            # Only use expanded score if it indicates a strong match
            if expanded_sim > 0.85:
                similarity = max(similarity, expanded_sim)

        if similarity > best_score:
            best_score = similarity
            best_match = target_col

    if best_score >= threshold:
        return best_match, best_score

    return None


def generate_mapping_rationale(source_col: str, target_col: str, source_type: str, target_type: str) -> str:
    """
    Generate rationale for a schema mapping.

    Args:
        source_col: Source column name
        target_col: Target column name
        source_type: Source data type
        target_type: Target data type

    Returns:
        Human-readable rationale
    """
    source_lower = source_col.lower()
    target_lower = target_col.lower()

    # Same name, different type
    if source_col == target_col and source_type != target_type:
        return f"Type changed from {source_type} to {target_type} for improved data integrity and validation"

    # Renamed column
    if source_lower.replace('_date', '') == target_lower.replace('_at', ''):
        return "Standardized timestamp naming to use '_at' suffix for consistency across temporal fields"

    if 'name' in source_lower and 'name' in target_lower:
        return f"Renamed from {source_col} to {target_col} for improved clarity and naming consistency"

    # Generic rename
    return f"Renamed from {source_col} to {target_col} to align with modern naming conventions"


def generate_glossary(
    legacy_conn: psycopg2.extensions.connection,
    modern_conn: psycopg2.extensions.connection,
    tables: List[str]
) -> Dict:
    """
    Generate glossary.json from database schemas.

    Args:
        legacy_conn: Legacy database connection
        modern_conn: Modern database connection
        tables: List of table names to process

    Returns:
        Glossary dict with columns and confidence scores
    """
    logger.info("Generating glossary from database schemas...")

    glossary = {"columns": []}
    stats = {"total": 0, "high_confidence": 0, "medium_confidence": 0, "low_confidence": 0}

    for table in tables:
        # Process legacy columns
        try:
            legacy_schema = get_table_schema(legacy_conn, table)
            for col_info in legacy_schema:
                desc, confidence = infer_description(
                    col_info['column_name'],
                    col_info['data_type'],
                    col_info['is_nullable'] == 'YES'
                )

                entry = {
                    "name": col_info['column_name'],
                    "description": desc,
                    "system": "legacy",
                    "pii": detect_pii(col_info['column_name']),
                    "confidence": round(confidence, 2),
                    "table": table
                }
                glossary["columns"].append(entry)
                stats["total"] += 1

                if confidence >= 0.8:
                    stats["high_confidence"] += 1
                elif confidence >= 0.6:
                    stats["medium_confidence"] += 1
                else:
                    stats["low_confidence"] += 1

        except Exception as e:
            logger.warning(f"Could not process legacy table {table}: {e}")

        # Process modern columns
        try:
            modern_schema = get_table_schema(modern_conn, table)
            for col_info in modern_schema:
                desc, confidence = infer_description(
                    col_info['column_name'],
                    col_info['data_type'],
                    col_info['is_nullable'] == 'YES'
                )

                entry = {
                    "name": col_info['column_name'],
                    "description": desc,
                    "system": "modern",
                    "pii": detect_pii(col_info['column_name']),
                    "confidence": round(confidence, 2),
                    "table": table
                }
                glossary["columns"].append(entry)
                stats["total"] += 1

                if confidence >= 0.8:
                    stats["high_confidence"] += 1
                elif confidence >= 0.6:
                    stats["medium_confidence"] += 1
                else:
                    stats["low_confidence"] += 1

        except Exception as e:
            logger.warning(f"Could not process modern table {table}: {e}")

    logger.info(f"Generated {stats['total']} column descriptions:")
    logger.info(f"  High confidence (≥80%): {stats['high_confidence']}")
    logger.info(f"  Medium confidence (60-80%): {stats['medium_confidence']}")
    logger.info(f"  Low confidence (<60%): {stats['low_confidence']}")

    glossary["_metadata"] = stats

    return glossary


def generate_mappings(
    legacy_conn: psycopg2.extensions.connection,
    modern_conn: psycopg2.extensions.connection,
    tables: List[str]
) -> Dict:
    """
    Generate mappings.json by comparing schemas.

    Args:
        legacy_conn: Legacy database connection
        modern_conn: Modern database connection
        tables: List of table names to process

    Returns:
        Mappings dict with transformation rationales
    """
    logger.info("Detecting schema mappings...")

    mappings = {"mappings": []}
    stats = {"total": 0, "high_confidence": 0, "medium_confidence": 0}

    for table in tables:
        try:
            legacy_schema = get_table_schema(legacy_conn, table)
            modern_schema = get_table_schema(modern_conn, table)

            # Build modern column list
            modern_cols = [col['column_name'] for col in modern_schema]
            modern_types = {col['column_name']: col['data_type'] for col in modern_schema}

            # Track columns consumed by direct matches so fuzzy matching doesn't reuse them
            consumed_modern_cols = set()

            # First pass: identify direct matches
            for legacy_col_info in legacy_schema:
                legacy_col = legacy_col_info['column_name']
                if legacy_col in modern_cols:
                    consumed_modern_cols.add(legacy_col)

            for legacy_col_info in legacy_schema:
                legacy_col = legacy_col_info['column_name']
                legacy_type = legacy_col_info['data_type']

                # Check if column exists in modern
                if legacy_col in modern_cols:
                    # Column exists - check for type change
                    modern_type = modern_types[legacy_col]
                    if legacy_type != modern_type:
                        mapping = {
                            "source": legacy_col,
                            "target": legacy_col,
                            "rationale": generate_mapping_rationale(legacy_col, legacy_col, legacy_type, modern_type),
                            "confidence": 0.95,
                            "table": table,
                            "type": "type_change"
                        }
                        mappings["mappings"].append(mapping)
                        stats["total"] += 1
                        stats["high_confidence"] += 1
                else:
                    # Column missing - find potential mapping (excluding already-consumed columns)
                    available_cols = [c for c in modern_cols if c not in consumed_modern_cols]
                    match_result = find_column_mapping(legacy_col, available_cols)

                    if match_result:
                        target_col, confidence = match_result
                        target_type = modern_types[target_col]
                        consumed_modern_cols.add(target_col)

                        mapping = {
                            "source": legacy_col,
                            "target": target_col,
                            "rationale": generate_mapping_rationale(legacy_col, target_col, legacy_type, target_type),
                            "confidence": round(confidence, 2),
                            "table": table,
                            "type": "rename"
                        }
                        mappings["mappings"].append(mapping)
                        stats["total"] += 1

                        if confidence >= 0.8:
                            stats["high_confidence"] += 1
                        else:
                            stats["medium_confidence"] += 1
                    else:
                        # No match found - column removed
                        mapping = {
                            "source": legacy_col,
                            "target": None,
                            "rationale": f"Column removed from modern system. May require data archival or migration to alternate storage.",
                            "confidence": 0.7,
                            "table": table,
                            "type": "removed"
                        }
                        mappings["mappings"].append(mapping)
                        stats["total"] += 1
                        stats["medium_confidence"] += 1

        except Exception as e:
            logger.warning(f"Could not process mappings for table {table}: {e}")

    logger.info(f"Detected {stats['total']} schema mappings:")
    logger.info(f"  High confidence (≥80%): {stats['high_confidence']}")
    logger.info(f"  Medium confidence (<80%): {stats['medium_confidence']}")

    mappings["_metadata"] = stats

    return mappings


def interactive_refinement(glossary: Dict, mappings: Dict, confidence_threshold: float = 0.7) -> Tuple[Dict, Dict]:
    """
    Prompt user to refine low-confidence items.

    Args:
        glossary: Generated glossary dict
        mappings: Generated mappings dict
        confidence_threshold: Only prompt for items below this confidence

    Returns:
        Tuple of (refined_glossary, refined_mappings)
    """
    # Find low-confidence glossary entries
    low_confidence_columns = [
        col for col in glossary["columns"]
        if col.get("confidence", 1.0) < confidence_threshold
    ]

    if low_confidence_columns:
        print(f"\n⚠️  Found {len(low_confidence_columns)} column(s) with low confidence.")
        print("Please help refine these descriptions:\n")

        for col in low_confidence_columns:
            print(f"  Column: {col['name']} ({col['system']} system)")
            print(f"  Auto-generated: \"{col['description']}\"")
            print(f"  Confidence: {col['confidence']*100:.0f}%")

            better = input("  Provide better description? (or press Enter to keep): ").strip()
            if better:
                col['description'] = better
                col['confidence'] = 1.0
                col['user_refined'] = True
                print("  ✓ Updated\n")
            else:
                print("  ✓ Kept auto-generated\n")

    # Find low-confidence mappings
    low_confidence_mappings = [
        m for m in mappings["mappings"]
        if m.get("confidence", 1.0) < confidence_threshold
    ]

    if low_confidence_mappings:
        print(f"\n⚠️  Found {len(low_confidence_mappings)} mapping(s) with low confidence.")
        print("Please confirm or refine:\n")

        for mapping in low_confidence_mappings:
            if mapping['target']:
                print(f"  Mapping: {mapping['source']} → {mapping['target']}")
            else:
                print(f"  Mapping: {mapping['source']} → (removed)")

            print(f"  Rationale: \"{mapping['rationale']}\"")
            print(f"  Confidence: {mapping['confidence']*100:.0f}%")

            action = input("  Correct? (y/n/edit): ").strip().lower()

            if action == 'n':
                mapping['confidence'] = 0.0
                mapping['user_rejected'] = True
                print("  ✗ Marked as incorrect\n")
            elif action == 'edit':
                new_rationale = input("  New rationale: ").strip()
                if new_rationale:
                    mapping['rationale'] = new_rationale
                    mapping['confidence'] = 1.0
                    mapping['user_refined'] = True
                print("  ✓ Updated\n")
            else:
                mapping['confidence'] = 1.0
                mapping['user_confirmed'] = True
                print("  ✓ Confirmed\n")

    return glossary, mappings


def save_metadata(glossary: Dict, mappings: Dict, output_dir: str = './metadata'):
    """
    Save glossary and mappings to JSON files.

    Args:
        glossary: Glossary dict
        mappings: Mappings dict
        output_dir: Output directory
    """
    os.makedirs(output_dir, exist_ok=True)

    # Remove metadata stats before saving
    glossary_copy = glossary.copy()
    mappings_copy = mappings.copy()
    glossary_copy.pop('_metadata', None)
    mappings_copy.pop('_metadata', None)

    glossary_path = os.path.join(output_dir, 'glossary.json')
    with open(glossary_path, 'w') as f:
        json.dump(glossary_copy, f, indent=2)
    logger.info(f"Saved glossary to {glossary_path}")

    mappings_path = os.path.join(output_dir, 'mappings.json')
    with open(mappings_path, 'w') as f:
        json.dump(mappings_copy, f, indent=2)
    logger.info(f"Saved mappings to {mappings_path}")


def generate_metadata(
    legacy_conn: psycopg2.extensions.connection,
    modern_conn: psycopg2.extensions.connection,
    tables: List[str],
    interactive: bool = True,
    confidence_threshold: float = 0.7,
    output_dir: str = './metadata'
) -> Tuple[Dict, Dict]:
    """
    Main function to generate metadata with optional user refinement.

    Args:
        legacy_conn: Legacy database connection
        modern_conn: Modern database connection
        tables: List of table names
        interactive: Whether to prompt user for low-confidence items
        confidence_threshold: Confidence threshold for prompts
        output_dir: Output directory for metadata files

    Returns:
        Tuple of (glossary, mappings)
    """
    print("\n🔍 Analyzing database schemas...")
    print(f"   Tables: {', '.join(tables)}\n")

    # Generate glossary
    glossary = generate_glossary(legacy_conn, modern_conn, tables)

    # Generate mappings
    mappings = generate_mappings(legacy_conn, modern_conn, tables)

    # Interactive refinement if enabled
    if interactive:
        glossary, mappings = interactive_refinement(glossary, mappings, confidence_threshold)

    # Save to files
    save_metadata(glossary, mappings, output_dir)

    print("\n✅ Metadata generation complete!")
    print(f"   📄 Glossary: {len(glossary['columns'])} columns")
    print(f"   🔄 Mappings: {len(mappings['mappings'])} transformations")
    print(f"   📁 Saved to: {output_dir}/")
    print("\n💡 RAG is now ready to provide intelligent explanations!\n")

    return glossary, mappings
