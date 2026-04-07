"""
Generic metadata generation for RAG explanations.

Generates glossary.json and mappings.json from database schemas.
Domain-specific column overrides are provided by plugins via the
dm_get_column_overrides hook.
"""

import json
import logging
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Pattern-based description inference (generic, not domain-specific)
PATTERN_DESCRIPTIONS = {
    "_id": ("Unique identifier", 0.8),
    "created_at": ("Timestamp when record was created", 0.9),
    "updated_at": ("Timestamp when record was last updated", 0.9),
    "_date": ("Date field", 0.7),
    "_name": ("Name field", 0.7),
    "_code": ("Code or identifier", 0.7),
    "_type": ("Type classification", 0.7),
    "_status": ("Status field", 0.7),
    "_amount": ("Monetary amount", 0.7),
    "_count": ("Count or quantity", 0.7),
    "email": ("Email address", 0.8),
    "phone": ("Phone number", 0.8),
    "address": ("Physical address", 0.8),
}

PII_KEYWORDS = [
    "ssn", "social_security", "passport", "drivers_license",
    "credit_card", "card_number", "cvv", "account_number",
    "routing_number", "bank", "dob", "date_of_birth",
    "salary", "income", "email", "phone", "address",
    "zip", "latitude", "longitude", "ip_address",
]


def detect_pii(column_name: str, extra_keywords: Optional[List[str]] = None) -> bool:
    """Check if a column name indicates PII."""
    col_lower = column_name.lower()
    keywords = PII_KEYWORDS + (extra_keywords or [])
    return any(kw in col_lower for kw in keywords)


def infer_description(column_name: str, data_type: str) -> Tuple[str, float]:
    """Infer a column description from its name and type. Returns (description, confidence)."""
    col_lower = column_name.lower()
    for pattern, (desc, conf) in PATTERN_DESCRIPTIONS.items():
        if pattern in col_lower:
            return f"{desc} for {column_name}", conf
    return f"{data_type.capitalize()} value for {column_name}", 0.3


def find_matching_column(
    source_col: str,
    target_columns: List[str],
    threshold: float = 0.7,
) -> Optional[Tuple[str, float]]:
    """Fuzzy-match a source column to the closest target column."""
    best_match = None
    best_ratio = 0.0

    for target_col in target_columns:
        ratio = SequenceMatcher(None, source_col.lower(), target_col.lower()).ratio()
        if ratio > best_ratio and ratio >= threshold:
            best_ratio = ratio
            best_match = target_col

    return (best_match, best_ratio) if best_match else None


def generate_metadata(
    legacy_conn: Any,
    modern_conn: Any,
    tables: List[str],
    output_dir: str = "./metadata",
    plugin_manager: Any = None,
    interactive: bool = False,
    confidence_threshold: float = 0.7,
) -> Tuple[Dict, Dict]:
    """Generate glossary.json and mappings.json from database schemas.

    Args:
        legacy_conn: Legacy database connector.
        modern_conn: Modern database connector.
        tables: Tables to introspect.
        output_dir: Where to write the JSON files.
        plugin_manager: pluggy PluginManager (for dm_get_column_overrides hook).
        interactive: If True, prompt for low-confidence entries.
        confidence_threshold: Minimum confidence before prompting.

    Returns:
        Tuple of (glossary_data, mappings_data).
    """
    glossary_entries = []
    mappings_list = []

    pii_keywords = PII_KEYWORDS

    for table in tables:
        logger.info(f"Generating metadata for table: {table}")

        legacy_schema = legacy_conn.get_table_schema(table)
        modern_schema = modern_conn.get_table_schema(table)

        legacy_cols = {col["column_name"]: col["data_type"] for col in legacy_schema}
        modern_cols = {col["column_name"]: col["data_type"] for col in modern_schema}

        # Get plugin overrides for this table
        overrides = {}
        if plugin_manager:
            results = plugin_manager.hook.dm_get_column_overrides(table=table)
            for result in results:
                if result:
                    overrides.update(result)

        # Process legacy columns
        for col_name, data_type in legacy_cols.items():
            desc, conf = infer_description(col_name, data_type)
            is_pii = detect_pii(col_name, pii_keywords)

            entry = {
                "name": col_name,
                "description": desc,
                "system": "legacy",
                "pii": is_pii,
                "confidence": conf,
                "table": table,
            }

            # Enrich via plugins
            if plugin_manager:
                results = plugin_manager.hook.dm_enrich_glossary_entry(entry=entry)
                for result in results:
                    if result:
                        entry = result

            glossary_entries.append(entry)

            # Build mapping
            col_lower = col_name.lower()
            if col_lower in overrides:
                override = overrides[col_lower]
                mappings_list.append({
                    "source": col_name,
                    "target": override.get("target"),
                    "type": override.get("type", "transform"),
                    "rationale": override.get("rationale", ""),
                    "confidence": override.get("confidence", 1.0),
                    "table": table,
                })
            else:
                # Try fuzzy matching
                match = find_matching_column(col_name, list(modern_cols.keys()))
                if match:
                    target_col, ratio = match
                    mappings_list.append({
                        "source": col_name,
                        "target": target_col,
                        "type": "rename",
                        "rationale": f"Renamed from {col_name} to {target_col} (similarity: {ratio:.2f})",
                        "confidence": round(ratio, 2),
                        "table": table,
                    })
                elif col_name in modern_cols:
                    mappings_list.append({
                        "source": col_name,
                        "target": col_name,
                        "type": "rename",
                        "rationale": "Column name unchanged",
                        "confidence": 1.0,
                        "table": table,
                    })

        # Process modern-only columns for glossary
        for col_name, data_type in modern_cols.items():
            if col_name not in legacy_cols:
                desc, conf = infer_description(col_name, data_type)
                glossary_entries.append({
                    "name": col_name,
                    "description": desc,
                    "system": "modern",
                    "pii": detect_pii(col_name, pii_keywords),
                    "confidence": conf,
                    "table": table,
                })

    glossary_data = {"columns": glossary_entries}
    mappings_data = {"mappings": mappings_list}

    # Save to files
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    with open(output_path / "glossary.json", "w") as f:
        json.dump(glossary_data, f, indent=2)
    logger.info(f"Saved glossary.json ({len(glossary_entries)} entries)")

    with open(output_path / "mappings.json", "w") as f:
        json.dump(mappings_data, f, indent=2)
    logger.info(f"Saved mappings.json ({len(mappings_list)} mappings)")

    return glossary_data, mappings_data


def generate_metadata_from_om(
    om_enricher: Any,
    tables: List[str],
    output_dir: str = "./metadata",
    plugin_manager: Any = None,
) -> Tuple[Dict, Dict]:
    """Generate glossary.json and mappings.json from OpenMetadata catalog.

    Uses OM as the source of truth for legacy schema — no modern DB needed.
    Column descriptions, PII tags, and glossary terms come from OM.
    Plugin overrides are applied on top.

    Args:
        om_enricher: An OpenMetadataEnricher instance (connected).
        tables: Table names to process.
        output_dir: Where to write the JSON files.
        plugin_manager: pluggy PluginManager for hooks.

    Returns:
        Tuple of (glossary_data, mappings_data).
    """
    glossary_entries = []
    mappings_list = []

    pii_keywords = PII_KEYWORDS

    for table in tables:
        logger.info(f"Generating metadata from OM for table: {table}")

        # Fetch schema from OM catalog (rich: includes descriptions, tags)
        om_schema = om_enricher.get_table_schema(table)

        # Fetch OM profiling stats
        try:
            om_profile = om_enricher.get_table_profile(table)
        except Exception:
            logger.warning(f"Could not fetch profiling for {table}")
            om_profile = {"columns": {}}

        # Fetch OM glossary terms for this table
        try:
            om_glossary = om_enricher.get_glossary_for_table(table)
        except Exception:
            om_glossary = {}

        # Get plugin overrides
        overrides = {}
        if plugin_manager:
            results = plugin_manager.hook.dm_get_column_overrides(table=table)
            for result in results:
                if result:
                    overrides.update(result)

        for col in om_schema:
            col_name = col["column_name"]
            data_type = col.get("data_type", "VARCHAR")
            om_desc = col.get("description", "")
            om_tags = col.get("tags", [])

            # Build glossary entry — prefer OM data over inference
            if om_desc:
                desc = om_desc
                conf = 1.0
            else:
                desc, conf = infer_description(col_name, data_type)

            # PII: trust OM tags first, fall back to keyword matching
            pii_tags = [t for t in om_tags if "PII" in t or "Sensitive" in t]
            is_pii = bool(pii_tags) or detect_pii(col_name, pii_keywords)

            # Glossary term from OM
            glossary_term = om_glossary.get(col_name, {})

            entry = {
                "name": col_name,
                "description": desc,
                "system": "legacy",
                "pii": is_pii,
                "pii_tags": pii_tags if pii_tags else [],
                "confidence": conf,
                "table": table,
                "data_type": data_type,
                "data_type_display": col.get("data_type_display", data_type),
                "is_nullable": col.get("is_nullable", "YES"),
            }

            if glossary_term:
                entry["glossary_term"] = glossary_term.get("term_name", "")
                entry["glossary_fqn"] = glossary_term.get("term_fqn", "")

            # Add profiling stats if available
            col_profile = om_profile.get("columns", {}).get(col_name, {})
            if col_profile:
                entry["profiling"] = col_profile

            # Enrich via plugins (OM plugin + domain plugins)
            if plugin_manager:
                results = plugin_manager.hook.dm_enrich_glossary_entry(entry=entry)
                for result in results:
                    if result:
                        entry = result

            glossary_entries.append(entry)

            # Build mapping
            col_lower = col_name.lower()
            if col_lower in overrides:
                override = overrides[col_lower]
                mappings_list.append({
                    "source": col_name,
                    "target": override.get("target"),
                    "type": override.get("type", "transform"),
                    "rationale": override.get("rationale", ""),
                    "confidence": override.get("confidence", 1.0),
                    "table": table,
                })
            else:
                # No modern schema to match against — mark as pending
                # The schema generator will resolve these later
                mappings_list.append({
                    "source": col_name,
                    "target": None,
                    "type": "pending",
                    "rationale": "Awaiting schema generation",
                    "confidence": conf,
                    "table": table,
                })

    glossary_data = {"columns": glossary_entries}
    mappings_data = {"mappings": mappings_list}

    # Save to files
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    with open(output_path / "glossary.json", "w") as f:
        json.dump(glossary_data, f, indent=2)
    logger.info(f"Saved glossary.json ({len(glossary_entries)} entries)")

    with open(output_path / "mappings.json", "w") as f:
        json.dump(mappings_data, f, indent=2)
    logger.info(f"Saved mappings.json ({len(mappings_list)} mappings)")

    return glossary_data, mappings_data
