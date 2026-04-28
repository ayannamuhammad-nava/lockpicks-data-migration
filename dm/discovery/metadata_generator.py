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
    # COBOL abbreviated PII patterns
    "bact", "brtn", "bacct", "broute", "bkact", "bkrtn",
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


# ── COBOL Abbreviation Expansion ─────────────────────────────────────────────
# Maps common COBOL copybook abbreviations to their expanded modern equivalents.
# Used to bridge the gap between legacy abbreviated names (cl_fnam, bp_payam)
# and modern descriptive names (first_name, payment_amount) that fall below
# the default SequenceMatcher similarity threshold.

COBOL_ABBREVIATIONS = {
    # Name fields
    "fnam": "first_name", "lnam": "last_name", "mnam": "middle_name",
    "name": "name", "nm": "name",
    # Identifiers — note: recid is intentionally not mapped to a single target
    # because the modern name depends on the table (payment_id, claimant_id, etc.)
    # The containment matcher will resolve it via the table-specific prefix.
    "seqno": "sequence_number", "seqnbr": "sequence_number",
    "ein": "ein", "tin": "tin",
    # Dates and times
    "dob": "date_of_birth", "dt": "date", "dte": "date",
    "fildt": "filing_date", "paydt": "payment_date", "effdt": "effective_date",
    "rgdt": "registered_at", "lupdt": "updated_at", "crtdt": "created_at",
    "bystr": "benefit_year_start", "byend": "benefit_year_end",
    # Name extras
    "sufx": "name_suffix", "sfx": "name_suffix", "prfx": "name_prefix",
    "gndr": "gender", "gnd": "gender", "sex": "gender",
    "ethn": "ethnicity", "race": "ethnicity",
    # Contact info
    "phon": "phone_number", "phn": "phone_number", "tel": "phone_number",
    "ptel": "primary_phone", "mtel": "mobile_phone", "wtel": "work_phone",
    "htel": "home_phone", "fax": "fax_number",
    "emal": "email", "email": "email",
    "adr1": "address_line1", "adr2": "address_line2", "addr": "address",
    "madr1": "mailing_address_line1", "madr2": "mailing_address_line2",
    "mcity": "mailing_city", "mst": "mailing_state", "mzip": "mailing_zip_code",
    "adtyp": "address_type", "atyp": "address_type",
    "city": "city", "st": "state", "zip": "zip_code",
    # Financial
    "payam": "payment_amount", "amt": "amount", "wkamt": "weekly_benefit_amount",
    "mxamt": "max_benefit_amount", "totpd": "total_paid",
    "bact": "bank_account", "brtn": "bank_routing",
    # Status and codes
    "stat": "status", "sts": "status", "typ": "type", "cd": "code",
    "ind": "industry", "methd": "method",
    # Counts
    "wkcnt": "weeks_claimed", "cnt": "count", "qty": "quantity",
    # References
    "clmnt": "claimant_id", "clmid": "claim_id", "emplr": "employer_id",
    "custid": "customer_id", "empid": "employee_id",
    # Separation / reason
    "seprs": "separation_reason", "rsn": "reason",
    # Flags
    "dcsd": "is_deceased", "flg": "flag", "indr": "indicator",
    "vetf": "is_veteran", "disf": "is_disabled",
    # Personal / demographic
    "mstat": "marital_status", "dpnds": "dependents_count",
    "lang": "language_preference", "srccd": "source_code",
    # Emergency contact
    "emrg": "emergency_contact_name", "etel": "emergency_contact_phone",
    "erel": "emergency_contact_relation",
    # Driver's license
    "dln": "drivers_license_number", "dlno": "drivers_license_number",
    "dlst": "drivers_license_state",
    # Update dates
    "upddt": "updated_at", "moddt": "modified_at",
    # Misc
    "chkno": "check_number", "nbr": "number", "num": "number",
    "desc": "description", "cmnt": "comment", "rmrk": "remark",
    "wkedt": "week_ending_date",
    "fil1": "_filler", "fil2": "_filler", "fil3": "_filler",
    "filler": "_filler",
}

# Common COBOL table prefixes (2-3 char) that should be stripped before matching
# e.g., cl_fnam -> fnam, bp_payam -> payam, er_name -> name, cm_stat -> stat
_PREFIX_PATTERN = None


def _strip_cobol_prefix(col_name: str) -> str:
    """Strip a 2-3 character COBOL table prefix from a column name.

    Examples: cl_fnam -> fnam, bp_payam -> payam, er_name -> name
    """
    import re
    # Match 2-3 lowercase letters followed by underscore at the start
    match = re.match(r'^[a-z]{2,3}_(.+)$', col_name.lower())
    return match.group(1) if match else col_name.lower()


def parse_cobol_description(col_name: str, description: str) -> Optional[Tuple[str, str]]:
    """Parse a COBOL copybook description to extract an abbreviation mapping.

    COBOL descriptions follow patterns like:
        CONTACT-FIRST-NAME, CLAIMANT-SSN, PAYMENT-PROCESS-DATE,
        EMPLOYER-INDUSTRY-CODE, CONTACT-VETERAN-FLAG

    Steps:
    1. Strip the table/entity prefix (CONTACT-, CLAIMANT-, PAYMENT-, etc.)
    2. Convert remainder to snake_case (FIRST-NAME -> first_name)
    3. Return (suffix, modern_name) tuple

    Args:
        col_name: COBOL column name (e.g., "ct_fnam")
        description: COBOL copybook description (e.g., "CONTACT-FIRST-NAME")

    Returns:
        Tuple of (suffix, modern_name) or None if unparseable.

    Examples:
        ("ct_fnam", "CONTACT-FIRST-NAME") -> ("fnam", "first_name")
        ("bp_paydt", "PAYMENT-PROCESS-DATE") -> ("paydt", "payment_process_date")
        ("ct_vetf", "CONTACT-VETERAN-FLAG") -> ("vetf", "veteran_flag")
    """
    if not description:
        return None

    # Clean up description — remove extra whitespace, parens, etc.
    desc = description.strip().upper()

    # Skip non-COBOL descriptions (already modern or free-text)
    if " " in desc and "-" not in desc:
        return None  # Free-text description, not a copybook name

    # Remove common trailing noise: (Y/N), (PIC ...), etc.
    import re
    desc = re.sub(r'\s*\(.*?\)\s*$', '', desc)

    parts = desc.split("-")
    if len(parts) < 2:
        return None

    # Strip the first part (table/entity prefix: CONTACT, CLAIMANT, PAYMENT, etc.)
    meaningful_parts = parts[1:]

    # Convert to snake_case
    modern_name = "_".join(p.lower() for p in meaningful_parts if p)

    if not modern_name:
        return None

    # Get the COBOL suffix
    suffix = _strip_cobol_prefix(col_name)

    # Skip fillers
    if suffix.startswith("fil") or modern_name == "filler":
        return None

    # Don't override if suffix == modern_name (no abbreviation to learn)
    if suffix == modern_name:
        return None

    return (suffix, modern_name)


def generate_abbreviations_yaml(
    columns: List[dict],
    output_path: str,
    table_name: str = "",
) -> Dict[str, str]:
    """Auto-generate abbreviations.yaml from COBOL copybook descriptions.

    Parses the description field of each column to extract abbreviation mappings,
    then writes them to abbreviations.yaml in the project metadata directory.

    Args:
        columns: List of column dicts with 'column_name' and 'description' keys.
        output_path: Directory to write abbreviations.yaml.
        table_name: Optional table name for logging.

    Returns:
        Dict of {suffix: modern_name} mappings that were generated.
    """
    import yaml

    generated = {}
    for col in columns:
        col_name = col.get("column_name", col.get("name", ""))
        desc = col.get("description", "")
        result = parse_cobol_description(col_name, desc)
        if result:
            suffix, modern_name = result
            # Don't overwrite if already in the built-in dictionary
            if suffix not in COBOL_ABBREVIATIONS:
                generated[suffix] = modern_name

    if generated:
        out = Path(output_path)
        out.mkdir(parents=True, exist_ok=True)
        abbrev_file = out / "abbreviations.yaml"

        # Merge with existing file if present
        existing = {}
        if abbrev_file.exists():
            try:
                data = yaml.safe_load(abbrev_file.read_text()) or {}
                existing = data.get("abbreviations", {})
            except Exception:
                pass

        merged = {**existing, **generated}

        content = {
            "abbreviations": merged,
            "_generated_from": f"COBOL copybook descriptions for table: {table_name}" if table_name else "COBOL copybook descriptions",
            "_note": "Auto-generated by dm discover. Project-specific abbreviations are merged with the built-in dictionary during dm enrich. You can add or override entries manually.",
        }

        abbrev_file.write_text(
            yaml.dump(content, default_flow_style=False, sort_keys=False),
            encoding="utf-8",
        )
        logger.info(f"Generated abbreviations.yaml ({len(merged)} entries, {len(generated)} new)")

    return generated


def load_project_abbreviations(project_dir: str) -> Dict[str, str]:
    """Load project-specific abbreviations from abbreviations.yaml.

    Args:
        project_dir: Project directory path.

    Returns:
        Dict of {suffix: modern_name} abbreviation overrides.
    """
    import yaml

    # Check metadata dir first, then project root
    for subdir in ["metadata", "."]:
        abbrev_file = Path(project_dir) / subdir / "abbreviations.yaml"
        if abbrev_file.exists():
            try:
                data = yaml.safe_load(abbrev_file.read_text()) or {}
                abbrevs = data.get("abbreviations", {})
                if abbrevs:
                    logger.info(f"Loaded {len(abbrevs)} project abbreviations from {abbrev_file}")
                    return abbrevs
            except Exception as e:
                logger.warning(f"Could not load abbreviations.yaml: {e}")
    return {}


def expand_cobol_abbreviation(col_name: str, project_abbreviations: Optional[Dict[str, str]] = None) -> str:
    """Expand a COBOL abbreviated column name to its modern equivalent.

    Looks up the suffix in three sources (priority order):
    1. Project-specific abbreviations (from abbreviations.yaml)
    2. Built-in COBOL abbreviation dictionary
    3. Returns the raw suffix if no match

    Examples:
        cl_fnam -> first_name
        bp_payam -> payment_amount
        er_ein -> ein
        cl_fil1 -> _filler
        bp_recid -> _record_id (special: table-dependent primary key)
    """
    suffix = _strip_cobol_prefix(col_name)
    if suffix in ("recid", "recno", "rec_id"):
        return "_record_id"  # Sentinel — caller should match to table-specific *_id

    # Project abbreviations take priority over built-in
    if project_abbreviations and suffix in project_abbreviations:
        return project_abbreviations[suffix]

    return COBOL_ABBREVIATIONS.get(suffix, suffix)


def find_matching_column(
    source_col: str,
    target_columns: List[str],
    threshold: float = 0.7,
    table_name: str = "",
    project_abbreviations: Optional[Dict[str, str]] = None,
) -> Optional[Tuple[str, float]]:
    """Match a source column to the closest target column.

    Uses a multi-strategy approach:
    1. Exact match (after lowercasing)
    2. COBOL abbreviation expansion + exact match
    3. COBOL abbreviation expansion + substring/containment match
    4. Fuzzy match (SequenceMatcher) on both original and expanded names

    This handles COBOL copybook naming (cl_fnam, bp_payam) which has very low
    string similarity to modern names (first_name, payment_amount) and would
    fail a pure fuzzy match at the 0.7 threshold.

    Args:
        source_col: Legacy column name (e.g., "cl_fnam")
        target_columns: Modern column names to match against
        threshold: Minimum fuzzy match ratio (default 0.7)
        table_name: Table name for context-aware matching (e.g., "claimants")
    """
    source_lower = source_col.lower()
    target_lower_map = {t.lower(): t for t in target_columns}

    # Strategy 0: If expanded form is a known archived/filler pattern, skip matching
    expanded_check = expand_cobol_abbreviation(source_col, project_abbreviations)
    if expanded_check == "_filler":
        return None  # Let caller handle as removed
    if expanded_check in ("bank_account", "bank_routing"):
        return None  # Let caller handle as archived

    # Also catch COBOL abbreviated PII patterns that aren't in PII_KEYWORDS
    suffix = _strip_cobol_prefix(source_col)
    if suffix in ("bact", "brtn", "bacct", "broute", "bkact", "bkrtn"):
        return None  # Let caller handle as archived

    # Strategy 1: Exact match
    if source_lower in target_lower_map:
        return (target_lower_map[source_lower], 1.0)

    # Strategy 2: Expand COBOL abbreviation and try exact match
    expanded = expand_cobol_abbreviation(source_col, project_abbreviations)

    # Special handling for record ID fields — find the table's primary key (*_id)
    if expanded == "_record_id":
        id_candidates = [t for t in target_lower_map if t.endswith("_id")]
        if id_candidates and table_name:
            # Derive expected PK name from table: claimants -> claimant_id,
            # benefit_payments -> payment_id, employers -> employer_id
            table_lower = table_name.lower().rstrip("s")  # naive singularize
            # Also try removing common prefixes like "benefit_"
            table_parts = table_lower.split("_")
            # Build candidates: "claimant_id", "payment_id", "employer_id", "claim_id"
            expected_names = [f"{table_lower}_id"]
            if len(table_parts) > 1:
                expected_names.append(f"{table_parts[-1]}_id")  # last word + _id
            for expected in expected_names:
                if expected in target_lower_map:
                    return (target_lower_map[expected], 0.95)
            # Fallback: pick the _id column that best matches the table name
            best_id = None
            best_score = 0
            for cand in id_candidates:
                score = SequenceMatcher(None, table_lower, cand.replace("_id", "")).ratio()
                if score > best_score:
                    best_score = score
                    best_id = cand
            if best_id:
                return (target_lower_map[best_id], 0.90)
        elif id_candidates:
            id_candidates.sort(key=len)
            return (target_lower_map[id_candidates[0]], 0.85)
        return None

    if expanded != source_lower and expanded in target_lower_map:
        return (target_lower_map[expanded], 0.95)

    # Strategy 3: Expanded name is contained in target or vice versa
    # e.g., expanded "status" matches "claimant_status", "payment_status"
    if expanded != source_lower and expanded != "_filler":
        containment_matches = []
        for target_low, target_orig in target_lower_map.items():
            if expanded in target_low or target_low in expanded:
                # Score by how much of the target the expansion covers
                overlap = len(expanded) / max(len(target_low), 1)
                containment_matches.append((target_orig, min(overlap, 0.92)))
            # Also check if expanded words appear as components
            # e.g., "first_name" matches target "first_name" via word overlap
            elif _word_overlap_score(expanded, target_low) >= 0.8:
                containment_matches.append((target_orig, 0.90))

        if containment_matches:
            # Pick the best containment match
            containment_matches.sort(key=lambda x: x[1], reverse=True)
            return containment_matches[0]

    # Strategy 4: Fuzzy match on both original and expanded names
    best_match = None
    best_ratio = 0.0

    candidates = [source_lower]
    if expanded != source_lower:
        candidates.append(expanded)

    for candidate in candidates:
        for target_col in target_columns:
            ratio = SequenceMatcher(None, candidate, target_col.lower()).ratio()
            if ratio > best_ratio and ratio >= threshold:
                best_ratio = ratio
                best_match = target_col

    return (best_match, best_ratio) if best_match else None


def _word_overlap_score(a: str, b: str) -> float:
    """Score based on overlapping word components between two underscore-separated names."""
    words_a = set(a.split("_"))
    words_b = set(b.split("_"))
    if not words_a or not words_b:
        return 0.0
    overlap = words_a & words_b
    return len(overlap) / max(len(words_a), len(words_b))


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
                match = find_matching_column(col_name, list(modern_cols.keys()), table_name=table)
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
    modern_conn: Any = None,
) -> Tuple[Dict, Dict]:
    """Generate glossary.json and mappings.json from OpenMetadata catalog.

    Uses OM as the source of truth for legacy schema. If a modern_conn is
    provided, attempts COBOL-aware column matching against the modern schema.
    Otherwise, uses COBOL abbreviation expansion to infer mapping types.

    Args:
        om_enricher: An OpenMetadataEnricher instance (connected).
        tables: Table names to process.
        output_dir: Where to write the JSON files.
        plugin_manager: pluggy PluginManager for hooks.
        modern_conn: Optional modern database connector for column matching.

    Returns:
        Tuple of (glossary_data, mappings_data).
    """
    glossary_entries = []
    mappings_list = []

    pii_keywords = PII_KEYWORDS

    # Auto-generate abbreviations.yaml from COBOL descriptions, then load them
    project_abbrevs = {}
    all_om_columns = []
    for table in tables:
        try:
            schema = om_enricher.get_table_schema(table)
            all_om_columns.extend(schema)
        except Exception:
            pass
    if all_om_columns:
        generated = generate_abbreviations_yaml(
            all_om_columns, output_dir,
            table_name=", ".join(tables),
        )
        if generated:
            logger.info(f"Auto-generated {len(generated)} abbreviations from COBOL descriptions")

    # Load project abbreviations (built-in + auto-generated + any manual additions)
    project_dir = str(Path(output_dir).parent)
    project_abbrevs = load_project_abbreviations(project_dir)

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
                # Try COBOL-aware matching against modern schema if available
                modern_cols_list = []
                if modern_conn:
                    try:
                        modern_schema = modern_conn.get_table_schema(table)
                        modern_cols_list = [c["column_name"] for c in modern_schema]
                    except Exception:
                        pass

                if modern_cols_list:
                    match = find_matching_column(col_name, modern_cols_list, table_name=table, project_abbreviations=project_abbrevs)
                    if match:
                        target_col, ratio = match
                        expanded = expand_cobol_abbreviation(col_name, project_abbrevs)
                        # Determine mapping type
                        if expanded == "_filler":
                            map_type = "removed"
                            rationale = f"COBOL FILLER field — no business value, not migrated"
                        elif is_pii and target_col != col_name:
                            map_type = "transform"
                            rationale = f"PII field renamed from {col_name} to {target_col} with potential data transformation"
                        elif ratio >= 0.99:
                            map_type = "rename"
                            rationale = f"Column name unchanged"
                        else:
                            map_type = "rename"
                            rationale = f"COBOL abbreviation expanded: {col_name} -> {target_col}"
                        mappings_list.append({
                            "source": col_name,
                            "target": target_col,
                            "type": map_type,
                            "rationale": rationale,
                            "confidence": round(ratio, 2),
                            "table": table,
                        })
                    else:
                        # No match found — likely archived or removed
                        expanded = expand_cobol_abbreviation(col_name, project_abbrevs)
                        if expanded == "_filler":
                            map_type, rationale = "removed", "COBOL FILLER field — no business value"
                        elif is_pii:
                            map_type, rationale = "archived", f"PII field {col_name} not present in modern schema — likely archived for compliance"
                        else:
                            map_type, rationale = "removed", f"No matching column found in modern schema for {col_name}"
                        mappings_list.append({
                            "source": col_name,
                            "target": None,
                            "type": map_type,
                            "rationale": rationale,
                            "confidence": 0.8,
                            "table": table,
                        })
                else:
                    # No modern schema available — use COBOL expansion to infer
                    expanded = expand_cobol_abbreviation(col_name, project_abbrevs)
                    if expanded == "_filler":
                        mappings_list.append({
                            "source": col_name, "target": None,
                            "type": "removed",
                            "rationale": "COBOL FILLER field — no business value",
                            "confidence": 0.95, "table": table,
                        })
                    elif is_pii and expanded in ("bank_account", "bank_routing"):
                        mappings_list.append({
                            "source": col_name, "target": None,
                            "type": "archived",
                            "rationale": f"PII financial field ({expanded}) — archived for compliance",
                            "confidence": 0.90, "table": table,
                        })
                    elif expanded != col_lower:
                        mappings_list.append({
                            "source": col_name, "target": expanded,
                            "type": "rename",
                            "rationale": f"COBOL abbreviation expanded: {col_name} -> {expanded}",
                            "confidence": 0.85, "table": table,
                        })
                    else:
                        mappings_list.append({
                            "source": col_name, "target": None,
                            "type": "pending",
                            "rationale": "Could not resolve mapping — manual review needed",
                            "confidence": conf, "table": table,
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
