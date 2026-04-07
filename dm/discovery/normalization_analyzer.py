"""
Normalization Analysis Engine

Analyzes legacy tables (via OpenMetadata catalog) and proposes normalized
decomposition into properly structured entities with relationships.

Produces a NormalizationPlan that the schema generator consumes to produce DDL.
"""

import json
import logging
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ── Data Classes ──────────────────────────────────────────────────────

@dataclass
class ColumnGroup:
    """A group of columns sharing a common prefix, likely belonging to one entity."""
    prefix: str
    columns: list
    suggested_entity: str
    suggested_pk: Optional[str] = None
    confidence: float = 0.5

    def __post_init__(self):
        if isinstance(self.columns, tuple):
            self.columns = list(self.columns)


@dataclass
class LookupTable:
    """A column whose low cardinality suggests it should be a reference/lookup table."""
    name: str
    source_column: str
    source_table: str
    distinct_values: list = field(default_factory=list)
    value_count: int = 0


@dataclass
class ProposedEntity:
    """A table to be created in the modern schema."""
    name: str
    columns: list  # list of dicts: {source_col, target_col, data_type, nullable, transform}
    primary_key: str
    source_table: str
    role: str  # "primary" | "child" | "lookup"
    rationale: str


@dataclass
class ProposedRelationship:
    """A foreign key relationship between two proposed entities."""
    parent_entity: str
    child_entity: str
    fk_column: str
    pk_column: str
    cardinality: str = "1:N"


@dataclass
class NormalizationPlan:
    """Complete normalization plan for one legacy table."""
    source_table: str
    entities: list  # list of ProposedEntity
    relationships: list  # list of ProposedRelationship
    lookup_tables: list  # list of LookupTable
    confidence: float = 0.0
    rationale: str = ""


# ── Address / Contact Patterns ────────────────────────────────────────

ADDRESS_PATTERNS = {
    "adr", "addr", "address", "street", "line1", "line2",
    "city", "town", "state", "st", "province",
    "zip", "zipcode", "postal", "country",
}

CONTACT_PATTERNS = {
    "phon", "phone", "tel", "fax",
    "emal", "email", "mail",
}

# Patterns that suggest a column is a PK/record ID
PK_SUFFIXES = ("_recid", "_id", "recid", "_pk", "_key")


# ── Main Analyzer ─────────────────────────────────────────────────────

class NormalizationAnalyzer:
    """Analyzes legacy tables and proposes normalized decomposition."""

    def __init__(
        self,
        om_enricher: Any,
        plugin_manager: Any = None,
        config: Optional[Dict] = None,
    ):
        """
        Args:
            om_enricher: Connected OpenMetadataEnricher instance.
            plugin_manager: pluggy PluginManager for dm_normalization_overrides.
            config: schema_generation config section.
        """
        self._om = om_enricher
        self._pm = plugin_manager
        self._config = config or {}
        norm_config = self._config.get("normalization", {})
        self._min_group_size = norm_config.get("min_group_size", 3)
        self._lookup_threshold = norm_config.get("lookup_threshold", 20)
        self._prefix_detection = norm_config.get("prefix_detection", True)

    # ── Public API ────────────────────────────────────────────────

    def analyze(self, tables: List[str]) -> Dict[str, NormalizationPlan]:
        """Analyze all legacy tables and return normalization plans.

        Returns:
            Dict of {table_name: NormalizationPlan}.
        """
        plans = {}
        for table in tables:
            logger.info(f"Analyzing table for normalization: {table}")
            plans[table] = self.analyze_table(table)
        return plans

    def analyze_table(self, table: str) -> NormalizationPlan:
        """Full analysis pipeline for one legacy table.

        Steps:
        1. Fetch schema from OM catalog
        2. Fetch OM profiling stats
        3. Fetch OM glossary terms
        4. Detect column groups by prefix
        5. Apply plugin overrides
        6. Detect lookup tables from OM profiling
        7. Detect address/contact sub-entities
        8. Validate with profiling stats
        9. Build normalization plan
        """
        # 1. Schema from OM
        schema = self._om.get_table_schema(table)
        if not schema:
            logger.warning(f"No schema found in OM for table: {table}")
            return NormalizationPlan(
                source_table=table, entities=[], relationships=[],
                lookup_tables=[], confidence=0.0,
                rationale="No schema found in OpenMetadata catalog",
            )

        # 2. Profiling from OM
        try:
            om_profile = self._om.get_table_profile(table)
        except Exception:
            logger.warning(f"No profiling data for {table}")
            om_profile = {"columns": {}}

        om_stats = om_profile.get("columns", {})

        # 3. Glossary from OM
        try:
            om_glossary = self._om.get_glossary_for_table(table)
        except Exception:
            om_glossary = {}

        # 4. Detect column groups
        groups = self.detect_column_groups(table, schema, om_glossary)

        # 5. Apply plugin overrides
        groups = self._apply_plugin_overrides(table, groups, schema)

        # 6. Detect lookup tables
        lookups = self.detect_lookup_tables(table, groups, om_stats)

        # 7. Detect address/contact sub-entities
        groups = self._detect_subentities(groups)

        # 8. Validate with profiling
        groups = self.validate_with_profiling(groups, om_stats)

        # 9. Build plan
        return self._build_normalization_plan(table, schema, groups, lookups, om_stats)

    # ── Column Group Detection ────────────────────────────────────

    def detect_column_groups(
        self,
        table: str,
        schema: List[Dict],
        om_glossary: Dict,
    ) -> List[ColumnGroup]:
        """Group columns by prefix pattern.

        Columns with a shared prefix (e.g., cl_, er_, cm_) and count >=
        min_group_size are grouped as a candidate entity.
        """
        if not self._prefix_detection:
            # No prefix detection — treat entire table as one entity
            all_cols = [col["column_name"] for col in schema]
            return [ColumnGroup(
                prefix="",
                columns=all_cols,
                suggested_entity=table,
                suggested_pk=self._find_pk_column(all_cols),
                confidence=0.8,
            )]

        # Extract 2-3 char prefixes (e.g., "cl_" from "cl_fnam")
        prefix_cols: Dict[str, List[str]] = defaultdict(list)
        unprefixed: List[str] = []

        for col in schema:
            col_name = col["column_name"]
            match = re.match(r'^([a-zA-Z]{2,4})_', col_name)
            if match:
                prefix = match.group(1).lower()
                prefix_cols[prefix].append(col_name)
            else:
                unprefixed.append(col_name)

        groups = []

        for prefix, cols in prefix_cols.items():
            if len(cols) >= self._min_group_size:
                # Derive entity name from prefix + OM glossary
                entity_name = self._derive_entity_name(prefix, cols, om_glossary)
                pk = self._find_pk_column(cols)
                groups.append(ColumnGroup(
                    prefix=f"{prefix}_",
                    columns=cols,
                    suggested_entity=entity_name,
                    suggested_pk=pk,
                    confidence=0.7,
                ))
            else:
                # Too few columns — merge into unprefixed
                unprefixed.extend(cols)

        # If there's only one prefix group, it's the primary entity — include unprefixed cols
        if len(groups) == 1:
            groups[0].columns.extend(unprefixed)
            groups[0].confidence = 0.9
        elif len(groups) == 0:
            # No prefix patterns found — entire table is one entity
            all_cols = [col["column_name"] for col in schema]
            groups.append(ColumnGroup(
                prefix="",
                columns=all_cols,
                suggested_entity=table,
                suggested_pk=self._find_pk_column(all_cols),
                confidence=0.8,
            ))
        elif unprefixed:
            # Multiple groups exist — assign unprefixed to the largest group (primary)
            largest = max(groups, key=lambda g: len(g.columns))
            largest.columns.extend(unprefixed)

        return groups

    def _derive_entity_name(
        self,
        prefix: str,
        columns: List[str],
        om_glossary: Dict,
    ) -> str:
        """Derive a human-readable entity name from prefix and OM glossary."""
        # Check OM glossary for hints
        for col in columns:
            term = om_glossary.get(col, {})
            term_name = term.get("term_name", "")
            if term_name:
                # Extract entity name from glossary term like "Claimant First Name"
                words = term_name.split()
                if len(words) >= 2:
                    # First word is likely the entity: "Claimant" -> "claimants"
                    entity_word = words[0].lower()
                    if not entity_word.endswith("s"):
                        entity_word += "s"
                    return entity_word

        # Fallback: common prefix mappings
        PREFIX_MAP = {
            "cl": "claimants", "cm": "claims", "bp": "benefit_payments",
            "er": "employers", "em": "employees", "cust": "customers",
            "ord": "orders", "inv": "invoices", "prod": "products",
            "usr": "users", "acct": "accounts", "txn": "transactions",
            "pay": "payments", "addr": "addresses",
        }
        if prefix in PREFIX_MAP:
            return PREFIX_MAP[prefix]

        return f"{prefix}_records"

    def _find_pk_column(self, columns: List[str]) -> Optional[str]:
        """Find the most likely primary key column from a list."""
        for col in columns:
            col_lower = col.lower()
            if any(col_lower.endswith(suffix) for suffix in PK_SUFFIXES):
                return col
        return None

    # ── Plugin Overrides ──────────────────────────────────────────

    def _apply_plugin_overrides(
        self,
        table: str,
        groups: List[ColumnGroup],
        schema: List[Dict],
    ) -> List[ColumnGroup]:
        """Apply dm_normalization_overrides hook to override auto-detection."""
        if not self._pm:
            return groups

        results = self._pm.hook.dm_normalization_overrides(table=table)
        for result in results:
            if not result:
                continue

            override_entities = result.get("entities", [])
            if not override_entities:
                continue

            # Replace auto-detected groups with plugin-defined ones
            logger.info(f"Applying normalization overrides for {table}: "
                        f"{len(override_entities)} entities defined")
            groups = []
            all_cols = {col["column_name"] for col in schema}

            for ent in override_entities:
                ent_cols = [c for c in ent.get("columns", []) if c in all_cols]
                groups.append(ColumnGroup(
                    prefix="",
                    columns=ent_cols,
                    suggested_entity=ent["name"],
                    suggested_pk=ent.get("pk"),
                    confidence=1.0,
                ))
            break  # First plugin result wins

        return groups

    # ── Lookup Table Detection ────────────────────────────────────

    def detect_lookup_tables(
        self,
        table: str,
        groups: List[ColumnGroup],
        om_stats: Dict,
    ) -> List[LookupTable]:
        """Identify columns that should become lookup/reference tables.

        Uses OM profiling distinct_count: columns with ≤ lookup_threshold
        distinct values are candidates for extraction into lookup tables.
        """
        lookups = []

        for group in groups:
            for col_name in group.columns:
                stats = om_stats.get(col_name, {})
                distinct = stats.get("distinct_count", 0)
                if not distinct or distinct > self._lookup_threshold:
                    continue
                # Skip likely PK columns
                if col_name == group.suggested_pk:
                    continue
                # Skip columns that are already IDs / foreign keys
                if col_name.lower().endswith(("_id", "_key", "_recid")):
                    continue

                # Check if it looks like a status/type/code column
                col_lower = col_name.lower()
                is_candidate = any(
                    pattern in col_lower
                    for pattern in ("stat", "status", "type", "code", "categ", "class", "flag")
                )
                if not is_candidate and distinct > 5:
                    continue

                # Build lookup
                frequencies = stats.get("value_frequencies", None)
                values = []
                if frequencies and isinstance(frequencies, list):
                    values = [str(v.get("value", v)) for v in frequencies[:50]]
                elif frequencies and isinstance(frequencies, dict):
                    values = [str(k) for k in list(frequencies.keys())[:50]]

                lookup_name = self._derive_lookup_name(col_name, group.suggested_entity)
                lookups.append(LookupTable(
                    name=lookup_name,
                    source_column=col_name,
                    source_table=table,
                    distinct_values=values,
                    value_count=distinct,
                ))

        return lookups

    def _derive_lookup_name(self, col_name: str, entity_name: str) -> str:
        """Derive a lookup table name from the column and parent entity."""
        # Strip common prefixes
        clean = re.sub(r'^[a-zA-Z]{2,4}_', '', col_name)
        if clean.endswith(("stat", "status")):
            return f"{entity_name.rstrip('s')}_status"
        if clean.endswith(("type", "typ")):
            return f"{entity_name.rstrip('s')}_type"
        if clean.endswith(("code", "cd")):
            return f"{entity_name.rstrip('s')}_code"
        return f"{clean}_lookup"

    # ── Sub-Entity Detection (Address, Contact) ───────────────────

    def _detect_subentities(self, groups: List[ColumnGroup]) -> List[ColumnGroup]:
        """Split address and contact columns into child entities."""
        new_groups = []

        for group in groups:
            address_cols = []
            contact_cols = []
            remaining_cols = []

            for col in group.columns:
                col_lower = col.lower()
                # Strip prefix for pattern matching
                col_stripped = re.sub(r'^[a-zA-Z]{2,4}_', '', col_lower)

                if any(p in col_stripped for p in ADDRESS_PATTERNS):
                    address_cols.append(col)
                elif any(p in col_stripped for p in CONTACT_PATTERNS):
                    contact_cols.append(col)
                else:
                    remaining_cols.append(col)

            # Only split if enough columns to form a meaningful sub-entity
            if len(address_cols) >= self._min_group_size:
                entity_base = group.suggested_entity.rstrip("s")
                new_groups.append(ColumnGroup(
                    prefix=group.prefix,
                    columns=address_cols,
                    suggested_entity=f"{entity_base}_addresses",
                    suggested_pk=None,  # Will get a generated PK
                    confidence=0.8,
                ))
            else:
                remaining_cols.extend(address_cols)

            if len(contact_cols) >= self._min_group_size:
                entity_base = group.suggested_entity.rstrip("s")
                new_groups.append(ColumnGroup(
                    prefix=group.prefix,
                    columns=contact_cols,
                    suggested_entity=f"{entity_base}_contacts",
                    suggested_pk=None,
                    confidence=0.7,
                ))
            else:
                remaining_cols.extend(contact_cols)

            # Update the main group with remaining columns
            group.columns = remaining_cols
            new_groups.insert(0, group)  # Primary entity first

        return new_groups

    # ── Profiling Validation ──────────────────────────────────────

    def validate_with_profiling(
        self,
        groups: List[ColumnGroup],
        om_stats: Dict,
    ) -> List[ColumnGroup]:
        """Refine entity groups using OM profiling data."""
        for group in groups:
            if not group.suggested_pk and group.columns:
                # Try to find PK from profiling: 100% unique column
                for col in group.columns:
                    stats = om_stats.get(col, {})
                    unique_pct = stats.get("unique_percent", 0)
                    if unique_pct >= 99.9 and col.lower().endswith(PK_SUFFIXES):
                        group.suggested_pk = col
                        group.confidence = min(group.confidence + 0.1, 1.0)
                        break

            # Boost confidence if OM has good profiling data for this group
            profiled_count = sum(
                1 for col in group.columns if col in om_stats
            )
            if profiled_count > 0:
                coverage = profiled_count / len(group.columns) if group.columns else 0
                group.confidence = min(group.confidence + (coverage * 0.1), 1.0)

        return groups

    # ── Plan Builder ──────────────────────────────────────────────

    def _build_normalization_plan(
        self,
        table: str,
        schema: List[Dict],
        groups: List[ColumnGroup],
        lookups: List[LookupTable],
        om_stats: Dict,
    ) -> NormalizationPlan:
        """Compose the final normalization plan from analysis results."""
        schema_map = {col["column_name"]: col for col in schema}
        entities = []
        relationships = []

        # Identify the primary entity (first/largest group)
        primary_group = groups[0] if groups else None

        for i, group in enumerate(groups):
            is_primary = (i == 0)
            role = "primary" if is_primary else "child"

            # Build column list for this entity
            entity_cols = []
            for col_name in group.columns:
                col_info = schema_map.get(col_name, {})
                entity_cols.append({
                    "source_col": col_name,
                    "target_col": None,  # Resolved by schema generator
                    "data_type": col_info.get("data_type", "VARCHAR"),
                    "data_type_display": col_info.get("data_type_display", ""),
                    "nullable": col_info.get("is_nullable", "YES") == "YES",
                    "transform": None,
                })

            # Determine PK
            pk = group.suggested_pk
            if not pk and is_primary:
                pk = f"{group.suggested_entity.rstrip('s')}_id"

            entities.append(ProposedEntity(
                name=group.suggested_entity,
                columns=entity_cols,
                primary_key=pk or f"{group.suggested_entity.rstrip('s')}_id",
                source_table=table,
                role=role,
                rationale=f"{'Primary entity' if is_primary else 'Child entity'} "
                          f"from {group.prefix or 'all'} columns "
                          f"(confidence: {group.confidence:.2f})",
            ))

            # Child entities get FK to primary
            if not is_primary and primary_group:
                parent_pk = primary_group.suggested_pk or \
                    f"{primary_group.suggested_entity.rstrip('s')}_id"
                fk_col = f"{primary_group.suggested_entity.rstrip('s')}_id"
                relationships.append(ProposedRelationship(
                    parent_entity=primary_group.suggested_entity,
                    child_entity=group.suggested_entity,
                    fk_column=fk_col,
                    pk_column=parent_pk,
                    cardinality="1:N",
                ))

        # Add lookup table entities
        for lookup in lookups:
            lookup_cols = [
                {
                    "source_col": lookup.source_column,
                    "target_col": "code",
                    "data_type": "VARCHAR",
                    "nullable": False,
                    "transform": None,
                },
                {
                    "source_col": None,
                    "target_col": "description",
                    "data_type": "VARCHAR",
                    "nullable": True,
                    "transform": None,
                },
            ]
            entities.append(ProposedEntity(
                name=lookup.name,
                columns=lookup_cols,
                primary_key="code",
                source_table=table,
                role="lookup",
                rationale=f"Lookup table for {lookup.source_column} "
                          f"({lookup.value_count} distinct values)",
            ))

        # Calculate overall confidence
        if entities:
            avg_conf = sum(g.confidence for g in groups) / len(groups)
        else:
            avg_conf = 0.0

        return NormalizationPlan(
            source_table=table,
            entities=entities,
            relationships=relationships,
            lookup_tables=lookups,
            confidence=round(avg_conf, 2),
            rationale=f"Decomposed {table} into {len(entities)} entities "
                      f"with {len(relationships)} relationships "
                      f"and {len(lookups)} lookup tables",
        )

    # ── Persistence ───────────────────────────────────────────────

    def save_plan(self, plans: Dict[str, NormalizationPlan], output_path: Path) -> None:
        """Write normalization_plan.json."""
        output_path.mkdir(parents=True, exist_ok=True)
        serializable = {}
        for table, plan in plans.items():
            serializable[table] = {
                "source_table": plan.source_table,
                "entities": [asdict(e) for e in plan.entities],
                "relationships": [asdict(r) for r in plan.relationships],
                "lookup_tables": [asdict(lt) for lt in plan.lookup_tables],
                "confidence": plan.confidence,
                "rationale": plan.rationale,
            }
        filepath = output_path / "normalization_plan.json"
        with open(filepath, "w") as f:
            json.dump(serializable, f, indent=2)
        logger.info(f"Saved normalization plan: {filepath}")
