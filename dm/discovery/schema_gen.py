"""
Modern Schema Generator

Consumes enriched metadata + normalization plan and generates normalized
PostgreSQL DDL with proper types, constraints, PII handling, and comments.

Output artifacts:
- Individual DDL files per table
- Combined full_schema.sql
- Transform script skeletons
- Schema diff report
- Updated datasets config and mappings
"""

import json
import logging
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dm.discovery.normalization_analyzer import (
    NormalizationPlan,
    ProposedEntity,
    ProposedRelationship,
)

logger = logging.getLogger(__name__)


# ── Data Classes ──────────────────────────────────────────────────────

@dataclass
class GeneratedColumn:
    name: str
    data_type: str
    nullable: bool
    constraints: list = field(default_factory=list)
    source_column: Optional[str] = None
    transform: Optional[str] = None
    pii_action: Optional[str] = None
    comment: Optional[str] = None


@dataclass
class GeneratedTable:
    name: str
    source_table: str
    role: str
    columns: list = field(default_factory=list)  # list of GeneratedColumn
    primary_key: str = ""
    foreign_keys: list = field(default_factory=list)
    indexes: list = field(default_factory=list)
    comment: str = ""


@dataclass
class SchemaGenResult:
    tables: list = field(default_factory=list)  # list of GeneratedTable
    ddl_files: dict = field(default_factory=dict)
    full_ddl: str = ""
    transform_files: dict = field(default_factory=dict)
    diff_report: dict = field(default_factory=dict)
    updated_datasets: list = field(default_factory=list)
    updated_mappings: dict = field(default_factory=dict)
    confidence: float = 0.0


# ── COBOL / Legacy Abbreviation Expansion ─────────────────────────────

ABBREVIATION_MAP = {
    "fnam": "first_name", "lnam": "last_name", "mnam": "middle_name",
    "dob": "date_of_birth", "ssn": "ssn", "emal": "email",
    "phon": "phone_number", "addr": "address", "adr1": "address_line1",
    "adr2": "address_line2", "city": "city", "st": "state",
    "zip": "zip_code", "stat": "status", "typ": "type",
    "cd": "code", "desc": "description", "amt": "amount",
    "dt": "date", "dtm": "datetime", "cnt": "count",
    "qty": "quantity", "num": "number", "flg": "flag",
    "ind": "indicator", "pct": "percent", "rt": "rate",
    "nm": "name", "id": "id", "recid": "id",
    "bact": "bank_account", "brtn": "bank_routing",
    "fildt": "filing_date", "rgdt": "registered_at",
    "dcsd": "is_deceased", "clmnt": "claimant_id",
    "clmid": "claim_id", "emplr": "employer_id",
    "payam": "payment_amount", "wkamt": "weekly_amount",
    "bystr": "benefit_year_start", "byend": "benefit_year_end",
    "ein": "ein",
}

# PII classification → action mapping
PII_ACTIONS = {
    "SSN": "hash", "Social Security": "hash", "Passport": "hash",
    "Driver": "hash",
    "Financial": "archive", "Credit Card": "archive",
    "Bank": "archive", "Account": "archive",
    "Email": "encrypt", "Phone": "encrypt", "Address": "encrypt",
    "DOB": "restrict", "Salary": "restrict", "Income": "restrict",
}

# Legacy SQL types → PostgreSQL type mapping
TYPE_MAP = {
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "numeric": "NUMERIC",
    "decimal": "NUMERIC",
    "real": "REAL",
    "double precision": "DOUBLE PRECISION",
    "float": "DOUBLE PRECISION",
    "character varying": "VARCHAR",
    "varchar": "VARCHAR",
    "character": "CHAR",
    "char": "CHAR",
    "text": "TEXT",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp without time zone": "TIMESTAMP",
    "timestamp with time zone": "TIMESTAMPTZ",
    "timestamp": "TIMESTAMPTZ",
    "time": "TIME",
    "bytea": "BYTEA",
    "uuid": "UUID",
    "json": "JSONB",
    "jsonb": "JSONB",
    # COBOL / mainframe types (from OM catalog)
    "string": "VARCHAR",
    "number": "NUMERIC",
    "int": "INTEGER",
}


class SchemaGenerator:
    """Generates normalized PostgreSQL schema from enriched legacy metadata."""

    def __init__(
        self,
        config: Dict,
        plugin_manager: Any = None,
        om_enricher: Any = None,
    ):
        self._config = config
        self._pm = plugin_manager
        self._om = om_enricher
        self._gen_config = config.get("schema_generation", {})
        self._defaults = self._gen_config.get("defaults", {})
        self._constraint_config = self._gen_config.get("constraints", {})
        self._pii_config = self._gen_config.get("pii_handling", {})

    # ── Public API ────────────────────────────────────────────────

    def generate(
        self,
        normalization_plan: NormalizationPlan,
        legacy_schema: List[Dict],
        glossary: Dict,
        mappings: Dict,
        om_profiles: Optional[Dict] = None,
    ) -> SchemaGenResult:
        """Generate modern schema for one legacy table's normalization plan."""
        om_profiles = om_profiles or {}
        schema_map = {col["column_name"]: col for col in legacy_schema}
        glossary_map = {}
        for entry in glossary.get("columns", []):
            if entry.get("table") == normalization_plan.source_table:
                glossary_map[entry["name"]] = entry
        mappings_map = {}
        for m in mappings.get("mappings", []):
            if m.get("table") == normalization_plan.source_table:
                mappings_map[m["source"]] = m

        # Get column tags from OM
        om_tags = {}
        if self._om:
            try:
                om_tags = self._om.get_column_tags(normalization_plan.source_table)
            except Exception:
                pass

        generated_tables = []
        all_mappings = []

        for entity in normalization_plan.entities:
            gen_table = self._generate_entity(
                entity, normalization_plan, schema_map,
                glossary_map, mappings_map, om_profiles, om_tags,
            )
            generated_tables.append(gen_table)

            # Collect updated mappings
            for col in gen_table.columns:
                if col.source_column:
                    all_mappings.append({
                        "source": col.source_column,
                        "target": col.name,
                        "target_table": gen_table.name,
                        "type": self._determine_mapping_type(col),
                        "rationale": col.comment or "",
                        "confidence": 1.0,
                        "table": normalization_plan.source_table,
                    })

        # Build results
        ddl_files = {}
        transform_files = {}
        for table in generated_tables:
            ddl_files[table.name] = self.render_ddl(table)
            transform_files[table.name] = self.render_transforms(
                normalization_plan.source_table, table,
            )

        full_ddl = self.render_full_ddl(generated_tables, normalization_plan)
        diff_report = self.render_diff_report(legacy_schema, generated_tables)
        updated_datasets = self.generate_updated_datasets(
            normalization_plan, generated_tables,
        )

        return SchemaGenResult(
            tables=generated_tables,
            ddl_files=ddl_files,
            full_ddl=full_ddl,
            transform_files=transform_files,
            diff_report=diff_report,
            updated_datasets=updated_datasets,
            updated_mappings={"mappings": all_mappings},
            confidence=normalization_plan.confidence,
        )

    # ── Entity Generation ─────────────────────────────────────────

    def _generate_entity(
        self,
        entity: ProposedEntity,
        plan: NormalizationPlan,
        schema_map: Dict,
        glossary_map: Dict,
        mappings_map: Dict,
        om_profiles: Dict,
        om_tags: Dict,
    ) -> GeneratedTable:
        """Generate a single table from a proposed entity."""
        columns = []

        # Add generated PK if the entity doesn't have a source PK
        pk_name = self._modernize_pk_name(entity)
        id_strategy = self._defaults.get("id_strategy", "serial")
        pk_type = "SERIAL" if id_strategy == "serial" else "UUID DEFAULT gen_random_uuid()" if id_strategy == "uuid" else "INTEGER GENERATED ALWAYS AS IDENTITY"
        has_source_pk = any(
            c.get("source_col") == entity.primary_key for c in entity.columns
        )

        if entity.role != "lookup":
            columns.append(GeneratedColumn(
                name=pk_name,
                data_type=pk_type,
                nullable=False,
                constraints=["PRIMARY KEY"],
                source_column=entity.primary_key if has_source_pk else None,
                comment=f"Primary key for {entity.name}",
            ))

        # Add FK column for child entities
        fk_cols = set()
        for rel in plan.relationships:
            if rel.child_entity == entity.name:
                fk_name = rel.fk_column
                fk_cols.add(fk_name)
                columns.append(GeneratedColumn(
                    name=fk_name,
                    data_type="INTEGER",
                    nullable=False,
                    constraints=[f"REFERENCES {rel.parent_entity}({rel.pk_column})"],
                    comment=f"FK to {rel.parent_entity}",
                ))

        # Process entity columns
        for col_def in entity.columns:
            source_col = col_def.get("source_col")
            if not source_col:
                # Lookup table "description" column or similar
                target = col_def.get("target_col", "value")
                columns.append(GeneratedColumn(
                    name=target,
                    data_type=col_def.get("data_type", "VARCHAR(100)"),
                    nullable=col_def.get("nullable", True),
                ))
                continue

            # Skip if already added as PK
            if has_source_pk and source_col == entity.primary_key:
                continue

            glossary_entry = glossary_map.get(source_col, {})
            mapping = mappings_map.get(source_col, {})
            stats = om_profiles.get(source_col, {})
            tags = om_tags.get(source_col, [])
            legacy_type = col_def.get("data_type", schema_map.get(source_col, {}).get("data_type", "VARCHAR"))

            # Rule 4: PII handling — check before other rules (may exclude column)
            pii_action = self.apply_pii_handling(source_col, tags, mapping)
            if pii_action == "archive":
                # Column excluded from modern schema
                continue

            # Rule 1: Column name
            modern_name = self.modernize_column_name(source_col, glossary_entry, mapping)

            # Rule 2: Data type
            modern_type = self.optimize_data_type(legacy_type, source_col, stats)

            # Handle hash transforms
            transform = None
            if pii_action == "hash":
                modern_type = "VARCHAR(64)"
                modern_name = modern_name if modern_name.endswith("_hash") else f"{modern_name}_hash"
                transform = "SHA-256"

            # Rule 3: Constraints
            constraints = self.infer_constraints(source_col, legacy_type, stats)

            # Rule 6: Comment
            comment = self._build_column_comment(source_col, glossary_entry, pii_action, transform)

            columns.append(GeneratedColumn(
                name=modern_name,
                data_type=modern_type,
                nullable=col_def.get("nullable", True) and "NOT NULL" not in constraints,
                constraints=constraints,
                source_column=source_col,
                transform=transform,
                pii_action=pii_action,
                comment=comment,
            ))

        # Add timestamp columns if configured
        if self._defaults.get("add_created_at", True) and entity.role != "lookup":
            columns.append(GeneratedColumn(
                name="created_at", data_type="TIMESTAMPTZ",
                nullable=False, constraints=["DEFAULT NOW()"],
            ))
        if self._defaults.get("add_updated_at", True) and entity.role != "lookup":
            columns.append(GeneratedColumn(
                name="updated_at", data_type="TIMESTAMPTZ",
                nullable=False, constraints=["DEFAULT NOW()"],
            ))

        # Build indexes
        indexes = []
        for rel in plan.relationships:
            if rel.child_entity == entity.name:
                indexes.append({
                    "columns": [rel.fk_column],
                    "name": f"idx_{entity.name}_{rel.fk_column}",
                })
        # Index hash columns
        for col in columns:
            if col.transform == "SHA-256":
                indexes.append({
                    "columns": [col.name],
                    "name": f"idx_{entity.name}_{col.name}",
                })

        # Table comment
        table_comment = self._build_table_comment(entity, plan)

        # Handle lookup table PK differently
        if entity.role == "lookup":
            pk_col = columns[0] if columns else None
            if pk_col:
                pk_col.constraints = ["PRIMARY KEY"]

        return GeneratedTable(
            name=entity.name,
            source_table=plan.source_table,
            role=entity.role,
            columns=columns,
            primary_key=pk_name if entity.role != "lookup" else (columns[0].name if columns else "code"),
            foreign_keys=[
                {"column": rel.fk_column, "references": f"{rel.parent_entity}({rel.pk_column})"}
                for rel in plan.relationships if rel.child_entity == entity.name
            ],
            indexes=indexes,
            comment=table_comment,
        )

    # ── Rule 1: Column Name Modernization ─────────────────────────

    def modernize_column_name(
        self,
        legacy_name: str,
        glossary_entry: Optional[Dict] = None,
        mapping: Optional[Dict] = None,
    ) -> str:
        """Modernize a legacy column name.

        Priority: plugin mapping target > OM glossary expansion > abbreviation map > snake_case.
        """
        # 1. Plugin mapping target (highest priority)
        if mapping and mapping.get("target"):
            return mapping["target"]

        # 2. OM glossary expansion
        if glossary_entry:
            desc = glossary_entry.get("description", "")
            glossary_term = glossary_entry.get("glossary_term", "")
            if glossary_term:
                # Convert "Claimant First Name" → "first_name"
                modern = self._glossary_to_column_name(glossary_term)
                if modern:
                    return modern

        # 3. Abbreviation map — strip prefix and look up
        stripped = re.sub(r'^[a-zA-Z]{2,4}_', '', legacy_name.lower())
        if stripped in ABBREVIATION_MAP:
            return ABBREVIATION_MAP[stripped]

        # 4. Fall back to cleaned snake_case
        return self._to_snake_case(legacy_name)

    def _glossary_to_column_name(self, term: str) -> Optional[str]:
        """Convert a glossary term like 'Claimant First Name' to 'first_name'."""
        words = term.strip().split()
        if len(words) < 2:
            return None
        # Drop entity prefix word (e.g., "Claimant", "Employer")
        name_words = words[1:] if len(words) > 2 else words
        result = "_".join(w.lower() for w in name_words)
        # Clean up
        result = re.sub(r'[^a-z0-9_]', '', result)
        return result if result else None

    def _to_snake_case(self, name: str) -> str:
        """Convert any name to clean snake_case."""
        # Strip common prefixes
        name = re.sub(r'^[a-zA-Z]{2,4}_', '', name)
        # CamelCase → snake_case
        name = re.sub(r'([A-Z])', r'_\1', name)
        # Clean up
        name = re.sub(r'[^a-zA-Z0-9]', '_', name)
        name = re.sub(r'_+', '_', name).strip('_').lower()
        return name or "column"

    # ── Rule 2: Data Type Optimization ────────────────────────────

    def optimize_data_type(
        self,
        legacy_type: str,
        column: str,
        om_stats: Optional[Dict] = None,
    ) -> str:
        """Optimize PostgreSQL data type using OM profiling stats."""
        if not self._gen_config.get("type_optimization", True):
            return self._map_base_type(legacy_type)

        stats = om_stats or {}
        base_type = legacy_type.lower().split("(")[0].strip()
        distinct_count = stats.get("distinct_count", 0)
        min_val = stats.get("min_value")
        max_val = stats.get("max_value")
        max_length = stats.get("max_length")

        # VARCHAR with exactly 2 values that look boolean → BOOLEAN
        frequencies = stats.get("value_frequencies", [])
        if base_type in ("character varying", "varchar", "text", "string", "char"):
            if distinct_count == 2 and frequencies:
                vals = set()
                if isinstance(frequencies, list):
                    vals = {str(v.get("value", v)).upper() for v in frequencies}
                elif isinstance(frequencies, dict):
                    vals = {str(k).upper() for k in frequencies}
                bool_pairs = [{"Y", "N"}, {"YES", "NO"}, {"T", "F"}, {"TRUE", "FALSE"}, {"1", "0"}]
                if vals in bool_pairs:
                    return "BOOLEAN"

            # Right-size VARCHAR
            if max_length and isinstance(max_length, (int, float)) and max_length > 0:
                # Add 50% headroom, round up to nearest 10
                sized = int(max_length * 1.5)
                sized = max(10, ((sized + 9) // 10) * 10)
                return f"VARCHAR({sized})"

            return self._map_base_type(legacy_type)

        # NUMERIC with no decimal places → INTEGER
        if base_type in ("numeric", "decimal", "number"):
            if min_val is not None and max_val is not None:
                try:
                    fmin, fmax = float(min_val), float(max_val)
                    # Check if all values are whole numbers
                    if fmin == int(fmin) and fmax == int(fmax):
                        if -2147483648 <= fmin and fmax <= 2147483647:
                            return "INTEGER"
                        return "BIGINT"
                except (ValueError, TypeError, OverflowError):
                    pass
            return self._map_base_type(legacy_type)

        # TIMESTAMP with all times at midnight → DATE
        if base_type in ("timestamp", "timestamp without time zone", "timestamp with time zone"):
            # Heuristic: if column name suggests date-only
            if any(p in column.lower() for p in ("_date", "_dt", "dob", "birth")):
                return "DATE"
            return "TIMESTAMPTZ"

        return self._map_base_type(legacy_type)

    def _map_base_type(self, legacy_type: str) -> str:
        """Map a legacy type to PostgreSQL, preserving size qualifiers."""
        base = legacy_type.lower().split("(")[0].strip()
        pg_type = TYPE_MAP.get(base, "VARCHAR")

        # Preserve size qualifier
        match = re.search(r'\(([^)]+)\)', legacy_type)
        if match and pg_type in ("VARCHAR", "CHAR", "NUMERIC"):
            return f"{pg_type}({match.group(1)})"
        return pg_type

    # ── Rule 3: Constraint Inference ──────────────────────────────

    def infer_constraints(
        self,
        column: str,
        legacy_type: str,
        om_stats: Optional[Dict] = None,
    ) -> List[str]:
        """Infer constraints from OM profiling data."""
        constraints = []
        stats = om_stats or {}

        if not self._constraint_config.get("infer_not_null", True):
            return constraints

        # NOT NULL from 0% nulls
        null_pct = stats.get("null_percent", None)
        if null_pct is not None and null_pct == 0:
            constraints.append("NOT NULL")

        # UNIQUE from 100% unique + ID-like column
        unique_pct = stats.get("unique_percent", 0)
        if (self._constraint_config.get("infer_unique", True)
                and unique_pct >= 99.9
                and column.lower().endswith(("_id", "_key", "_hash", "_code"))):
            constraints.append("UNIQUE")

        # CHECK from bounded numeric range
        if self._constraint_config.get("infer_check", True):
            min_val = stats.get("min_value")
            max_val = stats.get("max_value")
            distinct = stats.get("distinct_count", 0)

            # Small set of distinct values → CHECK IN
            frequencies = stats.get("value_frequencies", [])
            if 2 < distinct <= 10 and frequencies:
                vals = []
                if isinstance(frequencies, list):
                    vals = [str(v.get("value", v)) for v in frequencies]
                elif isinstance(frequencies, dict):
                    vals = [str(k) for k in frequencies]
                if vals:
                    escaped = ", ".join(f"'{v}'" for v in vals)
                    # Only add CHECK for string-like columns
                    base_type = legacy_type.lower().split("(")[0].strip()
                    if base_type in ("character varying", "varchar", "text", "string", "char"):
                        constraints.append(f"CHECK ({column} IN ({escaped}))")

        return constraints

    # ── Rule 4: PII Handling ──────────────────────────────────────

    def apply_pii_handling(
        self,
        column: str,
        om_tags: List[str],
        mapping: Optional[Dict] = None,
    ) -> Optional[str]:
        """Determine PII action from OM tags and plugin mappings."""
        # Plugin mapping takes precedence
        if mapping:
            mapping_type = mapping.get("type", "")
            if mapping_type == "archived":
                return "archive"
            if mapping_type == "transform" and "hash" in mapping.get("rationale", "").lower():
                return "hash"

        # OM tag-based classification
        default_action = self._pii_config.get("default_action", "hash")
        for tag in om_tags:
            tag_upper = tag.upper()
            for keyword, action in PII_ACTIONS.items():
                if keyword.upper() in tag_upper:
                    return action
            # Generic PII tag
            if "PII" in tag_upper or "SENSITIVE" in tag_upper:
                return default_action

        return None

    # ── Rule 6: Comments ──────────────────────────────────────────

    def _build_column_comment(
        self,
        source_col: str,
        glossary_entry: Dict,
        pii_action: Optional[str],
        transform: Optional[str],
    ) -> str:
        """Build a descriptive comment for a generated column."""
        parts = [f"Source: {source_col}"]
        if transform:
            parts.append(f"({transform})")
        if pii_action:
            parts.append(f"PII: {pii_action}")
        desc = glossary_entry.get("description", "")
        if desc and len(desc) < 100:
            parts.append(f"— {desc}")
        return " ".join(parts)

    def _build_table_comment(self, entity: ProposedEntity, plan: NormalizationPlan) -> str:
        """Build a table-level comment."""
        return (
            f"Migrated from legacy {plan.source_table} table. "
            f"Role: {entity.role}. {entity.rationale}"
        )

    def _modernize_pk_name(self, entity: ProposedEntity) -> str:
        """Generate a modern PK column name for an entity."""
        base = entity.name.rstrip("s")
        return f"{base}_id"

    def _determine_mapping_type(self, col: GeneratedColumn) -> str:
        """Determine the mapping type for a generated column."""
        if col.transform:
            return "transform"
        if col.pii_action == "archive":
            return "archived"
        if col.source_column and col.name != col.source_column:
            return "rename"
        return "rename"

    # ── DDL Rendering ─────────────────────────────────────────────

    def render_ddl(self, table: GeneratedTable) -> str:
        """Render CREATE TABLE DDL for a single table."""
        lines = []
        lines.append(f"-- Generated by DM Schema Generator")
        lines.append(f"-- Source: {table.source_table} ({table.role})")
        lines.append(f"-- Generated: {datetime.now(timezone.utc).isoformat()}")
        lines.append("")
        lines.append(f"CREATE TABLE {table.name} (")

        col_defs = []
        for col in table.columns:
            parts = [f"    {col.name:<25} {col.data_type}"]

            # Inline constraints (except CHECK which goes on its own line)
            inline = [c for c in col.constraints if not c.startswith("CHECK") and not c.startswith("REFERENCES")]
            for c in inline:
                parts.append(f" {c}")

            # Add comment as inline SQL comment
            if col.comment and col.source_column:
                parts.append(f"  -- {col.comment}")

            col_defs.append("".join(parts))

        # Add FK constraints
        for fk in table.foreign_keys:
            col_defs.append(
                f"    CONSTRAINT fk_{table.name}_{fk['column']} "
                f"FOREIGN KEY ({fk['column']}) REFERENCES {fk['references']}"
            )

        # Add CHECK constraints
        for col in table.columns:
            checks = [c for c in col.constraints if c.startswith("CHECK")]
            for check in checks:
                col_defs.append(f"    {check}")

        lines.append(",\n".join(col_defs))
        lines.append(");")

        # Indexes
        for idx in table.indexes:
            cols = ", ".join(idx["columns"])
            lines.append(f"\nCREATE INDEX {idx['name']} ON {table.name}({cols});")

        # Table comment
        if table.comment:
            escaped = table.comment.replace("'", "''")
            lines.append(f"\nCOMMENT ON TABLE {table.name} IS '{escaped}';")

        lines.append("")
        return "\n".join(lines)

    def render_full_ddl(
        self,
        tables: List[GeneratedTable],
        plan: NormalizationPlan,
    ) -> str:
        """Render combined DDL for all tables in dependency order."""
        lines = []
        lines.append("-- ============================================================")
        lines.append(f"-- DM Generated Schema")
        lines.append(f"-- Source: {plan.source_table}")
        lines.append(f"-- Tables: {len(tables)}")
        lines.append(f"-- Confidence: {plan.confidence}")
        lines.append(f"-- Generated: {datetime.now(timezone.utc).isoformat()}")
        lines.append("-- ============================================================")
        lines.append("")

        # Render lookup tables first, then primary, then children
        order = {"lookup": 0, "primary": 1, "child": 2}
        sorted_tables = sorted(tables, key=lambda t: order.get(t.role, 3))

        for table in sorted_tables:
            lines.append(self.render_ddl(table))

        return "\n".join(lines)

    # ── Transform Rendering ───────────────────────────────────────

    def render_transforms(self, source_table: str, target: GeneratedTable) -> str:
        """Render INSERT INTO ... SELECT ... skeleton for migration."""
        lines = []
        lines.append(f"-- Transform script: {source_table} → {target.name}")
        lines.append(f"-- Review and customize before execution")
        lines.append("")

        target_cols = []
        select_exprs = []

        for col in target.columns:
            if not col.source_column:
                if col.name in ("created_at", "updated_at"):
                    target_cols.append(col.name)
                    select_exprs.append("NOW()")
                continue

            target_cols.append(col.name)

            if col.transform == "SHA-256":
                select_exprs.append(
                    f"encode(sha256({col.source_column}::bytea), 'hex')"
                )
            elif col.data_type == "BOOLEAN":
                select_exprs.append(
                    f"CASE WHEN UPPER({col.source_column}) IN ('Y','YES','T','TRUE','1') "
                    f"THEN TRUE ELSE FALSE END"
                )
            elif col.data_type == "DATE" and col.source_column:
                select_exprs.append(f"{col.source_column}::DATE")
            else:
                select_exprs.append(col.source_column)

        if target_cols:
            cols_str = ",\n    ".join(target_cols)
            exprs_str = ",\n    ".join(select_exprs)
            lines.append(f"INSERT INTO {target.name} (")
            lines.append(f"    {cols_str}")
            lines.append(f")")
            lines.append(f"SELECT")
            lines.append(f"    {exprs_str}")
            lines.append(f"FROM {source_table};")
        else:
            lines.append(f"-- No columns to migrate for {target.name}")

        lines.append("")
        return "\n".join(lines)

    # ── Diff Report ───────────────────────────────────────────────

    def render_diff_report(
        self,
        legacy_schema: List[Dict],
        tables: List[GeneratedTable],
    ) -> Dict:
        """Generate a schema diff report."""
        legacy_cols = {col["column_name"] for col in legacy_schema}
        modern_cols = {}
        for table in tables:
            for col in table.columns:
                if col.source_column:
                    modern_cols[col.source_column] = {
                        "target_table": table.name,
                        "target_column": col.name,
                        "transform": col.transform,
                        "pii_action": col.pii_action,
                    }

        mapped_sources = set(modern_cols.keys())
        archived = set()
        for table in tables:
            for col in table.columns:
                if col.pii_action == "archive":
                    archived.add(col.source_column)

        unmapped = legacy_cols - mapped_sources - archived

        renamed = {
            src: info for src, info in modern_cols.items()
            if info["target_column"] != src and not info.get("transform")
        }
        transformed = {
            src: info for src, info in modern_cols.items()
            if info.get("transform")
        }

        return {
            "source_table": legacy_schema[0]["column_name"] if legacy_schema else "unknown",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "legacy_column_count": len(legacy_cols),
            "modern_table_count": len(tables),
            "columns": {
                "renamed": renamed,
                "transformed": transformed,
                "archived": list(archived),
                "unmapped": list(unmapped),
            },
        }

    # ── Dataset Config Generation ─────────────────────────────────

    def generate_updated_datasets(
        self,
        plan: NormalizationPlan,
        tables: List[GeneratedTable],
    ) -> List[Dict]:
        """Generate updated datasets config for project.yaml."""
        primary = None
        children = []

        for table in tables:
            entry = {
                "table": table.name,
                "role": table.role,
                "key": table.primary_key,
            }
            if table.role == "primary":
                primary = entry
            else:
                # Find FK for this child
                for fk in table.foreign_keys:
                    entry["fk"] = fk["column"]
                    break
                children.append(entry)

        modern_tables = []
        if primary:
            modern_tables.append(primary)
        modern_tables.extend(children)

        return [{
            "name": plan.source_table,
            "legacy_table": plan.source_table,
            "modern_tables": modern_tables,
            "primary_key": plan.entities[0].primary_key if plan.entities else None,
            "generated": True,
            "generation_confidence": plan.confidence,
        }]

    # ── Save Artifacts ────────────────────────────────────────────

    def save_artifacts(self, result: SchemaGenResult, output_path: Path) -> None:
        """Write all generated artifacts to disk."""
        output_path.mkdir(parents=True, exist_ok=True)

        # Full DDL
        (output_path / "full_schema.sql").write_text(result.full_ddl)

        # Individual DDL files
        for name, ddl in result.ddl_files.items():
            (output_path / f"{name}.sql").write_text(ddl)

        # Transform files
        for name, transform in result.transform_files.items():
            (output_path / f"{name}_transforms.sql").write_text(transform)

        # Diff report
        with open(output_path / "diff_report.json", "w") as f:
            json.dump(result.diff_report, f, indent=2)

        # Updated datasets
        import yaml
        with open(output_path / "updated_datasets.yaml", "w") as f:
            yaml.dump({"datasets": result.updated_datasets}, f, default_flow_style=False)

        # Updated mappings
        with open(output_path / "updated_mappings.json", "w") as f:
            json.dump(result.updated_mappings, f, indent=2)

        logger.info(f"Schema artifacts saved to {output_path}")
