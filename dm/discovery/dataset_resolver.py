"""
Dataset Resolver — Normalization-Aware Table Resolution

Resolves legacy-to-modern table relationships when normalization has
decomposed one legacy table into multiple modern tables. Provides
JOIN specifications for reconstructing the denormalized view during
post-migration validation.
"""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class JoinSpec:
    """Specification for reconstructing a denormalized view from normalized tables."""
    primary_table: str
    primary_key: str
    joins: list = field(default_factory=list)  # [{table, fk_column, pk_column, join_type}]
    select_columns: dict = field(default_factory=dict)  # {table_name: [col1, col2]}


class DatasetResolver:
    """Resolves 1:1 and 1:N table relationships for validation.

    Checks for `modern_tables` (list) vs `modern_table` (string) in dataset
    config to determine the resolution path. Backward compatible with
    existing 1:1 datasets.
    """

    def __init__(self, config: Dict):
        self._datasets = config.get("datasets", [])
        self._norm_plan = self._load_normalization_plan(config)
        self._config = config

    def _load_normalization_plan(self, config: Dict) -> Dict:
        """Load normalization_plan.json if it exists."""
        project_dir = config.get("_project_dir", ".")
        metadata_path = config.get("metadata", {}).get("path", "./metadata")
        plan_file = Path(project_dir) / metadata_path / "normalization_plan.json"
        if plan_file.exists():
            with open(plan_file) as f:
                return json.load(f)
        return {}

    def _get_dataset(self, dataset: str) -> Optional[Dict]:
        """Find a dataset definition by name."""
        for ds in self._datasets:
            if isinstance(ds, str):
                if ds == dataset:
                    return {"name": ds}
            elif isinstance(ds, dict) and ds.get("name") == dataset:
                return ds
        return None

    # ── Public API ────────────────────────────────────────────────

    def is_normalized(self, dataset: str) -> bool:
        """True if this dataset maps to multiple modern tables."""
        ds = self._get_dataset(dataset)
        if ds and isinstance(ds.get("modern_tables"), list):
            return len(ds["modern_tables"]) > 1
        # Check normalization plan
        return dataset in self._norm_plan

    def get_modern_tables(self, dataset: str) -> List[Dict]:
        """Return [{table, role, key, fk}] for all modern tables.

        For 1:1 datasets, returns a single-element list.
        """
        ds = self._get_dataset(dataset)
        if not ds:
            return [{"table": dataset, "role": "primary", "key": None}]

        # New format: modern_tables list
        if isinstance(ds.get("modern_tables"), list):
            return ds["modern_tables"]

        # Old format: modern_table string
        modern_table = ds.get("modern_table", dataset)
        modern_key = ds.get("modern_key", ds.get("primary_key"))
        return [{"table": modern_table, "role": "primary", "key": modern_key}]

    def get_primary_table(self, dataset: str) -> str:
        """Return the primary entity table name."""
        tables = self.get_modern_tables(dataset)
        for t in tables:
            if t.get("role") == "primary":
                return t["table"]
        # Fallback to first table
        return tables[0]["table"] if tables else dataset

    def get_primary_key(self, dataset: str) -> Optional[str]:
        """Return the primary key column of the primary table."""
        tables = self.get_modern_tables(dataset)
        for t in tables:
            if t.get("role") == "primary":
                return t.get("key")
        return None

    def get_join_spec(self, dataset: str) -> Optional[JoinSpec]:
        """Return JOIN specification to reconstruct the denormalized view.

        Returns None for 1:1 datasets (no join needed).
        """
        if not self.is_normalized(dataset):
            return None

        tables = self.get_modern_tables(dataset)
        primary = None
        joins = []

        for t in tables:
            if t.get("role") == "primary":
                primary = t
            elif t.get("role") == "child":
                joins.append({
                    "table": t["table"],
                    "fk_column": t.get("fk", ""),
                    "pk_column": primary["key"] if primary else "",
                    "join_type": "LEFT JOIN",
                })

        if not primary:
            return None

        return JoinSpec(
            primary_table=primary["table"],
            primary_key=primary.get("key", ""),
            joins=joins,
        )

    def build_reconstruction_query(self, dataset: str) -> Optional[str]:
        """Build a SELECT with LEFT JOINs to reconstruct the flat view.

        Returns None if dataset is not normalized.
        """
        spec = self.get_join_spec(dataset)
        if not spec:
            return None

        parts = [f"SELECT *\nFROM {spec.primary_table}"]
        for join in spec.joins:
            parts.append(
                f"  LEFT JOIN {join['table']} "
                f"ON {spec.primary_table}.{spec.primary_key} = "
                f"{join['table']}.{join['fk_column']}"
            )

        return "\n".join(parts)

    def get_child_tables(self, dataset: str) -> List[Dict]:
        """Return only child tables (excluding primary and lookups)."""
        return [
            t for t in self.get_modern_tables(dataset)
            if t.get("role") == "child"
        ]

    def get_lookup_tables(self, dataset: str) -> List[Dict]:
        """Return only lookup tables."""
        return [
            t for t in self.get_modern_tables(dataset)
            if t.get("role") == "lookup"
        ]
