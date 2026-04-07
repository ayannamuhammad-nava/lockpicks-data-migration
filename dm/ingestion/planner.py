"""
DM Migration Planner — L-Ingestor plan generation.

Reads normalization metadata and referential integrity config to produce
a dependency-ordered migration plan with per-table strategies.
"""

import json
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class TableMigrationStep:
    """A single table's migration instructions."""

    table: str
    strategy: str  # full_load | incremental | cdc | external
    depends_on: List[str] = field(default_factory=list)
    transform_path: Optional[str] = None


@dataclass
class MigrationPlan:
    """Complete ordered migration plan."""

    tables: List[str]  # Tables in dependency order (parents first)
    dependencies: Dict[str, List[str]] = field(default_factory=dict)
    strategies: Dict[str, TableMigrationStep] = field(default_factory=dict)


class MigrationPlanner:
    """Generate a dependency-ordered migration plan from project metadata.

    Reads:
        - metadata/normalization_plan.json — entity decomposition and relationships
        - project.yaml referential_integrity — FK dependency graph

    Args:
        config: Project configuration dict (from project.yaml).
        plugin_manager: Optional pluggy PluginManager for dm_ingest_strategy hook.
    """

    def __init__(self, config: Dict, plugin_manager: Any = None) -> None:
        self.config = config
        self.pm = plugin_manager

    def generate_plan(self, tables: List[str]) -> MigrationPlan:
        """Generate a migration plan for the given tables.

        Steps:
            1. Load normalization plan and referential integrity config.
            2. Build a dependency graph (adjacency list).
            3. Topological sort — tables with no FK deps come first.
            4. Assign a strategy per table (full_load default, plugin override).
            5. Resolve transform SQL file paths.

        Args:
            tables: List of table names to include in the plan.

        Returns:
            MigrationPlan with tables in execution order.
        """
        # Load dependency information
        dependencies = self._build_dependency_graph(tables)

        # Topological sort
        ordered_tables = self._topological_sort(tables, dependencies)

        # Assign strategies
        strategies: Dict[str, TableMigrationStep] = {}
        for table in ordered_tables:
            strategy = self._resolve_strategy(table)
            transform_path = self._resolve_transform_path(table)
            deps = dependencies.get(table, [])

            strategies[table] = TableMigrationStep(
                table=table,
                strategy=strategy,
                depends_on=deps,
                transform_path=transform_path,
            )
            logger.info(
                f"Plan: {table} — strategy={strategy}, "
                f"depends_on={deps}, transform={transform_path}"
            )

        return MigrationPlan(
            tables=ordered_tables,
            dependencies=dependencies,
            strategies=strategies,
        )

    def _build_dependency_graph(
        self, tables: List[str]
    ) -> Dict[str, List[str]]:
        """Build a dependency graph from normalization plan and project config.

        Returns:
            Dict mapping each table to a list of tables it depends on.
        """
        deps: Dict[str, List[str]] = {t: [] for t in tables}
        table_set = set(tables)

        # Source 1: normalization_plan.json relationships
        norm_plan = self._load_normalization_plan()
        if norm_plan:
            for table_key, plan_data in norm_plan.items():
                relationships = []
                if isinstance(plan_data, dict):
                    relationships = plan_data.get("relationships", [])
                elif isinstance(plan_data, list):
                    relationships = plan_data

                for rel in relationships:
                    parent = rel.get("parent", rel.get("parent_table", ""))
                    child = rel.get("child", rel.get("child_table", ""))
                    if child in table_set and parent in table_set:
                        if parent not in deps.get(child, []):
                            deps.setdefault(child, []).append(parent)

        # Source 2: project.yaml referential_integrity section
        ref_integrity = self.config.get("validation", {}).get(
            "referential_integrity", []
        )
        for check in ref_integrity:
            parent = check.get("parent", check.get("parent_table", ""))
            child = check.get("child", check.get("child_table", ""))
            if child in table_set and parent in table_set:
                if parent not in deps.get(child, []):
                    deps.setdefault(child, []).append(parent)

        return deps

    def _topological_sort(
        self, tables: List[str], dependencies: Dict[str, List[str]]
    ) -> List[str]:
        """Kahn's algorithm: topological sort with cycle detection.

        Tables with no dependencies come first so parent tables are
        populated before children that reference them.

        Args:
            tables: All tables to sort.
            dependencies: Graph of table → [tables it depends on].

        Returns:
            Tables in dependency-safe execution order.

        Raises:
            ValueError: If a circular dependency is detected.
        """
        table_set = set(tables)

        # Build in-degree map and adjacency list (reverse: parent → children)
        in_degree: Dict[str, int] = {t: 0 for t in tables}
        children_of: Dict[str, List[str]] = defaultdict(list)

        for table, deps in dependencies.items():
            if table not in table_set:
                continue
            for dep in deps:
                if dep in table_set:
                    in_degree[table] = in_degree.get(table, 0) + 1
                    children_of[dep].append(table)

        # Seed queue with zero-dependency tables
        queue: deque = deque()
        for t in tables:
            if in_degree.get(t, 0) == 0:
                queue.append(t)

        ordered: List[str] = []
        while queue:
            table = queue.popleft()
            ordered.append(table)
            for child in children_of.get(table, []):
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(ordered) != len(table_set):
            missing = table_set - set(ordered)
            raise ValueError(
                f"Circular dependency detected among tables: {missing}. "
                f"Resolved {len(ordered)} of {len(table_set)} tables."
            )

        return ordered

    def _resolve_strategy(self, table: str) -> str:
        """Determine the migration strategy for a table.

        Priority:
            1. Plugin override via dm_ingest_strategy hook
            2. Per-table config in project.yaml datasets section
            3. Default: full_load
        """
        # Plugin override
        if self.pm:
            try:
                results = self.pm.hook.dm_ingest_strategy(dataset=table)
                for result in results:
                    if result:
                        logger.info(f"Plugin override strategy for {table}: {result}")
                        return result
            except Exception as e:
                logger.warning(f"Plugin strategy hook failed for {table}: {e}")

        # Per-table config
        datasets = self.config.get("datasets", [])
        for ds in datasets:
            if isinstance(ds, dict):
                name = ds.get("name", "")
                if name == table:
                    return ds.get("strategy", "full_load")

        return "full_load"

    def _resolve_transform_path(self, table: str) -> Optional[str]:
        """Find the transform SQL file for a table.

        Looks in artifacts/generated_schema/{table}_transforms.sql.
        """
        project_dir = self.config.get("_project_dir", ".")
        transform_file = (
            Path(project_dir) / "artifacts" / "generated_schema" / f"{table}_transforms.sql"
        )
        if transform_file.exists():
            return str(transform_file)

        # Also check without _transforms suffix
        plain_file = (
            Path(project_dir) / "artifacts" / "generated_schema" / f"{table}.sql"
        )
        if plain_file.exists():
            return str(plain_file)

        logger.debug(f"No transform SQL found for {table}")
        return None

    def _load_normalization_plan(self) -> Optional[Dict]:
        """Load the normalization plan from metadata/normalization_plan.json."""
        project_dir = self.config.get("_project_dir", ".")
        metadata_rel = self.config.get("metadata", {}).get("path", "./metadata")
        plan_path = Path(project_dir) / metadata_rel / "normalization_plan.json"

        if not plan_path.exists():
            logger.debug(f"No normalization plan found at {plan_path}")
            return None

        try:
            with open(plan_path, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load normalization plan: {e}")
            return None
