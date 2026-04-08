"""
DM Hook Specifications

Defines all extension points for Lockpicks Data Migration.
Plugins implement these hooks to provide domain-specific behaviour.
"""

import pluggy

PROJECT_NAME = "dm"

hookspec = pluggy.HookspecMarker(PROJECT_NAME)
hookimpl = pluggy.HookimplMarker(PROJECT_NAME)


class DMHookSpec:
    """All extension points for Lockpicks Data Migration."""

    # ── Discovery Phase ──────────────────────────────────────────────

    @hookspec
    def dm_get_column_overrides(self, table: str) -> dict:
        """Return curated column mapping overrides for a table.

        Returns:
            Dict of {column_name: {target, rationale, confidence, type}}.
            These take precedence over auto-generated fuzzy-match results.
        """

    @hookspec
    def dm_enrich_glossary_entry(self, entry: dict) -> dict:
        """Modify or enrich a glossary entry before it is saved.

        Args:
            entry: A glossary entry dict with keys: name, description,
                   system, pii, confidence, table.

        Returns:
            The modified entry dict.
        """

    # ── Enrichment Phase ──────────────────────────────────────────────

    @hookspec
    def dm_get_profiling_stats(self, table: str, column: str) -> dict:
        """Return column-level profiling metrics from OpenMetadata.

        Returns:
            Dict with keys: null_percent, unique_percent, distinct_count,
            min_value, max_value, mean_value, stddev, value_frequencies,
            histogram.
        """

    @hookspec
    def dm_get_lineage(self, table: str) -> dict:
        """Return column-level lineage from OpenMetadata.

        Returns:
            Dict with keys: columns (dict of column_name to
            {upstream: [{table, column}], downstream: [{table, column}]}).
        """

    # ── Schema Generation Phase ───────────────────────────────────────

    @hookspec
    def dm_normalization_overrides(self, table: str) -> dict:
        """Return explicit entity decomposition rules for normalization.

        Returns:
            Dict with keys:
                entities: list of {name, columns, pk}
                relationships: list of {parent, child, fk}
        """

    # ── Pre-Migration Phase ──────────────────────────────────────────

    @hookspec
    def dm_pre_validators(self) -> list:
        """Return additional PreValidator instances to run during pre-migration.

        Returns:
            List of PreValidator subclass instances.
        """

    @hookspec
    def dm_data_quality_rules(self, dataset: str) -> list:
        """Return cross-field data quality anomaly rules for a dataset.

        Each rule is a dict with:
            name: str — rule identifier
            severity: str — HIGH | MEDIUM | LOW
            description: str — human-readable description
            check_fn: callable(df) -> anomaly_dict or None

        Returns:
            List of rule dicts.
        """

    # ── Post-Migration Phase ─────────────────────────────────────────

    @hookspec
    def dm_post_validators(self) -> list:
        """Return additional PostValidator instances to run during post-migration.

        Returns:
            List of PostValidator subclass instances.
        """

    @hookspec
    def dm_custom_aggregates(self, dataset: str) -> list:
        """Return additional aggregate checks beyond what is in project.yaml.

        Each aggregate is a dict with:
            name, legacy_query, modern_query, comparison, tolerance (optional).

        Returns:
            List of aggregate dicts.
        """

    # ── Scoring ──────────────────────────────────────────────────────

    @hookspec
    def dm_adjust_score(self, phase: str, base_score: float, results: dict) -> float:
        """Adjust the confidence score after base calculation.

        Args:
            phase: 'pre' or 'post'
            base_score: The score calculated by DM (0-100)
            results: Full validation results dict

        Returns:
            Adjusted score (0-100). Return base_score to leave unchanged.
        """

    # ── Reporting ────────────────────────────────────────────────────

    @hookspec
    def dm_extra_report_sections(self, phase: str, results: dict) -> list:
        """Return additional markdown sections to append to reports.

        Returns:
            List of strings (markdown sections).
        """

    # ── Connectors ───────────────────────────────────────────────────

    @hookspec
    def dm_register_connectors(self) -> dict:
        """Register custom database connectors.

        Returns:
            Dict of {connector_type_name: ConnectorClass}.
        """

    # ── Rationalization Phase ─────────────────────────────────────

    @hookspec
    def dm_rationalization_overrides(self, table: str) -> dict:
        """Override auto-calculated table relevance scores.

        Returns:
            Dict with keys: score (float), recommendation (str),
            rationale (str). Allows plugins to force-include or
            force-exclude tables from migration scope.
        """

    # ── Target Platforms ──────────────────────────────────────────

    @hookspec
    def dm_register_targets(self) -> dict:
        """Register custom target platform adapters.

        Returns:
            Dict of {target_name: TargetAdapterClass}.
        """

    # ── Code Conversion ───────────────────────────────────────────

    @hookspec
    def dm_conversion_overrides(self, source_sql: str, target: str) -> str:
        """Override or patch specific SQL conversion patterns.

        Args:
            source_sql: The SQL being converted.
            target: Target platform name (e.g., 'postgres').

        Returns:
            Modified SQL string, or None to use default conversion.
        """

    # ── Ingestion Phase ───────────────────────────────────────────

    @hookspec
    def dm_ingest_strategy(self, dataset: str) -> str:
        """Override the migration strategy for a dataset.

        Returns:
            Strategy name: 'full_load' | 'incremental' | 'cdc' | 'external'
        """

    @hookspec
    def dm_post_ingest(self, dataset: str, result: dict) -> None:
        """Called after each table is ingested.

        Use for notifications, logging, or custom post-load actions.
        """

    # ── Observer Phase ────────────────────────────────────────────

    @hookspec
    def dm_observer_checks(self) -> list:
        """Return additional observation checks for pipeline monitoring.

        Each check is a dict with:
            name: str — check identifier
            check_fn: callable(modern_conn, baseline) -> dict

        Returns:
            List of check dicts.
        """

    @hookspec
    def dm_on_drift_detected(self, check_name: str, details: dict) -> None:
        """Called when data drift is detected by the observer.

        Use for custom alerting, remediation, or escalation.
        """
