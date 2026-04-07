"""
L-Discoverer — Migration Scope Rationalization Engine

Analyzes tables via OpenMetadata enrichment data (query activity, lineage,
freshness, completeness, tier) to produce a scored recommendation for each
table: Migrate, Review, or Archive/Decommission.

This allows teams to reduce migration scope by identifying tables that are
unused, stale, or low-value before investing engineering effort.
"""

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dm.rationalization.scoring import (
    DEFAULT_WEIGHTS,
    calculate_relevance,
    score_completeness,
    score_downstream,
    score_freshness,
    score_query_activity,
    score_tier,
)

logger = logging.getLogger(__name__)


# ── Data Classes ─────────────────────────────────────────────────────

@dataclass
class TableRelevance:
    """Relevance assessment for a single table."""

    table: str
    score: float
    recommendation: str  # "migrate" | "review" | "archive"
    breakdown: dict  # {query_activity: x, downstream: x, freshness: x, ...}
    rationale: str


@dataclass
class RationalizationReport:
    """Aggregate rationalization results for a set of tables."""

    tables: list  # list of TableRelevance
    migrate_count: int = 0
    review_count: int = 0
    archive_count: int = 0
    scope_reduction_pct: float = 0.0


# ── Classification Thresholds ────────────────────────────────────────

MIGRATE_THRESHOLD = 70.0   # score >= 70 → Migrate
REVIEW_THRESHOLD = 40.0    # score 40-69 → Review
# score < 40 → Archive / Decommission


def classify(score: float) -> str:
    """Classify a relevance score into a recommendation."""
    if score >= MIGRATE_THRESHOLD:
        return "migrate"
    elif score >= REVIEW_THRESHOLD:
        return "review"
    else:
        return "archive"


def build_rationale(table: str, recommendation: str, breakdown: dict) -> str:
    """Build a human-readable rationale string from score breakdown."""
    parts = []

    if recommendation == "migrate":
        parts.append(f"Table '{table}' is recommended for migration.")
    elif recommendation == "review":
        parts.append(f"Table '{table}' requires manual review before migration decision.")
    else:
        parts.append(f"Table '{table}' is a candidate for archival/decommission.")

    # Highlight strongest and weakest signals
    sorted_dims = sorted(breakdown.items(), key=lambda x: x[1], reverse=True)

    if sorted_dims:
        strongest = sorted_dims[0]
        weakest = sorted_dims[-1]
        parts.append(
            f"Strongest signal: {strongest[0]} ({strongest[1]:.0f}/100)."
        )
        if weakest[1] < 40:
            parts.append(
                f"Concern: low {weakest[0]} score ({weakest[1]:.0f}/100)."
            )

    return " ".join(parts)


# ── Main Rationalizer ───────────────────────────────────────────────

class MigrationRationalizer:
    """Analyzes tables to determine migration scope via relevance scoring.

    Uses the OpenMetadata enricher to fetch metadata, profiling, and lineage,
    then applies a weighted scoring formula to classify each table.
    """

    def __init__(
        self,
        om_enricher: Any,
        plugin_manager: Any = None,
        weights: Optional[Dict[str, float]] = None,
    ):
        """
        Args:
            om_enricher: An OpenMetadataEnricher instance (connected).
            plugin_manager: Optional pluggy PluginManager for hook overrides.
            weights: Optional custom weights dict overriding DEFAULT_WEIGHTS.
        """
        self._om = om_enricher
        self._pm = plugin_manager
        self._weights = weights or DEFAULT_WEIGHTS

    def rationalize(self, tables: List[str]) -> RationalizationReport:
        """Score and classify a list of tables for migration scope.

        Args:
            tables: List of table names to evaluate.

        Returns:
            A RationalizationReport with scored TableRelevance entries.
        """
        results: List[TableRelevance] = []

        for table in tables:
            try:
                relevance = self._evaluate_table(table)
            except Exception as e:
                logger.error(f"Failed to evaluate table '{table}': {e}")
                # On failure, mark for review with a low-confidence score
                relevance = TableRelevance(
                    table=table,
                    score=0.0,
                    recommendation="review",
                    breakdown={},
                    rationale=f"Evaluation failed: {e}",
                )
            results.append(relevance)

        # Build report
        migrate_count = sum(1 for r in results if r.recommendation == "migrate")
        review_count = sum(1 for r in results if r.recommendation == "review")
        archive_count = sum(1 for r in results if r.recommendation == "archive")

        total = len(results)
        scope_reduction_pct = (
            round((archive_count / total) * 100, 1) if total > 0 else 0.0
        )

        return RationalizationReport(
            tables=results,
            migrate_count=migrate_count,
            review_count=review_count,
            archive_count=archive_count,
            scope_reduction_pct=scope_reduction_pct,
        )

    def _evaluate_table(self, table: str) -> TableRelevance:
        """Fetch OM data and compute relevance for a single table."""

        # Fetch metadata, profile, and lineage from OpenMetadata
        metadata = self._om.get_table_metadata(table)
        profile = self._om.get_table_profile(table)
        lineage = self._om.get_lineage(table)

        # Compute individual dimension scores
        breakdown = {
            "query_activity": score_query_activity(metadata),
            "downstream": score_downstream(lineage),
            "freshness": score_freshness(profile),
            "completeness": score_completeness(profile),
            "tier": score_tier(metadata),
        }

        # Calculate weighted relevance
        score = calculate_relevance(breakdown, self._weights)
        recommendation = classify(score)
        rationale = build_rationale(table, recommendation, breakdown)

        # Apply plugin overrides (dm_rationalization_overrides hook)
        if self._pm:
            try:
                overrides_list = self._pm.hook.dm_rationalization_overrides(
                    table=table
                )
                for override in overrides_list:
                    if override:
                        if "score" in override:
                            score = float(override["score"])
                        if "recommendation" in override:
                            recommendation = override["recommendation"]
                        else:
                            recommendation = classify(score)
                        if "rationale" in override:
                            rationale = override["rationale"]
                        logger.info(
                            f"Plugin override applied to '{table}': "
                            f"score={score}, recommendation={recommendation}"
                        )
            except Exception as e:
                logger.warning(f"Plugin override failed for '{table}': {e}")

        return TableRelevance(
            table=table,
            score=score,
            recommendation=recommendation,
            breakdown=breakdown,
            rationale=rationale,
        )

    # ── Report Persistence ───────────────────────────────────────

    def save_report(
        self,
        report: RationalizationReport,
        output_path: str,
    ) -> None:
        """Save rationalization report as Markdown, JSON, and YAML artifacts.

        Args:
            report: The RationalizationReport to persist.
            output_path: Directory path for output files.
        """
        out = Path(output_path)
        out.mkdir(parents=True, exist_ok=True)

        # 1. Markdown report
        self._write_markdown(report, out / "rationalization_report.md")

        # 2. JSON report
        self._write_json(report, out / "rationalization_report.json")

        # 3. Migration scope YAML (list of tables to migrate)
        self._write_scope_yaml(report, out / "migration_scope.yaml")

        logger.info(f"Rationalization report saved to {out}")

    def _write_markdown(
        self, report: RationalizationReport, path: Path
    ) -> None:
        """Write a human-readable Markdown rationalization report."""
        lines = [
            "# Migration Scope Rationalization Report",
            "",
            f"**Generated:** {datetime.now(timezone.utc).isoformat()}",
            f"**Tables Analyzed:** {len(report.tables)}",
            "",
            "## Summary",
            "",
            f"| Category | Count |",
            f"|----------|-------|",
            f"| Migrate  | {report.migrate_count} |",
            f"| Review   | {report.review_count} |",
            f"| Archive  | {report.archive_count} |",
            f"| **Scope Reduction** | **{report.scope_reduction_pct}%** |",
            "",
            "## Table Details",
            "",
            "| Table | Score | Recommendation | Query Activity | Downstream | Freshness | Completeness | Tier |",
            "|-------|-------|----------------|---------------|------------|-----------|-------------|------|",
        ]

        for t in sorted(report.tables, key=lambda x: x.score, reverse=True):
            b = t.breakdown
            lines.append(
                f"| {t.table} | {t.score:.1f} | **{t.recommendation.upper()}** "
                f"| {b.get('query_activity', 0):.0f} "
                f"| {b.get('downstream', 0):.0f} "
                f"| {b.get('freshness', 0):.0f} "
                f"| {b.get('completeness', 0):.0f} "
                f"| {b.get('tier', 0):.0f} |"
            )

        lines.extend([
            "",
            "## Rationale",
            "",
        ])

        for t in sorted(report.tables, key=lambda x: x.score, reverse=True):
            lines.append(f"### {t.table}")
            lines.append(f"")
            lines.append(f"{t.rationale}")
            lines.append(f"")

        lines.extend([
            "---",
            "*Generated by DM L-Discoverer (Migration Rationalization Engine)*",
        ])

        path.write_text("\n".join(lines), encoding="utf-8")

    def _write_json(
        self, report: RationalizationReport, path: Path
    ) -> None:
        """Write the full report as structured JSON."""
        data = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total_tables": len(report.tables),
                "migrate_count": report.migrate_count,
                "review_count": report.review_count,
                "archive_count": report.archive_count,
                "scope_reduction_pct": report.scope_reduction_pct,
            },
            "tables": [asdict(t) for t in report.tables],
        }

        path.write_text(
            json.dumps(data, indent=2, default=str), encoding="utf-8"
        )

    def _write_scope_yaml(
        self, report: RationalizationReport, path: Path
    ) -> None:
        """Write migration_scope.yaml listing tables by classification."""
        import yaml

        scope = {
            "migration_scope": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "migrate": [
                    {"table": t.table, "score": t.score}
                    for t in report.tables
                    if t.recommendation == "migrate"
                ],
                "review": [
                    {"table": t.table, "score": t.score, "rationale": t.rationale}
                    for t in report.tables
                    if t.recommendation == "review"
                ],
                "archive": [
                    {"table": t.table, "score": t.score, "rationale": t.rationale}
                    for t in report.tables
                    if t.recommendation == "archive"
                ],
            },
        }

        path.write_text(
            yaml.dump(scope, default_flow_style=False, sort_keys=False),
            encoding="utf-8",
        )
