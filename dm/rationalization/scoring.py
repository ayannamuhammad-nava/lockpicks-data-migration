"""
Migration Rationalization Scoring Helpers

Provides individual scoring functions for the weighted relevance formula
used by MigrationRationalizer to classify tables as Migrate / Review / Archive.

Each scorer returns a float in the range 0-100.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# ── Default Weights ──────────────────────────────────────────────────

DEFAULT_WEIGHTS = {
    "query_activity": 0.35,
    "downstream": 0.25,
    "freshness": 0.20,
    "completeness": 0.10,
    "tier": 0.10,
}


# ── Individual Scoring Functions ─────────────────────────────────────

def score_query_activity(metadata: dict) -> float:
    """Score based on OpenMetadata query activity metrics.

    Looks at usageSummary / queryCount tags from the metadata dict.
    Tables with heavy query activity score higher (important to migrate).

    Args:
        metadata: Table metadata dict from OM enricher (get_table_metadata).

    Returns:
        Score in 0-100 range.
    """
    # Check for usage / query activity indicators in tags
    tags = metadata.get("tags", [])

    # Direct query count (if available from OM usage API)
    query_count = metadata.get("query_count", 0)
    if query_count > 0:
        # Logarithmic scale: 1 query=20, 10=50, 100=80, 1000+=100
        import math
        return min(100.0, 20.0 + 25.0 * math.log10(max(query_count, 1)))

    # Fall back to usage tags
    usage_keywords = {
        "frequentlyused": 100.0,
        "highlyactive": 100.0,
        "active": 75.0,
        "moderatelyused": 50.0,
        "rarelyused": 25.0,
        "unused": 0.0,
        "deprecated": 0.0,
    }
    for tag in tags:
        tag_lower = tag.lower().replace(".", "").replace(" ", "").replace("-", "").replace("_", "")
        for keyword, score in usage_keywords.items():
            if keyword in tag_lower:
                return score

    # No usage information available — assume moderate
    return 50.0


def score_downstream(lineage: dict) -> float:
    """Score based on lineage consumer count.

    Tables that feed many downstream consumers are more critical to migrate.

    Args:
        lineage: Lineage dict from OM enricher (get_lineage), with structure:
                 {columns: {col_name: {upstream: [...], downstream: [...]}}}

    Returns:
        Score in 0-100 range.
    """
    columns = lineage.get("columns", {})
    if not columns:
        return 0.0

    # Count unique downstream consumers (tables)
    downstream_tables = set()
    for col_name, col_lineage in columns.items():
        for downstream in col_lineage.get("downstream", []):
            table = downstream.get("table", "")
            if table:
                downstream_tables.add(table)

    count = len(downstream_tables)

    if count == 0:
        return 0.0
    elif count == 1:
        return 30.0
    elif count <= 3:
        return 55.0
    elif count <= 5:
        return 75.0
    elif count <= 10:
        return 90.0
    else:
        return 100.0


def score_freshness(profile: dict) -> float:
    """Score based on how recently the table was profiled.

    A recently-profiled table is likely still in active use. Stale data
    suggests the table may be a candidate for archival.

    Args:
        profile: Table profile dict from OM enricher (get_table_profile).

    Returns:
        Score in 0-100 range.
    """
    profiled_at = profile.get("profiled_at")
    if not profiled_at:
        return 25.0  # No profiling data — penalize but don't kill

    # Parse timestamp
    try:
        if isinstance(profiled_at, (int, float)):
            # Epoch milliseconds
            profiled_dt = datetime.fromtimestamp(profiled_at / 1000, tz=timezone.utc)
        elif isinstance(profiled_at, str):
            # ISO format
            profiled_dt = datetime.fromisoformat(profiled_at.replace("Z", "+00:00"))
        elif isinstance(profiled_at, datetime):
            profiled_dt = profiled_at if profiled_at.tzinfo else profiled_at.replace(tzinfo=timezone.utc)
        else:
            return 25.0
    except (ValueError, TypeError, OSError):
        return 25.0

    now = datetime.now(timezone.utc)
    age_days = (now - profiled_dt).days

    if age_days <= 7:
        return 100.0
    elif age_days <= 30:
        return 85.0
    elif age_days <= 90:
        return 65.0
    elif age_days <= 180:
        return 45.0
    elif age_days <= 365:
        return 25.0
    else:
        return 10.0


def score_completeness(profile: dict) -> float:
    """Score based on inverse of null sparseness across columns.

    Tables with mostly complete (non-null) data score higher. A table
    where every column is 90%+ null is likely dead data.

    Args:
        profile: Table profile dict from OM enricher (get_table_profile).

    Returns:
        Score in 0-100 range.
    """
    columns = profile.get("columns", {})
    if not columns:
        return 50.0  # No profiling — assume moderate

    null_percents = []
    for col_name, stats in columns.items():
        null_pct = stats.get("null_percent")
        if null_pct is not None:
            try:
                null_percents.append(float(null_pct))
            except (ValueError, TypeError):
                continue

    if not null_percents:
        return 50.0

    avg_null_pct = sum(null_percents) / len(null_percents)

    # Invert: 0% nulls = 100 score, 100% nulls = 0 score
    return max(0.0, min(100.0, 100.0 - avg_null_pct))


def score_tier(metadata: dict) -> float:
    """Score from OpenMetadata tier tag.

    Tier mapping:
        Tier.Tier1 → 100 (most critical)
        Tier.Tier2 → 80
        Tier.Tier3 → 60
        Tier.Tier4 → 40
        Tier.Tier5 → 20 (least critical)

    Args:
        metadata: Table metadata dict from OM enricher (get_table_metadata).

    Returns:
        Score in 0-100 range.
    """
    tier = metadata.get("tier")

    tier_scores = {
        "Tier.Tier1": 100.0,
        "Tier.Tier2": 80.0,
        "Tier.Tier3": 60.0,
        "Tier.Tier4": 40.0,
        "Tier.Tier5": 20.0,
    }

    if tier and tier in tier_scores:
        return tier_scores[tier]

    # Try matching partial tier strings
    if tier:
        tier_lower = tier.lower()
        for key, score in tier_scores.items():
            if key.lower() in tier_lower or tier_lower in key.lower():
                return score

    # No tier assigned — assume mid-tier
    return 50.0


# ── Weighted Composite Score ─────────────────────────────────────────

def calculate_relevance(scores: dict, weights: Optional[dict] = None) -> float:
    """Calculate the weighted relevance score from individual dimension scores.

    Args:
        scores: Dict of {dimension_name: score} where each score is 0-100.
                Expected keys: query_activity, downstream, freshness,
                completeness, tier.
        weights: Dict of {dimension_name: weight} where weights sum to ~1.0.
                 Defaults to DEFAULT_WEIGHTS.

    Returns:
        Weighted score in 0-100 range.
    """
    w = weights or DEFAULT_WEIGHTS

    total = 0.0
    weight_sum = 0.0

    for dimension, weight in w.items():
        if dimension in scores:
            total += weight * scores[dimension]
            weight_sum += weight

    # Normalize if not all dimensions are present
    if weight_sum > 0 and weight_sum != 1.0:
        total = total / weight_sum

    return round(min(100.0, max(0.0, total)), 2)
