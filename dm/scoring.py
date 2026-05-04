"""
DM Scoring Engine

Calculates confidence scores from validator results.
Supports target-platform-aware scoring: each target platform has different
capabilities (CHECK constraints, FK enforcement, native types) that affect
the confidence score.

Extracted from agents/orchestrator.py to be reusable across projects.
"""

import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


# ── Target Platform Penalties ─────────────────────────────────────────────────
# Each platform's gaps introduce scoring penalties across the three dimensions.
# These reflect real-world limitations that affect migration confidence.

TARGET_PENALTIES = {
    "postgres": {
        "structure": 0,     # Full type support, native BOOLEAN, JSONB
        "integrity": 0,     # FK constraints enforced
        "governance": 0,    # CHECK constraints enforced
        "notes": [],
    },
    "snowflake": {
        "structure": 2,     # No native UUID, INTERVAL stored as VARCHAR
        "integrity": 5,     # FK constraints declared but not enforced
        "governance": 4,    # CHECK constraints not supported
        "notes": [
            "FK constraints are not enforced — integrity depends on load process",
            "CHECK constraints not supported — validation must occur at ETL layer",
            "UUID stored as VARCHAR(36) — no native UUID type",
        ],
    },
    "oracle": {
        "structure": 3,     # No native BOOLEAN (NUMBER(1)), JSONB→CLOB
        "integrity": 0,     # FK constraints enforced
        "governance": 0,    # CHECK constraints enforced
        "notes": [
            "BOOLEAN mapped to NUMBER(1) — application layer must enforce 0/1",
            "JSON stored as CLOB — no native JSON query operators pre-21c",
        ],
    },
    "redshift": {
        "structure": 3,     # No native binary, UUID as VARCHAR, INTERVAL as VARCHAR
        "integrity": 8,     # PK/UNIQUE/FK constraints declared but never enforced
        "governance": 4,    # CHECK constraints not enforced
        "notes": [
            "PK, UNIQUE, and FK constraints are not enforced — informational only",
            "CHECK constraints are not enforced",
            "No native binary type — BYTEA mapped to VARCHAR(65535)",
            "DISTKEY/SORTKEY choices affect query performance, not correctness",
        ],
    },
}


def get_target_penalties(target: str) -> Dict:
    """Return the penalty adjustments for a target platform."""
    return TARGET_PENALTIES.get(target, TARGET_PENALTIES["postgres"])


def calculate_confidence(
    structure_score: float,
    integrity_score: float,
    governance_score: float,
    config: Dict,
    target: Optional[str] = None,
) -> Dict:
    """Calculate weighted confidence score, optionally adjusted for target platform.

    Args:
        structure_score: 0-100 (pre-migration structural readiness)
        integrity_score: 0-100 (post-migration data integrity)
        governance_score: 0-100 (compliance and standards)
        config: Project configuration with scoring weights and thresholds.
        target: Optional target platform name (postgres, snowflake, oracle, redshift).
                When provided, platform-specific penalties are applied.

    Returns:
        Dict with 'score', 'status', 'target', 'target_penalties', and
        component scores ('structure_score', 'integrity_score', 'governance_score').
    """
    scoring = config.get("scoring", config.get("confidence", {}))
    weights = scoring.get("weights", {
        "structure": 0.4,
        "integrity": 0.4,
        "governance": 0.2,
    })

    # Apply target-specific penalties
    penalties = get_target_penalties(target) if target else get_target_penalties("postgres")
    adj_structure = max(0, structure_score - penalties["structure"])
    adj_integrity = max(0, integrity_score - penalties["integrity"])
    adj_governance = max(0, governance_score - penalties["governance"])

    final_score = (
        weights.get("structure", 0.4) * adj_structure
        + weights.get("integrity", 0.4) * adj_integrity
        + weights.get("governance", 0.2) * adj_governance
    )

    status = get_traffic_light(final_score, config)

    result = {
        "score": round(final_score, 2),
        "status": status,
        "target": target or "postgres",
        "structure_score": round(adj_structure, 2),
        "integrity_score": round(adj_integrity, 2),
        "governance_score": round(adj_governance, 2),
        "target_penalties": penalties,
    }

    if penalties.get("notes"):
        result["target_notes"] = penalties["notes"]

    return result


def calculate_confidence_all_targets(
    structure_score: float,
    integrity_score: float,
    governance_score: float,
    config: Dict,
) -> Dict[str, Dict]:
    """Calculate confidence for all available target platforms.

    Returns:
        Dict of {target_name: confidence_result} for postgres, snowflake,
        oracle, and redshift.
    """
    results = {}
    for target in TARGET_PENALTIES:
        results[target] = calculate_confidence(
            structure_score, integrity_score, governance_score,
            config, target=target,
        )
    return results


def get_traffic_light(score: float, config: Dict) -> str:
    """Map a numeric score to a traffic-light status."""
    scoring = config.get("scoring", config.get("confidence", {}))
    thresholds = scoring.get("thresholds", {"green": 90, "yellow": 70})

    if score >= thresholds.get("green", 90):
        return "GREEN"
    elif score >= thresholds.get("yellow", 70):
        return "YELLOW"
    else:
        return "RED"


def sum_penalties(results: List) -> float:
    """Sum score_penalty from a list of ValidatorResult objects."""
    return sum(getattr(r, "score_penalty", 0) for r in results)


def score_from_penalties(penalties: float) -> float:
    """Convert total penalties to a 0-100 score."""
    return round(max(0, 100 - penalties), 2)
