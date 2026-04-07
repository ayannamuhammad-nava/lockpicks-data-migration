"""
DM Scoring Engine

Calculates confidence scores from validator results.
Extracted from agents/orchestrator.py to be reusable across projects.
"""

import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def calculate_confidence(
    structure_score: float,
    integrity_score: float,
    governance_score: float,
    config: Dict,
) -> Dict:
    """Calculate weighted confidence score.

    Args:
        structure_score: 0-100 (pre-migration structural readiness)
        integrity_score: 0-100 (post-migration data integrity)
        governance_score: 0-100 (compliance and standards)
        config: Project configuration with scoring weights and thresholds.

    Returns:
        Dict with 'score' (float) and 'status' (GREEN/YELLOW/RED).
    """
    scoring = config.get("scoring", config.get("confidence", {}))
    weights = scoring.get("weights", {
        "structure": 0.4,
        "integrity": 0.4,
        "governance": 0.2,
    })

    final_score = (
        weights.get("structure", 0.4) * structure_score
        + weights.get("integrity", 0.4) * integrity_score
        + weights.get("governance", 0.2) * governance_score
    )

    status = get_traffic_light(final_score, config)

    return {"score": round(final_score, 2), "status": status}


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
