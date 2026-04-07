"""
Core report generation utilities.

Provides functions for saving markdown, CSV, JSON reports and
creating timestamped artifact folders.
"""

import csv
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def create_artifact_folder(base_path: str = "./artifacts") -> str:
    """Create a timestamped artifact folder.

    Returns:
        Path to the created folder.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    folder = os.path.join(base_path, f"run_{timestamp}")
    os.makedirs(folder, exist_ok=True)
    logger.info(f"Created artifact folder: {folder}")
    return folder


def save_markdown_report(content: str, filepath: str) -> None:
    """Save a markdown report to a file."""
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        f.write(content)
    logger.info(f"Saved report: {filepath}")


def save_csv_report(rows: List[Dict], filepath: str) -> None:
    """Save a CSV report from a list of dicts."""
    if not rows:
        return
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Saved CSV report: {filepath}")


def save_json_log(data: Any, filepath: str) -> None:
    """Save JSON data to a file."""
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)
    logger.info(f"Saved JSON log: {filepath}")


def save_confidence_score(score: float, status: str, filepath: str) -> None:
    """Save confidence score and status to a text file."""
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        f.write(f"Confidence Score: {score}/100\n")
        f.write(f"Status: {status}\n")
    logger.info(f"Saved confidence score: {filepath}")


def save_run_metadata(metadata: Dict, filepath: str) -> None:
    """Save run metadata as JSON."""
    metadata["timestamp"] = datetime.now().isoformat()
    save_json_log(metadata, filepath)
