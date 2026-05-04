"""
Git Repo Loader

Clones a git repository containing mainframe artifacts (copybooks, data files,
legacy SQL) and scaffolds a DM project from the contents.

Usage:
    dm init my-project --repo https://github.com/org/mainframe-extracts.git

The loader scans the cloned repo for:
  - .cpy files  → COBOL copybooks (schema definitions)
  - .dat files  → Fixed-width data files (paired with copybooks)
  - .csv files  → Delimited data files
  - .sql files  → Legacy DDL/DML (CREATE TABLE, INSERT statements)
  - .txt files  → May be data files (auto-detected by content)

It generates a project.yaml with connections pointing to the discovered files,
datasets derived from copybook/file names, and metadata ready for the pipeline.
"""

import logging
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class DiscoveredArtifact:
    """A mainframe artifact found in the repo."""
    path: str
    artifact_type: str  # copybook, datafile, sql, csv
    table_name: str  # inferred logical table name
    paired_with: Optional[str] = None  # e.g., datafile paired with copybook


def clone_repo(repo_url: str, target_dir: Optional[str] = None) -> str:
    """Clone a git repository and return the local path.

    Args:
        repo_url: Git URL (https or ssh).
        target_dir: Optional local directory to clone into.
                    If not provided, clones into a temp directory.

    Returns:
        Path to the cloned repository.
    """
    if target_dir:
        clone_path = Path(target_dir)
    else:
        clone_path = Path(tempfile.mkdtemp(prefix="dm_repo_"))

    if clone_path.exists() and any(clone_path.iterdir()):
        logger.info(f"Directory {clone_path} already exists, pulling latest")
        result = subprocess.run(
            ["git", "-C", str(clone_path), "pull"],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            logger.warning(f"Git pull failed: {result.stderr}")
    else:
        logger.info(f"Cloning {repo_url} into {clone_path}")
        result = subprocess.run(
            ["git", "clone", repo_url, str(clone_path)],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Git clone failed: {result.stderr}")

    logger.info(f"Repo ready at {clone_path}")
    return str(clone_path)


def scan_repo(repo_path: str) -> List[DiscoveredArtifact]:
    """Scan a cloned repo for mainframe artifacts.

    Looks for:
      - .cpy files (COBOL copybooks)
      - .dat/.bin files (fixed-width data extracts)
      - .csv/.tsv files (delimited data)
      - .sql files (legacy DDL/DML)

    Pairs copybooks with data files when names match.

    Returns:
        List of DiscoveredArtifact objects.
    """
    repo = Path(repo_path)
    artifacts = []
    copybooks = {}  # stem -> path
    datafiles = {}  # stem -> path

    for f in sorted(repo.rglob("*")):
        if not f.is_file():
            continue
        if f.name.startswith("."):
            continue

        suffix = f.suffix.lower()
        stem = f.stem.upper()
        # Infer table name from filename
        table_name = _infer_table_name(f.stem)

        if suffix in (".cpy", ".cob", ".copybook"):
            artifacts.append(DiscoveredArtifact(
                path=str(f), artifact_type="copybook",
                table_name=table_name,
            ))
            copybooks[stem] = str(f)

        elif suffix in (".dat", ".bin", ".raw", ".ebcdic"):
            artifacts.append(DiscoveredArtifact(
                path=str(f), artifact_type="datafile",
                table_name=table_name,
            ))
            datafiles[stem] = str(f)

        elif suffix in (".csv", ".tsv"):
            artifacts.append(DiscoveredArtifact(
                path=str(f), artifact_type="csv",
                table_name=table_name,
            ))

        elif suffix == ".sql":
            artifacts.append(DiscoveredArtifact(
                path=str(f), artifact_type="sql",
                table_name=table_name,
            ))

        elif suffix == ".txt":
            # Auto-detect: if file has fixed-width looking content, treat as data
            try:
                head = f.read_text(errors="replace")[:500]
                lines = head.strip().splitlines()
                if len(lines) > 1:
                    lengths = [len(l) for l in lines[:10]]
                    if len(set(lengths)) == 1 and lengths[0] > 20:
                        # All lines same length — likely fixed-width
                        artifacts.append(DiscoveredArtifact(
                            path=str(f), artifact_type="datafile",
                            table_name=table_name,
                        ))
                        datafiles[stem] = str(f)
                    elif "," in lines[0] or "\t" in lines[0]:
                        artifacts.append(DiscoveredArtifact(
                            path=str(f), artifact_type="csv",
                            table_name=table_name,
                        ))
            except Exception:
                pass

    # Pair copybooks with data files by matching stems
    for a in artifacts:
        stem = Path(a.path).stem.upper()
        if a.artifact_type == "copybook" and stem in datafiles:
            a.paired_with = datafiles[stem]
        elif a.artifact_type == "datafile" and stem in copybooks:
            a.paired_with = copybooks[stem]

    logger.info(
        f"Scanned {repo_path}: found {len(artifacts)} artifacts "
        f"({sum(1 for a in artifacts if a.artifact_type == 'copybook')} copybooks, "
        f"{sum(1 for a in artifacts if a.artifact_type == 'datafile')} data files, "
        f"{sum(1 for a in artifacts if a.artifact_type == 'csv')} CSV files, "
        f"{sum(1 for a in artifacts if a.artifact_type == 'sql')} SQL files)"
    )

    return artifacts


def generate_project_from_repo(
    project_name: str,
    repo_path: str,
    project_dir: str,
    target_type: str = "postgres",
) -> Dict:
    """Generate a project.yaml and directory structure from a scanned repo.

    Args:
        project_name: Name for the DM project.
        repo_path: Path to the cloned repo.
        project_dir: Where to create the project.
        target_type: Default target platform (postgres, snowflake, oracle, redshift).

    Returns:
        Dict with project config and scan summary.
    """
    artifacts = scan_repo(repo_path)

    project = Path(project_dir)
    project.mkdir(parents=True, exist_ok=True)
    (project / "metadata").mkdir(exist_ok=True)
    (project / "artifacts").mkdir(exist_ok=True)
    (project / "plugins").mkdir(exist_ok=True)
    (project / "plugins" / "__init__.py").touch()

    # Build connections and datasets from discovered artifacts
    connections = {}
    datasets = []
    source_idx = 0

    # Group copybooks with their data files
    processed_tables = set()

    for art in artifacts:
        if art.table_name in processed_tables:
            continue

        if art.artifact_type == "copybook":
            source_name = f"source_{art.table_name}"
            conn = {
                "type": "copybook",
                "copybook": art.path,
                "table_name": art.table_name,
            }
            if art.paired_with:
                conn["datafile"] = art.paired_with
                # Detect encoding
                try:
                    raw = Path(art.paired_with).read_bytes()[:10]
                    if any(b > 127 for b in raw):
                        conn["encoding"] = "ebcdic"
                    else:
                        conn["encoding"] = "utf-8"
                except Exception:
                    conn["encoding"] = "utf-8"
            conn["format"] = "fixed"

            connections[source_name] = conn
            datasets.append({
                "name": art.table_name,
                "source": source_name,
                "target": "modern",
                "legacy_table": art.table_name,
            })
            processed_tables.add(art.table_name)

        elif art.artifact_type in ("csv", "datafile") and art.table_name not in processed_tables:
            source_name = f"source_{art.table_name}"
            fmt = "csv" if art.artifact_type == "csv" else "fixed"
            conn = {
                "type": "flatfile",
                "datafile": art.path,
                "format": fmt,
                "table_name": art.table_name,
            }
            if art.paired_with:
                conn["copybook"] = art.paired_with
                conn["format"] = "fixed"
                conn["type"] = "copybook"

            connections[source_name] = conn
            datasets.append({
                "name": art.table_name,
                "source": source_name,
                "target": "modern",
                "legacy_table": art.table_name,
            })
            processed_tables.add(art.table_name)

    # Add modern (target) connection placeholder
    connections["modern"] = {
        "type": target_type,
        "host": "${DB_MODERN_HOST:localhost}",
        "port": 5432,
        "database": "${DB_MODERN_NAME:modern_db}",
        "user": "${DB_MODERN_USER:app}",
        "password": "${DB_MODERN_PASSWORD:secret123}",
    }

    # Build project config
    config = {
        "project": {
            "name": project_name,
            "description": f"Data migration from mainframe artifacts in {Path(repo_path).name}",
            "version": "1.0",
            "source_repo": repo_path,
        },
        "connections": connections,
        "datasets": datasets,
        "validation": {
            "sample_size": 1000,
            "governance": {
                "pii_keywords": [
                    "ssn", "social_security", "dob", "date_of_birth",
                    "email", "phone", "address", "bank", "account",
                    "routing", "credit_card", "salary", "income",
                ],
                "naming_regex": "^[a-z0-9_]+$",
                "max_null_percent": 10,
            },
        },
        "scoring": {
            "weights": {"structure": 0.4, "integrity": 0.4, "governance": 0.2},
            "thresholds": {"green": 90, "yellow": 70},
        },
        "metadata": {"path": "./metadata"},
        "artifacts": {"base_path": "./artifacts"},
        "plugins": [],
    }

    # Write project.yaml
    config_path = project / "project.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    # Copy SQL files into the project for reference
    sql_artifacts = [a for a in artifacts if a.artifact_type == "sql"]
    if sql_artifacts:
        sql_dir = project / "legacy_sql"
        sql_dir.mkdir(exist_ok=True)
        for a in sql_artifacts:
            shutil.copy2(a.path, sql_dir / Path(a.path).name)

    summary = {
        "project_dir": str(project),
        "config_path": str(config_path),
        "copybooks": sum(1 for a in artifacts if a.artifact_type == "copybook"),
        "datafiles": sum(1 for a in artifacts if a.artifact_type == "datafile"),
        "csv_files": sum(1 for a in artifacts if a.artifact_type == "csv"),
        "sql_files": len(sql_artifacts),
        "datasets": len(datasets),
        "connections": len(connections),
    }

    logger.info(
        f"Project '{project_name}' created at {project} with "
        f"{len(datasets)} datasets from {len(artifacts)} artifacts"
    )

    return summary


def _infer_table_name(filename: str) -> str:
    """Infer a logical table name from a filename.

    Examples:
        'SG01_CLAIMANT' -> 'claimants'
        'BENEFIT-PAYMENTS.cpy' -> 'benefit_payments'
        'QC_SAMPLE_2024' -> 'qc_sample'
    """
    name = filename.upper()
    # Remove common mainframe prefixes (SG01_, RSA_, etc.)
    name = re.sub(r'^[A-Z]{2,4}\d{0,2}_', '', name)
    # Remove date suffixes
    name = re.sub(r'_?\d{4,8}$', '', name)
    # Convert to snake_case
    name = name.replace("-", "_").lower()
    # Simple pluralization for common patterns
    if not name.endswith("s") and not name.endswith("data"):
        name += "s"
    return name
