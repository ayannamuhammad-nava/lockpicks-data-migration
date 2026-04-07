"""
DM Configuration

Loads project.yaml, resolves environment variables, and provides
typed access to configuration values.
"""

import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

_ENV_VAR_PATTERN = re.compile(r'\$\{(\w+)(?::([^}]*))?\}')


def _resolve_env_vars(value: Any) -> Any:
    """Recursively resolve ${VAR:default} patterns in config values."""
    if isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_vars(item) for item in value]
    if isinstance(value, str):
        def _replace(match):
            var_name = match.group(1)
            default = match.group(2)
            if default is not None:
                return os.environ.get(var_name, default)
            return os.environ[var_name]
        return _ENV_VAR_PATTERN.sub(_replace, value)
    return value


def load_project_config(project_dir: str = ".") -> Dict:
    """Load project.yaml from the given project directory.

    Args:
        project_dir: Path to the project directory containing project.yaml.

    Returns:
        Fully resolved configuration dict.
    """
    project_path = Path(project_dir)
    config_file = project_path / "project.yaml"

    if not config_file.exists():
        raise FileNotFoundError(
            f"No project.yaml found in {project_path.resolve()}. "
            f"Run 'dm init <name>' to create a new project."
        )

    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    config = _resolve_env_vars(config)
    config["_project_dir"] = str(project_path.resolve())

    logger.info(f"Loaded project config from {config_file}")
    return config


def get_connection_config(config: Dict, system: str) -> Dict:
    """Extract connection config for 'legacy' or 'modern' system."""
    connections = config.get("connections", {})
    if system not in connections:
        raise KeyError(f"No '{system}' connection defined in project.yaml")
    return connections[system]


def get_datasets(config: Dict) -> List[Dict]:
    """Return the list of dataset definitions from config."""
    return config.get("datasets", [])


def get_dataset_config(config: Dict, dataset_name: str) -> Optional[Dict]:
    """Find a specific dataset configuration by name."""
    for ds in get_datasets(config):
        name = ds if isinstance(ds, str) else ds.get("name", ds)
        if name == dataset_name:
            return ds if isinstance(ds, dict) else {"name": ds}
    return None


def get_validation_config(config: Dict) -> Dict:
    """Return the validation section of config."""
    return config.get("validation", {})


def get_governance_config(config: Dict) -> Dict:
    """Return governance-specific configuration."""
    return get_validation_config(config).get("governance", {})


def get_scoring_config(config: Dict) -> Dict:
    """Return scoring weights and thresholds."""
    return config.get("scoring", config.get("confidence", {}))


def get_metadata_path(config: Dict) -> Path:
    """Return the resolved path to the metadata directory."""
    project_dir = config.get("_project_dir", ".")
    metadata_rel = config.get("metadata", {}).get("path", "./metadata")
    return Path(project_dir) / metadata_rel


def get_artifacts_path(config: Dict) -> str:
    """Return the base path for artifacts."""
    project_dir = config.get("_project_dir", ".")
    artifacts_rel = config.get("artifacts", {}).get("base_path", "./artifacts")
    return str(Path(project_dir) / artifacts_rel)


def get_plugin_specs(config: Dict) -> List[str]:
    """Return the list of plugin module.ClassName specs."""
    return config.get("plugins", [])


def get_openmetadata_config(config: Dict) -> Dict:
    """Return the openmetadata config section.

    Raises:
        KeyError: If no openmetadata section is configured.
    """
    om = config.get("openmetadata")
    if not om:
        raise KeyError(
            "No 'openmetadata' section in project.yaml. "
            "Configure host, auth_token, legacy_service, legacy_database."
        )
    return om


def get_schema_generation_config(config: Dict) -> Dict:
    """Return schema_generation config with sensible defaults."""
    defaults = {
        "target": "postgres",
        "naming_convention": "snake_case",
        "abbreviation_expansion": True,
        "type_optimization": True,
        "pii_handling": {"default_action": "hash"},
        "normalization": {
            "enabled": True,
            "min_group_size": 3,
            "prefix_detection": True,
            "lookup_threshold": 20,
        },
        "constraints": {
            "infer_not_null": True,
            "infer_unique": True,
            "infer_check": True,
        },
        "defaults": {
            "add_created_at": True,
            "add_updated_at": True,
            "id_strategy": "serial",
        },
    }
    user_config = config.get("schema_generation", {})
    # Shallow merge — user values override defaults
    merged = {**defaults, **user_config}
    for key in ("normalization", "constraints", "defaults", "pii_handling"):
        if key in user_config and isinstance(user_config[key], dict):
            merged[key] = {**defaults.get(key, {}), **user_config[key]}
    return merged


def get_generated_schema_path(config: Dict) -> Path:
    """Return the path for generated schema artifacts."""
    project_dir = config.get("_project_dir", ".")
    return Path(project_dir) / "artifacts" / "generated_schema"
