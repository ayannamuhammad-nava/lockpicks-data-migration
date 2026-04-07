"""
DM Plugin Manager

Discovers, loads, and manages plugins using pluggy.
Plugins can be registered from:
  1. project.yaml `plugins:` list (module path + class name)
  2. Entry points (for pip-installed plugins)
"""

import importlib
import logging
from typing import Any, Dict, List, Optional

import pluggy

from dm.hookspecs import DMHookSpec, PROJECT_NAME

logger = logging.getLogger(__name__)


def create_plugin_manager() -> pluggy.PluginManager:
    """Create and return a configured PluginManager with hookspecs registered."""
    pm = pluggy.PluginManager(PROJECT_NAME)
    pm.add_hookspecs(DMHookSpec)
    return pm


def load_plugins_from_config(
    pm: pluggy.PluginManager,
    plugin_specs: List[str],
    project_dir: Optional[str] = None,
) -> None:
    """Load plugin classes listed in project.yaml `plugins:` section.

    Each spec is a dotted path like 'plugins.loops_plugin.LoopsPlugin'.
    The project_dir is prepended to sys.path so relative imports work.

    Args:
        pm: The PluginManager to register plugins with.
        plugin_specs: List of dotted module.ClassName strings.
        project_dir: Optional project directory to add to sys.path.
    """
    import sys

    if project_dir and project_dir not in sys.path:
        sys.path.insert(0, project_dir)

    for spec in plugin_specs:
        try:
            module_path, class_name = spec.rsplit(".", 1)
            module = importlib.import_module(module_path)
            plugin_class = getattr(module, class_name)
            plugin_instance = plugin_class()
            pm.register(plugin_instance, name=spec)
            logger.info(f"Loaded plugin: {spec}")
        except Exception as e:
            logger.error(f"Failed to load plugin '{spec}': {e}")
            raise


def load_entry_point_plugins(pm: pluggy.PluginManager) -> None:
    """Discover and load plugins registered via setuptools entry_points.

    Entry point group: 'dm.plugins'
    """
    pm.load_setuptools_entrypoints(PROJECT_NAME)


def get_plugin_manager(
    plugin_specs: Optional[List[str]] = None,
    project_dir: Optional[str] = None,
) -> pluggy.PluginManager:
    """Convenience: create PM, load config plugins and entry points.

    Args:
        plugin_specs: Plugin class paths from project.yaml.
        project_dir: Project directory for relative plugin imports.

    Returns:
        Fully configured PluginManager.
    """
    pm = create_plugin_manager()

    if plugin_specs:
        load_plugins_from_config(pm, plugin_specs, project_dir)

    load_entry_point_plugins(pm)

    return pm
