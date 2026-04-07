"""
Pipeline Observer — Main Orchestrator

Runs all drift-detection checks against the modern database, compares
results with a stored baseline, and dispatches alerts through configured
channels.  Integrates with the DM plugin system for extensibility.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dm.observer.alerts.base import BaseAlertChannel
from dm.observer.alerts.log import LogAlertChannel
from dm.observer.baseline import BaselineManager
from dm.observer.checks.freshness import check_freshness
from dm.observer.checks.integrity import check_fk_integrity
from dm.observer.checks.schema_drift import check_schema_drift
from dm.observer.checks.volume import check_volume_anomaly

logger = logging.getLogger(__name__)


class PipelineObserver:
    """Runs observation checks and dispatches alerts on drift or anomalies.

    Integrates with the DM plugin system via the ``dm_observer_checks``
    and ``dm_on_drift_detected`` hooks.
    """

    def __init__(self, config: dict, plugin_manager: Any = None):
        """Initialize the observer.

        Args:
            config: Full project configuration dict (from project.yaml).
            plugin_manager: Optional pluggy PluginManager for hook invocation.
        """
        self.config = config
        self.plugin_manager = plugin_manager

        # Resolve observer-specific config
        observer_cfg = config.get("observer", {})
        baseline_path = observer_cfg.get(
            "baseline_path",
            _default_baseline_path(config),
        )
        self.baseline_manager = BaselineManager(baseline_path)

        self.tables: List[str] = observer_cfg.get("tables", [])
        if not self.tables:
            # Fall back to datasets from the main config
            datasets = config.get("datasets", [])
            self.tables = [
                ds if isinstance(ds, str) else ds.get("name", "")
                for ds in datasets
            ]

        self.volume_threshold: float = observer_cfg.get("volume_threshold", 0.3)
        self.freshness_hours: int = observer_cfg.get("freshness_hours", 24)

    # ── Public API ────────────────────────────────────────────────────

    def set_baseline(self, modern_conn: Any) -> None:
        """Capture and save a new baseline snapshot.

        Args:
            modern_conn: A BaseConnector instance connected to the modern DB.
        """
        logger.info("Capturing new baseline snapshot ...")
        baseline = self.baseline_manager.capture(modern_conn, self.tables)
        self.baseline_manager.save(baseline)
        logger.info("Baseline saved successfully")

    def run_once(self, modern_conn: Any) -> List[dict]:
        """Run all observation checks once and return results.

        For each check that indicates drift or anomaly, alerts are dispatched
        through configured alert channels and the ``dm_on_drift_detected``
        hook is invoked.

        Args:
            modern_conn: A BaseConnector instance connected to the modern DB.

        Returns:
            List of result dicts, one per check execution.
        """
        if not self.baseline_manager.exists():
            logger.warning(
                "No baseline found. Run set_baseline() first. "
                "Skipping observation checks."
            )
            return []

        baseline = self.baseline_manager.load()
        results: List[Dict] = []
        alert_channels = self.get_alert_channels()
        run_timestamp = datetime.now(timezone.utc).isoformat()

        for table in self.tables:
            # 1. Schema drift
            schema_result = check_schema_drift(modern_conn, table, baseline)
            entry = self._make_result_entry(
                "schema_drift", table, schema_result, run_timestamp,
            )
            results.append(entry)
            if schema_result.get("drifted"):
                self._dispatch_alert(
                    alert_channels, "schema_drift", entry, "HIGH"
                )

            # 2. Volume anomaly
            volume_result = check_volume_anomaly(
                modern_conn, table, baseline, threshold=self.volume_threshold,
            )
            entry = self._make_result_entry(
                "volume_anomaly", table, volume_result, run_timestamp,
            )
            results.append(entry)
            if volume_result.get("anomaly"):
                self._dispatch_alert(
                    alert_channels, "volume_anomaly", entry, "MEDIUM"
                )

            # 3. Freshness
            freshness_result = check_freshness(
                modern_conn, table,
                expected_interval_hours=self.freshness_hours,
            )
            entry = self._make_result_entry(
                "freshness", table, freshness_result, run_timestamp,
            )
            results.append(entry)
            if freshness_result.get("stale"):
                self._dispatch_alert(
                    alert_channels, "freshness", entry, "MEDIUM"
                )

            # 4. FK integrity
            integrity_result = check_fk_integrity(modern_conn, table, self.config)
            entry = self._make_result_entry(
                "fk_integrity", table, integrity_result, run_timestamp,
            )
            results.append(entry)
            if integrity_result.get("violations", 0) > 0:
                self._dispatch_alert(
                    alert_channels, "fk_integrity", entry, "HIGH"
                )

        # 5. Plugin-provided checks
        plugin_results = self._run_plugin_checks(modern_conn, baseline)
        for plugin_entry in plugin_results:
            results.append(plugin_entry)
            # Plugin checks indicate drift via a 'drifted' or 'anomaly' key
            if plugin_entry.get("details", {}).get("drifted") or \
               plugin_entry.get("details", {}).get("anomaly"):
                self._dispatch_alert(
                    alert_channels,
                    plugin_entry.get("check", "plugin_check"),
                    plugin_entry,
                    "MEDIUM",
                )

        logger.info(
            f"Observer run complete: {len(results)} check(s) across "
            f"{len(self.tables)} table(s)"
        )
        return results

    def get_alert_channels(self) -> List[BaseAlertChannel]:
        """Build alert channels from the observer config.

        Supported channel types:
            - ``log``: Writes to a log file or standard logging.
            - ``slack``: Posts to a Slack incoming webhook.

        Returns:
            List of configured BaseAlertChannel instances.
        """
        channels: List[BaseAlertChannel] = []
        observer_cfg = self.config.get("observer", {})
        alert_configs = observer_cfg.get("alerts", [])

        for alert_cfg in alert_configs:
            channel_type = alert_cfg.get("type", "log")

            if channel_type == "log":
                channels.append(
                    LogAlertChannel(log_file=alert_cfg.get("file"))
                )
            elif channel_type == "slack":
                from dm.observer.alerts.slack import SlackAlertChannel

                channels.append(
                    SlackAlertChannel(webhook_url=alert_cfg.get("webhook_url"))
                )
            else:
                logger.warning(f"Unknown alert channel type: '{channel_type}'")

        # Default to logging if no channels are configured
        if not channels:
            channels.append(LogAlertChannel())

        return channels

    # ── Internal helpers ──────────────────────────────────────────────

    @staticmethod
    def _make_result_entry(
        check: str, table: str, details: dict, timestamp: str,
    ) -> dict:
        """Wrap a check result into a standardized entry."""
        return {
            "check": check,
            "table": table,
            "timestamp": timestamp,
            "details": details,
        }

    def _dispatch_alert(
        self,
        channels: List[BaseAlertChannel],
        check_name: str,
        entry: dict,
        severity: str,
    ) -> None:
        """Send an alert through all channels and invoke the drift hook."""
        for channel in channels:
            try:
                channel.send(check_name, entry, severity)
            except Exception as e:
                logger.error(
                    f"Alert channel {type(channel).__name__} failed: {e}"
                )

        # Invoke the plugin hook for drift detection
        if self.plugin_manager is not None:
            try:
                self.plugin_manager.hook.dm_on_drift_detected(
                    check_name=check_name, details=entry,
                )
            except Exception as e:
                logger.error(f"dm_on_drift_detected hook failed: {e}")

    def _run_plugin_checks(
        self, modern_conn: Any, baseline: dict,
    ) -> List[dict]:
        """Collect and execute additional checks registered by plugins.

        Plugins implement ``dm_observer_checks`` returning a list of
        dicts with ``name`` and ``check_fn(modern_conn, baseline)`` keys.

        Returns:
            List of result entry dicts from plugin checks.
        """
        if self.plugin_manager is None:
            return []

        plugin_entries: List[dict] = []
        timestamp = datetime.now(timezone.utc).isoformat()

        try:
            hook_results = self.plugin_manager.hook.dm_observer_checks()
        except Exception as e:
            logger.error(f"Failed to collect plugin observer checks: {e}")
            return []

        # hook_results is a list-of-lists (one list per plugin)
        for check_list in hook_results:
            if not isinstance(check_list, list):
                continue
            for check_def in check_list:
                name = check_def.get("name", "unknown_plugin_check")
                check_fn = check_def.get("check_fn")
                if not callable(check_fn):
                    logger.warning(
                        f"Plugin check '{name}' has no callable check_fn"
                    )
                    continue
                try:
                    result = check_fn(modern_conn, baseline)
                    plugin_entries.append(
                        self._make_result_entry(name, "*", result, timestamp)
                    )
                except Exception as e:
                    logger.error(f"Plugin check '{name}' failed: {e}")
                    plugin_entries.append(
                        self._make_result_entry(
                            name, "*", {"error": str(e)}, timestamp,
                        )
                    )

        return plugin_entries


def _default_baseline_path(config: dict) -> str:
    """Derive a default baseline file path from the project directory."""
    project_dir = config.get("_project_dir", ".")
    return f"{project_dir}/artifacts/observer_baseline.json"
