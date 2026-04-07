"""
Log File Alert Channel

Writes observer alerts to a log file using the standard logging framework.
"""

import json
import logging
from datetime import datetime, timezone

from dm.observer.alerts.base import BaseAlertChannel

logger = logging.getLogger(__name__)


class LogAlertChannel(BaseAlertChannel):
    """Alert channel that writes check results to a log file.

    If a dedicated log file path is provided, alerts are appended as
    JSON-lines entries.  Otherwise, alerts are emitted through the
    standard ``logging`` module.
    """

    def __init__(self, log_file: str = None):
        """Initialize the log alert channel.

        Args:
            log_file: Optional path to a dedicated alert log file.
                      If None, uses the standard logging framework.
        """
        self.log_file = log_file

    def send(self, check_name: str, details: dict, severity: str) -> None:
        """Write an alert to the log.

        Args:
            check_name: Identifier of the check that triggered the alert.
            details: Full result dict from the check function.
            severity: Alert severity level.
        """
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "check": check_name,
            "severity": severity,
            "details": details,
        }

        if self.log_file:
            try:
                with open(self.log_file, "a") as f:
                    f.write(json.dumps(entry, default=str) + "\n")
            except OSError as e:
                logger.error(f"Failed to write alert to {self.log_file}: {e}")
        else:
            # Use standard logging at an appropriate level
            log_level = _severity_to_log_level(severity)
            logger.log(
                log_level,
                f"[Observer Alert] {check_name} ({severity}): "
                f"{json.dumps(details, default=str)}",
            )


def _severity_to_log_level(severity: str) -> int:
    """Map alert severity to a Python logging level."""
    mapping = {
        "CRITICAL": logging.CRITICAL,
        "HIGH": logging.ERROR,
        "MEDIUM": logging.WARNING,
        "LOW": logging.INFO,
        "INFO": logging.INFO,
    }
    return mapping.get(severity.upper(), logging.INFO)
