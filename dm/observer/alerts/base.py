"""
Abstract Base Alert Channel

Defines the interface for all alert channels used by the PipelineObserver.
"""

from abc import ABC, abstractmethod


class BaseAlertChannel(ABC):
    """Abstract base class for observer alert channels.

    Implementations deliver check results to external systems
    (log files, Slack, email, PagerDuty, etc.).
    """

    @abstractmethod
    def send(self, check_name: str, details: dict, severity: str) -> None:
        """Send an alert for a check result.

        Args:
            check_name: Identifier of the check that triggered the alert
                        (e.g. 'schema_drift', 'volume_anomaly').
            details: Full result dict from the check function.
            severity: Alert severity level (INFO, LOW, MEDIUM, HIGH, CRITICAL).
        """
