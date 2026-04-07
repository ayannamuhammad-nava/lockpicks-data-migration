"""
Slack Alert Channel

Posts observer alerts to a Slack webhook URL using the ``requests`` library.
"""

import json
import logging
from datetime import datetime, timezone

from dm.observer.alerts.base import BaseAlertChannel

try:
    import requests

    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

logger = logging.getLogger(__name__)


class SlackAlertChannel(BaseAlertChannel):
    """Alert channel that posts check results to a Slack incoming webhook.

    Requires the ``requests`` library and a valid webhook URL.  If either
    is missing, alerts fall back to standard logging with a warning.
    """

    def __init__(self, webhook_url: str = None):
        """Initialize the Slack alert channel.

        Args:
            webhook_url: Slack incoming webhook URL.  If None or empty,
                         alerts will be logged as warnings instead.
        """
        self.webhook_url = webhook_url

    def send(self, check_name: str, details: dict, severity: str) -> None:
        """Post an alert to Slack.

        Args:
            check_name: Identifier of the check that triggered the alert.
            details: Full result dict from the check function.
            severity: Alert severity level.
        """
        if not self.webhook_url:
            logger.warning(
                f"Slack webhook URL not configured; skipping alert for "
                f"'{check_name}' ({severity})"
            )
            return

        if not _HAS_REQUESTS:
            logger.warning(
                "The 'requests' package is not installed; cannot send Slack alert. "
                "Install it with: uv sync (requests is a core dependency)"
            )
            return

        emoji = _severity_emoji(severity)
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        details_text = json.dumps(details, indent=2, default=str)

        # Truncate details if too long for Slack
        if len(details_text) > 2500:
            details_text = details_text[:2500] + "\n... (truncated)"

        payload = {
            "text": (
                f"{emoji} *DM Observer Alert* — `{check_name}`\n"
                f"*Severity:* {severity}\n"
                f"*Time:* {timestamp}\n"
                f"```\n{details_text}\n```"
            ),
        }

        try:
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10,
            )
            if response.status_code != 200:
                logger.error(
                    f"Slack webhook returned {response.status_code}: "
                    f"{response.text}"
                )
            else:
                logger.debug(f"Slack alert sent for '{check_name}'")
        except Exception as e:
            logger.error(f"Failed to send Slack alert for '{check_name}': {e}")


def _severity_emoji(severity: str) -> str:
    """Return a Slack-friendly emoji for the severity level."""
    mapping = {
        "CRITICAL": ":rotating_light:",
        "HIGH": ":red_circle:",
        "MEDIUM": ":large_orange_circle:",
        "LOW": ":large_yellow_circle:",
        "INFO": ":information_source:",
    }
    return mapping.get(severity.upper(), ":grey_question:")
