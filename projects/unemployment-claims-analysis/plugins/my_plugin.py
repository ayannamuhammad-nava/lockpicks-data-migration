"""
Domain-specific plugin for unemployment-claims-analysis migration.

Implements DM hooks to provide custom validation rules,
column mapping overrides, and business logic checks.
"""

from dm.hookspecs import hookimpl


class UnemploymentClaimsAnalysisPlugin:
    """Custom validation rules for unemployment-claims-analysis migration."""

    @hookimpl
    def dm_get_column_overrides(self, table):
        """Return curated column mapping overrides.

        Example:
            if table == "my_table":
                return {
                    "old_column": {
                        "target": "new_column",
                        "type": "rename",
                        "rationale": "Renamed for clarity",
                        "confidence": 1.0,
                    },
                }
        """
        return {}

    @hookimpl
    def dm_data_quality_rules(self, dataset):
        """Return cross-field data quality rules.

        Example:
            if dataset == "my_table":
                return [{
                    "name": "my_rule",
                    "severity": "HIGH",
                    "description": "Description of the check",
                    "check_fn": self._check_my_rule,
                }]
        """
        return []

    # def _check_my_rule(self, df):
    #     """Example cross-field check. Return anomaly dict or None."""
    #     bad_rows = df[df["status"] == "INVALID"]
    #     if not bad_rows.empty:
    #         return {
    #             "count": len(bad_rows),
    #             "record_ids": bad_rows.iloc[:5].index.tolist(),
    #             "risk": "Description of the risk",
    #             "action": "What to do about it",
    #         }
    #     return None
