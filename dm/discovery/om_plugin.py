"""
OpenMetadata Plugin — Pluggy adapter for DM hook system.

Wraps the OpenMetadataEnricher and implements DM hooks so that OM
enrichment data flows into the standard discovery pipeline.
"""

import logging

from dm.discovery.openmetadata_enricher import OpenMetadataEnricher
from dm.hookspecs import hookimpl

logger = logging.getLogger(__name__)


class OpenMetadataPlugin:
    """Pluggy plugin that delegates to OpenMetadataEnricher."""

    def __init__(self, enricher: OpenMetadataEnricher):
        self._enricher = enricher

    @hookimpl
    def dm_enrich_glossary_entry(self, entry: dict) -> dict:
        """Replace low-confidence inferences with OM catalog data."""
        return self._enricher.enrich_glossary_entry(entry)

    @hookimpl
    def dm_get_profiling_stats(self, table: str, column: str) -> dict:
        """Return column profiling stats from OM profiler."""
        return self._enricher.get_column_profile(table, column)

    @hookimpl
    def dm_get_lineage(self, table: str) -> dict:
        """Return column-level lineage from OM."""
        return self._enricher.get_lineage(table)
