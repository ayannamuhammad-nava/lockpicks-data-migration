"""
OpenMetadata Enrichment Layer

Wraps the OpenMetadata REST API to provide schema discovery, column-level
profiling stats, glossary terms, PII classification tags, and lineage data
for the DM discovery pipeline.

Uses the OM REST API directly (via requests) to avoid the heavy
openmetadata-ingestion SDK dependency.
"""

import logging
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)


class OpenMetadataEnricher:
    """Connects to OpenMetadata and provides enrichment data for DM discovery."""

    def __init__(self, om_config: dict):
        """
        Args:
            om_config: Dict with keys:
                host: OM server URL (e.g., http://localhost:8585)
                auth_token: JWT bearer token (optional for local dev)
                legacy_service: OM service name for legacy data source
                legacy_database: Database name within the service
                legacy_schema: Schema name (default: "public")
        """
        self._host = om_config["host"].rstrip("/")
        self._token = om_config.get("auth_token", "")
        self._service = om_config["legacy_service"]
        self._database = om_config["legacy_database"]
        self._schema = om_config.get("legacy_schema", "public")
        self._session: Optional[requests.Session] = None

    # ── Connection lifecycle ──────────────────────────────────────

    def connect(self) -> None:
        """Establish an HTTP session to the OpenMetadata server."""
        self._session = requests.Session()
        if self._token:
            self._session.headers["Authorization"] = f"Bearer {self._token}"
        self._session.headers["Content-Type"] = "application/json"

        # Verify connectivity
        resp = self._session.get(f"{self._host}/api/v1/system/version")
        resp.raise_for_status()
        version = resp.json().get("version", "unknown")
        logger.info(f"Connected to OpenMetadata {version} at {self._host}")

    def close(self) -> None:
        """Close the HTTP session."""
        if self._session:
            self._session.close()
            self._session = None

    # ── Helpers ───────────────────────────────────────────────────

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """Make a GET request to the OM API."""
        if not self._session:
            raise RuntimeError("Not connected. Call connect() first.")
        url = f"{self._host}/api/v1/{path}"
        resp = self._session.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    def _table_fqn(self, table: str) -> str:
        """Build the fully qualified name for a table in OM."""
        return f"{self._service}.{self._database}.{self._schema}.{table}"

    def _get_table_entity(self, table: str, fields: str = "") -> dict:
        """Fetch a table entity by FQN."""
        fqn = self._table_fqn(table)
        params = {}
        if fields:
            params["fields"] = fields
        return self._get(f"tables/name/{quote(fqn, safe='')}", params=params)

    # ── Schema Discovery ─────────────────────────────────────────

    def get_tables(self) -> List[str]:
        """List all table names in the legacy service/database/schema."""
        fqn_prefix = f"{self._service}.{self._database}.{self._schema}"
        data = self._get("tables", params={
            "database": f"{self._service}.{self._database}",
            "limit": 1000,
            "fields": "columns",
        })
        tables = []
        for table in data.get("data", []):
            fqn = table.get("fullyQualifiedName", "")
            if fqn.startswith(fqn_prefix):
                tables.append(table["name"])
        return tables

    def get_table_schema(self, table: str) -> List[Dict]:
        """Fetch column metadata from OM catalog for a legacy table.

        Returns a list of dicts compatible with BaseConnector.get_table_schema(),
        enriched with OM descriptions and tags:
            [{column_name, data_type, is_nullable, description, tags, glossary_terms}]
        """
        entity = self._get_table_entity(table, fields="columns,tags")
        columns = []
        for col in entity.get("columns", []):
            col_tags = [
                t.get("tagFQN", "")
                for t in col.get("tags", [])
            ]
            glossary_terms = [
                t.get("tagFQN", "")
                for t in col.get("tags", [])
                if t.get("source") == "Glossary"
            ]
            columns.append({
                "column_name": col["name"],
                "data_type": col.get("dataType", "VARCHAR"),
                "data_type_display": col.get("dataTypeDisplay", col.get("dataType", "")),
                "is_nullable": "YES" if col.get("constraint") != "NOT_NULL" else "NO",
                "description": col.get("description", ""),
                "tags": col_tags,
                "glossary_terms": glossary_terms,
                "ordinal_position": col.get("ordinalPosition", 0),
            })
        columns.sort(key=lambda c: c["ordinal_position"])
        return columns

    # ── Table Metadata ────────────────────────────────────────────

    def get_table_metadata(self, table: str) -> Dict:
        """Fetch table-level metadata: description, owner, tier, tags."""
        entity = self._get_table_entity(table, fields="owners,tags")
        tags = [t.get("tagFQN", "") for t in entity.get("tags", [])]
        tier = None
        for tag in tags:
            if tag.startswith("Tier."):
                tier = tag
                break
        owners = entity.get("owners", entity.get("owner", {}))
        if isinstance(owners, list):
            owner = owners[0] if owners else {}
        else:
            owner = owners
        return {
            "name": entity.get("name", table),
            "description": entity.get("description", ""),
            "owner": owner.get("name", owner.get("displayName", "")),
            "tier": tier,
            "tags": tags,
            "table_type": entity.get("tableType", "Regular"),
        }

    # ── Profiling ─────────────────────────────────────────────────

    def get_table_profile(self, table: str) -> Dict:
        """Fetch latest profiler run results for a table.

        Returns:
            {row_count, column_count, profiled_at,
             columns: {col_name: {null_percent, unique_percent, distinct_count,
                        min_value, max_value, mean_value, stddev,
                        histogram, value_frequencies}}}
        """
        fqn = self._table_fqn(table)
        try:
            data = self._get(
                f"tables/name/{quote(fqn, safe='')}/tableProfile/latest"
            )
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code in (404, 500):
                logger.warning(f"No profiler data for table {table}")
                return {"columns": {}, "row_count": 0, "profiled_at": None}
            raise

        result = {
            "row_count": data.get("rowCount", 0),
            "column_count": data.get("columnCount", 0),
            "profiled_at": data.get("timestamp"),
            "columns": {},
        }

        # Fetch column-level profiles
        col_profiles = data.get("columnProfile", [])
        if not col_profiles:
            col_profiles = self._fetch_column_profiles(table)

        for cp in col_profiles:
            col_name = cp.get("name", "")
            if not col_name:
                continue
            result["columns"][col_name] = self._parse_column_profile(cp)

        return result

    def _fetch_column_profiles(self, table: str) -> List[Dict]:
        """Fetch column profiles via the columns endpoint."""
        fqn = self._table_fqn(table)
        try:
            data = self._get(
                f"tables/name/{quote(fqn, safe='')}/columnProfile",
                params={"limit": 500},
            )
            return data.get("data", data) if isinstance(data, dict) else data
        except requests.HTTPError:
            return []

    def _parse_column_profile(self, cp: dict) -> Dict:
        """Parse a column profile response into a standardized dict."""
        return {
            "null_percent": cp.get("nullProportion", cp.get("nullCount", 0)),
            "unique_percent": cp.get("uniqueProportion", 0),
            "distinct_count": cp.get("distinctCount", 0),
            "min_value": cp.get("min", None),
            "max_value": cp.get("max", None),
            "mean_value": cp.get("mean", None),
            "stddev": cp.get("stddev", None),
            "min_length": cp.get("minLength", None),
            "max_length": cp.get("maxLength", None),
            "histogram": cp.get("histogram", None),
            "value_frequencies": cp.get("distinctValueFrequencies", None),
        }

    def get_column_profile(self, table: str, column: str) -> Dict:
        """Fetch profiling stats for a single column."""
        profile = self.get_table_profile(table)
        return profile.get("columns", {}).get(column, {})

    # ── Tags / PII Classification ─────────────────────────────────

    def get_column_tags(self, table: str) -> Dict[str, List[str]]:
        """Fetch PII/classification tags per column.

        Returns:
            Dict of {column_name: [tag_fqn, ...]}.
        """
        entity = self._get_table_entity(table, fields="columns,tags")
        result = {}
        for col in entity.get("columns", []):
            tags = [t.get("tagFQN", "") for t in col.get("tags", [])]
            if tags:
                result[col["name"]] = tags
        return result

    # ── Glossary ──────────────────────────────────────────────────

    def get_glossary_term(self, term: str) -> Optional[Dict]:
        """Look up a business glossary term by name.

        Returns:
            Dict with {name, description, related_terms, reviewers} or None.
        """
        try:
            data = self._get("glossaryTerms", params={
                "limit": 5,
                "fields": "relatedTerms,reviewers,tags",
            })
            for item in data.get("data", []):
                if item.get("name", "").lower() == term.lower():
                    return {
                        "name": item["name"],
                        "description": item.get("description", ""),
                        "related_terms": [
                            rt.get("name", "")
                            for rt in item.get("relatedTerms", [])
                        ],
                        "reviewers": [
                            r.get("name", "")
                            for r in item.get("reviewers", [])
                        ],
                    }
        except requests.HTTPError:
            logger.debug(f"Glossary term lookup failed for: {term}")
        return None

    def get_glossary_for_table(self, table: str) -> Dict[str, Dict]:
        """Fetch all glossary terms linked to columns of a table.

        Returns:
            Dict of {column_name: {term_name, description}}.
        """
        entity = self._get_table_entity(table, fields="columns,tags")
        result = {}
        for col in entity.get("columns", []):
            for tag in col.get("tags", []):
                if tag.get("source") == "Glossary":
                    fqn = tag.get("tagFQN", "")
                    term_name = fqn.split(".")[-1] if fqn else ""
                    result[col["name"]] = {
                        "term_fqn": fqn,
                        "term_name": term_name,
                        "description": tag.get("description", ""),
                    }
                    break  # Take the first glossary term per column
        return result

    # ── Lineage ───────────────────────────────────────────────────

    def get_lineage(self, table: str) -> Dict:
        """Fetch column-level lineage for a table.

        Returns:
            {columns: {col_name: {upstream: [{table, column}],
                                   downstream: [{table, column}]}}}
        """
        fqn = self._table_fqn(table)
        try:
            data = self._get(f"lineage/table/name/{quote(fqn, safe='')}", params={
                "upstreamDepth": 1,
                "downstreamDepth": 1,
            })
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                logger.debug(f"No lineage data for table {table}")
                return {"columns": {}}
            raise

        columns: Dict[str, Dict[str, list]] = {}

        # Parse edges for column-level lineage
        for edge in data.get("downstreamEdges", []):
            for col_lineage in edge.get("columnLineage", []):
                for from_col in col_lineage.get("fromColumns", []):
                    col_name = from_col.split(".")[-1] if "." in from_col else from_col
                    if col_name not in columns:
                        columns[col_name] = {"upstream": [], "downstream": []}
                    to_col = col_lineage.get("toColumn", "")
                    if to_col:
                        to_table = edge.get("toEntity", {}).get("name", "")
                        columns[col_name]["downstream"].append({
                            "table": to_table,
                            "column": to_col.split(".")[-1] if "." in to_col else to_col,
                        })

        for edge in data.get("upstreamEdges", []):
            for col_lineage in edge.get("columnLineage", []):
                to_col = col_lineage.get("toColumn", "")
                col_name = to_col.split(".")[-1] if "." in to_col else to_col
                if col_name and col_name not in columns:
                    columns[col_name] = {"upstream": [], "downstream": []}
                for from_col in col_lineage.get("fromColumns", []):
                    from_table = edge.get("fromEntity", {}).get("name", "")
                    if col_name:
                        columns[col_name]["upstream"].append({
                            "table": from_table,
                            "column": from_col.split(".")[-1] if "." in from_col else from_col,
                        })

        return {"columns": columns}

    # ── Combined Enrichment ───────────────────────────────────────

    def enrich_glossary_entry(self, entry: dict) -> dict:
        """Enrich a DM glossary entry with OM data.

        Replaces low-confidence inferences with OM descriptions, PII tags,
        and glossary terms. Upgrades confidence to 1.0 for OM-confirmed entries.

        Args:
            entry: Dict with keys: name, description, system, pii, confidence, table.

        Returns:
            The enriched entry dict.
        """
        table = entry.get("table", "")
        col_name = entry.get("name", "")
        if not table or not col_name:
            return entry

        # Try to get OM column metadata
        try:
            schema = self.get_table_schema(table)
        except (requests.HTTPError, RuntimeError):
            return entry

        om_col = None
        for col in schema:
            if col["column_name"] == col_name:
                om_col = col
                break

        if not om_col:
            return entry

        # Upgrade description if OM has one
        om_desc = om_col.get("description", "")
        if om_desc and (entry["confidence"] < 0.9 or not entry["description"]):
            entry["description"] = om_desc
            entry["confidence"] = 1.0

        # Upgrade PII detection from OM tags
        om_tags = om_col.get("tags", [])
        pii_tags = [t for t in om_tags if "PII" in t or "Sensitive" in t]
        if pii_tags:
            entry["pii"] = True
            entry["pii_tags"] = pii_tags

        # Add glossary term reference
        glossary_terms = om_col.get("glossary_terms", [])
        if glossary_terms:
            entry["glossary_terms"] = glossary_terms
            if entry["confidence"] < 1.0:
                entry["confidence"] = 1.0

        return entry
