"""
End-to-end test for multi-target schema generation, target-aware scoring,
multi-source config, cross-source referential integrity, and source connectors.
"""
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ── 1. Target Adapters ──────────────────────────────────────────────────

from dm.targets.postgres import (
    PostgresTargetAdapter,
    get_available_targets,
    get_target_adapter,
    BUILTIN_TARGETS,
)
from dm.targets.snowflake import SnowflakeTargetAdapter
from dm.targets.oracle import OracleTargetAdapter
from dm.targets.redshift import RedshiftTargetAdapter


class TestTargetAdapters:
    """Test that all four target adapters produce correct dialect-specific output."""

    @pytest.fixture(params=["postgres", "snowflake", "oracle", "redshift"])
    def adapter(self, request):
        return get_target_adapter(request.param)

    def test_dialect_name(self, adapter):
        assert adapter.dialect_name() in ("postgres", "snowflake", "oracle", "redshift")

    def test_map_varchar(self, adapter):
        result = adapter.map_type("varchar(100)")
        assert "100" in result
        if adapter.dialect_name() == "oracle":
            assert "VARCHAR2" in result
        else:
            assert "VARCHAR" in result

    def test_map_timestamp(self, adapter):
        result = adapter.map_type("timestamp")
        assert "TIMESTAMP" in result.upper()

    def test_map_json(self, adapter):
        result = adapter.map_type("jsonb")
        dialect = adapter.dialect_name()
        if dialect == "postgres":
            assert result == "JSONB"
        elif dialect == "snowflake":
            assert result == "VARIANT"
        elif dialect == "oracle":
            assert result == "CLOB"
        elif dialect == "redshift":
            assert result == "SUPER"

    def test_render_create_table(self, adapter):
        columns = [
            {"name": "id", "data_type": "INTEGER", "nullable": False, "constraints": ["PRIMARY KEY"], "comment": ""},
            {"name": "name", "data_type": "VARCHAR(100)", "nullable": True, "constraints": [], "comment": ""},
        ]
        ddl = adapter.render_create_table("test_table", columns, "id")
        assert "CREATE" in ddl
        assert "test_table" in ddl
        assert "id" in ddl
        assert "name" in ddl

    def test_translate_function_nvl(self, adapter):
        result = adapter.translate_function("nvl", ["col1", "'default'"])
        # All adapters should handle NVL
        assert "col1" in result

    def test_supports_serial(self, adapter):
        assert isinstance(adapter.supports_serial(), bool)

    def test_supports_check_constraints(self, adapter):
        result = adapter.supports_check_constraints()
        dialect = adapter.dialect_name()
        if dialect in ("snowflake", "redshift"):
            assert result is False
        else:
            assert result is True


class TestTargetRegistry:
    def test_get_available_targets(self):
        targets = get_available_targets()
        assert "postgres" in targets
        assert "snowflake" in targets
        assert "oracle" in targets
        assert "redshift" in targets
        assert targets["postgres"] == "PostgreSQL"
        assert targets["redshift"] == "AWS (Redshift)"

    def test_aws_alias(self):
        adapter = get_target_adapter("aws")
        assert adapter.dialect_name() == "redshift"

    def test_unknown_target(self):
        with pytest.raises(ValueError, match="Unknown target"):
            get_target_adapter("mongodb")


# ── 2. Target-Aware Scoring ──────────────────────────────────────────────

from dm.scoring import (
    calculate_confidence,
    calculate_confidence_all_targets,
    get_target_penalties,
    TARGET_PENALTIES,
)


class TestTargetScoring:
    CONFIG = {
        "scoring": {
            "weights": {"structure": 0.4, "integrity": 0.4, "governance": 0.2},
            "thresholds": {"green": 90, "yellow": 70},
        }
    }

    def test_postgres_no_penalty(self):
        result = calculate_confidence(95, 92, 88, self.CONFIG, target="postgres")
        assert result["score"] == 92.4
        assert result["status"] == "GREEN"
        assert result["target"] == "postgres"

    def test_snowflake_penalized(self):
        result = calculate_confidence(95, 92, 88, self.CONFIG, target="snowflake")
        assert result["score"] < 92.4  # Lower than postgres
        assert result["target"] == "snowflake"
        assert "target_notes" in result
        assert any("FK" in n for n in result["target_notes"])

    def test_redshift_most_penalized(self):
        pg = calculate_confidence(95, 92, 88, self.CONFIG, target="postgres")
        rs = calculate_confidence(95, 92, 88, self.CONFIG, target="redshift")
        assert rs["score"] < pg["score"]

    def test_oracle_minor_penalty(self):
        pg = calculate_confidence(95, 92, 88, self.CONFIG, target="postgres")
        ora = calculate_confidence(95, 92, 88, self.CONFIG, target="oracle")
        assert ora["score"] < pg["score"]
        assert ora["score"] > 90  # Still GREEN

    def test_all_targets(self):
        results = calculate_confidence_all_targets(95, 92, 88, self.CONFIG)
        assert len(results) == 4
        assert all(t in results for t in ["postgres", "snowflake", "oracle", "redshift"])
        # Postgres should have the highest score
        scores = {t: r["score"] for t, r in results.items()}
        assert scores["postgres"] >= max(scores["snowflake"], scores["oracle"], scores["redshift"])

    def test_backward_compat_no_target(self):
        result = calculate_confidence(95, 92, 88, self.CONFIG)
        assert result["target"] == "postgres"  # Default

    def test_target_penalties_defined(self):
        for target in ["postgres", "snowflake", "oracle", "redshift"]:
            p = get_target_penalties(target)
            assert "structure" in p
            assert "integrity" in p
            assert "governance" in p
            assert "notes" in p


# ── 3. Multi-Source Config ──────────────────────────────────────────────

from dm.config import (
    get_connection_config,
    get_dataset_source,
    get_dataset_target,
    get_all_sources,
    get_dataset_config,
)


class TestMultiSourceConfig:
    SINGLE_CONFIG = {
        "connections": {
            "legacy": {"type": "postgres", "host": "localhost"},
            "modern": {"type": "postgres", "host": "localhost"},
        },
        "datasets": [
            {"name": "claimants", "legacy_table": "claimants"},
            {"name": "claims", "legacy_table": "claims"},
        ],
    }

    MULTI_CONFIG = {
        "connections": {
            "eligibility_db": {"type": "db2", "host": "mainframe"},
            "claims_db": {"type": "db2", "host": "mainframe"},
            "fed_feed": {"type": "flatfile", "path": "/data/qc.csv"},
            "modern": {"type": "postgres", "host": "localhost"},
        },
        "datasets": [
            {"name": "claimants", "source": "eligibility_db", "target": "modern"},
            {"name": "claims", "source": "claims_db", "target": "modern"},
            {"name": "providers", "source": "eligibility_db"},
            {"name": "federal_sample", "source": "fed_feed", "target": "modern"},
        ],
    }

    def test_backward_compat_defaults_to_legacy(self):
        assert get_dataset_source(self.SINGLE_CONFIG, "claimants") == "legacy"
        assert get_dataset_target(self.SINGLE_CONFIG, "claimants") == "modern"

    def test_multi_source_resolves(self):
        assert get_dataset_source(self.MULTI_CONFIG, "claimants") == "eligibility_db"
        assert get_dataset_source(self.MULTI_CONFIG, "claims") == "claims_db"
        assert get_dataset_source(self.MULTI_CONFIG, "federal_sample") == "fed_feed"

    def test_multi_target_defaults_to_modern(self):
        # providers has no target specified — should default to modern
        assert get_dataset_target(self.MULTI_CONFIG, "providers") == "modern"

    def test_get_all_sources(self):
        sources = get_all_sources(self.MULTI_CONFIG)
        assert "eligibility_db" in sources
        assert "claims_db" in sources
        assert "fed_feed" in sources
        assert len(sources) == 3

    def test_single_source_returns_legacy(self):
        sources = get_all_sources(self.SINGLE_CONFIG)
        assert sources == ["legacy"]

    def test_connection_config_error_lists_available(self):
        with pytest.raises(KeyError, match="Available:.*eligibility_db"):
            get_connection_config(self.MULTI_CONFIG, "nonexistent")

    def test_unknown_dataset_defaults(self):
        assert get_dataset_source(self.MULTI_CONFIG, "unknown_table") == "legacy"
        assert get_dataset_target(self.MULTI_CONFIG, "unknown_table") == "modern"


# ── 4. Schema Generator with Target Adapter ─────────────────────────────

from dm.discovery.schema_gen import SchemaGenerator, SchemaGenResult
from dm.discovery.normalization_analyzer import NormalizationPlan, ProposedEntity


class TestSchemaGenMultiTarget:
    @pytest.fixture
    def plan(self):
        return NormalizationPlan(
            source_table="claimants",
            entities=[ProposedEntity(
                name="claimants",
                columns=[
                    {"source_col": "cl_recid", "data_type": "integer", "nullable": False},
                    {"source_col": "cl_fnam", "data_type": "varchar(50)", "nullable": True},
                    {"source_col": "cl_dob", "data_type": "timestamp", "nullable": True},
                ],
                primary_key="cl_recid",
                source_table="claimants",
                role="primary",
                rationale="Test entity",
            )],
            relationships=[], lookup_tables=[],
            confidence=0.95, rationale="Test",
        )

    @pytest.fixture
    def legacy_schema(self):
        return [
            {"column_name": "cl_recid", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "cl_fnam", "data_type": "varchar(50)", "is_nullable": "YES"},
            {"column_name": "cl_dob", "data_type": "timestamp", "is_nullable": "YES"},
        ]

    def test_postgres_ddl(self, plan, legacy_schema):
        adapter = get_target_adapter("postgres")
        gen = SchemaGenerator(config={"schema_generation": {}}, target_adapter=adapter)
        result = gen.generate(plan, legacy_schema, {"columns": []}, {"mappings": []})
        assert "CREATE TABLE" in result.full_ddl
        assert "TIMESTAMPTZ" in result.full_ddl or "DATE" in result.full_ddl

    def test_snowflake_ddl(self, plan, legacy_schema):
        adapter = get_target_adapter("snowflake")
        gen = SchemaGenerator(config={"schema_generation": {}}, target_adapter=adapter)
        result = gen.generate(plan, legacy_schema, {"columns": []}, {"mappings": []})
        assert "CREATE OR REPLACE TABLE" in result.full_ddl

    def test_oracle_ddl(self, plan, legacy_schema):
        adapter = get_target_adapter("oracle")
        gen = SchemaGenerator(config={"schema_generation": {}}, target_adapter=adapter)
        result = gen.generate(plan, legacy_schema, {"columns": []}, {"mappings": []})
        assert "CREATE TABLE" in result.full_ddl
        # Oracle should use VARCHAR2
        assert "VARCHAR2" in result.full_ddl

    def test_redshift_ddl(self, plan, legacy_schema):
        adapter = get_target_adapter("redshift")
        gen = SchemaGenerator(config={"schema_generation": {}}, target_adapter=adapter)
        result = gen.generate(plan, legacy_schema, {"columns": []}, {"mappings": []})
        assert "CREATE TABLE" in result.full_ddl
        assert "DISTSTYLE KEY" in result.full_ddl or "DISTKEY" in result.full_ddl

    def test_no_adapter_fallback(self, plan, legacy_schema):
        """Without an adapter, should fall back to built-in PostgreSQL rendering."""
        gen = SchemaGenerator(config={"schema_generation": {}})
        result = gen.generate(plan, legacy_schema, {"columns": []}, {"mappings": []})
        assert "CREATE TABLE" in result.full_ddl

    def test_save_all_targets(self, plan, legacy_schema):
        gen = SchemaGenerator(config={"schema_generation": {}})
        result = gen.generate(plan, legacy_schema, {"columns": []}, {"mappings": []})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_base = Path(tmpdir)
            results = gen.save_all_targets(
                tables=result.tables,
                plan=plan,
                legacy_schema=legacy_schema,
                glossary={"columns": []},
                mappings={"mappings": []},
                om_profiles=None,
                output_base=output_base,
            )

            # Should have 4 subdirectories
            for target in ["postgres", "snowflake", "oracle", "redshift"]:
                target_dir = output_base / target
                assert target_dir.exists(), f"Missing {target} directory"
                assert (target_dir / "full_schema.sql").exists(), f"Missing {target}/full_schema.sql"

            # Verify Oracle DDL has VARCHAR2
            oracle_ddl = (output_base / "oracle" / "full_schema.sql").read_text()
            assert "VARCHAR2" in oracle_ddl

            # Verify Snowflake DDL has CREATE OR REPLACE
            sf_ddl = (output_base / "snowflake" / "full_schema.sql").read_text()
            assert "CREATE OR REPLACE TABLE" in sf_ddl

            assert len(results) == 4


# ── 5. Connector Registry ───────────────────────────────────────────────

from dm.connectors.postgres import get_connector, BUILTIN_CONNECTORS


class TestConnectorRegistry:
    def test_postgres_registered(self):
        assert "postgres" in BUILTIN_CONNECTORS
        assert "postgresql" in BUILTIN_CONNECTORS

    def test_db2_registered(self):
        assert "db2" in BUILTIN_CONNECTORS

    def test_oracle_registered(self):
        assert "oracle" in BUILTIN_CONNECTORS

    def test_unknown_connector_error(self):
        with pytest.raises(ValueError, match="Unknown connector type"):
            get_connector({"type": "mongodb"})

    def test_db2_lazy_load(self):
        """DB2 connector class should load without ibm_db installed at registry level."""
        entry = BUILTIN_CONNECTORS["db2"]
        # It's a callable (lazy loader), not a class
        assert callable(entry)

    def test_oracle_lazy_load(self):
        entry = BUILTIN_CONNECTORS["oracle"]
        assert callable(entry)


# ── 6. Cross-Source Referential Integrity ────────────────────────────────

from dm.validators.post.referential import (
    _parse_fk_check,
    _check_cross_source,
    ReferentialIntegrityValidator,
)


class TestCrossSourceRefIntegrity:
    def test_parse_fk_check_standard_format(self):
        parsed = _parse_fk_check({
            "child_table": "claims",
            "parent_table": "claimants",
            "fk_column": "claimant_id",
            "pk_column": "claimant_id",
            "child_source": "claims_db",
            "parent_source": "eligibility_db",
        })
        assert parsed["child_table"] == "claims"
        assert parsed["parent_table"] == "claimants"
        assert parsed["child_source"] == "claims_db"
        assert parsed["parent_source"] == "eligibility_db"

    def test_parse_fk_check_dot_format(self):
        parsed = _parse_fk_check({
            "child": "claims.claimant_id",
            "parent": "claimants.claimant_id",
            "child_source": "db_a",
            "parent_source": "db_b",
        })
        assert parsed["child_table"] == "claims"
        assert parsed["fk_column"] == "claimant_id"
        assert parsed["child_source"] == "db_a"

    def test_parse_fk_check_no_sources(self):
        parsed = _parse_fk_check({
            "child_table": "claims",
            "parent_table": "claimants",
            "fk_column": "claimant_id",
        })
        assert parsed["child_source"] is None
        assert parsed["parent_source"] is None

    def test_cross_source_finds_orphans(self):
        """When child has FK values not in parent, those are orphans."""
        child_conn = MagicMock()
        child_conn.execute_query.return_value = pd.DataFrame(
            {"claimant_id": [1, 2, 3, 4, 5]}
        )
        parent_conn = MagicMock()
        parent_conn.execute_query.return_value = pd.DataFrame(
            {"claimant_id": [1, 2, 3]}
        )

        result = _check_cross_source(
            child_conn, parent_conn,
            "claims", "claimants",
            "claimant_id", "claimant_id",
        )

        assert result["orphan_count"] == 2
        assert set(result["orphan_sample"]) == {4, 5}
        assert result["cross_source"] is True

    def test_cross_source_no_orphans(self):
        child_conn = MagicMock()
        child_conn.execute_query.return_value = pd.DataFrame(
            {"claimant_id": [1, 2, 3]}
        )
        parent_conn = MagicMock()
        parent_conn.execute_query.return_value = pd.DataFrame(
            {"claimant_id": [1, 2, 3, 4, 5]}
        )

        result = _check_cross_source(
            child_conn, parent_conn,
            "claims", "claimants",
            "claimant_id", "claimant_id",
        )

        assert result["orphan_count"] == 0

    def test_validator_same_source_uses_join(self):
        """When no child_source/parent_source, should use the modern_conn JOIN."""
        mock_modern = MagicMock()
        mock_modern.check_referential_integrity.return_value = {
            "orphan_count": 0, "orphan_sample": [],
        }

        config = {
            "validation": {
                "referential_integrity": {
                    "claims": [{
                        "child_table": "claims",
                        "parent_table": "claimants",
                        "fk_column": "claimant_id",
                        "pk_column": "claimant_id",
                    }],
                },
            },
            "_project_dir": ".",
            "metadata": {"path": "./metadata"},
        }

        validator = ReferentialIntegrityValidator()
        result = validator.run(None, mock_modern, "claims", config)

        assert result.status == "PASS"
        mock_modern.check_referential_integrity.assert_called_once()

    def test_validator_cross_source_opens_connections(self):
        """When child_source != parent_source, should open separate connections."""
        config = {
            "connections": {
                "claims_db": {"type": "postgres", "host": "h1", "port": 5432, "database": "claims", "user": "u", "password": "p"},
                "elig_db": {"type": "postgres", "host": "h2", "port": 5432, "database": "elig", "user": "u", "password": "p"},
            },
            "validation": {
                "referential_integrity": {
                    "claims": [{
                        "child_table": "claims",
                        "child_source": "claims_db",
                        "parent_table": "claimants",
                        "parent_source": "elig_db",
                        "fk_column": "claimant_id",
                        "pk_column": "claimant_id",
                    }],
                },
            },
            "_project_dir": ".",
            "metadata": {"path": "./metadata"},
        }

        # Mock get_connector at the source module where it's imported from
        mock_child_conn = MagicMock()
        mock_child_conn.execute_query.return_value = pd.DataFrame({"claimant_id": [1, 2]})
        mock_parent_conn = MagicMock()
        mock_parent_conn.execute_query.return_value = pd.DataFrame({"claimant_id": [1, 2]})

        with patch("dm.connectors.postgres.get_connector") as mock_get_conn:
            mock_get_conn.side_effect = [mock_child_conn, mock_parent_conn]

            validator = ReferentialIntegrityValidator()
            result = validator.run(None, MagicMock(), "claims", config)

        assert result.status == "PASS"
        # Should have opened 2 connections
        assert mock_child_conn.connect.called
        assert mock_parent_conn.connect.called


# ── 7. Transform Dialect Rendering ──────────────────────────────────────

class TestTransformDialects:
    @pytest.fixture
    def plan(self):
        return NormalizationPlan(
            source_table="claimants",
            entities=[ProposedEntity(
                name="claimants",
                columns=[
                    {"source_col": "cl_recid", "data_type": "integer", "nullable": False},
                    {"source_col": "cl_fnam", "data_type": "varchar(50)", "nullable": True},
                ],
                primary_key="cl_recid",
                source_table="claimants",
                role="primary",
                rationale="Test",
            )],
            relationships=[], lookup_tables=[],
            confidence=0.9, rationale="Test",
        )

    @pytest.fixture
    def legacy_schema(self):
        return [
            {"column_name": "cl_recid", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "cl_fnam", "data_type": "varchar(50)", "is_nullable": "YES"},
        ]

    def test_transform_includes_target_dialect(self, plan, legacy_schema):
        for target_key in ["postgres", "snowflake", "oracle", "redshift"]:
            adapter = get_target_adapter(target_key)
            gen = SchemaGenerator(config={"schema_generation": {}}, target_adapter=adapter)
            result = gen.generate(plan, legacy_schema, {"columns": []}, {"mappings": []})

            for table_name, transform in result.transform_files.items():
                assert f"-- Target: {adapter.dialect_name()}" in transform
