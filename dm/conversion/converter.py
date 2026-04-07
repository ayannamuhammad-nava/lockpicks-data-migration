"""
DM Code Converter — L-Converter orchestration.

Coordinates the rule engine and optional AI refiner to translate
SQL source files from a legacy dialect to a modern target dialect.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from dm.conversion.ai_refiner import AIRefiner
from dm.conversion.rule_engine import SQLRuleEngine

logger = logging.getLogger(__name__)


@dataclass
class ConversionResult:
    """Result of a SQL code conversion run."""

    source_path: str
    target_dialect: str
    translated_sql: str
    ai_suggestions: List[str] = field(default_factory=list)
    prompt_file_path: Optional[str] = None
    warnings: List[str] = field(default_factory=list)


class CodeConverter:
    """Orchestrates SQL translation: rule engine → AI refiner → plugin overrides.

    Args:
        config: Project configuration dict (from project.yaml).
        plugin_manager: Optional pluggy PluginManager for dm_conversion_overrides hook.
    """

    def __init__(self, config: Dict, plugin_manager: Any = None) -> None:
        self.config = config
        self.pm = plugin_manager
        self.rule_engine = SQLRuleEngine()

        # Load AI config if present
        self._ai_config = config.get("ai", config.get("conversion", {}).get("ai", {}))

    def convert(
        self,
        source_path: str,
        target: str,
        ai_refine: bool = False,
        dry_run: bool = False,
    ) -> ConversionResult:
        """Convert a SQL source file to the target dialect.

        Pass 1: Deterministic translation via SQLRuleEngine.
        Pass 2 (optional): AI refinement via Claude or prompt file generation.
        Pass 3: Apply plugin overrides via dm_conversion_overrides hook.

        Args:
            source_path: Path to the SQL file to convert.
            target: Target dialect name (e.g., 'postgres').
            ai_refine: If True, attempt AI-powered refinement after rule-based translation.
            dry_run: If True, do not write output files.

        Returns:
            ConversionResult with the translated SQL and metadata.
        """
        path = Path(source_path)
        if not path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

        source_sql = path.read_text(encoding="utf-8")
        source_dialect = self._detect_source_dialect(source_sql)
        warnings: List[str] = []
        ai_suggestions: List[str] = []
        prompt_file_path: Optional[str] = None

        logger.info(
            f"Converting {source_path}: {source_dialect} → {target}"
        )

        # ── Pass 1: Rule-based translation ───────────────────────────
        translated_sql = self.rule_engine.translate(
            source_sql, source_dialect, target
        )
        logger.info("Pass 1 complete: rule-based translation")

        # ── Pass 2: AI refinement (optional) ─────────────────────────
        if ai_refine and self._ai_config:
            translated_sql, ai_suggestions, prompt_file_path = self._run_ai_pass(
                source_sql=source_sql,
                translated_sql=translated_sql,
                target=target,
                source_path=source_path,
                dry_run=dry_run,
            )
        elif ai_refine:
            warnings.append(
                "AI refinement requested but no 'ai' config found in project.yaml"
            )

        # ── Pass 3: Plugin overrides ─────────────────────────────────
        translated_sql = self._apply_plugin_overrides(translated_sql, target)

        # ── Write output ─────────────────────────────────────────────
        if not dry_run:
            output_path = self._get_output_path(source_path, target)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(translated_sql, encoding="utf-8")
            logger.info(f"Wrote translated SQL to {output_path}")

        return ConversionResult(
            source_path=source_path,
            target_dialect=target,
            translated_sql=translated_sql,
            ai_suggestions=ai_suggestions,
            prompt_file_path=prompt_file_path,
            warnings=warnings,
        )

    def _detect_source_dialect(self, sql: str) -> str:
        """Detect the source SQL dialect from config or heuristics.

        Checks the project config first; falls back to simple keyword-based
        heuristics.
        """
        # Check project config
        connections = self.config.get("connections", {})
        legacy = connections.get("legacy", {})
        conn_type = legacy.get("type", "").lower()

        if conn_type in ("oracle", "mysql", "mssql", "sqlserver", "postgres", "postgresql"):
            dialect = conn_type
            if dialect == "sqlserver":
                dialect = "mssql"
            if dialect == "postgresql":
                dialect = "postgres"
            return dialect

        # Heuristic detection
        sql_upper = sql.upper()
        if "NVL(" in sql_upper or "SYSDATE" in sql_upper or "VARCHAR2" in sql_upper:
            return "oracle"
        if "GETDATE()" in sql_upper or "ISNULL(" in sql_upper or "TOP " in sql_upper:
            return "mssql"
        if "IFNULL(" in sql_upper or "AUTO_INCREMENT" in sql_upper:
            return "mysql"

        # Default fallback
        return self.config.get("conversion", {}).get("source_dialect", "oracle")

    def _run_ai_pass(
        self,
        source_sql: str,
        translated_sql: str,
        target: str,
        source_path: str,
        dry_run: bool,
    ) -> tuple:
        """Execute the AI refinement pass.

        Returns:
            (translated_sql, ai_suggestions, prompt_file_path)
        """
        refiner = AIRefiner(self._ai_config)
        glossary = self._load_glossary()
        ai_suggestions: List[str] = []
        prompt_file_path: Optional[str] = None

        if refiner.is_available:
            logger.info("Pass 2: AI refinement via Claude")
            result = refiner.refine(
                source_sql=source_sql,
                translated_sql=translated_sql,
                glossary=glossary,
                target=target,
            )
            translated_sql = result["refined_sql"]
            ai_suggestions = result["suggestions"]
            logger.info(f"AI refinement complete: {len(ai_suggestions)} suggestions")
        else:
            # Generate prompt file for manual review
            logger.info("Pass 2: generating prompt file for manual AI review")
            prompt_output = self._get_prompt_path(source_path, target)
            if not dry_run:
                prompt_file_path = refiner.generate_prompt_file(
                    source_sql=source_sql,
                    translated_sql=translated_sql,
                    glossary=glossary,
                    target=target,
                    output_path=str(prompt_output),
                )
            else:
                prompt_file_path = str(prompt_output)
            ai_suggestions = ["Prompt file generated for manual review"]

        return translated_sql, ai_suggestions, prompt_file_path

    def _apply_plugin_overrides(self, sql: str, target: str) -> str:
        """Apply dm_conversion_overrides hook from plugins."""
        if not self.pm:
            return sql

        try:
            results = self.pm.hook.dm_conversion_overrides(
                source_sql=sql, target=target
            )
            for result in results:
                if result is not None:
                    logger.info("Applied plugin conversion override")
                    sql = result
        except Exception as e:
            logger.warning(f"Plugin conversion override failed: {e}")

        return sql

    def _load_glossary(self) -> Dict:
        """Load the project glossary from metadata/glossary.json."""
        import json

        project_dir = self.config.get("_project_dir", ".")
        metadata_rel = self.config.get("metadata", {}).get("path", "./metadata")
        glossary_path = Path(project_dir) / metadata_rel / "glossary.json"

        if glossary_path.exists():
            try:
                with open(glossary_path, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load glossary: {e}")

        return {}

    def _get_output_path(self, source_path: str, target: str) -> Path:
        """Determine the output file path for the translated SQL."""
        project_dir = self.config.get("_project_dir", ".")
        source = Path(source_path)
        output_dir = Path(project_dir) / "artifacts" / "converted" / target
        return output_dir / source.name

    def _get_prompt_path(self, source_path: str, target: str) -> Path:
        """Determine the output path for the AI prompt file."""
        project_dir = self.config.get("_project_dir", ".")
        source = Path(source_path)
        output_dir = Path(project_dir) / "artifacts" / "prompts" / target
        return output_dir / f"{source.stem}_review.md"
