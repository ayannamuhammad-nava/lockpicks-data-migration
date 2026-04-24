"""
DM SQL Rule Engine — L-Converter

Deterministic SQL translation using sqlglot for parsing/transpilation,
with regex-based fallback rules for patterns sqlglot cannot handle.
"""

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import sqlglot

logger = logging.getLogger(__name__)

# ── Dialect mapping: DM names → sqlglot dialect identifiers ─────────

DIALECT_MAP = {
    "oracle": "oracle",
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "mssql": "tsql",
    "sqlserver": "tsql",
    "bigquery": "bigquery",
    "snowflake": "snowflake",
    "redshift": "redshift",
    "sqlite": "sqlite",
}

# ── Regex-based fallback rules ───────────────────────────────────────
# Each rule: (compiled_pattern, replacement_template, description)
# Applied when sqlglot cannot handle a specific pattern.

REGEX_RULES: Dict[Tuple[str, str], List[Tuple[re.Pattern, str, str]]] = {
    # Oracle → PostgreSQL
    ("oracle", "postgres"): [
        (
            re.compile(r"\bNVL\s*\(", re.IGNORECASE),
            "COALESCE(",
            "NVL → COALESCE",
        ),
        (
            re.compile(r"\bSYSDATE\b", re.IGNORECASE),
            "NOW()",
            "SYSDATE → NOW()",
        ),
        (
            re.compile(r"\bROWNUM\b", re.IGNORECASE),
            "ROW_NUMBER() OVER()",
            "ROWNUM → ROW_NUMBER() OVER()",
        ),
        (
            re.compile(
                r"\bDECODE\s*\(\s*([^,]+),\s*([^,]+),\s*([^,]+),\s*([^)]+)\)",
                re.IGNORECASE,
            ),
            r"CASE WHEN \1 = \2 THEN \3 ELSE \4 END",
            "DECODE(a,b,c,d) → CASE WHEN a=b THEN c ELSE d END",
        ),
        (
            re.compile(r"\bVARCHAR2\s*\(", re.IGNORECASE),
            "VARCHAR(",
            "VARCHAR2 → VARCHAR",
        ),
        (
            re.compile(r"\bNUMBER\b(?!\s*\()", re.IGNORECASE),
            "NUMERIC",
            "NUMBER → NUMERIC",
        ),
        (
            re.compile(r"\bNUMBER\s*\(", re.IGNORECASE),
            "NUMERIC(",
            "NUMBER(p,s) → NUMERIC(p,s)",
        ),
        (
            re.compile(
                r"\bTO_DATE\s*\(\s*([^,]+),\s*'YYYY-MM-DD'\s*\)",
                re.IGNORECASE,
            ),
            r"CAST(\1 AS DATE)",
            "TO_DATE(x, 'YYYY-MM-DD') → CAST(x AS DATE)",
        ),
        (
            re.compile(r"\bNVL2\s*\(\s*([^,]+),\s*([^,]+),\s*([^)]+)\)", re.IGNORECASE),
            r"CASE WHEN \1 IS NOT NULL THEN \2 ELSE \3 END",
            "NVL2(a,b,c) → CASE WHEN a IS NOT NULL THEN b ELSE c END",
        ),
    ],
    # MySQL → PostgreSQL
    ("mysql", "postgres"): [
        (
            re.compile(r"\bIFNULL\s*\(", re.IGNORECASE),
            "COALESCE(",
            "IFNULL → COALESCE",
        ),
        (
            re.compile(r"\bLIMIT\s+(\d+)\s*,\s*(\d+)", re.IGNORECASE),
            r"LIMIT \2 OFFSET \1",
            "LIMIT offset,count → LIMIT count OFFSET offset",
        ),
        (
            re.compile(r"`([^`]+)`"),
            r'"\1"',
            "Backtick quoting → double-quote quoting",
        ),
    ],
    # MSSQL → PostgreSQL
    ("mssql", "postgres"): [
        (
            re.compile(r"\bISNULL\s*\(", re.IGNORECASE),
            "COALESCE(",
            "ISNULL → COALESCE",
        ),
        (
            re.compile(r"\bGETDATE\s*\(\s*\)", re.IGNORECASE),
            "NOW()",
            "GETDATE() → NOW()",
        ),
        (
            re.compile(r"\bTOP\s+(\d+)\b", re.IGNORECASE),
            "",
            "TOP N removed (handled separately with LIMIT)",
        ),
    ],
}


class SQLRuleEngine:
    """Deterministic SQL translation engine.

    Uses sqlglot as the primary transpiler, falling back to regex-based
    pattern replacement for constructs sqlglot cannot handle.
    """

    def __init__(self) -> None:
        self._custom_rules: Dict[Tuple[str, str], List[Tuple[re.Pattern, str, str]]] = {}

    def add_rule(
        self,
        source_dialect: str,
        target_dialect: str,
        pattern: str,
        replacement: str,
        description: str = "",
    ) -> None:
        """Register an additional regex translation rule.

        Args:
            source_dialect: Source SQL dialect name.
            target_dialect: Target SQL dialect name.
            pattern: Regex pattern to match.
            replacement: Replacement string (may use \\1, \\2 groups).
            description: Human-readable description of the rule.
        """
        key = (source_dialect.lower(), target_dialect.lower())
        if key not in self._custom_rules:
            self._custom_rules[key] = []
        self._custom_rules[key].append(
            (re.compile(pattern, re.IGNORECASE), replacement, description)
        )

    def translate(
        self,
        source_sql: str,
        source_dialect: str,
        target_dialect: str,
    ) -> str:
        """Translate SQL from source dialect to target dialect.

        Strategy:
            1. Try sqlglot transpilation for each statement.
            2. If sqlglot fails on a statement, fall back to regex rules.
            3. If regex also cannot handle it, preserve as a comment with TODO.

        Args:
            source_sql: The SQL source code to translate.
            source_dialect: Source dialect name (e.g., 'oracle', 'mysql').
            target_dialect: Target dialect name (e.g., 'postgres').

        Returns:
            Translated SQL string.
        """
        src = source_dialect.lower()
        tgt = target_dialect.lower()
        src_sqlglot = DIALECT_MAP.get(src, src)
        tgt_sqlglot = DIALECT_MAP.get(tgt, tgt)

        # Split into individual statements
        statements = self._split_statements(source_sql)
        translated: List[str] = []
        warnings: List[str] = []

        for stmt in statements:
            stripped = stmt.strip()
            if not stripped:
                continue

            result = self._translate_statement(
                stripped, src, tgt, src_sqlglot, tgt_sqlglot, warnings
            )
            translated.append(result)

        return "\n\n".join(translated)

    def _translate_statement(
        self,
        stmt: str,
        src: str,
        tgt: str,
        src_sqlglot: str,
        tgt_sqlglot: str,
        warnings: List[str],
    ) -> str:
        """Translate a single SQL statement."""
        # Pass 1: try sqlglot
        try:
            result = sqlglot.transpile(
                stmt,
                read=src_sqlglot,
                write=tgt_sqlglot,
                pretty=True,
            )
            if result:
                translated = result[0]
                # Apply regex rules as a second pass for anything sqlglot missed
                translated = self._apply_regex_rules(translated, src, tgt)
                return translated
        except sqlglot.errors.ParseError:
            pass
        except Exception as e:
            logger.debug(f"sqlglot failed on statement: {e}")

        # Pass 2: regex-only fallback
        try:
            regex_result = self._apply_regex_rules(stmt, src, tgt)
            if regex_result != stmt:
                logger.info("Used regex fallback for statement translation")
                return regex_result
        except Exception as e:
            logger.warning(f"Regex fallback also failed: {e}")

        # Pass 3: preserve as comment with TODO
        warning = f"Could not translate statement — preserved as comment"
        warnings.append(warning)
        logger.warning(warning)
        return (
            f"-- TODO: Manual translation required. Original {src} SQL:\n"
            f"-- {stmt.replace(chr(10), chr(10) + '-- ')}"
        )

    def _apply_regex_rules(self, sql: str, src: str, tgt: str) -> str:
        """Apply regex-based translation rules for the given dialect pair."""
        key = (src, tgt)
        result = sql

        # Built-in rules
        for pattern, replacement, desc in REGEX_RULES.get(key, []):
            new_result = pattern.sub(replacement, result)
            if new_result != result:
                logger.debug(f"Applied regex rule: {desc}")
                result = new_result

        # Custom rules
        for pattern, replacement, desc in self._custom_rules.get(key, []):
            new_result = pattern.sub(replacement, result)
            if new_result != result:
                logger.debug(f"Applied custom rule: {desc}")
                result = new_result

        return result

    def _split_statements(self, sql: str) -> List[str]:
        """Split SQL text into individual statements on semicolons.

        Respects quoted strings and block comments to avoid splitting
        inside them.
        """
        statements: List[str] = []
        current: List[str] = []
        in_single_quote = False
        in_double_quote = False
        in_block_comment = False
        i = 0

        while i < len(sql):
            char = sql[i]

            # Block comment handling
            if not in_single_quote and not in_double_quote:
                if i + 1 < len(sql) and sql[i : i + 2] == "/*":
                    in_block_comment = True
                    current.append("/*")
                    i += 2
                    continue
                if in_block_comment and i + 1 < len(sql) and sql[i : i + 2] == "*/":
                    in_block_comment = False
                    current.append("*/")
                    i += 2
                    continue

            if in_block_comment:
                current.append(char)
                i += 1
                continue

            # Quote handling
            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote

            # Statement separator
            if char == ";" and not in_single_quote and not in_double_quote:
                stmt = "".join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
            else:
                current.append(char)

            i += 1

        # Final statement (no trailing semicolon)
        remaining = "".join(current).strip()
        if remaining:
            statements.append(remaining)

        return statements

    def translate_file(
        self,
        filepath: str,
        source_dialect: str,
        target_dialect: str,
    ) -> str:
        """Read a SQL file and translate its contents.

        Args:
            filepath: Path to the source SQL file.
            source_dialect: Source dialect name.
            target_dialect: Target dialect name.

        Returns:
            Translated SQL string.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"SQL file not found: {filepath}")

        source_sql = path.read_text(encoding="utf-8")
        logger.info(f"Translating {filepath} from {source_dialect} to {target_dialect}")

        return self.translate(source_sql, source_dialect, target_dialect)
