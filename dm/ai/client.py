"""
AI Client — Anthropic SDK Wrapper

Provides a unified interface for AI-assisted operations in DM.
Supports Anthropic's Claude models with graceful fallback when the SDK
or API key is not available.
"""

import logging
from typing import Optional

from dm.ai.prompts import (
    CODE_CONVERSION_PROMPT,
    COLUMN_UNDERSTANDING_PROMPT,
    DATA_QUALITY_PROMPT,
    NORMALIZATION_REVIEW_PROMPT,
    SCHEMA_REFINEMENT_PROMPT,
)

logger = logging.getLogger(__name__)

try:
    import anthropic

    _HAS_ANTHROPIC = True
except ImportError:
    _HAS_ANTHROPIC = False


class AIClient:
    """Wrapper around the Anthropic SDK for DM AI operations.

    Supports two modes:
        - ``anthropic``: Uses the Anthropic Python SDK for completions.
        - ``manual``: AI is not used; callers should fall back to
          prompt-file workflows (see ``dm.ai.fallback``).
    """

    def __init__(self, ai_config: dict):
        """Initialize the AI client.

        Args:
            ai_config: Configuration dict with keys:
                provider: "anthropic" | "manual"
                api_key: Anthropic API key (required for provider="anthropic")
                model: Primary model identifier (e.g. "claude-sonnet-4-20250514")
                model_refine: Model for refinement tasks (defaults to model)
                max_tokens: Maximum tokens for completions (default 4096)
                temperature: Sampling temperature (default 0.0)
        """
        self.provider = ai_config.get("provider", "manual")
        self.api_key = ai_config.get("api_key", "")
        self.model = ai_config.get("model", "claude-sonnet-4-20250514")
        self.model_refine = ai_config.get("model_refine", self.model)
        self.max_tokens = ai_config.get("max_tokens", 4096)
        self.temperature = ai_config.get("temperature", 0.0)

        self._client = None
        if self.is_available():
            self._client = anthropic.Anthropic(api_key=self.api_key)

    def is_available(self) -> bool:
        """Check whether the AI client is ready for use.

        Returns:
            True if the provider is 'anthropic', the SDK is installed,
            and an API key is configured.
        """
        return (
            _HAS_ANTHROPIC
            and self.provider == "anthropic"
            and bool(self.api_key)
        )

    def complete(
        self,
        prompt: str,
        system: str = "",
        model: Optional[str] = None,
    ) -> str:
        """Send a completion request to the Anthropic API.

        Args:
            prompt: The user message / prompt text.
            system: Optional system prompt.
            model: Model to use (overrides the default).

        Returns:
            The model's text response.

        Raises:
            RuntimeError: If the AI client is not available.
        """
        if not self.is_available() or self._client is None:
            raise RuntimeError(
                "AI not configured. Set provider='anthropic' and provide "
                "a valid api_key, or use the manual fallback workflow."
            )

        resolved_model = model or self.model

        logger.debug(
            f"AI completion request: model={resolved_model}, "
            f"max_tokens={self.max_tokens}, temperature={self.temperature}"
        )

        kwargs = {
            "model": resolved_model,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system:
            kwargs["system"] = system

        try:
            response = self._client.messages.create(**kwargs)

            # Extract text from the response content blocks
            text_parts = []
            for block in response.content:
                if hasattr(block, "text"):
                    text_parts.append(block.text)
            return "\n".join(text_parts)

        except Exception as e:
            logger.error(f"AI completion failed: {e}")
            raise

    def refine_schema(self, ddl: str, glossary: dict) -> str:
        """Use AI to review and refine generated DDL.

        Args:
            ddl: The generated DDL statements to review.
            glossary: Business glossary dict for context.

        Returns:
            Refined DDL string with AI suggestions applied.
        """
        import json

        glossary_text = json.dumps(glossary, indent=2, default=str)
        prompt = SCHEMA_REFINEMENT_PROMPT.format(
            ddl=ddl,
            glossary=glossary_text,
        )

        return self.complete(
            prompt=prompt,
            system="You are a database schema expert reviewing generated DDL.",
            model=self.model_refine,
        )

    def refine_conversion(
        self, source: str, translated: str, target: str,
    ) -> str:
        """Use AI to review and refine a SQL code conversion.

        Args:
            source: Original source SQL.
            translated: Machine-translated SQL.
            target: Target platform name (e.g. 'postgres').

        Returns:
            Refined SQL with AI corrections applied.
        """
        prompt = CODE_CONVERSION_PROMPT.format(
            source_sql=source,
            translated_sql=translated,
            target_platform=target,
        )

        return self.complete(
            prompt=prompt,
            system=(
                f"You are a SQL migration expert converting code to {target}."
            ),
            model=self.model_refine,
        )

    def understand_columns(
        self, fields: list, context: str = "contact", domain: str = "government services",
    ) -> list:
        """Use AI to map COBOL field names to modern column names.

        Args:
            fields: List of dicts with 'name', 'pic', 'sql_type' keys.
            context: What kind of record (e.g., "contact", "account", "claim").
            domain: Business domain (e.g., "government services", "banking").

        Returns:
            List of dicts with 'source', 'modern_name', 'description', 'data_type_suggestion'.
            Returns empty list if AI is not available.
        """
        import json

        if not self.is_available():
            return []

        fields_text = "\n".join(
            f"- {f['name']:30s} PIC {f.get('pic', '?'):15s} (current SQL type: {f.get('sql_type', '?')})"
            for f in fields
        )

        prompt = COLUMN_UNDERSTANDING_PROMPT.format(
            fields=fields_text,
            context=context,
            domain=domain,
        )

        try:
            response = self.complete(
                prompt=prompt,
                system="You are a COBOL mainframe expert helping with data migration.",
            )
            # Parse JSON from response
            response = response.strip()
            if response.startswith("```"):
                response = response.split("\n", 1)[1].rsplit("```", 1)[0]
            return json.loads(response)
        except Exception as e:
            logger.warning(f"AI column understanding failed: {e}")
            return []

    def review_normalization(
        self, table_name: str, columns: list, proposed_plan: dict, profiling: dict,
    ) -> dict:
        """Use AI to review and refine a rule-based normalization plan.

        Args:
            table_name: Name of the source table.
            columns: List of column names.
            proposed_plan: The rule engine's normalization output.
            profiling: Profiling stats for the table.

        Returns:
            Dict with 'approved', 'changes', 'rationale' keys.
            Returns None if AI is not available.
        """
        import json

        if not self.is_available():
            return None

        prompt = NORMALIZATION_REVIEW_PROMPT.format(
            table_name=table_name,
            column_count=len(columns),
            columns="\n".join(f"- {c}" for c in columns),
            proposed_plan=json.dumps(proposed_plan, indent=2, default=str)[:3000],
            profiling=json.dumps(profiling, indent=2, default=str)[:2000],
        )

        try:
            response = self.complete(
                prompt=prompt,
                system="You are a database architect reviewing a normalization plan.",
            )
            response = response.strip()
            if response.startswith("```"):
                response = response.split("\n", 1)[1].rsplit("```", 1)[0]
            return json.loads(response)
        except Exception as e:
            logger.warning(f"AI normalization review failed: {e}")
            return None

    def assess_data_quality(
        self, table_name: str, profiling_stats: dict, sample_data: str,
    ) -> list:
        """Use AI to find data quality issues that rules can't catch.

        Args:
            table_name: Name of the table.
            profiling_stats: Column-level profiling statistics.
            sample_data: String representation of first few rows.

        Returns:
            List of finding dicts with 'column', 'severity', 'finding', 'recommendation'.
            Returns empty list if AI is not available.
        """
        import json

        if not self.is_available():
            return []

        row_count = profiling_stats.get("row_count", 0)
        col_count = profiling_stats.get("column_count", 0)

        prompt = DATA_QUALITY_PROMPT.format(
            table_name=table_name,
            row_count=row_count,
            column_count=col_count,
            profiling_stats=json.dumps(profiling_stats.get("columns", {}), indent=2, default=str)[:3000],
            sample_data=sample_data[:2000],
        )

        try:
            response = self.complete(
                prompt=prompt,
                system="You are a data quality analyst reviewing migration data.",
            )
            response = response.strip()
            if response.startswith("```"):
                response = response.split("\n", 1)[1].rsplit("```", 1)[0]
            return json.loads(response)
        except Exception as e:
            logger.warning(f"AI data quality assessment failed: {e}")
            return []
