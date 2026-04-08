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
