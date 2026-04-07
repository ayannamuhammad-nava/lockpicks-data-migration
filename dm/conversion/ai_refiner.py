"""
DM AI Refiner — optional Claude-powered SQL refinement.

Uses the Anthropic SDK to review and improve machine-translated SQL,
with a manual fallback that generates a prompt file for offline use.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Guard import — anthropic is an optional dependency
try:
    import anthropic

    _HAS_ANTHROPIC = True
except ImportError:
    anthropic = None  # type: ignore[assignment]
    _HAS_ANTHROPIC = False


class AIRefiner:
    """Refine machine-translated SQL using Claude AI.

    If the anthropic SDK is not installed or the provider is not 'anthropic',
    the refiner gracefully degrades to generating a prompt file for manual
    review.

    Args:
        ai_config: Dict with keys:
            provider: str — 'anthropic' (only supported provider)
            api_key: str — Anthropic API key (or set ANTHROPIC_API_KEY env var)
            model: str — Model name (default: 'claude-sonnet-4-20250514')
            max_tokens: int — Max response tokens (default: 4096)
    """

    def __init__(self, ai_config: Dict) -> None:
        self.provider = ai_config.get("provider", "anthropic")
        self.api_key = ai_config.get("api_key")
        self.model = ai_config.get("model", "claude-sonnet-4-20250514")
        self.max_tokens = ai_config.get("max_tokens", 4096)
        self._client: Any = None

    @property
    def is_available(self) -> bool:
        """Return True if the AI backend is ready to use."""
        return (
            _HAS_ANTHROPIC
            and self.provider == "anthropic"
            and bool(self.api_key or _has_env_key())
        )

    def _get_client(self) -> Any:
        """Lazily instantiate the Anthropic client."""
        if self._client is None:
            if not _HAS_ANTHROPIC:
                raise ImportError(
                    "The 'anthropic' package is required for AI refinement. "
                    "Install it with: uv sync --extra ai"
                )
            kwargs: Dict[str, Any] = {}
            if self.api_key:
                kwargs["api_key"] = self.api_key
            self._client = anthropic.Anthropic(**kwargs)
        return self._client

    def refine(
        self,
        source_sql: str,
        translated_sql: str,
        glossary: Dict,
        target: str,
    ) -> Dict:
        """Send translated SQL to Claude for review and refinement.

        Args:
            source_sql: Original SQL in the source dialect.
            translated_sql: Machine-translated SQL from the rule engine.
            glossary: Domain glossary dict for context (column meanings, etc.).
            target: Target dialect name (e.g., 'postgres').

        Returns:
            Dict with keys:
                refined_sql: str — the improved SQL
                suggestions: list[str] — improvement suggestions
                diff: str — summary of changes made
        """
        if not self.is_available:
            logger.warning(
                "AI refinement not available — returning original translation. "
                "Use generate_prompt_file() for manual review."
            )
            return {
                "refined_sql": translated_sql,
                "suggestions": ["AI refinement unavailable — manual review recommended"],
                "diff": "",
            }

        prompt = self._build_prompt(source_sql, translated_sql, glossary, target)

        try:
            client = self._get_client()
            message = client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                messages=[
                    {"role": "user", "content": prompt},
                ],
                system=(
                    "You are an expert SQL migration engineer. Review the machine-translated SQL "
                    "and return a JSON object with exactly three keys: "
                    '"refined_sql" (the corrected SQL), '
                    '"suggestions" (a list of improvement notes), '
                    '"diff" (a brief summary of what you changed). '
                    "Return only valid JSON, no markdown fences."
                ),
            )

            response_text = message.content[0].text
            return self._parse_response(response_text, translated_sql)

        except Exception as e:
            logger.error(f"AI refinement failed: {e}")
            return {
                "refined_sql": translated_sql,
                "suggestions": [f"AI refinement failed: {e}"],
                "diff": "",
            }

    def generate_prompt_file(
        self,
        source_sql: str,
        translated_sql: str,
        glossary: Dict,
        target: str,
        output_path: str,
    ) -> str:
        """Write a markdown prompt file for manual AI-assisted review.

        This is the offline fallback when the Anthropic SDK is not available
        or the user prefers to review translations manually.

        Args:
            source_sql: Original SQL in the source dialect.
            translated_sql: Machine-translated SQL from the rule engine.
            glossary: Domain glossary dict.
            target: Target dialect name.
            output_path: Path to write the prompt file.

        Returns:
            The absolute path to the generated prompt file.
        """
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        glossary_text = self._format_glossary(glossary)

        content = f"""# SQL Translation Review Prompt

## Target Dialect
{target}

## Original Source SQL
```sql
{source_sql}
```

## Machine-Translated SQL
```sql
{translated_sql}
```

## Domain Glossary Context
{glossary_text}

## Review Questions

1. Are there any semantic differences between the source and translated SQL?
2. Are all data types correctly mapped for the target platform?
3. Are there any function calls that need manual adjustment?
4. Are there any performance concerns with the translated SQL?
5. Does the translated SQL preserve the original business logic?

## Instructions

Please review the machine-translated SQL above and provide:
- A corrected version of the SQL (if changes are needed)
- A list of suggestions or concerns
- A brief diff summary of changes made
"""
        path.write_text(content, encoding="utf-8")
        logger.info(f"Generated prompt file: {path}")
        return str(path.resolve())

    def _build_prompt(
        self,
        source_sql: str,
        translated_sql: str,
        glossary: Dict,
        target: str,
    ) -> str:
        """Build the prompt for Claude API."""
        glossary_text = self._format_glossary(glossary)

        return f"""Review this SQL migration translation and return a JSON object.

Target dialect: {target}

## Original Source SQL
{source_sql}

## Machine-Translated SQL
{translated_sql}

## Domain Glossary
{glossary_text}

Check for:
1. Semantic correctness — does the translation preserve business logic?
2. Data type accuracy for {target}
3. Function/syntax compatibility
4. Performance implications

Return JSON with keys: refined_sql, suggestions (list of strings), diff (summary string)."""

    def _format_glossary(self, glossary: Dict) -> str:
        """Format the glossary dict into readable text for the prompt."""
        if not glossary:
            return "(No glossary provided)"

        lines: List[str] = []
        columns = glossary.get("columns", [])
        if isinstance(columns, list):
            for entry in columns[:50]:  # Limit to avoid token overflow
                name = entry.get("name", "?")
                desc = entry.get("description", "")
                pii = " [PII]" if entry.get("pii") else ""
                lines.append(f"- **{name}**: {desc}{pii}")
        elif isinstance(columns, dict):
            for name, info in list(columns.items())[:50]:
                desc = info if isinstance(info, str) else info.get("description", "")
                lines.append(f"- **{name}**: {desc}")

        return "\n".join(lines) if lines else "(Glossary is empty)"

    def _parse_response(self, response_text: str, fallback_sql: str) -> Dict:
        """Parse Claude's JSON response, with graceful fallback."""
        import json

        # Try to extract JSON from the response
        text = response_text.strip()

        # Strip markdown code fences if present
        if text.startswith("```"):
            lines = text.split("\n")
            # Remove first and last fence lines
            lines = [l for l in lines if not l.strip().startswith("```")]
            text = "\n".join(lines)

        try:
            data = json.loads(text)
            return {
                "refined_sql": data.get("refined_sql", fallback_sql),
                "suggestions": data.get("suggestions", []),
                "diff": data.get("diff", ""),
            }
        except json.JSONDecodeError:
            logger.warning("Could not parse AI response as JSON — using raw text")
            return {
                "refined_sql": fallback_sql,
                "suggestions": [response_text[:500]],
                "diff": "Could not parse structured response",
            }


def _has_env_key() -> bool:
    """Check whether the ANTHROPIC_API_KEY environment variable is set."""
    import os

    return bool(os.environ.get("ANTHROPIC_API_KEY"))
