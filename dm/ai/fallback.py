"""
Manual Fallback Workflow

When AI is not configured (provider="manual"), this module generates
markdown prompt files that users can copy into their preferred AI tool
(ChatGPT, Claude web UI, etc.) and paste the response back.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from dm.ai.prompts import (
    CODE_CONVERSION_PROMPT,
    DRIFT_EXPLANATION_PROMPT,
    RATIONALIZATION_PROMPT,
    SCHEMA_REFINEMENT_PROMPT,
)

logger = logging.getLogger(__name__)

# Map prompt types to their template strings and required context keys
_PROMPT_REGISTRY: Dict[str, dict] = {
    "schema_refinement": {
        "template": SCHEMA_REFINEMENT_PROMPT,
        "required_keys": ["ddl", "glossary"],
        "title": "Schema Refinement Review",
    },
    "code_conversion": {
        "template": CODE_CONVERSION_PROMPT,
        "required_keys": ["source_sql", "translated_sql", "target_platform"],
        "title": "SQL Code Conversion Review",
    },
    "drift_explanation": {
        "template": DRIFT_EXPLANATION_PROMPT,
        "required_keys": [
            "check_name", "table", "severity", "details", "baseline_context",
        ],
        "title": "Data Drift Explanation",
    },
    "rationalization": {
        "template": RATIONALIZATION_PROMPT,
        "required_keys": [
            "table_name", "row_count", "column_count", "last_updated",
            "schema", "profiling", "relevance_score",
        ],
        "title": "Table Rationalization Review",
    },
}


def generate_prompt_file(
    prompt_type: str,
    context: Dict[str, Any],
    output_path: str,
) -> str:
    """Assemble a prompt and write it to a markdown file.

    The generated file contains the fully rendered prompt, ready to be
    copied into an AI chat interface.

    Args:
        prompt_type: One of 'schema_refinement', 'code_conversion',
                     'drift_explanation', or 'rationalization'.
        context: Dict of placeholder values for the prompt template.
                 Values that are dicts or lists are auto-serialized to JSON.
        output_path: File path where the markdown file will be written.

    Returns:
        The absolute path to the generated file.

    Raises:
        ValueError: If prompt_type is not recognized.
        KeyError: If a required context key is missing.
    """
    if prompt_type not in _PROMPT_REGISTRY:
        available = ", ".join(sorted(_PROMPT_REGISTRY.keys()))
        raise ValueError(
            f"Unknown prompt type '{prompt_type}'. Available: {available}"
        )

    registry_entry = _PROMPT_REGISTRY[prompt_type]
    template = registry_entry["template"]
    title = registry_entry["title"]
    required_keys = registry_entry["required_keys"]

    # Validate required keys
    missing = [k for k in required_keys if k not in context]
    if missing:
        raise KeyError(
            f"Missing required context keys for '{prompt_type}': {missing}"
        )

    # Serialize complex values to JSON strings for template formatting
    formatted_context: Dict[str, str] = {}
    for key, value in context.items():
        if isinstance(value, (dict, list)):
            formatted_context[key] = json.dumps(value, indent=2, default=str)
        else:
            formatted_context[key] = str(value)

    # Render the prompt
    rendered_prompt = template.format(**formatted_context)

    # Build the markdown file
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    markdown = (
        f"# DM — {title}\n\n"
        f"**Generated:** {timestamp}  \n"
        f"**Prompt Type:** `{prompt_type}`\n\n"
        f"---\n\n"
        f"Copy the prompt below into your AI tool and paste the response "
        f"back into the appropriate artifact file.\n\n"
        f"---\n\n"
        f"{rendered_prompt}\n"
    )

    # Write to disk
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(markdown, encoding="utf-8")

    resolved = str(out.resolve())
    logger.info(f"Prompt file generated: {resolved}")
    return resolved
