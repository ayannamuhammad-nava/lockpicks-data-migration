"""
RAG Tool for semantic search over migration metadata.

Loads glossary.json and mappings.json, builds embeddings, and answers
questions like "Why did cl_fnam disappear?" or "What is cl_bact?"
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

logger = logging.getLogger(__name__)


class RAGTool:
    """Semantic search over migration metadata using sentence-transformers."""

    def __init__(self, metadata_path: Optional[str] = None):
        self.metadata_path = Path(metadata_path) if metadata_path else None
        self.glossary: List[Dict] = []
        self.mappings: List[Dict] = []
        self.texts: List[str] = []
        self.embeddings: Optional[np.ndarray] = None
        self._model = None

        if self.metadata_path:
            self.load_metadata()

    def load_metadata(self, metadata_path: Optional[str] = None) -> None:
        """Load glossary and mappings from JSON files."""
        path = Path(metadata_path) if metadata_path else self.metadata_path
        if not path:
            return

        glossary_file = path / "glossary.json"
        mappings_file = path / "mappings.json"

        if glossary_file.exists():
            self.glossary = json.loads(glossary_file.read_text()).get("columns", [])

        if mappings_file.exists():
            self.mappings = json.loads(mappings_file.read_text()).get("mappings", [])

        self._build_texts()

    def _build_texts(self) -> None:
        """Build searchable text entries from metadata."""
        self.texts = []

        for entry in self.glossary:
            text = f"{entry['name']}: {entry.get('description', '')} (system: {entry.get('system', '')}, table: {entry.get('table', '')})"
            self.texts.append(text)

        for mapping in self.mappings:
            source = mapping.get("source", "")
            target = mapping.get("target", "N/A")
            rationale = mapping.get("rationale", "")
            mtype = mapping.get("type", "")
            text = f"Maps {source} to {target} ({mtype}): {rationale}"
            self.texts.append(text)

    def build_embeddings(self) -> None:
        """Generate embeddings for all metadata texts."""
        if not self.texts:
            return

        try:
            from sentence_transformers import SentenceTransformer
            if self._model is None:
                self._model = SentenceTransformer("all-MiniLM-L6-v2")
            self.embeddings = self._model.encode(self.texts, show_progress_bar=False)
            logger.info(f"Built embeddings for {len(self.texts)} metadata entries")
        except ImportError:
            logger.warning("sentence-transformers not installed; RAG explanations disabled")

    def explain_column(self, column_name: str, top_k: int = 3) -> List[Dict]:
        """Search for explanations of a column.

        Returns:
            List of {text, score} dicts, sorted by relevance.
        """
        if self.embeddings is None:
            self.build_embeddings()

        if self.embeddings is None or len(self.texts) == 0:
            return []

        query = f"What is {column_name}? Why was it changed?"
        query_embedding = self._model.encode([query])

        similarities = np.dot(self.embeddings, query_embedding.T).flatten()
        top_indices = similarities.argsort()[-top_k:][::-1]

        return [
            {"text": self.texts[i], "score": float(similarities[i])}
            for i in top_indices
            if similarities[i] > 0.3
        ]

    def enrich_schema_diff(self, schema_diff: Dict) -> Dict[str, str]:
        """Add RAG explanations to schema diff entries.

        Returns:
            Dict mapping column_name -> explanation string.
        """
        explanations = {}

        for col in schema_diff.get("missing_in_modern", []):
            results = self.explain_column(col)
            if results:
                explanations[col] = results[0]["text"]

        for col in schema_diff.get("missing_in_legacy", []):
            results = self.explain_column(col)
            if results:
                explanations[col] = results[0]["text"]

        return explanations
