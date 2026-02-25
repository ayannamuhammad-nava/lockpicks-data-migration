"""
RAG (Retrieval-Augmented Generation) tool for semantic search and schema explanations.
Uses sentence-transformers for embeddings and cosine similarity for retrieval.
"""
import hashlib
import json
import os
from typing import List, Dict, Optional
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import logging

logger = logging.getLogger(__name__)


class RAGTool:
    """
    RAG tool for explaining schema mappings and column purposes using semantic search.
    """

    def __init__(self, metadata_dir: str = './metadata', model_name: str = 'all-MiniLM-L6-v2',
                 explain_threshold: float = 0.5, mapping_threshold: float = 0.3):
        """
        Initialize the RAG tool.

        Args:
            metadata_dir: Directory containing glossary.json and mappings.json
            model_name: Sentence transformer model name
            explain_threshold: Minimum similarity score for column explanations
            mapping_threshold: Minimum similarity score for mapping suggestions
        """
        self.metadata_dir = metadata_dir
        self.model_name = model_name
        self.model = None

        self.glossary = []
        self.mappings = []

        self.column_embeddings = []
        self.column_texts = []

        self.explain_threshold = explain_threshold
        self.mapping_threshold = mapping_threshold

        logger.info(f"Initializing RAG tool with model: {model_name}")

    def load_model(self):
        """Load the sentence transformer model (lazy loading)."""
        if self.model is None:
            logger.info(f"Loading sentence transformer model: {self.model_name}")
            self.model = SentenceTransformer(self.model_name)

    def load_metadata(self):
        """Load metadata from JSON files."""
        glossary_path = os.path.join(self.metadata_dir, 'glossary.json')
        mappings_path = os.path.join(self.metadata_dir, 'mappings.json')

        # Load glossary
        if os.path.exists(glossary_path):
            with open(glossary_path, 'r') as f:
                data = json.load(f)
                self.glossary = data.get('columns', [])
            logger.info(f"Loaded {len(self.glossary)} entries from glossary")
        else:
            logger.warning(f"Glossary not found: {glossary_path}")

        # Load mappings
        if os.path.exists(mappings_path):
            with open(mappings_path, 'r') as f:
                data = json.load(f)
                self.mappings = data.get('mappings', [])
            logger.info(f"Loaded {len(self.mappings)} mappings")
        else:
            logger.warning(f"Mappings not found: {mappings_path}")

    def _glossary_hash(self) -> str:
        """Compute MD5 hash of glossary content for cache invalidation."""
        content = json.dumps(self.glossary, sort_keys=True)
        return hashlib.md5(content.encode()).hexdigest()

    def build_embeddings(self):
        """Generate embeddings for all column descriptions, using cache when possible."""
        # Build text list from glossary
        self.column_texts = []
        for entry in self.glossary:
            text = f"{entry.get('name', '')}: {entry.get('description', '')}"
            self.column_texts.append(text)

        if not self.column_texts:
            return

        # Check for cached embeddings
        cache_dir = self.metadata_dir
        cache_path = os.path.join(cache_dir, '.embeddings_cache.npz')
        current_hash = self._glossary_hash()

        if os.path.exists(cache_path):
            try:
                cached = np.load(cache_path, allow_pickle=False)
                if cached['hash'].item() == current_hash:
                    self.column_embeddings = cached['embeddings']
                    logger.info(f"Loaded cached embeddings for {len(self.column_texts)} columns")
                    return
            except Exception as e:
                logger.debug(f"Cache miss or corrupt: {e}")

        # Generate fresh embeddings
        self.load_model()
        logger.info(f"Generating embeddings for {len(self.column_texts)} columns")
        self.column_embeddings = self.model.encode(self.column_texts)

        # Save to cache
        try:
            np.savez(cache_path, embeddings=self.column_embeddings, hash=np.array(current_hash))
            logger.info("Embeddings cached successfully")
        except Exception as e:
            logger.warning(f"Could not cache embeddings: {e}")

    def explain_column(self, column_name: str, top_k: int = 1) -> str:
        """
        Get explanation for a column using semantic search.

        Args:
            column_name: Name of the column to explain
            top_k: Number of top results to return

        Returns:
            Explanation string
        """
        if len(self.column_embeddings) == 0:
            self.load_metadata()
            self.build_embeddings()

        if len(self.column_embeddings) == 0:
            return f"No metadata available for {column_name}"

        # First try exact match
        for entry in self.glossary:
            if entry.get('name', '').lower() == column_name.lower():
                desc = entry.get('description', 'No description available')
                pii = " [PII]" if entry.get('pii', False) else ""
                return f"{desc}{pii}"

        # Fall back to semantic search
        self.load_model()
        query_embedding = self.model.encode([column_name])

        # Compute cosine similarity
        similarities = cosine_similarity(query_embedding, self.column_embeddings)[0]

        # Get top match
        top_idx = np.argmax(similarities)
        top_score = similarities[top_idx]

        if top_score > self.explain_threshold:
            entry = self.glossary[top_idx]
            return entry.get('description', 'No description available')

        return f"No explanation found for {column_name}"

    def explain_mapping(self, source_col: str, target_col: str) -> str:
        """
        Get explanation for a schema mapping (source -> target).

        Args:
            source_col: Source column name
            target_col: Target column name

        Returns:
            Mapping rationale string
        """
        if not self.mappings:
            self.load_metadata()

        # Look for exact mapping
        for mapping in self.mappings:
            if (mapping.get('source', '').lower() == source_col.lower() and
                mapping.get('target', '').lower() == target_col.lower()):
                return mapping.get('rationale', 'No rationale provided')

        # Try reverse mapping
        for mapping in self.mappings:
            if (mapping.get('target', '').lower() == source_col.lower() and
                mapping.get('source', '').lower() == target_col.lower()):
                return f"Reverse mapping: {mapping.get('rationale', 'No rationale provided')}"

        return f"No mapping found between {source_col} and {target_col}"

    def find_potential_mapping(self, column_name: str, top_k: int = 3) -> List[Dict]:
        """
        Find potential mappings for a column using semantic search.

        Args:
            column_name: Column name to find mappings for
            top_k: Number of top suggestions

        Returns:
            List of potential mapping dicts with score
        """
        if len(self.column_embeddings) == 0:
            self.load_metadata()
            self.build_embeddings()

        if len(self.column_embeddings) == 0:
            return []

        self.load_model()
        query_embedding = self.model.encode([column_name])

        # Compute similarities
        similarities = cosine_similarity(query_embedding, self.column_embeddings)[0]

        # Get top-k indices
        top_indices = np.argsort(similarities)[-top_k:][::-1]

        results = []
        for idx in top_indices:
            if similarities[idx] > self.mapping_threshold:
                entry = self.glossary[idx]
                results.append({
                    'column': entry.get('name'),
                    'description': entry.get('description'),
                    'similarity': float(similarities[idx]),
                    'system': entry.get('system')
                })

        return results

    def enrich_schema_diff(self, schema_diff: Dict) -> Dict[str, str]:
        """
        Enrich schema diff with RAG explanations.

        For each missing column, the knowledge-base mappings.json is consulted first.
        If a confirmed mapping exists, its rationale is used directly (highest accuracy).
        Only columns with no confirmed mapping fall back to cosine-similarity suggestions.

        Args:
            schema_diff: Schema comparison result dict

        Returns:
            Dict mapping column_name -> explanation
        """
        # Ensure metadata (glossary + mappings) is loaded before we use it
        if not self.mappings and not self.glossary:
            self.load_metadata()
            self.build_embeddings()

        # Build a fast lookup: source_col -> mapping entry
        mapping_lookup = {
            m.get("source", "").lower(): m
            for m in self.mappings
        }

        explanations = {}

        # Explain missing columns
        for col in schema_diff.get('missing_in_modern', []):
            confirmed = mapping_lookup.get(col.lower())
            if confirmed:
                # Use the confirmed mapping rationale directly
                target = confirmed.get("target")
                m_type = confirmed.get("type", "removed")
                rationale = confirmed.get("rationale", "")
                if target:
                    explanations[col] = f"Maps to '{target}' ({m_type}): {rationale}"
                else:
                    explanations[col] = f"{m_type.upper()}: {rationale}"
            else:
                # No confirmed mapping — fall back to cosine-similarity suggestion
                potential = self.find_potential_mapping(col, top_k=1)
                if potential:
                    explanations[col] = f"Possibly mapped to '{potential[0]['column']}': {potential[0]['description']}"
                else:
                    explanations[col] = self.explain_column(col)

        # Explain type mismatches
        for mismatch in schema_diff.get('type_mismatches', []):
            col = mismatch['column']
            explanations[col] = self.explain_column(col)

        return explanations


# Module-level convenience functions

_rag_instance = None


def get_rag_tool(metadata_dir: str = './metadata', config: Optional[Dict] = None) -> RAGTool:
    """Get or create a singleton RAG tool instance."""
    global _rag_instance
    if _rag_instance is None:
        rag_config = (config or {}).get('rag', {})
        _rag_instance = RAGTool(
            metadata_dir,
            explain_threshold=rag_config.get('explain_threshold', 0.5),
            mapping_threshold=rag_config.get('mapping_threshold', 0.3),
        )
    return _rag_instance


def explain_column(column_name: str, metadata_dir: str = './metadata') -> str:
    """Convenience function to explain a column."""
    tool = get_rag_tool(metadata_dir)
    return tool.explain_column(column_name)


def explain_mapping(source_col: str, target_col: str, metadata_dir: str = './metadata') -> str:
    """Convenience function to explain a mapping."""
    tool = get_rag_tool(metadata_dir)
    return tool.explain_mapping(source_col, target_col)
