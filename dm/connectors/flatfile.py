"""
Flat File / Copybook Connector

Implements BaseConnector for reading mainframe data extracts:
  - Fixed-width files parsed using COBOL copybook layout
  - CSV/TSV files
  - EBCDIC-encoded files (auto-converted to UTF-8)

Config in project.yaml:

    connections:
      mainframe_extract:
        type: copybook
        copybook: /data/CLAIMANT.cpy
        datafile: /data/CLAIMANT.dat
        encoding: ebcdic          # or utf-8, ascii (default: utf-8)
        format: fixed             # or csv (default: fixed)
        table_name: claimants     # logical table name

      csv_feed:
        type: flatfile
        datafile: /data/federal_qc_sample.csv
        format: csv
        delimiter: ","
        table_name: federal_sample
"""

import csv
import hashlib
import logging
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from dm.connectors.base import BaseConnector
from dm.connectors.copybook_parser import CopybookLayout, parse_copybook

logger = logging.getLogger(__name__)

# EBCDIC to ASCII translation table (CP037 — US/Canada mainframes)
try:
    _EBCDIC_CODEC = "cp037"
except Exception:
    _EBCDIC_CODEC = None


class FlatFileConnector(BaseConnector):
    """Connector for flat files (fixed-width via copybook, CSV, TSV)."""

    def __init__(self, config: Dict):
        super().__init__(config)
        self._df: Optional[pd.DataFrame] = None
        self._layout: Optional[CopybookLayout] = None
        self._table_name = config.get("table_name", "data")

    def connect(self) -> Any:
        datafile = self.config.get("datafile", self.config.get("path", ""))
        file_format = self.config.get("format", "").lower()
        encoding = self.config.get("encoding", "utf-8").lower()
        copybook_path = self.config.get("copybook", "")

        if not datafile:
            raise ValueError("Flat file connector requires 'datafile' in config")

        data_path = Path(datafile)
        if not data_path.exists():
            raise FileNotFoundError(f"Data file not found: {datafile}")

        # Auto-detect format
        if not file_format:
            if copybook_path:
                file_format = "fixed"
            elif data_path.suffix.lower() in (".csv", ".tsv", ".txt"):
                file_format = "csv"
            else:
                file_format = "fixed" if copybook_path else "csv"

        if file_format == "fixed":
            self._df = self._read_fixed_width(data_path, copybook_path, encoding)
        elif file_format in ("csv", "tsv"):
            self._df = self._read_csv(data_path, encoding)
        else:
            raise ValueError(f"Unknown format '{file_format}'. Use 'fixed' or 'csv'.")

        logger.info(
            f"Loaded {len(self._df)} rows, {len(self._df.columns)} columns "
            f"from {data_path.name} ({file_format})"
        )
        self._conn = self._df
        return self._df

    def _read_fixed_width(
        self, data_path: Path, copybook_path: str, encoding: str
    ) -> pd.DataFrame:
        """Read a fixed-width file using a copybook layout."""
        if not copybook_path:
            raise ValueError(
                "Fixed-width format requires a 'copybook' path in config"
            )

        cpy_path = Path(copybook_path)
        if not cpy_path.exists():
            raise FileNotFoundError(f"Copybook not found: {copybook_path}")

        self._layout = parse_copybook(str(cpy_path))
        fields = self._layout.data_fields()

        # Read raw bytes
        if encoding in ("ebcdic", "cp037", "cp500"):
            raw = data_path.read_bytes()
            text = raw.decode(_EBCDIC_CODEC or "cp037", errors="replace")
        else:
            text = data_path.read_text(encoding=encoding, errors="replace")

        # Parse fixed-width records
        record_len = self._layout.record_length
        rows = []
        lines = text.splitlines() if record_len == 0 else None

        if lines:
            # Line-based: each line is a record
            for line in lines:
                if not line.strip():
                    continue
                row = {}
                for f in fields:
                    val = line[f.offset:f.offset + f.length].strip()
                    row[f.name.lower().replace("-", "_")] = val
                rows.append(row)
        else:
            # Byte-based: split by record length
            pos = 0
            while pos + record_len <= len(text):
                record = text[pos:pos + record_len]
                row = {}
                for f in fields:
                    val = record[f.offset:f.offset + f.length].strip()
                    row[f.name.lower().replace("-", "_")] = val
                rows.append(row)
                pos += record_len

        return pd.DataFrame(rows)

    def _read_csv(self, data_path: Path, encoding: str) -> pd.DataFrame:
        """Read a CSV/TSV file."""
        delimiter = self.config.get("delimiter", ",")
        if encoding in ("ebcdic", "cp037", "cp500"):
            raw = data_path.read_bytes()
            text = raw.decode(_EBCDIC_CODEC or "cp037", errors="replace")
            from io import StringIO
            return pd.read_csv(StringIO(text), delimiter=delimiter)
        else:
            return pd.read_csv(data_path, delimiter=delimiter, encoding=encoding)

    def close(self) -> None:
        self._df = None
        self._conn = None

    # ── Schema introspection ─────────────────────────────────────────

    def get_table_schema(self, table_name: str) -> List[Dict]:
        if self._layout:
            return self._layout.to_schema()

        # CSV fallback: infer from DataFrame columns
        df = self._ensure_loaded()
        schema = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            if "int" in dtype:
                sql_type = "INTEGER"
            elif "float" in dtype:
                sql_type = "NUMERIC"
            else:
                max_len = df[col].astype(str).str.len().max()
                sql_type = f"VARCHAR({max(10, int(max_len * 1.5))})"

            schema.append({
                "column_name": col,
                "data_type": sql_type,
                "is_nullable": "YES" if df[col].isnull().any() else "NO",
            })
        return schema

    # ── Query execution ──────────────────────────────────────────────

    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        df = self._ensure_loaded()
        # Simple query support: SELECT * FROM table_name
        # For flat files, any query returns the full dataframe
        # More complex queries could use pandasql in the future
        return df

    def execute_scalar(self, query: str, params: Optional[tuple] = None) -> Any:
        df = self._ensure_loaded()
        # Handle common scalar queries
        q = query.strip().upper()
        if "COUNT(*)" in q:
            return len(df)
        return df.iloc[0, 0] if not df.empty else None

    # ── Validation helpers ───────────────────────────────────────────

    def get_row_count(self, table_name: str) -> int:
        return len(self._ensure_loaded())

    def get_column_hash(self, table_name: str, column_name: str) -> str:
        df = self._ensure_loaded()
        if column_name not in df.columns:
            return ""
        values = df[column_name].sort_values().astype(str).str.cat()
        return hashlib.md5(values.encode()).hexdigest()

    def check_referential_integrity(
        self,
        child_table: str,
        parent_table: str,
        fk_column: str,
        pk_column: Optional[str] = None,
    ) -> Dict:
        # Can't check cross-table FK on a single flat file
        return {"orphan_count": 0, "orphan_sample": [], "note": "flat file — no FK check"}

    def get_null_percentage(self, table_name: str, column_name: str) -> float:
        df = self._ensure_loaded()
        if column_name not in df.columns:
            return 0.0
        null_count = df[column_name].isnull().sum() + (df[column_name] == "").sum()
        return round(null_count / len(df) * 100, 2) if len(df) > 0 else 0.0

    def get_duplicate_count(self, table_name: str, column_name: str) -> int:
        df = self._ensure_loaded()
        if column_name not in df.columns:
            return 0
        counts = df[column_name].value_counts()
        return int((counts > 1).sum())

    def compute_checksum(self, table_name: str, columns: List[str]) -> str:
        df = self._ensure_loaded()
        available = [c for c in columns if c in df.columns]
        if not available:
            return ""
        combined = df[available].fillna("NULL").astype(str).agg("".join, axis=1)
        sorted_vals = combined.sort_values().str.cat()
        return hashlib.md5(sorted_vals.encode()).hexdigest()

    def _ensure_loaded(self) -> pd.DataFrame:
        if self._df is None:
            self.connect()
        return self._df
