"""
COBOL Copybook Parser

Parses COBOL copybook (.cpy) files to extract field definitions including
name, PIC clause, offset, length, and data type. Handles REDEFINES, OCCURS,
and nested group levels.

Converts PIC clauses to SQL-compatible types for schema generation.
"""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class CopybookField:
    """A single field parsed from a COBOL copybook."""
    level: int
    name: str
    pic: str  # raw PIC clause (e.g., "X(25)", "9(5)V99", "S9(7)")
    offset: int  # byte offset in the record
    length: int  # byte length
    sql_type: str  # mapped SQL type
    decimals: int = 0  # decimal places for numeric fields
    occurs: int = 1  # OCCURS count (arrays)
    redefines: Optional[str] = None  # field this redefines
    is_filler: bool = False
    is_group: bool = False  # group item (no PIC, contains children)
    description: str = ""  # from comments


@dataclass
class CopybookLayout:
    """Complete parsed layout from a copybook file."""
    name: str  # copybook/record name
    fields: List[CopybookField] = field(default_factory=list)
    record_length: int = 0
    source_file: str = ""

    def data_fields(self) -> List[CopybookField]:
        """Return only non-filler, non-group fields (actual data columns)."""
        return [f for f in self.fields if not f.is_filler and not f.is_group]

    def to_schema(self) -> List[Dict]:
        """Convert to the schema format expected by BaseConnector.get_table_schema()."""
        return [
            {
                "column_name": f.name.lower().replace("-", "_"),
                "data_type": f.sql_type,
                "is_nullable": "YES",
                "pic": f.pic,
                "length": f.length,
                "offset": f.offset,
                "decimals": f.decimals,
            }
            for f in self.data_fields()
        ]


# ── PIC clause parsing ───────────────────────────────────────────────

def _expand_pic(pic: str) -> str:
    """Expand PIC shorthand: X(5) -> XXXXX, 9(3)V99 -> 999V99."""
    def _expand_group(match):
        char = match.group(1)
        count = int(match.group(2))
        return char * count
    return re.sub(r'([X9AZ])\((\d+)\)', _expand_group, pic.upper())


def _pic_length(pic: str) -> int:
    """Calculate byte length from a PIC clause."""
    expanded = _expand_pic(pic)
    # Remove sign (S), assumed decimal (V), and usage indicators
    clean = expanded.replace("S", "").replace("V", "").replace(".", "")
    return len(clean)


def _pic_decimals(pic: str) -> int:
    """Count decimal places from a PIC clause (digits after V)."""
    expanded = _expand_pic(pic)
    if "V" in expanded:
        return len(expanded.split("V")[1])
    return 0


def _pic_to_sql_type(pic: str) -> str:
    """Map a COBOL PIC clause to a SQL data type."""
    upper = pic.upper().strip()
    expanded = _expand_pic(upper)

    # Alphanumeric: PIC X, PIC X(n), PIC A(n)
    if re.match(r'^[XA]', expanded.replace("S", "")):
        length = _pic_length(upper)
        if length <= 1:
            return "CHAR(1)"
        return f"VARCHAR({length})"

    # Numeric with decimals: PIC 9(5)V99, PIC S9(7)V9(2)
    if "V" in expanded or "." in upper:
        total_digits = len(re.sub(r'[^9]', '', expanded))
        decimal_places = _pic_decimals(upper)
        return f"NUMERIC({total_digits},{decimal_places})"

    # Pure numeric: PIC 9, PIC 9(n), PIC S9(n)
    if re.match(r'^S?9', expanded):
        digits = len(re.sub(r'[^9]', '', expanded))
        if digits <= 4:
            return "SMALLINT"
        if digits <= 9:
            return "INTEGER"
        if digits <= 18:
            return "BIGINT"
        return f"NUMERIC({digits})"

    # Fallback
    length = _pic_length(upper) or 50
    return f"VARCHAR({length})"


# ── Copybook file parser ─────────────────────────────────────────────

def parse_copybook(source: str, name: Optional[str] = None) -> CopybookLayout:
    """Parse a COBOL copybook from a string or file path.

    Args:
        source: Either a file path to a .cpy file, or the copybook text content.
        name: Optional name for the layout (defaults to filename or 'RECORD').

    Returns:
        CopybookLayout with all fields parsed.
    """
    # Detect if source is a file path or raw text
    is_file = False
    if "\n" not in source and len(source) < 500:
        source_path = Path(source)
        try:
            is_file = source_path.exists()
        except (OSError, ValueError):
            is_file = False

    if is_file:
        source_path = Path(source)
        text = source_path.read_text(encoding="utf-8", errors="replace")
        layout_name = name or source_path.stem.upper()
        source_file = str(source_path)
    else:
        text = source
        layout_name = name or "RECORD"
        source_file = ""

    fields = []
    offset = 0
    current_comment = ""

    # Normalize: join continuation lines (lines starting with spaces after a non-period line)
    lines = []
    for line in text.splitlines():
        # Skip comment lines (column 7 = *)
        stripped = line.rstrip()
        if len(stripped) > 6 and stripped[6] == "*":
            # Extract comment text for field descriptions
            current_comment = stripped[7:].strip()
            continue
        # Skip empty lines
        if not stripped or stripped.isspace():
            continue
        lines.append(stripped)

    # Join multi-line statements (everything until a period)
    statements = []
    current = ""
    for line in lines:
        # Strip sequence numbers (cols 1-6) and identification (cols 73+)
        content = line[6:72] if len(line) > 6 else line
        content = content.rstrip()
        current += " " + content
        if "." in content:
            statements.append(current.strip())
            current = ""
    if current.strip():
        statements.append(current.strip())

    for stmt in statements:
        # Remove trailing period
        stmt = stmt.rstrip(".")

        # Parse level number
        level_match = re.match(r'^\s*(\d{1,2})\s+', stmt)
        if not level_match:
            continue

        level = int(level_match.group(1))
        rest = stmt[level_match.end():].strip()

        # Skip 88-level condition names
        if level == 88:
            continue

        # Parse field name
        name_match = re.match(r'([\w-]+)', rest)
        if not name_match:
            continue
        field_name = name_match.group(1).upper()
        rest = rest[name_match.end():].strip()

        is_filler = field_name in ("FILLER", "FILLER-X")

        # Parse REDEFINES
        redefines = None
        redef_match = re.search(r'REDEFINES\s+([\w-]+)', rest, re.IGNORECASE)
        if redef_match:
            redefines = redef_match.group(1).upper()

        # Parse OCCURS
        occurs = 1
        occurs_match = re.search(r'OCCURS\s+(\d+)', rest, re.IGNORECASE)
        if occurs_match:
            occurs = int(occurs_match.group(1))

        # Parse PIC clause
        pic_match = re.search(r'PIC(?:TURE)?\s+([^\s.]+)', rest, re.IGNORECASE)
        if pic_match:
            pic = pic_match.group(1)
            length = _pic_length(pic) * occurs
            decimals = _pic_decimals(pic)
            sql_type = _pic_to_sql_type(pic)

            fields.append(CopybookField(
                level=level,
                name=field_name,
                pic=pic,
                offset=offset,
                length=length,
                sql_type=sql_type,
                decimals=decimals,
                occurs=occurs,
                redefines=redefines,
                is_filler=is_filler,
                is_group=False,
                description=current_comment,
            ))

            # Only advance offset for non-REDEFINES fields
            if redefines is None:
                offset += length
        else:
            # Group item (no PIC) — container for child fields
            fields.append(CopybookField(
                level=level,
                name=field_name,
                pic="",
                offset=offset,
                length=0,
                sql_type="",
                decimals=0,
                occurs=occurs,
                redefines=redefines,
                is_filler=is_filler,
                is_group=True,
                description=current_comment,
            ))

        current_comment = ""

    layout = CopybookLayout(
        name=layout_name,
        fields=fields,
        record_length=offset,
        source_file=source_file,
    )

    logger.info(
        f"Parsed copybook '{layout_name}': {len(layout.data_fields())} data fields, "
        f"{layout.record_length} bytes/record"
    )

    return layout
