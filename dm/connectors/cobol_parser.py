"""
COBOL Program Parser — Business Rule Extraction

Parses COBOL source files (.cbl, .cob) to extract business rules:
  - Validation rules (IF/EVALUATE conditions)
  - Calculation rules (COMPUTE/ADD/SUBTRACT/MULTIPLY/DIVIDE)
  - Data movement rules (MOVE with transformations)
  - Process flow (PERFORM paragraphs)
  - External dependencies (CALL statements)
  - Default values and initializations

Produces structured JSON output for the dashboard.
"""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class BusinessRule:
    """A single business rule extracted from COBOL source."""
    rule_type: str  # validation, calculation, data_movement, process_flow, external_call, default
    description: str  # human-readable summary
    source_line: int  # line number in source file
    source_text: str  # raw COBOL text
    condition: str = ""  # IF/EVALUATE condition
    action: str = ""  # what happens when condition is true
    fields: List[str] = field(default_factory=list)  # fields involved
    paragraph: str = ""  # which paragraph this rule is in
    severity: str = "INFO"  # INFO, MEDIUM, HIGH (for complex rules)


@dataclass
class ProgramAnalysis:
    """Complete analysis of a COBOL program."""
    program_name: str
    source_file: str
    total_lines: int
    division_lines: Dict[str, int] = field(default_factory=dict)  # {division: line_count}
    paragraphs: List[str] = field(default_factory=list)
    rules: List[BusinessRule] = field(default_factory=list)
    copybooks_used: List[str] = field(default_factory=list)
    programs_called: List[str] = field(default_factory=list)
    fields_referenced: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "program_name": self.program_name,
            "source_file": self.source_file,
            "total_lines": self.total_lines,
            "division_lines": self.division_lines,
            "paragraphs": self.paragraphs,
            "rules": [
                {
                    "type": r.rule_type,
                    "description": r.description,
                    "source_line": r.source_line,
                    "source_text": r.source_text,
                    "condition": r.condition,
                    "action": r.action,
                    "fields": r.fields,
                    "paragraph": r.paragraph,
                    "severity": r.severity,
                }
                for r in self.rules
            ],
            "copybooks_used": self.copybooks_used,
            "programs_called": self.programs_called,
            "fields_referenced": sorted(set(self.fields_referenced)),
            "summary": {
                "total_rules": len(self.rules),
                "validations": sum(1 for r in self.rules if r.rule_type == "validation"),
                "calculations": sum(1 for r in self.rules if r.rule_type == "calculation"),
                "data_movements": sum(1 for r in self.rules if r.rule_type == "data_movement"),
                "process_flows": sum(1 for r in self.rules if r.rule_type == "process_flow"),
                "external_calls": sum(1 for r in self.rules if r.rule_type == "external_call"),
                "defaults": sum(1 for r in self.rules if r.rule_type == "default"),
            },
        }


def parse_cobol_program(source: str, name: Optional[str] = None) -> ProgramAnalysis:
    """Parse a COBOL program and extract business rules.

    Args:
        source: File path to a .cbl/.cob file, or raw COBOL source text.
        name: Optional program name (defaults to filename).

    Returns:
        ProgramAnalysis with extracted rules.
    """
    source_path = None
    if "\n" not in source and len(source) < 500:
        p = Path(source)
        try:
            if p.exists():
                source_path = p
        except (OSError, ValueError):
            pass

    if source_path:
        text = source_path.read_text(encoding="utf-8", errors="replace")
        prog_name = name or source_path.stem.upper()
        source_file = str(source_path)
    else:
        text = source
        prog_name = name or "PROGRAM"
        source_file = ""

    lines = text.splitlines()
    analysis = ProgramAnalysis(
        program_name=prog_name,
        source_file=source_file,
        total_lines=len(lines),
    )

    # Clean lines: strip sequence numbers (cols 1-6) and identification (cols 73+)
    clean_lines = []
    for i, line in enumerate(lines):
        if len(line) > 6:
            # Skip comment lines (col 7 = *)
            if line[6] == '*':
                clean_lines.append(("", i + 1))
                continue
            content = line[6:72] if len(line) > 72 else line[6:]
            clean_lines.append((content.rstrip(), i + 1))
        else:
            clean_lines.append((line.rstrip(), i + 1))

    # Track which division/paragraph we're in
    current_division = ""
    current_paragraph = ""
    division_line_counts = {}

    for content, line_num in clean_lines:
        upper = content.upper().strip()

        # Track divisions
        if "DIVISION" in upper and upper.endswith("."):
            for div in ["IDENTIFICATION", "ENVIRONMENT", "DATA", "PROCEDURE"]:
                if div in upper:
                    current_division = div
                    division_line_counts[div] = 0
                    break

        if current_division:
            division_line_counts[current_division] = division_line_counts.get(current_division, 0) + 1

        # Only extract rules from PROCEDURE DIVISION
        if current_division != "PROCEDURE":
            # But check for COPY statements anywhere
            copy_match = re.search(r'COPY\s+([\w-]+)', upper)
            if copy_match:
                analysis.copybooks_used.append(copy_match.group(1))
            continue

        # Detect paragraph names (start at column 8, end with period)
        if content and not content.startswith(" " * 4) and upper.endswith(".") and " " not in upper.strip().rstrip("."):
            current_paragraph = upper.rstrip(".")
            analysis.paragraphs.append(current_paragraph)
            continue

        # Extract rules
        _extract_rules(content, upper, line_num, current_paragraph, analysis)

    analysis.division_lines = division_line_counts

    logger.info(
        f"Parsed {prog_name}: {len(lines)} lines, "
        f"{len(analysis.rules)} rules, {len(analysis.paragraphs)} paragraphs"
    )

    return analysis


def _extract_rules(content: str, upper: str, line_num: int, paragraph: str, analysis: ProgramAnalysis):
    """Extract business rules from a single line of PROCEDURE DIVISION."""

    # ── Validation: IF conditions ──
    if_match = re.match(r'\s*IF\s+(.+)', upper)
    if if_match:
        condition = if_match.group(1).rstrip(".")
        fields = _extract_fields(condition)
        analysis.fields_referenced.extend(fields)

        # Determine severity
        severity = "INFO"
        if any(kw in condition for kw in ["NOT", "NUMERIC", "ALPHABETIC", "ZERO", "SPACE"]):
            severity = "MEDIUM"
        if any(kw in condition for kw in ["INVALID", "ERROR", "REJECT"]):
            severity = "HIGH"

        analysis.rules.append(BusinessRule(
            rule_type="validation",
            description=f"Condition check: {_simplify_condition(condition)}",
            source_line=line_num,
            source_text=content.strip(),
            condition=condition,
            fields=fields,
            paragraph=paragraph,
            severity=severity,
        ))
        return

    # ── Validation: EVALUATE (switch/case) ──
    eval_match = re.match(r'\s*EVALUATE\s+(.+)', upper)
    if eval_match:
        subject = eval_match.group(1).rstrip(".")
        fields = _extract_fields(subject)
        analysis.fields_referenced.extend(fields)
        analysis.rules.append(BusinessRule(
            rule_type="validation",
            description=f"Decision table on {subject}",
            source_line=line_num,
            source_text=content.strip(),
            condition=f"EVALUATE {subject}",
            fields=fields,
            paragraph=paragraph,
            severity="MEDIUM",
        ))
        return

    # ── Calculation: COMPUTE ──
    compute_match = re.match(r'\s*COMPUTE\s+([\w-]+)\s*=\s*(.+)', upper)
    if compute_match:
        target = compute_match.group(1)
        formula = compute_match.group(2).rstrip(".")
        fields = [target] + _extract_fields(formula)
        analysis.fields_referenced.extend(fields)
        analysis.rules.append(BusinessRule(
            rule_type="calculation",
            description=f"Calculate {target} = {formula}",
            source_line=line_num,
            source_text=content.strip(),
            action=f"{target} = {formula}",
            fields=fields,
            paragraph=paragraph,
        ))
        return

    # ── Calculation: ADD/SUBTRACT/MULTIPLY/DIVIDE ──
    arith_match = re.match(r'\s*(ADD|SUBTRACT|MULTIPLY|DIVIDE)\s+(.+)', upper)
    if arith_match:
        op = arith_match.group(1)
        rest = arith_match.group(2).rstrip(".")
        fields = _extract_fields(rest)
        analysis.fields_referenced.extend(fields)
        analysis.rules.append(BusinessRule(
            rule_type="calculation",
            description=f"{op} operation: {rest}",
            source_line=line_num,
            source_text=content.strip(),
            action=f"{op} {rest}",
            fields=fields,
            paragraph=paragraph,
        ))
        return

    # ── Data Movement: MOVE ──
    move_match = re.match(r'\s*MOVE\s+(.+?)\s+TO\s+(.+)', upper)
    if move_match:
        source_val = move_match.group(1).rstrip(".")
        target_val = move_match.group(2).rstrip(".")
        fields = _extract_fields(source_val) + _extract_fields(target_val)
        analysis.fields_referenced.extend(fields)

        # Is this a default/initialization or a transform?
        is_literal = source_val.startswith("'") or source_val.startswith('"') or source_val in (
            "SPACES", "ZEROS", "ZEROES", "LOW-VALUES", "HIGH-VALUES", "SPACE", "ZERO"
        )
        rule_type = "default" if is_literal else "data_movement"
        desc = f"Set {target_val} to {source_val}" if is_literal else f"Move {source_val} to {target_val}"

        analysis.rules.append(BusinessRule(
            rule_type=rule_type,
            description=desc,
            source_line=line_num,
            source_text=content.strip(),
            action=f"MOVE {source_val} TO {target_val}",
            fields=fields,
            paragraph=paragraph,
        ))
        return

    # ── Process Flow: PERFORM ──
    perform_match = re.match(r'\s*PERFORM\s+([\w-]+)', upper)
    if perform_match:
        target_para = perform_match.group(1)
        if target_para not in ("UNTIL", "VARYING", "TIMES", "WITH", "TEST"):
            analysis.rules.append(BusinessRule(
                rule_type="process_flow",
                description=f"Execute {target_para}",
                source_line=line_num,
                source_text=content.strip(),
                action=f"PERFORM {target_para}",
                paragraph=paragraph,
            ))
        return

    # ── External Call: CALL ──
    call_match = re.match(r"\s*CALL\s+['\"]?([\w-]+)['\"]?", upper)
    if call_match:
        called_prog = call_match.group(1)
        analysis.programs_called.append(called_prog)
        analysis.rules.append(BusinessRule(
            rule_type="external_call",
            description=f"Call external program {called_prog}",
            source_line=line_num,
            source_text=content.strip(),
            action=f"CALL {called_prog}",
            paragraph=paragraph,
            severity="HIGH",
        ))
        return


def _extract_fields(text: str) -> List[str]:
    """Extract COBOL field names from a text fragment."""
    # Match COBOL field names: letters, digits, hyphens (at least one hyphen to filter keywords)
    candidates = re.findall(r'\b([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)+)\b', text.upper())
    # Filter out COBOL keywords
    KEYWORDS = {
        "NOT", "AND", "OR", "EQUAL", "GREATER", "LESS", "THAN",
        "TO", "FROM", "BY", "GIVING", "INTO", "REMAINDER",
        "ON", "SIZE", "ERROR", "END-IF", "END-EVALUATE", "END-PERFORM",
        "END-COMPUTE", "END-ADD", "END-SUBTRACT", "END-MULTIPLY", "END-DIVIDE",
        "END-CALL", "END-READ", "END-WRITE", "END-STRING", "END-UNSTRING",
        "LOW-VALUES", "HIGH-VALUES", "LOW-VALUE", "HIGH-VALUE",
    }
    return [f for f in candidates if f not in KEYWORDS]


def _simplify_condition(condition: str) -> str:
    """Simplify a COBOL condition for display."""
    # Remove extra whitespace
    simplified = re.sub(r'\s+', ' ', condition).strip()
    # Truncate if too long
    if len(simplified) > 80:
        simplified = simplified[:77] + "..."
    return simplified


def scan_programs(repo_path: str) -> List[ProgramAnalysis]:
    """Scan a directory for COBOL programs and parse all of them.

    Args:
        repo_path: Path to scan for .cbl and .cob files.

    Returns:
        List of ProgramAnalysis objects.
    """
    repo = Path(repo_path)
    programs = []

    for f in sorted(repo.rglob("*")):
        if f.suffix.lower() in (".cbl", ".cob") and f.is_file():
            try:
                analysis = parse_cobol_program(str(f))
                if analysis.rules:  # Only include programs with extractable rules
                    programs.append(analysis)
            except Exception as e:
                logger.warning(f"Failed to parse {f.name}: {e}")

    logger.info(f"Scanned {repo_path}: {len(programs)} programs with business rules")
    return programs
