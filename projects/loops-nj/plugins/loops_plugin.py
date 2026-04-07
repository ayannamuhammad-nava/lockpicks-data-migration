"""
LOOPS NJ UI Modernization Plugin

Domain-specific validation rules for the LOOPS legacy system migration
to the modernized PostgreSQL data model. Provides:
  - Curated column mapping overrides (HIPAA, PCI-DSS compliance)
  - Cross-field data quality anomaly rules (deceased-active check)
"""

from dm.hookspecs import hookimpl


class LoopsPlugin:
    """LOOPS-specific validation rules for NJ UI claimants migration."""

    @hookimpl
    def dm_get_column_overrides(self, table):
        """Return curated column mapping overrides for LOOPS tables."""
        overrides = _OVERRIDES.get(table.lower(), {})
        return overrides

    @hookimpl
    def dm_data_quality_rules(self, dataset):
        """Return LOOPS-specific cross-field data quality rules."""
        if dataset.lower() == "claimants":
            return [{
                "name": "deceased_active",
                "severity": "HIGH",
                "description": "Deceased claimant with active benefit status",
                "check_fn": self._check_deceased_active,
            }]
        return []

    def _check_deceased_active(self, df):
        """Flag deceased claimants that still have active status."""
        if "cl_dcsd" not in df.columns or "cl_stat" not in df.columns:
            return None

        deceased_mask = df["cl_dcsd"].astype(str).str.strip() == "Y"
        active_mask = df["cl_stat"].astype(str).str.strip().str.upper().isin(["ACTIVE", "ACT"])
        bad_rows = df[deceased_mask & active_mask]

        if bad_rows.empty:
            return None

        ids = bad_rows["cl_recid"].tolist() if "cl_recid" in bad_rows.columns else []
        return {
            "count": len(bad_rows),
            "record_ids": ids[:5],
            "detail": "cl_dcsd = 'Y'  AND  cl_stat IN ('ACTIVE', 'ACT')",
            "risk": (
                "Active benefits may be disbursed to deceased claimants — "
                "potential fraud exposure and audit finding. "
                "Migrating these records as-is carries the anomaly into production."
            ),
            "action": (
                "Close or suspend affected records before migration. "
                "Refer to benefits fraud review team."
            ),
        }


# ── Curated Column Mapping Overrides ─────────────────────────────────
# These take precedence over auto-generated fuzzy-match results so that
# running `dm discover` never overwrites compliance-critical rationales.

_OVERRIDES = {
    "claimants": {
        "cl_ssn": {
            "target": "ssn_hash",
            "rationale": (
                "SHA-256 hash for HIPAA compliance — raw SSN must NEVER be written to modern system; "
                "duplicate SSNs indicate identity conflicts requiring manual resolution before migration proceeds"
            ),
            "confidence": 1.0,
            "type": "transform",
        },
        "cl_bact": {
            "target": None,
            "rationale": (
                "Bank account number NOT migrated — PCI-DSS regulated financial identifier; "
                "must be archived to encrypted vault and claimants re-enrolled through secure portal. "
                "Archive alongside cl_brtn. Any plaintext exposure is a PCI-DSS Level 1 violation. "
                "Do NOT include in ETL pipeline."
            ),
            "confidence": 1.0,
            "type": "archived",
        },
        "cl_brtn": {
            "target": None,
            "rationale": (
                "Bank routing number NOT migrated — PCI-DSS regulated financial identifier. "
                "Archive alongside cl_bact to encrypted vault. Routing numbers combined with account numbers "
                "constitute full banking credentials — any plaintext exposure is a PCI-DSS Level 1 violation. "
                "Do NOT include in ETL pipeline."
            ),
            "confidence": 1.0,
            "type": "archived",
        },
        "cl_stat": {
            "target": "claimant_status",
            "rationale": (
                "Status codes normalised: 'ACTIVE'->'active', 'INACTIVE'->'inactive', 'SUSPENDED'->'suspended'. "
                "Legacy freetext values must be mapped; unmapped values default to 'pending_review' and flagged for manual triage."
            ),
            "confidence": 1.0,
            "type": "transform",
        },
        "cl_fil1": {
            "target": None,
            "rationale": (
                "Legacy filler/padding field with no business meaning — safe to drop. "
                "Verify no application code reads this field before finalising ETL."
            ),
            "confidence": 1.0,
            "type": "removed",
        },
    },
    "employers": {
        "er_recid": {
            "target": "employer_id",
            "rationale": (
                "er_recid (legacy integer sequence) migrated to UUID-based employer_id in modern system; "
                "ETL must generate stable UUIDs and update all FK references in claims and payments before cutover."
            ),
            "confidence": 1.0,
            "type": "transform",
        },
        "er_stat": {
            "target": "employer_status",
            "rationale": (
                "Employer status codes normalised to modern enum values; "
                "legacy freetext statuses require mapping table review before ETL runs."
            ),
            "confidence": 1.0,
            "type": "transform",
        },
    },
    "claims": {
        "cm_emplr": {
            "target": "employer_id",
            "rationale": (
                "Renamed to employer_id as FK to employers table; "
                "orphan risk if employer records not fully migrated first — run employer migration before claims ETL."
            ),
            "confidence": 1.0,
            "type": "rename",
        },
        "cm_bystr": {
            "target": "benefit_year_start",
            "rationale": (
                "Renamed from cm_bystr to benefit_year_start; date format normalised from YYYYMMDD integer "
                "to ISO-8601 date string — ETL must parse and reformat all values."
            ),
            "confidence": 1.0,
            "type": "transform",
        },
        "cm_stat": {
            "target": "claim_status",
            "rationale": (
                "Claim status normalised to modern enum; CRITICAL business rule: claims with cm_stat='PAID' "
                "where cm_totpd < cm_mxamt indicate potential overpayment — flag for compliance review before migration completes."
            ),
            "confidence": 1.0,
            "type": "transform",
        },
    },
    "benefit_payments": {
        "bp_stat": {
            "target": "payment_status",
            "rationale": (
                "Payment status normalised to modern enum values ('ISSUED', 'CLEARED', 'VOIDED'); "
                "legacy 'CANC' code maps to 'VOIDED' — validate all payment records before and after transform."
            ),
            "confidence": 1.0,
            "type": "transform",
        },
    },
}
