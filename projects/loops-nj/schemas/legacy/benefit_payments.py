"""
Auto-generated Pandera schema for benefit_payments
Generated from: legacy
"""
import pandera as pa
from pandera import Column, DataFrameSchema


Benefit_paymentsSchema = DataFrameSchema({
    "bp_recid": Column(int, nullable=True, coerce=True),
    "bp_clmid": Column(int, nullable=True, coerce=True),
    "bp_paydt": Column(str, nullable=True, coerce=True),
    "bp_payam": Column(float, nullable=True, coerce=True),
    "bp_methd": Column(str, nullable=True, coerce=True),
    "bp_wkedt": Column(str, nullable=True, coerce=True),
    "bp_stat": Column(str, nullable=True, coerce=True),
    "bp_chkno": Column(str, nullable=True, coerce=True),
}, strict=False, coerce=True)
