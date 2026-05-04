"""
Auto-generated Pandera schema for claims
Generated from: legacy
"""
import pandera as pa
from pandera import Column, DataFrameSchema


ClaimsSchema = DataFrameSchema({
    "cm_recid": Column(int, nullable=True, coerce=True),
    "cm_clmnt": Column(int, nullable=True, coerce=True),
    "cm_emplr": Column(int, nullable=True, coerce=True),
    "cm_seprs": Column(str, nullable=True, coerce=True),
    "cm_fildt": Column(str, nullable=True, coerce=True),
    "cm_bystr": Column(str, nullable=True, coerce=True),
    "cm_byend": Column(str, nullable=True, coerce=True),
    "cm_wkamt": Column(float, nullable=True, coerce=True),
    "cm_mxamt": Column(float, nullable=True, coerce=True),
    "cm_totpd": Column(float, nullable=True, coerce=True),
    "cm_wkcnt": Column(int, nullable=True, coerce=True),
    "cm_stat": Column(str, nullable=True, coerce=True),
    "cm_lupdt": Column(str, nullable=True, coerce=True),
}, strict=False, coerce=True)
