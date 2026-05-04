"""
Auto-generated Pandera schema for employers
Generated from: legacy
"""
import pandera as pa
from pandera import Column, DataFrameSchema


EmployersSchema = DataFrameSchema({
    "er_recid": Column(int, nullable=False, coerce=True),
    "er_name": Column(str, nullable=False, coerce=True),
    "er_ein": Column(str, nullable=True, coerce=True),
    "er_ind": Column(str, nullable=True, coerce=True),
    "er_adr1": Column(str, nullable=True, coerce=True),
    "er_city": Column(str, nullable=True, coerce=True),
    "er_st": Column(str, nullable=True, coerce=True),
    "er_zip": Column(str, nullable=True, coerce=True),
    "er_phon": Column(str, nullable=True, coerce=True),
    "er_stat": Column(str, nullable=True, coerce=True),
}, strict=False, coerce=True)
