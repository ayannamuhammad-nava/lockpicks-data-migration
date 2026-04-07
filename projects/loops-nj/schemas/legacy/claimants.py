"""
Auto-generated Pandera schema for claimants
Generated from: legacy
"""
import pandera as pa
from pandera import Column, DataFrameSchema


ClaimantsSchema = DataFrameSchema({
    "cl_recid": Column(int, nullable=True, coerce=True),
    "cl_fnam": Column(str, nullable=True, coerce=True),
    "cl_lnam": Column(str, nullable=True, coerce=True),
    "cl_ssn": Column(str, nullable=True, coerce=True),
    "cl_dob": Column(str, nullable=True, coerce=True),
    "cl_phon": Column(str, nullable=True, coerce=True),
    "cl_emal": Column(str, nullable=True, coerce=True),
    "cl_adr1": Column(str, nullable=True, coerce=True),
    "cl_city": Column(str, nullable=True, coerce=True),
    "cl_st": Column(str, nullable=True, coerce=True),
    "cl_zip": Column(str, nullable=True, coerce=True),
    "cl_bact": Column(str, nullable=True, coerce=True),
    "cl_brtn": Column(str, nullable=True, coerce=True),
    "cl_stat": Column(str, nullable=True, coerce=True),
    "cl_rgdt": Column(str, nullable=True, coerce=True),
    "cl_dcsd": Column(str, nullable=True, coerce=True),
    "cl_fil1": Column(str, nullable=True, coerce=True),
}, strict=False, coerce=True)
