"""
Auto-generated Pandera schema for contacts
Generated from: legacy
"""
import pandera as pa
from pandera import Column, DataFrameSchema


ContactsSchema = DataFrameSchema({
    "ct_recid": Column(int, nullable=True, coerce=True),
    "ct_fnam": Column(str, nullable=True, coerce=True),
    "ct_mnam": Column(str, nullable=True, coerce=True),
    "ct_lnam": Column(str, nullable=True, coerce=True),
    "ct_sufx": Column(str, nullable=True, coerce=True),
    "ct_ssn": Column(str, nullable=True, coerce=True),
    "ct_dob": Column(str, nullable=True, coerce=True),
    "ct_gndr": Column(str, nullable=True, coerce=True),
    "ct_ethn": Column(str, nullable=True, coerce=True),
    "ct_ptel": Column(str, nullable=True, coerce=True),
    "ct_mtel": Column(str, nullable=True, coerce=True),
    "ct_wtel": Column(str, nullable=True, coerce=True),
    "ct_emal": Column(str, nullable=True, coerce=True),
    "ct_adr1": Column(str, nullable=True, coerce=True),
    "ct_adr2": Column(str, nullable=True, coerce=True),
    "ct_city": Column(str, nullable=True, coerce=True),
    "ct_st": Column(str, nullable=True, coerce=True),
    "ct_zip": Column(str, nullable=True, coerce=True),
    "ct_adtyp": Column(str, nullable=True, coerce=True),
    "ct_madr1": Column(str, nullable=True, coerce=True),
    "ct_madr2": Column(str, nullable=True, coerce=True),
    "ct_mcity": Column(str, nullable=True, coerce=True),
    "ct_mst": Column(str, nullable=True, coerce=True),
    "ct_mzip": Column(str, nullable=True, coerce=True),
    "ct_emrg": Column(str, nullable=True, coerce=True),
    "ct_etel": Column(str, nullable=True, coerce=True),
    "ct_erel": Column(str, nullable=True, coerce=True),
    "ct_dln": Column(str, nullable=True, coerce=True),
    "ct_dlst": Column(str, nullable=True, coerce=True),
    "ct_bact": Column(str, nullable=True, coerce=True),
    "ct_brtn": Column(str, nullable=True, coerce=True),
    "ct_mstat": Column(str, nullable=True, coerce=True),
    "ct_dpnds": Column(int, nullable=True, coerce=True),
    "ct_lang": Column(str, nullable=True, coerce=True),
    "ct_vetf": Column(str, nullable=True, coerce=True),
    "ct_disf": Column(str, nullable=True, coerce=True),
    "ct_stat": Column(str, nullable=True, coerce=True),
    "ct_crtdt": Column(str, nullable=True, coerce=True),
    "ct_upddt": Column(str, nullable=True, coerce=True),
    "ct_srccd": Column(str, nullable=True, coerce=True),
    "ct_fil1": Column(str, nullable=True, coerce=True),
    "ct_fil2": Column(str, nullable=True, coerce=True),
}, strict=False, coerce=True)
