"""
Auto-generated Pandera schema for claimants
Generated from: modern
"""
import pandera as pa
from pandera import Column, DataFrameSchema


ClaimantsSchema = DataFrameSchema({
    "claimant_id": Column(int, nullable=False, unique=True, coerce=True),
    "first_name": Column(str, nullable=False, coerce=True),
    "last_name": Column(str, nullable=False, coerce=True),
    "ssn_hash": Column(str, nullable=True, coerce=True),
    "date_of_birth": Column('datetime64[ns]', nullable=True, coerce=True),
    "phone_number": Column(int, nullable=True, coerce=True),
    "email": Column(str, nullable=True, coerce=True),
    "address_line1": Column(str, nullable=True, coerce=True),
    "city": Column(str, nullable=True, coerce=True),
    "state": Column(str, nullable=True, coerce=True),
    "zip_code": Column(str, nullable=True, coerce=True),
    "claimant_status": Column(str, nullable=True, coerce=True),
    "registered_at": Column('datetime64[ns]', nullable=True, coerce=True),
    "is_deceased": Column(bool, nullable=True, coerce=True),
    "cl_bact": Column(str, nullable=True, coerce=True),
    "legacy_system_ref": Column(str, nullable=True, coerce=True),
}, strict=False, coerce=True)
