"""
Auto-generated Pandera schema for employers
Generated from: modern
"""
import pandera as pa
from pandera import Column, DataFrameSchema


EmployersSchema = DataFrameSchema({
    "employer_id": Column(int, nullable=False, unique=True, coerce=True),
    "employer_name": Column(str, nullable=False, coerce=True),
    "employer_ein": Column(str, nullable=True, coerce=True),
    "industry": Column(str, nullable=True, coerce=True),
    "address_line1": Column(str, nullable=True, coerce=True),
    "city": Column(str, nullable=True, coerce=True),
    "state": Column(str, nullable=True, coerce=True),
    "zip_code": Column(str, nullable=True, coerce=True),
    "phone_number": Column(int, nullable=True, coerce=True),
    "employer_status": Column(str, nullable=True, coerce=True),
}, strict=False, coerce=True)
