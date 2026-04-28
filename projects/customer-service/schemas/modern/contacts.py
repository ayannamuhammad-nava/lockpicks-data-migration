"""
Auto-generated Pandera schema for contacts
Generated from: modern
"""
import pandera as pa
from pandera import Column, DataFrameSchema


ContactsSchema = DataFrameSchema({
    "contact_id": Column(int, nullable=False, unique=True, coerce=True),
    "first_name": Column(str, nullable=False, coerce=True),
    "middle_name": Column(str, nullable=True, coerce=True),
    "last_name": Column(str, nullable=False, coerce=True),
    "name_suffix": Column(str, nullable=True, coerce=True),
    "ssn_hash": Column(str, nullable=True, coerce=True),
    "date_of_birth": Column('datetime64[ns]', nullable=True, coerce=True),
    "gender": Column(str, nullable=True, coerce=True),
    "ethnicity": Column(str, nullable=True, coerce=True),
    "primary_phone": Column(int, nullable=True, coerce=True),
    "mobile_phone": Column(int, nullable=True, coerce=True),
    "work_phone": Column(int, nullable=True, coerce=True),
    "email": Column(str, nullable=True, coerce=True),
    "address_line1": Column(str, nullable=True, coerce=True),
    "address_line2": Column(str, nullable=True, coerce=True),
    "city": Column(str, nullable=True, coerce=True),
    "state": Column(str, nullable=True, coerce=True),
    "zip_code": Column(str, nullable=True, coerce=True),
    "address_type": Column(str, nullable=True, coerce=True),
    "mailing_address_line1": Column(str, nullable=True, coerce=True),
    "mailing_address_line2": Column(str, nullable=True, coerce=True),
    "mailing_city": Column(str, nullable=True, coerce=True),
    "mailing_state": Column(str, nullable=True, coerce=True),
    "mailing_zip_code": Column(str, nullable=True, coerce=True),
    "emergency_contact_name": Column(str, nullable=True, coerce=True),
    "emergency_contact_phone": Column(int, nullable=True, coerce=True),
    "emergency_contact_relation": Column(str, nullable=True, coerce=True),
    "drivers_license_number": Column(str, nullable=True, coerce=True),
    "drivers_license_state": Column(str, nullable=True, coerce=True),
    "marital_status": Column(str, nullable=True, coerce=True),
    "dependents_count": Column(int, nullable=True, coerce=True),
    "language_preference": Column(str, nullable=True, coerce=True),
    "is_veteran": Column(bool, nullable=True, coerce=True),
    "is_disabled": Column(bool, nullable=True, coerce=True),
    "contact_status": Column(str, nullable=False, coerce=True),
    "created_at": Column('datetime64[ns]', nullable=True, coerce=True),
    "updated_at": Column('datetime64[ns]', nullable=True, coerce=True),
    "source_code": Column(str, nullable=True, coerce=True),
}, strict=False, coerce=True)
