"""
Auto-generated Pandera schema for benefit_payments
Generated from: modern
"""
import pandera as pa
from pandera import Column, DataFrameSchema


Benefit_paymentsSchema = DataFrameSchema({
    "payment_id": Column(int, nullable=False, unique=True, coerce=True),
    "claim_id": Column(int, nullable=True, coerce=True),
    "payment_date": Column('datetime64[ns]', nullable=False, coerce=True),
    "payment_amount": Column(float, nullable=False, coerce=True),
    "payment_method": Column(str, nullable=True, coerce=True),
    "week_ending_date": Column('datetime64[ns]', nullable=True, coerce=True),
    "payment_status": Column(str, nullable=True, coerce=True),
    "check_number": Column(str, nullable=True, coerce=True),
}, strict=False, coerce=True)
