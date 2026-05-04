"""
Auto-generated Pandera schema for claims
Generated from: modern
"""
import pandera as pa
from pandera import Column, DataFrameSchema


ClaimsSchema = DataFrameSchema({
    "claim_id": Column(int, nullable=False, unique=True, coerce=True),
    "claimant_id": Column(int, nullable=True, coerce=True),
    "employer_id": Column(int, nullable=True, coerce=True),
    "separation_reason": Column(str, nullable=True, coerce=True),
    "filing_date": Column('datetime64[ns]', nullable=False, coerce=True),
    "benefit_year_start": Column('datetime64[ns]', nullable=True, coerce=True),
    "benefit_year_end": Column('datetime64[ns]', nullable=True, coerce=True),
    "weekly_benefit_amount": Column(float, nullable=True, coerce=True),
    "max_benefit_amount": Column(float, nullable=True, coerce=True),
    "total_paid": Column(float, nullable=True, coerce=True),
    "weeks_claimed": Column(int, nullable=True, coerce=True),
    "claim_status": Column(str, nullable=True, coerce=True),
    "updated_at": Column('datetime64[ns]', nullable=True, coerce=True),
}, strict=False, coerce=True)
