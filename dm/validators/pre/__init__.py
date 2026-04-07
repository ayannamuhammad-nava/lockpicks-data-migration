"""Built-in pre-migration validators."""

from dm.validators.pre.schema_diff import SchemaDiffValidator
from dm.validators.pre.pandera_check import PanderaValidator
from dm.validators.pre.governance import GovernanceValidator
from dm.validators.pre.data_quality import DataQualityValidator
from dm.validators.pre.profile_risk import ProfileRiskValidator
from dm.validators.pre.etl_test import ETLTestValidator

BUILTIN_PRE_VALIDATORS = [
    SchemaDiffValidator,
    PanderaValidator,
    GovernanceValidator,
    DataQualityValidator,
    ProfileRiskValidator,
    ETLTestValidator,
]
