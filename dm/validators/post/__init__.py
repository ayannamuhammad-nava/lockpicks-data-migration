"""Built-in post-migration validators."""

from dm.validators.post.row_count import RowCountValidator
from dm.validators.post.checksums import ChecksumValidator
from dm.validators.post.referential import ReferentialIntegrityValidator
from dm.validators.post.sample_compare import SampleCompareValidator
from dm.validators.post.aggregates import AggregateValidator
from dm.validators.post.compliance import ArchivedLeakageValidator, UnmappedColumnsValidator
from dm.validators.post.normalization_integrity import NormalizationIntegrityValidator
from dm.validators.post.encoding import EncodingValidator

BUILTIN_POST_VALIDATORS = [
    RowCountValidator,
    ChecksumValidator,
    ReferentialIntegrityValidator,
    SampleCompareValidator,
    AggregateValidator,
    ArchivedLeakageValidator,
    UnmappedColumnsValidator,
    NormalizationIntegrityValidator,
    EncodingValidator,
]
