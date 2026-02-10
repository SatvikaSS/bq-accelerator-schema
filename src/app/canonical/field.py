from dataclasses import dataclass
from typing import Optional,List


@dataclass
class NumericMetadata:
    """
    Numeric characteristics inferred from data.
    Useful for Avro / Parquet / CSV profiling.
    """
    precision: int              # total digits
    scale: int                  # digits after decimal
    max_integer_digits: int     # precision - scale
    signed: bool = True


@dataclass
class CanonicalField:
    """
    Canonical representation of a column.
    Format-agnostic.
    """
    name: str
    data_type: str              # STRING, INTEGER, FLOAT, BOOLEAN, DATE, TIMESTAMP
    nullable: bool

    description: Optional[str] = None
    max_length: Optional[int] = None
    has_missing: bool = False
    numeric_metadata: Optional[NumericMetadata] = None

    is_array: bool = False
    element_type: Optional[str] = None
    children: Optional[List["CanonicalField"]] = None