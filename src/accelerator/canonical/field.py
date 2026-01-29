from dataclasses import dataclass
from typing import Optional

@dataclass
class NumericMetadata:
    precision: int              # total digits
    scale: int                  # digits after decimal
    max_integer_digits: int     # precision - scale
    signed: bool = True


@dataclass
class CanonicalField:
    """
    Canonical representation of a column (cloud-agnostic)
    """
    name: str
    data_type: str           
    nullable: bool

    description: str = None
    max_length: Optional[int] = None
    has_missing: bool = False
    numeric_metadata: Optional[NumericMetadata] = None
