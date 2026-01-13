from dataclasses import dataclass
from typing import Optional

@dataclass
class CanonicalField:
    """
    Represents a single column/field in the canonical schema.
    """
    name: str
    data_type: str
    nullable: bool = True
    description: Optional[str] = None
