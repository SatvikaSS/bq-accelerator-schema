from dataclasses import dataclass, field
from typing import List, Dict, Optional
from accelerator.canonical.field import CanonicalField

@dataclass
class CanonicalSchema:
    """
    Common internal representation used across all adapters.
    """
    source_type: str                  # csv,json,parquet,uml
    entity_name: str                  
    fields: List[CanonicalField]

    description: Optional[str] = None
    record_count: Optional[int] = None
    raw_metadata: Dict = field(default_factory=dict)


