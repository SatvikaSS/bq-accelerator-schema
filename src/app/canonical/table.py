from dataclasses import dataclass, field
from typing import List, Dict, Optional
from app.canonical.field import CanonicalField


@dataclass
class CanonicalTable:
    """
    Canonical representation of a physical table.
    Works for:
    - single CSV
    - JSON collections
    - Avro / Parquet datasets
    """
    name: str
    fields: List[CanonicalField]

    description: Optional[str] = None

    # Optional runtime / profiling metadata
    # e.g. record_count, source_file, row_stats
    metadata: Dict = field(default_factory=dict)