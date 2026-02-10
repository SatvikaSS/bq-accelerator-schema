from dataclasses import dataclass, field
from typing import List, Dict, Optional

from app.canonical.table import CanonicalTable


@dataclass
class CanonicalSchema:
    """
    Canonical, format-agnostic schema representation.

    This is the SINGLE source of truth for the pipeline.

    Lifecycle:
    Adapters → CanonicalSchema → Metadata → Naming → Datatype → Output
    """

    # Source identity
    source_type: str                 # csv | json | jsonl | avro | parquet

    # Dataset-level identity (used by naming pipeline)
    dataset: Dict[str, str]          # domain, environment, zone, layer, dataset_name

    # Logical tables
    tables: List[CanonicalTable]

    description: Optional[str] = None

    # Raw source metadata (non-semantic, informational)
    metadata: Dict = field(default_factory=dict)

    # Rename lineage (populated by naming pipeline)
    rename_mappings: Dict = field(default_factory=dict)

    # Convenience helpers
    def get_table(self, name: str) -> Optional[CanonicalTable]:
        """
        Retrieve a table by name.
        """
        for table in self.tables:
            if table.name == name:
                return table
        return None