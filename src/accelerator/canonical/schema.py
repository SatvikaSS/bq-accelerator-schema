from dataclasses import dataclass, field
from typing import List, Dict, Optional
from accelerator.canonical.field import CanonicalField
from datetime import datetime
import hashlib
import json

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

    # Schema versioning
    schema_hash: Optional[str] = None
    schema_version: Optional[str] = None
    generated_at: Optional[str] = None

    def compute_fingerprint(self):
        """
        Compute a deterministic hash of the schema structure.
        Used for schema versioning and drift detection.
        """
        payload = [
            {
                "name": f.name.lower(),
                "type": f.data_type,
                "nullable": f.nullable
            }
            for f in self.fields
        ]

        serialized = json.dumps(payload, sort_keys=True)
        self.schema_hash = hashlib.sha256(serialized.encode()).hexdigest()
        self.generated_at = datetime.utcnow().isoformat()


