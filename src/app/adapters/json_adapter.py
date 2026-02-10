import os
import json
from typing import List, Dict, Any, Optional
 
from app.canonical.schema import CanonicalSchema
from app.canonical.table import CanonicalTable
from app.canonical.field import CanonicalField
from app.inference.type_inference import infer_type
 
 
class JSONAdapter:
    """
    Recursive JSON ingestion adapter.
 
    Supports:
    - JSON object
    - JSON array
    - JSONL
    - Nested objects → RECORD
    - Arrays of primitives
    - Arrays of objects → REPEATED RECORD
    - Sparse records
    - Empty key repair
    """
 
    def __init__(
        self,
        file_path: str,
        entity_name: Optional[str] = None,
        sample_size: int = 100,
    ):
        self.file_path = file_path
        self.sample_size = sample_size
 
        self.entity_name = (
            entity_name
            if entity_name
            else os.path.splitext(os.path.basename(file_path))[0]
        )
 
    # ==================================================
    # ENTRYPOINT
    # ==================================================
 
    def parse(self) -> CanonicalSchema:
        records = self._read_json_records()
 
        if not records:
            raise ValueError("No valid records found in JSON input")
 
        records = records[: self.sample_size]
 
        fields = self._infer_fields(records)
 
        table = CanonicalTable(
            name=self.entity_name,
            fields=fields,
            metadata={
                "source_file": self.file_path,
                "sample_size": len(records),
            },
        )
 
        return CanonicalSchema(
            source_type="json",
            dataset={
                "domain": None,
                "environment": None,
                "zone": None,
                "layer": None,
                "dataset_name": self.entity_name,
            },
            tables=[table],
            metadata={"source_file": self.file_path},
        )
 
    # ==================================================
    # RECURSIVE FIELD INFERENCE
    # ==================================================
 
    def _infer_fields(self, records: List[dict]) -> List[CanonicalField]:
 
        field_samples: Dict[str, List[Any]] = {}
 
        # Collect samples
        for record in records:
            if not isinstance(record, dict):
                continue
 
            for key, value in record.items():
                clean_key = (key or "").strip()
 
                field_samples.setdefault(clean_key, [])
                field_samples[clean_key].append(value)
 
            # Sparse handling
            for key in field_samples:
                if key not in record:
                    field_samples[key].append(None)
 
        fields: List[CanonicalField] = []
 
        for idx, (raw_name, values) in enumerate(field_samples.items(), start=1):
 
            name = raw_name.strip() if raw_name else ""
            if not name:
                name = f"{self.entity_name}_{idx}"
 
            non_null_values = [v for v in values if v is not None]
 
            # -----------------------------
            # OBJECT → RECORD
            # -----------------------------
            if non_null_values and all(isinstance(v, dict) for v in non_null_values):
                nested_fields = self._infer_fields(non_null_values)
 
                field = CanonicalField(
                    name=name,
                    data_type="RECORD",
                    nullable=any(v is None for v in values),
                )
 
                # Attach nested fields dynamically
                field.fields = nested_fields
                fields.append(field)
                continue
 
            # -----------------------------
            # ARRAY HANDLING
            # -----------------------------
            if non_null_values and all(isinstance(v, list) for v in non_null_values):
 
                flattened = []
                for arr in non_null_values:
                    flattened.extend(arr)
 
                if flattened and all(isinstance(v, dict) for v in flattened):
                    nested_fields = self._infer_fields(flattened)
 
                    field = CanonicalField(
                        name=name,
                        data_type="RECORD",
                        nullable=True,
                        is_array=True,
                        element_type="RECORD",
                    )
                    field.fields = nested_fields
                    fields.append(field)
                    continue
 
                inferred = infer_type(flattened)
 
                fields.append(
                    CanonicalField(
                        name=name,
                        data_type=inferred,
                        nullable=True,
                        is_array=True,
                        element_type=inferred,
                    )
                )
                continue
 
            # -----------------------------
            # SCALAR
            # -----------------------------
            scalar_values = [
                "" if v is None else v
                for v in values
            ]
 
            inferred_type = infer_type(scalar_values)
 
            fields.append(
                CanonicalField(
                    name=name,
                    data_type=inferred_type,
                    nullable=any(v is None for v in values),
                )
            )
 
        return fields
 
    # ==================================================
    # JSON READER
    # ==================================================
 
    def _read_json_records(self) -> List[dict]:
 
        if not os.path.exists(self.file_path):
            raise ValueError(f"File not found: {self.file_path}")
 
        with open(self.file_path, "r", encoding="utf-8-sig") as f:
            raw = f.read().strip()
 
        if not raw:
            return []
 
        # Try full JSON
        try:
            parsed = json.loads(raw)
 
            if isinstance(parsed, list):
                return parsed
 
            if isinstance(parsed, dict):
                return [parsed]
 
        except json.JSONDecodeError:
            pass
 
        # JSONL fallback
        records = []
 
        with open(self.file_path, "r", encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
 
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        records.append(obj)
                except json.JSONDecodeError:
                    continue
 
        return records