import os
import json
from typing import List, Dict, Any, Optional
 
from app.canonical.schema import CanonicalSchema
from app.canonical.table import CanonicalTable
from app.canonical.field import CanonicalField
from app.inference.type_inference import infer_type
from app.inference.numeric_inference import infer_numeric_metadata
 
def _handle_json_duplicates(pairs):
    result = {}
    seen_counts = {}
    for key, value in pairs:
        if key in seen_counts:
            seen_counts[key] += 1
            result[f"{key}_{seen_counts[key]}"] = value
        else:
            seen_counts[key] = 1
            result[key] = value
    return result

def _reject_nonstandard_constant(value: str):
    raise ValueError(f"Invalid JSON constant: {value}")

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
 
    def _compute_stats(self, values: List[Any]) -> Dict[str, float]:
        total = len(values)
        if total == 0:
            return {}
        non_null = [v for v in values if v is not None and str(v).strip() != ""]
        null_count = total - len(non_null)
        distinct = len(set(str(v).strip() for v in non_null)) if non_null else 0
        return {
            "distinct_ratio": round(distinct / total, 4),
            "null_ratio": round(null_count / total, 4),
        }

    # ==================================================
    # ENTRYPOINT
    # ==================================================
 
    def parse(self) -> CanonicalSchema:
        records = self._read_json_records()
        row_count = len(records)
        records = records[: self.sample_size]
 
        if not records:
            raise ValueError("No valid records found in JSON input")
 
        fields = self._infer_fields(records)
 
        table = CanonicalTable(
            name=self.entity_name,
            fields=fields,
            metadata={
                "source_file": self.file_path,
                "sample_size": len(records),
                "row_count": row_count,
                "row_count_mode": "counted",
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
                    stats=self._compute_stats(values),
                )
 
                # Attach nested fields dynamically
                field.children = nested_fields
                fields.append(field)
                continue
 
            # -----------------------------
            # ARRAY HANDLING
            # -----------------------------
            if non_null_values and all(isinstance(v, list) for v in non_null_values):
 
                flattened = []
                for arr in non_null_values:
                    flattened.extend(arr)
 
                non_null_flattened = [v for v in flattened if v is not None]
                if non_null_flattened and all(isinstance(v, dict) for v in non_null_flattened):
                    nested_fields = self._infer_fields(non_null_flattened)
 
                    field = CanonicalField(
                        name=name,
                        data_type="RECORD",
                        nullable=True,
                        is_array=True,
                        element_type="RECORD",
                        stats=self._compute_stats(values),
                    )
                    field.children = nested_fields
                    fields.append(field)
                    continue
 
                inference_values = non_null_flattened if non_null_flattened else flattened
                inferred = infer_type(inference_values)

                numeric_metadata = None
                if inferred == "DECIMAL":
                    numeric_metadata = infer_numeric_metadata(inference_values)

                fields.append(
                    CanonicalField(
                        name=name,
                        data_type=inferred,
                        nullable=True,
                        is_array=True,
                        element_type=inferred,
                        numeric_metadata=numeric_metadata,
                        stats=self._compute_stats(values),
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
            numeric_metadata = None
            if inferred_type == "DECIMAL":
                numeric_metadata = infer_numeric_metadata(scalar_values)

            fields.append(
                CanonicalField(
                    name=name,
                    data_type=inferred_type,
                    nullable=any(v is None for v in values),
                    numeric_metadata=numeric_metadata,
                    stats=self._compute_stats(values),
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
            parsed = json.loads(
                raw,
                object_pairs_hook=_handle_json_duplicates,
                parse_constant=_reject_nonstandard_constant,
            )

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
                    obj = json.loads(
                            line,
                            object_pairs_hook=_handle_json_duplicates,
                            parse_constant=_reject_nonstandard_constant,
                        )

                    if isinstance(obj, dict):
                        records.append(obj)
                except json.JSONDecodeError:
                    continue
 
        return records