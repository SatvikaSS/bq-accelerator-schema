import os
import csv
from typing import List, Dict, Optional
from app.canonical.field import CanonicalField
from app.canonical.table import CanonicalTable
from app.canonical.schema import CanonicalSchema
from app.inference.type_inference import infer_type
from app.inference.numeric_inference import infer_numeric_metadata
# ------------------------------------------------------------------
# Delimiter detection
# ------------------------------------------------------------------
def detect_delimiter_from_lines(lines: List[str]) -> str:
    """
    Robust delimiter detection with safe fallback.
    """
    sample = "".join(lines[:20])
    try:
        dialect = csv.Sniffer().sniff(
            sample,
            delimiters=[",", ";", "\t", "|"]
        )
        return dialect.delimiter
    except csv.Error:
        if ";" in sample:
            return ";"
        elif "," in sample:
            return ","
        elif "\t" in sample:
            return "\t"
        else:
            return ","
# ------------------------------------------------------------------
# CSV Adapter
# ------------------------------------------------------------------
class CSVAdapter:
    """
    CSV ingestion adapter.
    Responsibilities:
    - Remove comments and empty lines
    - Detect delimiter
    - Read header safely
    - Repair empty column names using entity_name + position
    - Sample rows
    - Infer column names, data types, nullability, and lengths
    - Produce CanonicalSchema
    DOES NOT:
    - Normalize names
    - Deduplicate column names
    - Apply platform-specific rules
    """
    def __init__(
        self,
        file_path: str,
        entity_name: Optional[str] = None,
        sample_size: int = 100,
    ):
        self.file_path = file_path
        self.sample_size = sample_size
        if entity_name:
            self.entity_name = entity_name
        else:
            self.entity_name = os.path.splitext(
                os.path.basename(file_path)
            )[0]
    # --------------------------------------------------
    # REQUIRED BY ROUTER
    # --------------------------------------------------
    def parse(self) -> CanonicalSchema:
        """
        Router entrypoint.
        """
        return self._parse_csv()
    # --------------------------------------------------
    # Core CSV parsing logic
    # --------------------------------------------------
    def _parse_csv(self) -> CanonicalSchema:
        clean_lines = self._read_clean_lines(self.file_path)
        if not clean_lines:
            raise ValueError("CSV contains no valid (non-comment) lines")
        delimiter = detect_delimiter_from_lines(clean_lines)
        reader = csv.reader(clean_lines, delimiter=delimiter)
        raw_header = next(reader, None)
        if not raw_header or not any(h.strip() for h in raw_header):
            raise ValueError("CSV has no headers after removing comments")
        header: List[str] = []
        for idx, h in enumerate(raw_header, start=1):
            if h and h.strip():
                header.append(h.strip())
            else:
                header.append(f"{self.entity_name}_{idx}")
        column_samples: Dict[str, List[str]] = {col: [] for col in header}
        for i, row in enumerate(reader):
            if i >= self.sample_size:
                break
            for idx, col in enumerate(header):
                value = row[idx] if idx < len(row) else ""
                column_samples[col].append(value)
        fields = self._infer_fields(header, column_samples)
        table = CanonicalTable(
            name=self.entity_name,
            fields=fields,
        )
        return CanonicalSchema(
            source_type="csv",
            dataset={
                "domain": None,
                "environment": None,
                "zone": None,
                "layer": None,
                "entity": self.entity_name,
            },
            tables=[table],
            metadata={
                "sample_size": self.sample_size,
                "delimiter": delimiter,
                "source_file": self.file_path,
                "comments_ignored": True,
            },
        )
    # --------------------------------------------------
    # Internal helpers
    # --------------------------------------------------
    def _read_clean_lines(self, file_path: str) -> List[str]:
        """
        Removes:
        - empty lines
        - comment lines starting with '#' or '--'
        """
        with open(file_path, encoding="utf-8", errors="replace") as f:
            return [
                line
                for line in f
                if line.strip()
                and not line.lstrip().startswith(("#", "--"))
            ]
    def _infer_fields(
        self,
        header: List[str],
        column_samples: Dict[str, List[str]],
    ) -> List[CanonicalField]:
        fields: List[CanonicalField] = []
        for col in header:
            values = column_samples[col]
            data_type = infer_type(values)
            nullable = any(
                (v is None) or (str(v).strip() == "")
                for v in values
            )
            max_length = None
            if data_type == "STRING" and values:
                non_empty_values = [
                    str(v) for v in values
                    if v is not None and str(v).strip() != ""
                ]
                if non_empty_values:
                    max_length = max(len(v) for v in non_empty_values)
            numeric_metadata = None
            if data_type == "DECIMAL":
                numeric_metadata = infer_numeric_metadata(values)
            fields.append(
                CanonicalField(
                    name=col,
                    data_type=data_type,
                    nullable=nullable,
                    has_missing=nullable,
                    max_length=max_length,
                    numeric_metadata=numeric_metadata,
                )
            )
        return fields
