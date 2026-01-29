import csv
from typing import List, Optional

from accelerator.canonical.field import CanonicalField
from accelerator.canonical.schema import CanonicalSchema
from accelerator.inference.type_inference import infer_type
from accelerator.inference.numeric_inference import infer_numeric_metadata


def detect_delimiter_from_lines(lines: list[str]) -> str:
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


class CSVAdapter:
    """
    Adapter to convert CSV input into CanonicalSchema.
    Ignores comment lines starting with '#'.
    """

    def __init__(
        self,
        file_path: str,
        entity_name: Optional[str] = None,
        sample_size: int = 100,
    ):
        self.file_path = file_path
        self.entity_name = entity_name or "unknown_entity"
        self.sample_size = sample_size

    def _read_clean_lines(self) -> List[str]:
        """
        Reads CSV file and removes:
        - comment lines 
        - empty lines
        """
        with open(self.file_path, encoding="utf-8") as f:
            return [
                line
                for line in f
                if line.strip() and not line.lstrip().startswith("#")
            ]

    def parse(self) -> CanonicalSchema:
        clean_lines = self._read_clean_lines()

        if not clean_lines:
            raise ValueError("CSV contains no valid (non-comment) lines")
        
        delimiter = detect_delimiter_from_lines(clean_lines)

        reader = csv.DictReader(clean_lines, delimiter=delimiter)
        headers = reader.fieldnames

        if not headers:
            raise ValueError("CSV has no headers after removing comments")

        sample_rows = []
        for i, row in enumerate(reader):
            if i >= self.sample_size:
                break
            sample_rows.append(row)

        canonical_fields: List[CanonicalField] = []
        seen_columns: dict[str, int] = {}

        for idx, col in enumerate(headers, start=1):
            raw_col = col.strip() if col else ""

            # Handle empty column names
            if not raw_col:
                base_name = f"{self.entity_name}_{idx}"
            else:
                base_name = raw_col

            # Handle duplicate column names
            if base_name in seen_columns:
                seen_columns[base_name] += 1
                normalized_col = f"{base_name}_{seen_columns[base_name]}"
            else:
                seen_columns[base_name] = 1
                normalized_col = base_name

            values = [
                row.get(col)
                for row in sample_rows
                if col in row and row[col] not in ("", None)
            ]

            data_type = infer_type(values)
            has_missing = any(
                row.get(col) in ("", None)
                for row in sample_rows
            )

            nullable = has_missing  

            max_length = None
            if data_type == "STRING" and values:
                max_length = max(len(str(v)) for v in values)

            description = (
                f"{normalized_col.replace('_', ' ').title()} column. "
                f"Type: {data_type}. "
                f"{'Optional' if nullable else 'Required'}."
            )

            numeric_metadata = None
            if data_type in ("FLOAT", "DECIMAL"):
                numeric_metadata = infer_numeric_metadata(values)

            canonical_fields.append(
                CanonicalField(
                    name=normalized_col,
                    data_type=data_type,
                    nullable=nullable,
                    description=description,
                    max_length=max_length,
                    has_missing=has_missing,
                    numeric_metadata=numeric_metadata,
                )
            )

        schema = CanonicalSchema(
            source_type="csv",
            entity_name=self.entity_name,
            fields=canonical_fields,
            record_count=None,
            raw_metadata={
                "sample_size": len(sample_rows),
                "delimiter": delimiter,
                "comments_ignored": True,
            },
        )

        return schema
