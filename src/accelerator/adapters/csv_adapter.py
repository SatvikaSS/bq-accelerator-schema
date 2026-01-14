import csv
from typing import List, Optional

from accelerator.canonical.field import CanonicalField
from accelerator.canonical.schema import CanonicalSchema
from accelerator.inference.type_inference import infer_type


def detect_delimiter(file_path: str) -> str:
    """
    Detects CSV delimiter using a sample of the file.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        sample = f.read(1024)
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter


class CSVAdapter:
    """
    Adapter to convert CSV input into CanonicalSchema
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

    def parse(self) -> CanonicalSchema:
        delimiter = detect_delimiter(self.file_path)

        with open(self.file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile, delimiter=delimiter)
            headers = reader.fieldnames

            if not headers:
                raise ValueError("CSV has no headers")

            sample_rows = []
            for i, row in enumerate(reader):
                if i >= self.sample_size:
                    break
                sample_rows.append(row)

        canonical_fields: List[CanonicalField] = []

        for col in headers:
            values = [row[col] for row in sample_rows if row[col] not in ("", None)]
            data_type = infer_type(values)
            nullable = any(row[col] in ("", None) for row in sample_rows)

            max_length = None
            if data_type == "STRING" and values:
                max_length = max(len(str(v)) for v in values)
            

            canonical_fields.append(
                CanonicalField(
                    name=col.strip(),
                    data_type=data_type,
                    nullable=nullable,
                    description=f"Inferred from CSV column '{col}'",
                    max_length=max_length,
                )
            )

        return CanonicalSchema(
            source_type="csv",
            entity_name=self.entity_name,
            fields=canonical_fields,
            record_count=None,
            raw_metadata={
                "sample_size": len(sample_rows),
                "delimiter": delimiter,
            },
        )
