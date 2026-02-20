from typing import Optional, List
import pyarrow as pa
import pyarrow.parquet as pq

from app.canonical.field import CanonicalField, NumericMetadata
from app.canonical.schema import CanonicalSchema
from app.canonical.table import CanonicalTable


def map_parquet_type_to_canonical(field_type) -> str:
    if pa.types.is_decimal(field_type):
        return "DECIMAL"

    if pa.types.is_integer(field_type):
        return "INTEGER"

    if pa.types.is_float32(field_type) or pa.types.is_float64(field_type):
        return "FLOAT"

    if pa.types.is_boolean(field_type):
        return "BOOLEAN"

    if pa.types.is_string(field_type) or pa.types.is_large_string(field_type):
        return "STRING"

    if pa.types.is_binary(field_type) or pa.types.is_large_binary(field_type):
        return "STRING"

    if pa.types.is_timestamp(field_type):
        return "TIMESTAMP"

    if pa.types.is_date(field_type):
        return "DATE"

    return "STRING"


class ParquetAdapter:
    """
    Adapter to convert Parquet metadata schema into CanonicalSchema.

    Responsibilities:
    - Read Parquet schema
    - Infer canonical types
    - Extract precision/scale for DECIMAL
    - Preserve source descriptions

    DOES NOT:
    - Normalize names
    - Deduplicate fields
    - Apply platform-specific rules
    """

    def __init__(self, file_path: str, entity_name: Optional[str] = None):
        self.file_path = file_path
        self.entity_name = entity_name or "unknown_entity"

    def _get_column_description(self, field) -> Optional[str]:
        metadata = field.metadata
        if metadata and b"description" in metadata:
            return metadata[b"description"].decode("utf-8")
        return None

    def _parse_parquet_field(self, field: pa.Field) -> CanonicalField:
        name = field.name
        nullable = field.nullable
        description = self._get_column_description(field)
        numeric_metadata = None

        # --------------------
        # STRUCT / RECORD
        # --------------------
        if pa.types.is_struct(field.type):
            children = [
                self._parse_parquet_field(child)
                for child in field.type
            ]

            return CanonicalField(
                name=name,
                data_type="RECORD",
                nullable=nullable,
                description=description,
                children=children,
            )

        # --------------------
        # LIST / ARRAY
        # --------------------
        if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
            value_field = field.type.value_field
            element = self._parse_parquet_field(value_field)

            # Array of RECORD
            if element.data_type == "RECORD":
                return CanonicalField(
                    name=name,
                    data_type="RECORD",
                    nullable=nullable,
                    is_array=True,
                    children=element.children,
                    description=description,
                )

            # Array of scalar
            return CanonicalField(
                name=name,
                data_type=element.data_type,
                nullable=nullable,
                is_array=True,
                element_type=element.data_type,
                description=description,
            )   

        # Dictionary-encoded column
        if pa.types.is_dictionary(field.type):
            return self._parse_parquet_field(
                pa.field(
                    field.name,
                    field.type.value_type,
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
            )

        # MAP → JSON
        if pa.types.is_map(field.type):
            return CanonicalField(
                name=name,
                data_type="JSON",
                nullable=nullable,
                description=description,
                has_missing=nullable,
            )

        # UNION → reject
        if pa.types.is_union(field.type):
            raise ValueError(
                f"Unsupported UNION type in Parquet for field '{name}'"
            )

        # --------------------
        # DECIMAL
        # --------------------
        if pa.types.is_decimal(field.type):
            precision = field.type.precision
            scale = field.type.scale
            numeric_metadata = NumericMetadata(
                precision=precision,
                scale=scale,
                max_integer_digits=precision - scale,
                signed=True,
            )
            canonical_type = "DECIMAL"
        else:
            canonical_type = map_parquet_type_to_canonical(field.type)

        return CanonicalField(
            name=name,
            data_type=canonical_type,
            nullable=nullable,
            description=description,
            has_missing=nullable,
            numeric_metadata=numeric_metadata,
        )

    def parse(self) -> CanonicalSchema:
        pf = pq.ParquetFile(self.file_path)
        schema = pf.schema_arrow
        row_count = pf.metadata.num_rows if pf.metadata else None

        fields: List[CanonicalField] = []

        for field in schema:
            fields.append(self._parse_parquet_field(field))

        return CanonicalSchema(
            source_type="parquet",
            dataset={},   # injected later by router
            tables=[
                CanonicalTable(
                    name=self.entity_name,
                    fields=fields,
                    metadata={
                        "row_count": row_count,
                        "row_count_mode": "metadata",
                    },
                )
            ],
            metadata={
                "schema_source": "parquet_metadata",
            },
        )