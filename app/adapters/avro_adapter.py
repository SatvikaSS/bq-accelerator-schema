from typing import Optional, List
from fastavro import reader, is_avro

from app.canonical.table import CanonicalTable
from app.canonical.field import CanonicalField, NumericMetadata
from app.canonical.schema import CanonicalSchema


_AVRO_TYPE_MAP = {
    "string": "STRING",
    "int": "INTEGER",
    "long": "INTEGER",
    "float": "FLOAT",
    "double": "FLOAT",
    "boolean": "BOOLEAN",
    "bytes": "STRING",
}


class AvroAdapter:
    """
    Adapter to convert Avro header schema into CanonicalSchema.

    Responsibilities:
    - Validate Avro container file
    - Read writer schema
    - Infer canonical types
    - Handle union nullability
    - Handle logicalType DECIMAL

    DOES NOT:
    - Normalize names
    - Deduplicate fields
    - Apply platform-specific rules
    """

    def __init__(self, file_path: str, entity_name: Optional[str] = None):
        self.file_path = file_path
        self.entity_name = entity_name or "unknown_entity"

    def _parse_avro_type(
        self,
        name: str,
        avro_type,
        field_doc: Optional[str] = None
    ) -> CanonicalField:
        nullable = False
        numeric_metadata = None

        # Union type
        if isinstance(avro_type, list):
            nullable = "null" in avro_type
            non_null_types = [t for t in avro_type if t != "null"]
            
            # Numeric widening precedence
            numeric_order = ["int", "long", "float", "double", "decimal"]
            
            # All numeric union → choose widest type
            if all(t in numeric_order for t in non_null_types):
                avro_type = max(
                    non_null_types,
                    key=lambda t: numeric_order.index(t)
                )

            # Single non-null type → safe
            elif len(non_null_types) == 1:
                avro_type = non_null_types[0]

            # Anything else → reject
            else:
                raise ValueError(
                    f"Unsupported Avro union with incompatible types: {avro_type}"
            )

        # RECORD (nested)
        if isinstance(avro_type, dict) and avro_type.get("type") == "record":
            children = []
            for idx, subfield in enumerate(avro_type.get("fields", []), start=1):
                child_name = subfield.get("name", f"{name}_{idx}")
                child_doc = subfield.get("doc")
                children.append(
                    self._parse_avro_type(
                        name=child_name,
                        avro_type=subfield["type"],
                        field_doc=child_doc,
                    )
                )

            return CanonicalField(
                name=name,
                data_type="RECORD",
                nullable=nullable,
                description=field_doc,
                children=children,
            )

        # ARRAY
        if isinstance(avro_type, dict) and avro_type.get("type") == "array":
            items = avro_type.get("items")

            # Handle union inside array items
            if isinstance(items, list):
                non_null_items = [t for t in items if t != "null"]
                if not non_null_items:
                    raise ValueError(f"Invalid Avro array union for field '{name}'")
                items = non_null_items[0]  # documented behavior
            
            # Array of records
            if isinstance(items, dict) and items.get("type") == "record":
                element_field = self._parse_avro_type(
                    name=name,
                    avro_type=items,
                )
                return CanonicalField(
                    name=name,
                    data_type="RECORD",
                    nullable=nullable,
                    is_array=True,
                    children=element_field.children,
                    description=field_doc,
                )

            # Array of scalars
            element_type = _AVRO_TYPE_MAP.get(items, "STRING")
            return CanonicalField(
                name=name,
                data_type=element_type,
                nullable=nullable,
                is_array=True,
                element_type=element_type,
                description=field_doc,
            )

        # MAP → JSON (BigQuery has no MAP type)
        if isinstance(avro_type, dict) and avro_type.get("type") == "map":
            return CanonicalField(
                name=name,
                data_type="JSON",
                nullable=nullable,
                description=field_doc,
                has_missing=nullable,
            )

        # Logical types (DECIMAL)
        if isinstance(avro_type, dict):
            logical_type = avro_type.get("logicalType")
            if logical_type == "decimal":
                precision = avro_type.get("precision")
                scale = avro_type.get("scale")
                if precision is not None and scale is not None:
                    numeric_metadata = NumericMetadata(
                        precision=precision,
                        scale=scale,
                        max_integer_digits=precision - scale,
                        signed=True,
                    )
                canonical_type = "DECIMAL"
            else:
                canonical_type = _AVRO_TYPE_MAP.get(avro_type.get("type"), "STRING")
        else:
            canonical_type = _AVRO_TYPE_MAP.get(avro_type, "STRING")

        return CanonicalField(
            name=name,
            data_type=canonical_type,
            nullable=nullable,
            description=field_doc,
            has_missing=nullable,
            numeric_metadata=numeric_metadata,
        )

    def parse(self) -> CanonicalSchema:
        with open(self.file_path, "rb") as f:
            if not is_avro(f):
                raise ValueError(
                    "Invalid Avro file: expected Avro Object Container File "
                    "(raw/schemaless Avro is not supported)"
                )

            f.seek(0)
            avro_reader = reader(f)
            avro_schema = avro_reader.writer_schema
            table_description = avro_schema.get("doc")

        fields: List[CanonicalField] = []

        for idx, field in enumerate(avro_schema.get("fields", []), start=1):
            raw_name = field.get("name")
            name = raw_name.strip() if raw_name and raw_name.strip() else f"{self.entity_name}_{idx}"
            field_doc = field.get("doc")

            fields.append(
                self._parse_avro_type(
                    name=name,
                    avro_type=field["type"],
                    field_doc=field_doc,
                )
            )


        canonical_schema = CanonicalSchema(
            source_type="avro",
            dataset={},   # injected later by router
            tables=[
                CanonicalTable(
                    name=self.entity_name,
                    fields=fields,
                )
            ],
            metadata={
                "schema_source": "avro_header",
            },
        )

        # Table description (source-first)
        if table_description and table_description.strip():
            canonical_schema.description = table_description.strip()
        return canonical_schema