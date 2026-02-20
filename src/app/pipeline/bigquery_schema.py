from dataclasses import field
from typing import List

from app.canonical.schema import CanonicalSchema
from app.canonical.field import CanonicalField
from app.pipeline.datatype import map_canonical_to_bigquery
from app.governance.column_classifier import classify_column


class BigQueryField:
    """
    Represents a single BigQuery column definition.
    """

    def __init__(self, name: str, field_type: str, mode: str, description: str | None, subfields: List["BigQueryField"] | None = None, range_element_type: str | None = None):
        self.name = name
        self.field_type = field_type
        self.mode = mode
        self.description = description
        self.subfields = subfields or []
        self.security: dict | None = None
        self.range_element_type = range_element_type

    def to_dict(self) -> dict:
        field = {
            "name": self.name,
            "type": self.field_type,
            "mode": self.mode,
        }
        if self.description:
            field["description"] = self.description
        if self.field_type == "RECORD" and self.subfields:
            field["fields"] = [sf.to_dict() for sf in self.subfields]
        if self.field_type == "RANGE" and self.range_element_type:
            field["rangeElementType"] = {"type": self.range_element_type}
        return field


class BigQuerySchema:
    """
    Converts CanonicalSchema into a BigQuery-compatible schema.

    Assumptions:
    - Naming normalization already applied
    - Metadata injection already applied
    - Canonical schema is final and validated
    """

    def __init__(self, canonical_schema: CanonicalSchema, table_name: str):
        self.canonical_schema = canonical_schema
        self.table_name = table_name

    @property
    def table_description(self) -> str | None:
        return self.canonical_schema.description

    def _build_field(self, field: CanonicalField) -> BigQueryField:
        bq_type, bq_mode = map_canonical_to_bigquery(field)
 
        # Handle nested RECORD
        if field.data_type.upper() == "RECORD" and field.children:
            subfields = [
                self._build_field(subfield)
                for subfield in field.children
            ]
 
            return BigQueryField(
                name=field.name,
                field_type="RECORD",
                mode=bq_mode,
                description=field.description,
                subfields=subfields,
            )
 
        range_element_type = "DATE" if field.data_type.upper() == "RANGE_DATE" else None

        return BigQueryField(
            name=field.name,
            field_type=bq_type,
            mode=bq_mode,
            description=field.description,
            range_element_type=range_element_type,
        )
    

    def _classify_field_recursive(self, field: BigQueryField):
        field.security = classify_column(
            name=field.name,
            description=field.description,
        )

        for subfield in field.subfields:
            self._classify_field_recursive(subfield)

    def generate(self) -> List[BigQueryField]:
        """
        Generate BigQueryField objects from canonical schema.
        """
        fields: List[BigQueryField] = []

        # Step 1: Build fields from canonical schema
        for table in self.canonical_schema.tables:
            if table.name != self.table_name:
                continue

            for field in table.fields:
                fields.append(self._build_field(field))

        if not fields:
            raise ValueError(
                f"No fields found for table '{self.table_name}' "
                f"in canonical schema"
            )

        # Step 2: Phase-1 security classification
        for field in fields:
            self._classify_field_recursive(field)
        
        return fields

    def to_dict(self) -> list[dict]:
        """
        Return schema as list of dictionaries (BigQuery API format).
        """
        return [field.to_dict() for field in self.generate()]