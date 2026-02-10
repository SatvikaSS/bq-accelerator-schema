from typing import List

from app.canonical.schema import CanonicalSchema
from app.canonical.field import CanonicalField
from app.pipeline.datatype import map_canonical_to_bigquery


class BigQueryField:
    """
    Represents a single BigQuery column definition.
    """

    def __init__(self, name: str, field_type: str, mode: str, description: str | None, subfields: List["BigQueryField"] | None = None):
        self.name = name
        self.field_type = field_type
        self.mode = mode
        self.description = description
        self.subfields = subfields or []

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
 
        return BigQueryField(
            name=field.name,
            field_type=bq_type,
            mode=bq_mode,
            description=field.description,
        )
    

    def generate(self) -> List[BigQueryField]:
        """
        Generate BigQueryField objects from canonical schema.
        """
        fields: List[BigQueryField] = []

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

        return fields

    def to_dict(self) -> list[dict]:
        """
        Return schema as list of dictionaries (BigQuery API format).
        """
        return [field.to_dict() for field in self.generate()]