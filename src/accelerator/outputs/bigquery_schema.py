import re
from accelerator.canonical.schema import CanonicalSchema
from accelerator.canonical.field import CanonicalField


class BigQueryField:
    """
    Represents a single BigQuery column definition
    """

    def __init__(self, name, field_type, mode="NULLABLE", description=None):
        self.name = name
        self.field_type = field_type
        self.mode = mode
        self.description = description

    def to_dict(self):
        """
        Convert field into BigQuery-compatible dictionary
        """
        field_dict = {
            "name": self.name,
            "type": self.field_type,
            "mode": self.mode
        }

        if self.description:
            field_dict["description"] = self.description

        return field_dict


class BigQuerySchema:
    """
    Converts CanonicalSchema into a BigQuery-compatible schema
    """

    TYPE_MAPPING = {
        "INTEGER": "INT64",
        "FLOAT": "NUMERIC",      
        "DECIMAL": "NUMERIC",
        "BOOLEAN": "BOOL",
        "STRING": "STRING",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "DATETIME": "DATETIME"
    }

    def __init__(self, canonical_schema: CanonicalSchema):
        self.canonical_schema = canonical_schema

    @staticmethod
    def normalize_field_name(name: str) -> str:
        """
        Normalize column names to BigQuery standards
        """
        name = name.strip().lower()

        # Replace spaces and special characters with underscore
        name = re.sub(r"[^a-z0-9]", "_", name)

        # Collapse multiple underscores
        name = re.sub(r"_+", "_", name)

        # BigQuery requires column to start with letter or underscore
        if not re.match(r"[a-z_]", name[0]):
            name = f"_{name}"

        return name

    def map_type(self, canonical_type: str) -> str:
        """
        Map canonical type to BigQuery type
        """
        return self.TYPE_MAPPING.get(canonical_type.upper(), "STRING")

    def generate(self):
        """
        Generate list of BigQueryField objects
        """
        bq_fields = []

        for field in self.canonical_schema.fields:
            bq_type = self.map_type(field.data_type)
            mode = "NULLABLE" if field.nullable else "REQUIRED"

            bq_field = BigQueryField(
                name=self.normalize_field_name(field.name),
                field_type=bq_type,
                mode=mode,
                description=field.description
            )

            bq_fields.append(bq_field)

        return bq_fields

    def to_dict(self):
        """
        Return schema as list of dictionaries (BigQuery API format)
        """
        return [field.to_dict() for field in self.generate()]
