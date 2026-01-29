import re
from accelerator.canonical.schema import CanonicalSchema
from accelerator.standards.metadata_columns import get_standard_metadata_columns
from accelerator.standards.bigquery_reserved_keywords import is_bigquery_reserved_keyword


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
            "mode": self.mode,
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
        "DATETIME": "DATETIME",
    }


    def __init__(self, canonical_schema: CanonicalSchema, table_name: str):
        self.canonical_schema = canonical_schema
        self.table_name = table_name.lower()

    @property
    def table_description(self):
        return self.canonical_schema.description
        
    @staticmethod
    def normalize_field_name(name: str) -> str:
        """
        Normalize column names to BigQuery standards
        """
        name = name.strip().lower()
        name = re.sub(r"[^a-z0-9]", "_", name)
        name = re.sub(r"_+", "_", name)

        if not re.match(r"[a-z_]", name[0]):
            name = f"_{name}"

        return name

    def resolve_reserved_keyword(self, field_name: str) -> str:
        """
        Resolve BigQuery reserved keywords safely
        """
        if is_bigquery_reserved_keyword(field_name):
            return f"{self.table_name}_{field_name}"
        return field_name

    def map_type(self, field) -> str:
        """
            Map canonical field to BigQuery type,
            using numeric_metadata when available.
        """
        canonical_type = field.data_type.upper()
        
        if canonical_type == "INTEGER":
            return "INT64"
        # FLOAT / DECIMAL → NUMERIC or BIGNUMERIC
        if canonical_type in ("FLOAT", "DECIMAL"):
            meta = field.numeric_metadata

            if meta:
                if meta.precision <= 38 and meta.scale <= 9:
                    return "NUMERIC"
                else:
                    return "BIGNUMERIC"

            # fallback if metadata missing
            return "NUMERIC"

        return self.TYPE_MAPPING.get(canonical_type, "STRING")

    def generate(self):
        """
        Generate list of BigQueryField objects including platform metadata columns
        """
        bq_fields = []

        # Source derived fields
        for field in self.canonical_schema.fields:
            bq_type = self.map_type(field)
            mode = "NULLABLE" if field.nullable else "REQUIRED"

            normalized_name = self.normalize_field_name(field.name)
            safe_name = self.resolve_reserved_keyword(normalized_name)

            bq_field = BigQueryField(
                name=safe_name,
                field_type=bq_type,
                mode=mode,
                description=field.description,
            )

            bq_fields.append(bq_field)

        # Inject platform metadata columns
        existing_names = {f.name for f in bq_fields}

        for meta in get_standard_metadata_columns():
            if meta["name"] in existing_names:
                raise ValueError(
                    f"Metadata column name collision detected: {meta['name']}"
                )   

            bq_fields.append(
                BigQueryField(
                    name=meta["name"],
                    field_type=meta["type"],
                    mode=meta["mode"],
                    description=meta["description"],
                )
            )

        return bq_fields

    def to_dict(self):
        """
        Return schema as list of dictionaries (BigQuery API format)
        """
        return [field.to_dict() for field in self.generate()]
