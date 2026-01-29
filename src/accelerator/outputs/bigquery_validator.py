import re
from accelerator.outputs.bigquery_schema import BigQuerySchema
from accelerator.utils.exceptions import SchemaValidationError


class BigQuerySchemaValidator:
    """
    Validates BigQuery schema against platform rules and linting standards.
    This class ONLY validates. It never mutates schema.
    """

    MAX_COLUMN_NAME_LENGTH = 300
    MAX_COLUMNS = 10_000

    IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    ALLOWED_TYPES = {
        "STRING",
        "INT64",
        "NUMERIC",
        "BIGNUMERIC",
        "BOOL",
        "DATE",
        "TIMESTAMP",
        "DATETIME",
    }

    # Linting rules for table level
    MIN_TABLE_DESCRIPTION_LENGTH = 20
    FORBIDDEN_TABLE_DESCRIPTIONS = {
        "table",
        "data",
        "dataset",
        "tbd",
        "todo",
        "unknown",
    }

    # Linting rules for column level
    MIN_DESCRIPTION_LENGTH = 10
    FORBIDDEN_DESCRIPTION_PHRASES = {
        "todo",
        "tbd",
        "unknown",
        "n/a",
        "na",
        "none",
    }

    def __init__(self, bq_schema: BigQuerySchema):
        self.bq_schema = bq_schema
    
    # Table-level validation
    def validate_table_description(self, description: str):
        if not description:
            raise SchemaValidationError(
                "Table is missing a description"
            )

        desc = description.strip().lower()

        if len(desc) < self.MIN_TABLE_DESCRIPTION_LENGTH:
            raise SchemaValidationError(
                f"Table description is too short "
                f"(minimum {self.MIN_TABLE_DESCRIPTION_LENGTH} characters)"
            )

        if desc in self.FORBIDDEN_TABLE_DESCRIPTIONS:
            raise SchemaValidationError(
                f"Table description is not meaningful: '{description}'"
            )

    # Column-level validations
    def validate_name_length(self, field_name: str):
        if len(field_name) > self.MAX_COLUMN_NAME_LENGTH:
            raise SchemaValidationError(
                f"Invalid column name '{field_name}': exceeds {self.MAX_COLUMN_NAME_LENGTH} characters"
            )

    def validate_identifier_format(self, field_name: str):
        if not self.IDENTIFIER_PATTERN.match(field_name):
            raise SchemaValidationError(
                f"Invalid column name '{field_name}': "
                "must start with a letter or underscore and contain "
                "only letters, numbers, and underscores"
            )

    def validate_type(self, field_type: str):
        if field_type not in self.ALLOWED_TYPES:
            raise SchemaValidationError(
                f"Unsupported BigQuery type detected: '{field_type}'"
            )

    def validate_description(self, field_name: str, description: str):
        if not description:
            raise SchemaValidationError(
                f"Column '{field_name}' is missing a description"
            )

        desc = description.strip().lower()

        if len(desc) < self.MIN_DESCRIPTION_LENGTH:
            raise SchemaValidationError(
                f"Description for column '{field_name}' is too short "
                f"(minimum {self.MIN_DESCRIPTION_LENGTH} characters)"
            )

        if desc in self.FORBIDDEN_DESCRIPTION_PHRASES:
            raise SchemaValidationError(
                f"Description for column '{field_name}' is not meaningful: '{description}'"
            )

    # Schema-level validations
    def validate_column_count(self, fields):
        if not fields:
            raise SchemaValidationError(
                "BigQuery table must contain at least one column"
            )

        if len(fields) > self.MAX_COLUMNS:
            raise SchemaValidationError(
                f"BigQuery table has {len(fields)} columns; "
                f"maximum allowed is {self.MAX_COLUMNS}"
            )

    def validate_duplicates(self, field_names: list):
        duplicates = {name for name in field_names if field_names.count(name) > 1}
        if duplicates:
            raise SchemaValidationError(
                f"Duplicate column names detected after normalization: {duplicates}"
            )

    # Entry point
    def validate(self):
        """
        Run all BigQuery validation and linting rules
        """
        fields = self.bq_schema.generate()
        field_names = []

        # Table-level linting
        self.validate_table_description(self.bq_schema.table_description)

        # Column-level validation
        self.validate_column_count(fields)

        for field in fields:
            self.validate_name_length(field.name)
            self.validate_identifier_format(field.name)
            self.validate_type(field.field_type)
            self.validate_description(field.name, field.description)
            field_names.append(field.name)

        self.validate_duplicates(field_names)
        return True
