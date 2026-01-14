from accelerator.outputs.bigquery_schema import BigQuerySchema
from accelerator.utils.exceptions import SchemaValidationError


class BigQuerySchemaValidator:
    """
    Validates BigQuery schema against platform rules
    """

    # Minimal but important reserved keyword list
    RESERVED_KEYWORDS = {
        "select", "from", "where", "group", "order",
        "table", "limit", "join", "having", "insert",
        "update", "delete", "create", "drop", "alter"
    }

    MAX_COLUMN_NAME_LENGTH = 300

    def __init__(self, bq_schema: BigQuerySchema):
        self.bq_schema = bq_schema

    def validate_reserved_keywords(self, field_name: str):
        if field_name.lower() in self.RESERVED_KEYWORDS:
            raise SchemaValidationError(
                f"Invalid column name '{field_name}': reserved BigQuery keyword"
            )

    def validate_name_length(self, field_name: str):
        if len(field_name) > self.MAX_COLUMN_NAME_LENGTH:
            raise SchemaValidationError(
                f"Invalid column name '{field_name}': exceeds {self.MAX_COLUMN_NAME_LENGTH} characters"
            )

    def validate_duplicates(self, field_names: list):
        duplicates = {
            name for name in field_names if field_names.count(name) > 1
        }
        if duplicates:
            raise SchemaValidationError(
                f"Duplicate column names detected after normalization: {duplicates}"
            )

    def validate(self):
        """
        Run all BigQuery validations
        """
        fields = self.bq_schema.generate()
        field_names = []

        for field in fields:
            self.validate_reserved_keywords(field.name)
            self.validate_name_length(field.name)
            field_names.append(field.name)

        self.validate_duplicates(field_names)

        return True
