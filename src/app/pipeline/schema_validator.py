import re
from typing import List

from app.canonical import field
from app.pipeline.bigquery_schema import BigQuerySchema
from app.utils.exceptions import SchemaValidationError


class BigQuerySchemaValidator:
    """
    Validates BigQuery schema against platform rules and linting standards.

    This class:
    - NEVER mutates schema
    - Assumes naming & metadata injection already ran
    - Acts as a final safety gate before deployment
    """

    # -------------------------------
    # BigQuery limits
    # -------------------------------
    MAX_COLUMN_NAME_LENGTH = 300
    MAX_COLUMNS = 10_000
    MAX_NESTING_DEPTH = 13

    # Enforced to match naming.py (lowercase only)
    IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")

    ALLOWED_TYPES = {
        "STRING",
        "INTEGER",
        "FLOAT",
        "NUMERIC",
        "BIGNUMERIC",
        "BOOLEAN",
        "DATE",
        "TIMESTAMP",
        "DATETIME",
        "RECORD",
        "JSON",
        "GEOGRAPHY",
        "RANGE",
    }

    # -------------------------------
    # Linting rules
    # -------------------------------
    MIN_TABLE_DESCRIPTION_LENGTH = 20
    FORBIDDEN_TABLE_DESCRIPTIONS = {
        "table",
        "data",
        "dataset",
        "tbd",
        "todo",
        "unknown",
    }

    MIN_COLUMN_DESCRIPTION_LENGTH = 10
    FORBIDDEN_COLUMN_DESCRIPTIONS = {
        "todo",
        "tbd",
        "unknown",
        "n/a",
        "na",
        "none",
    }

    def __init__(self, bq_schema: BigQuerySchema):
        self.bq_schema = bq_schema

    # ------------------------------------------------------------------
    # Table-level validation
    # ------------------------------------------------------------------

    def validate_table_description(self):
        description = self.bq_schema.table_description

        if not description:
            raise SchemaValidationError(
                "Table is missing a description"
            )

        desc = description.strip().lower()

        if desc in self.FORBIDDEN_TABLE_DESCRIPTIONS:
            raise SchemaValidationError(
                f"Table description is not meaningful: '{description}'"
            )

        if len(desc) < self.MIN_TABLE_DESCRIPTION_LENGTH:
            raise SchemaValidationError(
                f"Table description is too short "
                f"(minimum {self.MIN_TABLE_DESCRIPTION_LENGTH} characters)"
            )

    # ------------------------------------------------------------------
    # Column-level validations
    # ------------------------------------------------------------------

    def validate_range_type(self, field):
        if field.field_type != "RANGE":
            return
        elem = getattr(field, "range_element_type", None)
        if elem not in {"DATE", "TIMESTAMP"}:
            raise SchemaValidationError(
                f"RANGE column '{field.name}' requires valid range element type "
                f"(DATE/TIMESTAMP), got: {elem}"
            )

    def _validate_field_depth(self, field, current_depth: int, path: str):
        # Only RECORD contributes to nesting depth
        if field.field_type != "RECORD":
            return

        if current_depth > self.MAX_NESTING_DEPTH:
            raise SchemaValidationError(
                f"Nesting depth exceeded for '{path}': "
                f"depth {current_depth} > max {self.MAX_NESTING_DEPTH}"
            )

        for child in field.subfields:
            child_path = f"{path}.{child.name}"
            self._validate_field_depth(child, current_depth + 1, child_path)

    def validate_nesting_depth(self, fields: List):
        for field in fields:
            self._validate_field_depth(field, current_depth=1, path=field.name)

    def validate_name_length(self, field_name: str):
        if len(field_name) > self.MAX_COLUMN_NAME_LENGTH:
            raise SchemaValidationError(
                f"Column name '{field_name}' exceeds "
                f"{self.MAX_COLUMN_NAME_LENGTH} characters"
            )

    def validate_identifier_format(self, field_name: str):
        if not self.IDENTIFIER_PATTERN.fullmatch(field_name):
            raise SchemaValidationError(
                f"Invalid column name '{field_name}': "
                "must start with a lowercase letter or underscore and contain "
                "only lowercase letters, numbers, and underscores"
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

        if desc in self.FORBIDDEN_COLUMN_DESCRIPTIONS:
            raise SchemaValidationError(
                f"Description for column '{field_name}' "
                f"is not meaningful: '{description}'"
            )

        if len(desc) < self.MIN_COLUMN_DESCRIPTION_LENGTH:
            raise SchemaValidationError(
                f"Description for column '{field_name}' is too short "
                f"(minimum {self.MIN_COLUMN_DESCRIPTION_LENGTH} characters)"
            )

    # ------------------------------------------------------------------
    # Schema-level validations
    # ------------------------------------------------------------------

    def validate_column_count(self, fields: List):
        if not fields:
            raise SchemaValidationError(
                "BigQuery table must contain at least one column"
            )

        if len(fields) > self.MAX_COLUMNS:
            raise SchemaValidationError(
                f"BigQuery table has {len(fields)} columns; "
                f"maximum allowed is {self.MAX_COLUMNS}"
            )

    def validate_duplicates(self, field_names: List[str]):
        seen = {}
        duplicates = set()

        for name in field_names:
            key = name.lower()

            if key in seen:
                duplicates.add(seen[key])
                duplicates.add(name)
            else:
                seen[key] = name

        if duplicates:
            raise SchemaValidationError(
                f"Duplicate column names detected after normalization: "
                f"{sorted(duplicates)}"
            )

    def validate_numeric_metadata(self):
        """
        Validate DECIMAL precision / scale consistency
        using canonical fields (table-scoped).
        """
        for table in self.bq_schema.canonical_schema.tables:
            for field in table.fields:
                if field.data_type == "DECIMAL":
                    meta = field.numeric_metadata

                    if meta is None:
                        raise SchemaValidationError(
                            f"DECIMAL column '{field.name}' "
                            f"is missing precision/scale metadata"
                        )

                    if meta.scale > meta.precision:
                        raise SchemaValidationError(
                            f"Invalid DECIMAL definition for column '{field.name}': "
                            f"scale ({meta.scale}) cannot exceed precision ({meta.precision})"
                        )

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def validate(self) -> bool:
        """
        Run all BigQuery validation and linting rules.
        """

        # Validate canonical numeric integrity first
        self.validate_numeric_metadata()

        # Table-level linting
        self.validate_table_description()

        # Generate final BigQuery fields
        fields = self.bq_schema.generate()
        self.validate_nesting_depth(fields)
        self.validate_column_count(fields)
        for field in fields:
            self.validate_range_type(field)

        field_names = []

        for field in fields:
            self.validate_name_length(field.name)
            self.validate_identifier_format(field.name)
            self.validate_type(field.field_type)
            self.validate_description(field.name, field.description)
            field_names.append(field.name)

        self.validate_duplicates(field_names)
        return True