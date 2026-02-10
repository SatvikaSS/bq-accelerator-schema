from typing import List, Optional

from app.pipeline.bigquery_schema import BigQuerySchema
from app.canonical.field import CanonicalField
from app.standards.metadata_columns import get_standard_metadata_columns


class DocumentationGenerator:
    """
    Generates human-readable documentation (Markdown) for schemas.

    Responsibilities:
    - Produce data dictionary style documentation
    - Flatten nested fields using dot-notation
    - Separate business and system columns
    - Include table metadata, partitioning, clustering

    DOES NOT:
    - Validate schema
    - Mutate schema
    - Generate executable artifacts
    """

    def __init__(
        self,
        bq_schema: BigQuerySchema,
        partitioning: Optional[dict] = None,
        clustering: Optional[dict] = None,
    ):
        self.bq_schema = bq_schema
        self.partitioning = partitioning
        self.clustering = clustering

        # System column names derived from platform standards
        self.system_column_names = {
            col["name"]
            for col in get_standard_metadata_columns()
        }

    # --------------------------------------------------
    # Public entrypoint
    # --------------------------------------------------

    def generate_markdown(self) -> str:
        lines: List[str] = []

        table_name = self.bq_schema.table_name
        description = self.bq_schema.table_description or "No description provided."

        lines.append(f"# {table_name}")
        lines.append("")
        lines.append("## Table Description")
        lines.append(description)
        lines.append("")
        lines.append("---")
        lines.append("")

        self._render_table_properties(lines)
        self._render_columns(lines)

        return "\n".join(lines)

    # --------------------------------------------------
    # Table-level sections
    # --------------------------------------------------

    def _render_table_properties(self, lines: List[str]):
        lines.append("## Table Properties")
        lines.append("")

        dataset_info = self.bq_schema.canonical_schema.dataset or {}

        lines.append(f"- **Domain**: {dataset_info.get('domain', 'N/A')}")
        lines.append(f"- **Environment**: {dataset_info.get('environment', 'N/A')}")
        lines.append(f"- **Zone**: {dataset_info.get('zone', 'N/A')}")
        lines.append(f"- **Layer**: {dataset_info.get('layer', 'N/A')}")

        if self.partitioning:
            p = self.partitioning.get("partitioning_suggestion", {})
            col = p.get("column")
            granularity = p.get("granularity")
            if col and granularity:
                lines.append(f"- **Partitioning**: {granularity}({col})")

        if self.clustering:
            c = self.clustering.get("clustering", {})
            if c.get("suggested"):
                cols = ", ".join(c.get("columns", []))
                lines.append(f"- **Clustering**: {cols}")

        lines.append("")
        lines.append("---")
        lines.append("")

    # --------------------------------------------------
    # Column sections
    # --------------------------------------------------

    def _render_columns(self, lines: List[str]):
        fields = self.bq_schema.canonical_schema.tables[0].fields

        business_fields = [
            f for f in fields if f.name not in self.system_column_names
        ]

        system_fields = [
            f for f in fields if f.name in self.system_column_names
        ]

        if business_fields:
            lines.append("## Business Columns")
            lines.append("")
            self._render_field_table(lines, business_fields)
            lines.append("")

        if system_fields:
            lines.append("## System / Metadata Columns")
            lines.append("")
            self._render_field_table(lines, system_fields)
            lines.append("")

    def _render_field_table(self, lines: List[str], fields: List[CanonicalField]):
        lines.append("| Column Name | Data Type | Mode | Description |")
        lines.append("|------------|----------|------|-------------|")

        for field in fields:
            self._render_field_recursive(lines, field)

    # --------------------------------------------------
    # Recursive field rendering
    # --------------------------------------------------

    def _render_field_recursive(
        self,
        lines: List[str],
        field: CanonicalField,
        parent: Optional[str] = None,
    ):
        name = f"{parent}.{field.name}" if parent else field.name

        if field.is_array:
            mode = "REPEATED"
        else:
            mode = "NULLABLE" if field.nullable else "REQUIRED"

        dtype = field.data_type
        description = field.description or ""

        lines.append(
            f"| {name} | {dtype} | {mode} | {description} |"
        )

        # Recurse for RECORD fields
        if field.data_type == "RECORD" and field.children:
            for child in field.children:
                self._render_field_recursive(lines, child, parent=name)