from typing import List, Optional, Dict
from datetime import datetime

from app.pipeline.bigquery_schema import BigQuerySchema
from app.canonical.field import CanonicalField
from app.standards.metadata_columns import get_standard_metadata_columns


class DocumentationGenerator:
    """
    Enterprise-grade Markdown documentation generator.

    Includes:
    - Table metadata
    - Partitioning & clustering
    - Retention
    - Versioning (optional)
    - Security classification summary
    - Classification coverage
    - Rename mappings
    - Business vs system columns
    - Nested field flattening
    """

    def __init__(
        self,
        bq_schema: BigQuerySchema,
        partitioning: Optional[dict] = None,
        clustering: Optional[dict] = None,
        security_analysis: Optional[Dict] = None,
        rename_mappings: Optional[Dict] = None,
        entity: Optional[str] = None,
        version: Optional[str] = None,
        decision: Optional[str] = None,
        drift_policy: Optional[str] = None,
    ):
        self.bq_schema = bq_schema
        self.partitioning = partitioning
        self.clustering = clustering
        self.security_analysis = security_analysis or {}
        self.rename_mappings = rename_mappings or {}
        self.entity = entity
        self.version = version
        self.decision = decision
        self.drift_policy = drift_policy

        self.system_column_names = {
            col["name"] for col in get_standard_metadata_columns()
        }

    # ======================================================
    # PUBLIC ENTRYPOINT
    # ======================================================

    def generate_markdown(self) -> str:
        lines: List[str] = []

        table_name = self.bq_schema.table_name
        description = self.bq_schema.table_description or "No description provided."

        lines.append(f"# {table_name}")
        lines.append("")
        lines.append("## Overview")
        lines.append(description)
        lines.append("")
        lines.append("---")
        lines.append("")

        self._render_table_properties(lines)
        self._render_versioning(lines)
        self._render_security_section(lines)
        self._render_columns(lines)
        self._render_rename_mappings(lines)
        self._render_footer(lines)

        return "\n".join(lines)

    # ======================================================
    # TABLE PROPERTIES
    # ======================================================

    def _render_table_properties(self, lines: List[str]):
        lines.append("## Table Properties")
        lines.append("")

        dataset_info = self.bq_schema.canonical_schema.dataset or {}

        lines.append(f"- **Domain**: {dataset_info.get('domain', 'N/A')}")
        lines.append(f"- **Environment**: {dataset_info.get('environment', 'N/A')}")
        lines.append(f"- **Zone**: {dataset_info.get('zone', 'N/A')}")
        lines.append(f"- **Layer**: {dataset_info.get('layer', 'N/A')}")

        # Partitioning
        if self.partitioning:
            p = self.partitioning.get("partitioning_suggestion", {})
            if p.get("strategy") == "COLUMN":
                lines.append(
                    f"- **Partitioning**: {p.get('granularity')}({p.get('column')})"
                )
            elif p.get("strategy") == "INGESTION_TIME":
                lines.append("- **Partitioning**: INGESTION_TIME")


        # Clustering
        if self.clustering:
            c = self.clustering.get("clustering", {})
            if c.get("suggested"):
                cols = ", ".join(c.get("columns", []))
                lines.append(f"- **Clustering**: {cols}")

        lines.append("")
        lines.append("---")
        lines.append("")

    # ======================================================
    # VERSIONING SECTION
    # ======================================================

    def _render_versioning(self, lines: List[str]):
        if not self.entity:
            return

        lines.append("## Versioning Information")
        lines.append("")
        lines.append(f"- **Entity**: {self.entity}")
        lines.append(f"- **Version**: {self.version or 'N/A'}")
        lines.append(f"- **Decision**: {self.decision or 'N/A'}")
        lines.append(f"- **Drift Policy Applied**: {self.drift_policy or 'N/A'}")
        lines.append("")
        lines.append("---")
        lines.append("")

    # ======================================================
    # SECURITY SECTION
    # ======================================================

    def _render_security_section(self, lines: List[str]):
        lines.append("## Security & Governance")
        lines.append("")

        total_columns = len(self.bq_schema.canonical_schema.tables[0].fields)
        classified_columns = len(self.security_analysis)

        lines.append("### Classification Summary")
        lines.append("")
        lines.append(f"- **Total Columns**: {total_columns}")
        lines.append(f"- **Classified Columns**: {classified_columns}")
        lines.append("")

        if not self.security_analysis:
            lines.append("_No PII or sensitive fields detected._")
            lines.append("")
            lines.append("---")
            lines.append("")
            return

        lines.append("### Classified Columns")
        lines.append("")
        lines.append(
            "| Column | Classification | Category | Confidence | Recommended Control |"
        )
        lines.append(
            "|--------|----------------|----------|------------|---------------------|"
        )

        for col, info in self.security_analysis.items():
            lines.append(
                f"| {col} | {info.get('classification')} | "
                f"{info.get('category')} | "
                f"{info.get('confidence')} | "
                f"{info.get('recommended_control')} |"
            )

        lines.append("")
        lines.append("---")
        lines.append("")

    # ======================================================
    # COLUMN DATA DICTIONARY
    # ======================================================

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

    def _render_field_recursive(
        self,
        lines: List[str],
        field: CanonicalField,
        parent: Optional[str] = None,
    ):
        name = f"{parent}.{field.name}" if parent else field.name

        mode = "REPEATED" if field.is_array else (
            "NULLABLE" if field.nullable else "REQUIRED"
        )

        dtype = field.data_type
        description = field.description or ""

        lines.append(
            f"| {name} | {dtype} | {mode} | {description} |"
        )

        if field.data_type == "RECORD" and field.children:
            for child in field.children:
                self._render_field_recursive(lines, child, parent=name)

    # ======================================================
    # RENAME MAPPINGS
    # ======================================================

    def _render_rename_mappings(self, lines: List[str]):
        if not self.rename_mappings:
            return

        columns = self.rename_mappings.get("columns", {})
        if not columns:
            return

        lines.append("## Naming Normalization")
        lines.append("")
        lines.append("| Original Column | Standardized Column |")
        lines.append("|----------------|---------------------|")

        for table_map in columns.values():
            for original, renamed in table_map.items():
                if original != renamed:
                    lines.append(f"| {original} | {renamed} |")

        lines.append("")
        lines.append("---")
        lines.append("")

    # ======================================================
    # FOOTER
    # ======================================================

    def _render_footer(self, lines: List[str]):
        lines.append("## Metadata")
        lines.append("")
        lines.append(
            f"- **Generated At (UTC)**: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"
        )