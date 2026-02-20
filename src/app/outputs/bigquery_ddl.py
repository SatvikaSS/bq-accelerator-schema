from dataclasses import field
from typing import Optional, Dict

from app.pipeline.bigquery_schema import BigQuerySchema
from app.pipeline.naming import build_dataset_name, build_table_name


class BigQueryDDLGenerator:
    """
    Generates BigQuery DDL statements.

    Responsibilities:
    - CREATE SCHEMA (dataset)
    - CREATE TABLE
    - Apply partitioning & clustering ONLY when explicitly provided
    - Apply naming conventions
    - Apply descriptions

    Design principles:
    - Advisory-first (router controls opt-in)
    - BigQuery-correct syntax
    - Deterministic output
    """

    def __init__(
        self,
        bq_schema: BigQuerySchema,
        partitioning: Optional[Dict] = None,
        clustering: Optional[Dict] = None,
        project: Optional[str] = None,
        location: str = "US",
    ):
        self.bq_schema = bq_schema
        self.partitioning = partitioning
        self.clustering = clustering
        self.project = project
        self.location = location

    def _render_field(self, field) -> str:
        """
        Render a BigQuery column definition from BigQueryField.
        Correctly handles:
        - SCALAR
        - ARRAY<SCALAR>
        - STRUCT
        - ARRAY<STRUCT>
        """

        # ----------------------------
        # Helper: escape description
        # ----------------------------
        def render_description(desc: str | None) -> str:
            if not desc:
                return ""
            escaped = desc.replace('"', '\\"')
            return f' OPTIONS(description="{escaped}")'

        # RANGE special-case (BigQuery requires RANGE<element_type>)
        if field.field_type == "RANGE":
            elem = getattr(field, "range_element_type", None) or "DATE"
            col = f"`{field.name}` RANGE<{elem}>"
            if field.mode == "REQUIRED":
                col += " NOT NULL"
            col += render_description(field.description)
            return col
        # SCALAR (non-RECORD)
        if field.field_type != "RECORD":
            if field.mode == "REPEATED":
                col = f"`{field.name}` ARRAY<{field.field_type}>"
            else:
                col = f"`{field.name}` {field.field_type}"
                if field.mode == "REQUIRED":
                    col += " NOT NULL"

            col += render_description(field.description)
            return col

        # RECORD / STRUCT
        nested_cols = [self._render_field(child) for child in field.subfields]
        nested_block = ", ".join(
            c.replace(" NOT NULL", "")  # STRUCT children cannot be NOT NULL
            for c in nested_cols
        )

        # STRUCT vs ARRAY<STRUCT>
        if field.mode == "REPEATED":
            col = f"`{field.name}` ARRAY<STRUCT<{nested_block}>>"
        else:
            col = f"`{field.name}` STRUCT<{nested_block}>"
            if field.mode == "REQUIRED":
                col += " NOT NULL"

        col += render_description(field.description)
        return col
    
    # --------------------------------------------------
    # DATASET DDL
    # --------------------------------------------------

    def generate_dataset_ddl(self, domain: str, env: str, zone: str) -> str:
        dataset = build_dataset_name(domain, env, zone)
        dataset_ref = f"`{self.project}.{dataset}`" if self.project else f"`{dataset}`"

        return (
            f"CREATE SCHEMA IF NOT EXISTS {dataset_ref}\n"
            f"OPTIONS(location='{self.location}');"
        )

    # --------------------------------------------------
    # TABLE DDL
    # --------------------------------------------------

    def generate_table_ddl(
        self,
        domain: str,
        env: str,
        zone: str,
        entity: str,
        layer: str,
        if_not_exists: bool = True,
    ) -> str:
        dataset = build_dataset_name(domain, env, zone)
        table_name = build_table_name(domain, entity, layer)

        if self.project:
            table_ref = f"`{self.project}.{dataset}.{table_name}`"
        else:
            table_ref = f"`{dataset}.{table_name}`"

        fields = self.bq_schema.generate()
        column_sql = []

        for field in fields:
            column_sql.append(self._render_field(field))

        columns_block = ",\n  ".join(column_sql)
        ine = "IF NOT EXISTS " if if_not_exists else ""

        partition_clause = self._build_partitioning_clause()
        clustering_clause = self._build_clustering_clause()

        options = {}

        # Description
        if self.bq_schema.table_description:
            options["description"] = self.bq_schema.table_description
        
        table_options = ""
        if options:
            rendered = []
            for k, v in options.items():
                if isinstance(v, str):
                    v = v.replace('"', '\\"')
                    rendered.append(f'{k}="{v}"')
                else:
                    rendered.append(f"{k}={v}")

            table_options = (
                "\nOPTIONS(\n  "
                + ",\n  ".join(rendered)
                + "\n)"
            )

        return (
            f"CREATE TABLE {ine}{table_ref} (\n"
            f"  {columns_block}\n"
            f")"
            f"{partition_clause}"
            f"{clustering_clause}"
            f"{table_options};"
        )

    # --------------------------------------------------
    # PARTITIONING & CLUSTERING HELPERS
    # --------------------------------------------------
    def _build_partitioning_clause(self) -> str:
        if not self.partitioning:
            return ""

        p = self.partitioning.get("partitioning_suggestion", {})
        strategy = p.get("strategy")
        column = p.get("column")
        column_type = p.get("column_type")

        # Column-based partitioning
        if strategy == "COLUMN" and column:
            # DATE column → native DATE partitioning
            if column_type == "DATE":
                return f"\nPARTITION BY `{column}`"

            # TIMESTAMP / DATETIME → DAY partition via DATE()
            if column_type in ("TIMESTAMP", "DATETIME"):
                return f"\nPARTITION BY DATE(`{column}`)"

        # Ingestion-time partitioning
        if strategy == "INGESTION_TIME":
            return "\nPARTITION BY _PARTITIONTIME"

        return ""

    def _build_clustering_clause(self) -> str:
        if not self.clustering:
            return ""

        c = self.clustering.get("clustering", {})
        if not c.get("suggested"):
            return ""

        columns = c.get("columns", [])
        if not columns:
            return ""

        cols = ", ".join(f"`{col}`" for col in columns)
        return f"\nCLUSTER BY {cols}"

    # --------------------------------------------------
    # COMBINED ENTRYPOINT (USED BY ROUTER)
    # --------------------------------------------------

    def generate(
        self,
        domain: str,
        env: str,
        zone: str,
        entity: str,
        layer: str,
    ) -> Dict[str, str]:
        """
        Generate dataset and table DDLs.
        """
        return {
            "dataset_ddl": self.generate_dataset_ddl(domain, env, zone),
            "table_ddl": self.generate_table_ddl(domain, env, zone, entity, layer),
        }