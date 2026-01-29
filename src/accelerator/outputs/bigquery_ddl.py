from accelerator.outputs.bigquery_schema import BigQuerySchema
from accelerator.utils.naming import build_table_name, build_dataset_name


class BigQueryDDLGenerator:
    """
    Generates BigQuery CREATE TABLE DDL from BigQuerySchema.
    Enforces:
      - Table naming: {domain}_{entity}_{layer}
      - Dataset naming: {domain}_{env}_{zone}
    """

    def __init__(self, bq_schema: BigQuerySchema):
        self.bq_schema = bq_schema

    def generate(
        self,
        domain: str,
        env: str,
        zone: str,
        entity: str,
        layer: str,
        if_not_exists: bool = True
    ) -> str:

        dataset = build_dataset_name(domain, env, zone)
        table_name = build_table_name(domain, entity, layer)

        fields = self.bq_schema.generate()
        column_definitions = []

        for field in fields:
            column_sql = f"`{field.name}` {field.field_type}"

            if field.mode == "REQUIRED":
                column_sql += " NOT NULL"

            if field.description:
                column_sql += f' OPTIONS(description="{field.description}")'

            column_definitions.append(column_sql)

        columns_sql = ",\n  ".join(column_definitions)
        ine_clause = "IF NOT EXISTS " if if_not_exists else ""

        # Table-level description
        table_description = self.bq_schema.table_description
        table_options_sql = ""

        if table_description:
            table_options_sql = (
                f'\nOPTIONS (\n'
                f'  description = "{table_description}"\n'
                f')'
            )

        return f"""
CREATE TABLE {ine_clause}`{dataset}.{table_name}` (
  {columns_sql}
){table_options_sql};
""".strip()