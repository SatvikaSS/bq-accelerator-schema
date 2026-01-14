from accelerator.outputs.bigquery_schema import BigQuerySchema


class BigQueryDDLGenerator:
    """
    Generates BigQuery CREATE TABLE DDL from BigQuerySchema
    """

    def __init__(self, bq_schema: BigQuerySchema):
        self.bq_schema = bq_schema

    def generate(
        self,
        dataset: str,
        table: str,
        if_not_exists: bool = True
    ) -> str:
        """
        Generate CREATE TABLE DDL

        :param dataset: BigQuery dataset name
        :param table: BigQuery table name
        :param if_not_exists: Whether to include IF NOT EXISTS
        """

        fields = self.bq_schema.generate()

        column_definitions = []

        for field in fields:
            column_sql = f"`{field.name}` {field.field_type}"

            if field.mode == "REQUIRED":
                column_sql += " NOT NULL"

            column_definitions.append(column_sql)

        columns_sql = ",\n  ".join(column_definitions)

        ine_clause = "IF NOT EXISTS " if if_not_exists else ""

        ddl = f"""
CREATE TABLE {ine_clause}`{dataset}.{table}` (
  {columns_sql}
);
""".strip()

        return ddl
