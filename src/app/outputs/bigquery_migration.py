from typing import List, Dict


class BigQueryMigrationGenerator:
    """
    Generate BigQuery ALTER TABLE statements for NON-BREAKING changes.

    Supported:
    - ADD NULLABLE column
    """

    def __init__(self, dataset: str, table: str):
        self.dataset = dataset
        self.table = table

    def generate(self, diff_report: Dict) -> List[str]:
        statements: List[str] = []

        non_breaking = diff_report.get("non_breaking_changes", [])
        added_columns = diff_report.get("added_columns", [])

        for change in non_breaking:
            if change.get("type") != "ADD_NULLABLE_COLUMN":
                continue

            column_name = change["column"]

            try:
                col_def = next(
                    c for c in added_columns
                    if c["name"] == column_name
                )
            except StopIteration:
                raise ValueError(
                    f"Missing column definition for '{column_name}' "
                    f"in diff_report['added_columns']"
                )

            if col_def.get("mode", "NULLABLE") != "NULLABLE":
                raise ValueError(
                    f"BigQuery ALTER TABLE supports only NULLABLE columns. "
                    f"Invalid mode for '{column_name}'."
                )

            stmt = (
                f"ALTER TABLE `{self.dataset}.{self.table}` "
                f"ADD COLUMN `{column_name}` {col_def['type']}"
            )

            description = col_def.get("description")
            if description:
                escaped = description.replace('"', '\\"')
                stmt += f' OPTIONS(description="{escaped}")'

            stmt += ";"
            statements.append(stmt)
        
        return statements