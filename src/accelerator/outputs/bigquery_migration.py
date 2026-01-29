from typing import List, Dict


class BigQueryMigrationGenerator:
    """
    Generates BigQuery ALTER TABLE statements
    for non-breaking schema changes.
    """

    def __init__(self, dataset: str, table: str):
        self.dataset = dataset
        self.table = table

    def generate(self, diff_report: Dict) -> List[str]:
        statements = []

        for change in diff_report["non_breaking_changes"]:
            if change["type"] == "ADD_NULLABLE_COLUMN":
                col = change["column"]
                col_def = next(
                    c for c in diff_report["added_columns"]
                    if c["name"] == col
                )

                stmt = (
                    f"ALTER TABLE `{self.dataset}.{self.table}` "
                    f"ADD COLUMN `{col}` {col_def['type']};"
                )
                statements.append(stmt)

        return statements
