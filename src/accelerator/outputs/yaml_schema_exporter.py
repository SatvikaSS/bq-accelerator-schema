import yaml
from typing import List, Dict


class YAMLSchemaExporter:
    """
    Exports schema into YAML format.
    Works with BigQuery-compatible schema dictionaries.
    """

    def __init__(self, schema: List[Dict]):
        """
        :param schema: List of BigQuery schema dictionaries
        """
        self.schema = schema

    def export_to_string(self) -> str:
        """
        Export schema as YAML string
        """
        return yaml.safe_dump(
            self.schema,
            sort_keys=False,
            default_flow_style=False
        )

    def export_to_file(self, file_path: str):
        """
        Export schema to YAML file
        """
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(self.export_to_string())
