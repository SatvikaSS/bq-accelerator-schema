import json
from typing import List, Dict


class BigQueryJSONSchemaExporter:
    """
    Exports BigQuery schema to JSON format.
    """

    def __init__(self, schema: List[Dict]):
        self.schema = schema

    def export(self) -> List[Dict]:
        """
        Return schema as JSON-serializable object.
        """
        return self.schema

    def export_to_string(self, indent: int = 2) -> str:
        """
        Export schema as formatted JSON string.
        """
        return json.dumps(self.schema, indent=indent)

    def export_to_file(self, file_path: str, indent: int = 2):
        """
        Write schema to a JSON file.
        """
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(self.schema, f, indent=indent)
