from typing import List, Dict

METADATA_COLUMNS = {
    "ingestion_timestamp",
    "source_system",
    "batch_id",
    "record_hash",
    "is_deleted",
    "deleted_timestamp",
    "op_type",
    "op_ts",
}

class SchemaDiff:
    """
    Computes schema drift between two BigQuery schemas.
    """

    def __init__(self, old_schema, new_schema):
        self.old_schema = {
            f["name"]: f
            for f in old_schema
            if f["name"] not in METADATA_COLUMNS
        }
        self.new_schema = {
            f["name"]: f
            for f in new_schema
            if f["name"] not in METADATA_COLUMNS
        }

    def diff(self) -> Dict:
        added = []
        removed = []
        modified = []

        for name, new_field in self.new_schema.items():
            if name not in self.old_schema:
                added.append(new_field)
            else:
                old_field = self.old_schema[name]
                if self._field_changed(old_field, new_field):
                    modified.append({
                        "column": name,
                        "old": old_field,
                        "new": new_field
                    })

        for name, old_field in self.old_schema.items():
            if name not in self.new_schema:
                removed.append(old_field)

        breaking, non_breaking = self._classify_changes(
            added, removed, modified
        )

        return {
            "added_columns": added,
            "removed_columns": removed,
            "modified_columns": modified,
            "breaking_changes": breaking,
            "non_breaking_changes": non_breaking,
        }

    def _field_changed(self, old: Dict, new: Dict) -> bool:
        return (
            old["type"] != new["type"]
            or old["mode"] != new["mode"]
            or old.get("description") != new.get("description")
        )

    def _classify_changes(
        self,
        added: List[Dict],
        removed: List[Dict],
        modified: List[Dict],
    ):
        breaking = []
        non_breaking = []

        # Added columns
        for col in added:
            if col["mode"] == "REQUIRED":
                breaking.append({
                    "type": "ADD_REQUIRED_COLUMN",
                    "column": col["name"]
                })
            else:
                non_breaking.append({
                    "type": "ADD_NULLABLE_COLUMN",
                    "column": col["name"]
                })

        # Removed columns → always breaking
        for col in removed:
            breaking.append({
                "type": "REMOVE_COLUMN",
                "column": col["name"]
            })

        # Modified columns
        for change in modified:
            old = change["old"]
            new = change["new"]

            if old["type"] != new["type"]:
                breaking.append({
                    "type": "TYPE_CHANGE",
                    "column": new["name"],
                    "from": old["type"],
                    "to": new["type"],
                })
            elif old["mode"] == "NULLABLE" and new["mode"] == "REQUIRED":
                breaking.append({
                    "type": "NULLABLE_TO_REQUIRED",
                    "column": new["name"]
                })
            else:
                non_breaking.append({
                    "type": "NON_BREAKING_MODIFICATION",
                    "column": new["name"]
                })

        return breaking, non_breaking
