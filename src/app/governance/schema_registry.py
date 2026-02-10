import json
import os
import hashlib
from datetime import datetime
from typing import Dict, List


def utc_now():
    return datetime.utcnow().isoformat() + "Z"


# --------------------------------------------------
# SCHEMA HASHING (ORDER-SAFE, STRUCTURAL)
# --------------------------------------------------

def normalize_schema_for_hash(schema: List[Dict]) -> List[Dict]:
    """
    Normalize schema so hashing is:
    - order-independent
    - structural-only (ignores descriptions)
    """
    normalized = []

    for field in schema:
        normalized.append({
            "name": field["name"],
            "type": field["type"],
            "mode": field.get("mode", "NULLABLE"),
        })

    return sorted(normalized, key=lambda x: x["name"])


def compute_schema_hash(schema: List[Dict]) -> str:
    normalized_schema = normalize_schema_for_hash(schema)
    payload = json.dumps(normalized_schema, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# --------------------------------------------------
# SCHEMA REGISTRY
# --------------------------------------------------

class SchemaRegistry:
    """
    Enterprise schema registry with versioning support.
    Stores structural schema snapshots only.
    """

    def __init__(self, path: str | None = None):
        if path is None:
            base_dir = os.path.dirname(__file__)
            self.path = os.path.join(base_dir, "schema_registry.json")
        else:
            self.path = path

        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            with open(self.path, "r") as f:
                self.data = json.load(f)
        else:
            self.data = {}

    def _save(self):
        with open(self.path, "w") as f:
            json.dump(self.data, f, indent=2)

    # --------------------------------------------------
    # READ OPERATIONS
    # --------------------------------------------------

    def get_entity(self, entity: str):
        return self.data.get(entity)

    def get_current_version(self, entity: str):
        entity_data = self.get_entity(entity)
        if not entity_data:
            return None
        return entity_data["current_version"]

    # --------------------------------------------------
    # WRITE OPERATIONS
    # --------------------------------------------------

    def register_new_entity(self, entity: str, schema: List[Dict]):
        if entity in self.data:
            raise ValueError(f"Entity '{entity}' already exists")

        schema_hash = compute_schema_hash(schema)

        self.data[entity] = {
            "entity": entity,
            "current_version": "v1",
            "versions": {
                "v1": {
                    "version": "v1",
                    "table_name": f"{entity}_v1",
                    "schema_hash": schema_hash,
                    "generated_at": utc_now(),
                    "modified_at": utc_now(),
                    "breaking_change": False,
                    "change_summary": ["initial version"],
                    "schema": schema,
                }
            },
        }

        self._save()
        return "v1"

    def register_new_version(
        self,
        entity: str,
        schema: List[Dict],
        breaking: bool,
        change_summary: List[Dict],
    ):
        if entity not in self.data:
            raise ValueError(f"Entity '{entity}' does not exist")

        entity_data = self.data[entity]
        current_version = entity_data["current_version"]
        current_entry = entity_data["versions"][current_version]

        new_schema_hash = compute_schema_hash(schema)

        # Prevent duplicate versions with identical schema
        if new_schema_hash == current_entry["schema_hash"]:
            return current_version

        next_version_num = int(current_version[1:]) + 1
        next_version = f"v{next_version_num}"

        entity_data["versions"][next_version] = {
            "version": next_version,
            "table_name": f"{entity}_{next_version}",
            "schema_hash": new_schema_hash,
            "generated_at": utc_now(),
            "modified_at": utc_now(),
            "breaking_change": breaking,
            "change_summary": change_summary,
            "schema": schema,
        }

        entity_data["current_version"] = next_version
        self._save()

        return next_version

    def update_current_version_schema(
        self,
        entity: str,
        schema: List[Dict],
        change_summary: List[Dict],
    ):
        if entity not in self.data:
            raise ValueError(f"Entity '{entity}' does not exist")

        entity_data = self.data[entity]
        current_version = entity_data["current_version"]
        version_entry = entity_data["versions"][current_version]

        new_schema_hash = compute_schema_hash(schema)

        # No-op if schema unchanged
        if new_schema_hash == version_entry["schema_hash"]:
            return current_version

        version_entry["schema"] = schema
        version_entry["schema_hash"] = new_schema_hash
        version_entry["modified_at"] = utc_now()
        version_entry["change_summary"].extend(change_summary)

        self._save()
        return current_version