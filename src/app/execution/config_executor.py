import os
from typing import Dict

import yaml
from fastapi import Request

from app.router import route


class ConfigRequest(Request):
    """
    Minimal Request wrapper for config-driven execution.
    Provides headers for identity extraction in router.
    """

    def __init__(self, user_id: str = "config_executor"):
        scope = {"type": "http", "headers": []}
        super().__init__(scope)
        self._user_id = user_id

    @property
    def headers(self):
        return {"x-user-id": self._user_id}


class ConfigExecutor:
    """
    Executes the full accelerator pipeline using YAML configuration.
    """

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()

    # ------------------------------------------
    # Load YAML
    # ------------------------------------------
    def _load_config(self) -> Dict:
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    # ------------------------------------------
    # Build Router Payload
    # ------------------------------------------
    def _build_payload(self) -> Dict:
        cfg = self.config

        source_cfg = cfg.get("source", {})
        settings = cfg.get("settings", {})

        return {
            "file_path": source_cfg.get("file_path"),
            "entity": cfg.get("entity"),
            "domain": cfg.get("domain"),
            "environment": cfg.get("environment"),
            "zone": cfg.get("zone"),
            "layer": cfg.get("layer"),
            "output": cfg.get("output", "ALL_FORMATS"),
            "drift_policy": settings.get("drift_policy", "WARN"),
            "table_description": settings.get("table_description"),
            "csv_override": source_cfg.get("csv_override"),
            "type_overrides": settings.get("type_overrides"),
            "partition_override": self._build_partition_override(),
            "clustering_override": self._build_clustering_override(),
            "user_id": "config_executor",
            "return_dict_for_ddl": True,
            "return_dict_for_documentation": True,
        }

    # ------------------------------------------
    # Partition Override
    # ------------------------------------------
    def _build_partition_override(self):
        partitioning = self.config.get("partitioning", {})
        mode = partitioning.get("mode", "AUTO")

        if mode == "MANUAL":
            return {
                "mode": "MANUAL",
                "partitioning": {
                    "strategy": partitioning.get("strategy"),
                    "column": partitioning.get("column"),
                    "granularity": partitioning.get("granularity", "DAY"),
                },
            }

        if mode == "SKIP":
            return {"mode": "SKIP"}

        return None  # AUTO

    # ------------------------------------------
    # Clustering Override
    # ------------------------------------------
    def _build_clustering_override(self):
        clustering = self.config.get("clustering", {})
        mode = clustering.get("mode", "AUTO")

        if mode == "MANUAL":
            return {
                "mode": "MANUAL",
                "columns": clustering.get("columns", []),
            }

        if mode == "SKIP":
            return {"mode": "SKIP"}

        return None  # AUTO

    # ------------------------------------------
    # Execute Pipeline
    # ------------------------------------------
    def execute(self) -> Dict:
        payload = self._build_payload()
        request = ConfigRequest(user_id=payload.get("user_id", "config_executor"))
        result = route(payload, request)
        self._save_outputs(result)
        return result

    # ------------------------------------------
    # Save Outputs
    # ------------------------------------------
    def _save_outputs(self, result: Dict):
        os.makedirs("outputs", exist_ok=True)

        entity = result.get("entity", "unknown")

        if "schema_json" in result:
            import json
            with open(f"outputs/{entity}.json", "w", encoding="utf-8") as f:
                json.dump(result["schema_json"], f, indent=2)

        if "schema_yaml" in result:
            with open(f"outputs/{entity}.yaml", "w", encoding="utf-8") as f:
                f.write(result["schema_yaml"])

        if "ddl" in result:
            ddl = result["ddl"]
            with open(f"outputs/{entity}.sql", "w", encoding="utf-8") as f:
                f.write(ddl.get("dataset_ddl", ""))
                f.write("\n\n")
                f.write(ddl.get("table_ddl", ""))

        if "documentation" in result:
            with open(f"outputs/{entity}.md", "w", encoding="utf-8") as f:
                f.write(result["documentation"]["content"])
