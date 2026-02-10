"""
Pipeline step: Canonical → BigQuery naming normalization

Responsibilities:
- Normalize dataset name (BigQuery-safe)
- Normalize table names (BigQuery-safe)
- Normalize column names (BigQuery-safe)
- Resolve BigQuery reserved keywords
- Track rename mappings (raw → BigQuery)

Runs AFTER canonical schema creation
Runs BEFORE BigQuery schema generation
"""

import re
from typing import Dict

from app.canonical.schema import CanonicalSchema
from app.standards.bigquery_reserved_keywords import (
    is_bigquery_reserved_keyword,
)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def normalize_identifier(name: str) -> str:
    """
    Normalize identifier to BigQuery standards.

    Rules:
    - Trim whitespace
    - Lowercase
    - Allow only [a-z0-9_]
    - Collapse multiple underscores
    - Must start with a letter or underscore
    """
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)

    if not re.match(r"[a-z_]", name[0]):
        name = f"_{name}"

    return name


def build_dataset_name(domain: str, env: str, zone: str) -> str:
    """
    Build BigQuery dataset name using:
    {domain}_{environment}_{zone}
    """
    return normalize_identifier(f"{domain}_{env}_{zone}")


def build_table_name(domain: str, entity: str, layer: str) -> str:
    """
    Build BigQuery table name using:
    {domain}_{entity}_{layer}
    """
    return normalize_identifier(f"{domain}_{entity}_{layer}")


# ------------------------------------------------------------------
# Pipeline entry
# ------------------------------------------------------------------

def apply_naming_normalization(schema: CanonicalSchema) -> CanonicalSchema:
    """
    Apply BigQuery-safe naming to dataset, tables, and columns.
    Populate rename_mappings for traceability.
    """

    # ------------------------------------------------------------------
    # Dataset naming
    # ------------------------------------------------------------------
    domain = schema.dataset.get("domain")
    env = schema.dataset.get("environment")
    zone = schema.dataset.get("zone")
    layer = schema.dataset.get("layer")

    if not (domain and env and zone and layer):
        raise ValueError(
            "Dataset metadata must include domain, environment, zone, and layer"
        )

    schema.dataset["dataset_name"] = build_dataset_name(domain, env, zone)

    # Initialize rename mappings
    schema.rename_mappings = {
        "tables": {},
        "columns": {},
    }

    # ------------------------------------------------------------------
    # Table & column naming
    # ------------------------------------------------------------------
    for table in schema.tables:
        raw_table_name = table.name

        canonical_table_name = build_table_name(
            domain=domain,
            entity=raw_table_name,
            layer=layer,
        )

        schema.rename_mappings["tables"][raw_table_name] = canonical_table_name
        table.name = canonical_table_name

        # Column rename tracking (table-scoped)
        schema.rename_mappings["columns"][canonical_table_name] = {}

        seen_columns: Dict[str, int] = {}

        for field in table.fields:
            raw_column_name = field.name

            # Normalize column name
            normalized = normalize_identifier(raw_column_name)

            # Reserved keyword handling
            if is_bigquery_reserved_keyword(normalized):
                normalized = f"{canonical_table_name}_{normalized}"

            # Deterministic deduplication
            count = seen_columns.get(normalized, 0) + 1
            seen_columns[normalized] = count

            final_column_name = (
                normalized if count == 1 else f"{normalized}_{count}"
            )

            schema.rename_mappings["columns"][canonical_table_name][
                raw_column_name
            ] = final_column_name

            field.name = final_column_name

    return schema