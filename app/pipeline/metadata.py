"""
Pipeline step: Metadata Injection + Description Enrichment

Responsibilities:
- Inject standard platform metadata columns
- Generate descriptions for source columns if missing
- Preserve canonical schema integrity
- Avoid circular dependencies

Runs AFTER adapters and BEFORE output rendering.
"""

from typing import Set

from app.standards.metadata_columns import (
    get_standard_metadata_columns,
)
from app.canonical.schema import CanonicalSchema
from app.canonical.table import CanonicalTable
from app.canonical.field import CanonicalField


class MetadataInjector:
    """
    Injects metadata columns and enriches column descriptions.
    """

    def apply(self, schema: CanonicalSchema) -> CanonicalSchema:
        metadata_defs = get_standard_metadata_columns()

        for table in schema.tables:
            self._inject_metadata(table, metadata_defs)
            self._enrich_descriptions(table)

        return schema

    # Metadata injection
    def _inject_metadata(
        self,
        table: CanonicalTable,
        metadata_defs: list[dict],
    ):
        existing_names: Set[str] = {
            field.name.lower() for field in table.fields
        }

        for meta in metadata_defs:
            if meta["name"].lower() in existing_names:
                continue

            table.fields.append(
                CanonicalField(
                    name=meta["name"],
                    data_type=meta["type"],
                    nullable=(meta["mode"] == "NULLABLE"),
                    description=meta.get("description"),
                    has_missing=False,
                    numeric_metadata=None,
                )
            )

    # Description enrichment (SOURCE columns)
    def _enrich_descriptions(self, table: CanonicalTable):
        """
        Generate human-readable descriptions for columns
        that do not already have one.
        """

        for field in table.fields:
            if field.description:
                continue

            field.description = self._generate_description(field)

    def _generate_description(self, field: CanonicalField) -> str:
        """
        Deterministic description generator.
        """
        base_name = field.name.replace("_", " ").lower()

        desc = f"{base_name.capitalize()} stored as {field.data_type.lower()} value."

        if field.nullable:
            desc += " Nullable field."
        else:
            desc += " Required field."

        return desc
