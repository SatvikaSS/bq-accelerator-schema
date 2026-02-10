from typing import Dict
from fastapi.responses import Response

# ---------------- Input dispatch ----------------
from app.input.format_detector import FormatDetector
from app.governance.adapter_registry import AdapterRegistry

# ---------------- Pipeline steps ----------------
from app.standards.metadata_columns import get_standard_metadata_columns
from app.pipeline.naming import apply_naming_normalization
from app.pipeline.metadata import MetadataInjector
from app.pipeline.partitioning import generate_partitioning_suggestion
from app.pipeline.clustering import generate_clustering_suggestion
from app.pipeline.schema_validator import BigQuerySchemaValidator

# ---------------- Governance ----------------
from app.governance.schema_diff import SchemaDiff
from app.governance.drift_policy import DriftPolicy, DriftPolicyEnforcer
from app.governance.schema_registry import SchemaRegistry

# ---------------- Outputs ----------------
from app.pipeline.bigquery_schema import BigQuerySchema
from app.outputs.bigquery_ddl import BigQueryDDLGenerator
from app.outputs.bigquery_migration import BigQueryMigrationGenerator
from app.outputs.bigquery_json_generator import BigQueryJSONSchemaExporter
from app.outputs.yaml_schema_generator import YAMLSchemaExporter
from app.outputs.documentation_generator import DocumentationGenerator


# ==========================================================
# ROUTER
# ==========================================================

def route(payload: Dict) -> Dict:
    """
    Accelerator main entry point.

    Flow:
    Source → Canonical → Advisory Intelligence →
    BigQuery Schema → Validation → Governance → Outputs
    """

    # --------------------------------------------------
    # Required inputs
    # --------------------------------------------------
    file_path = payload["file_path"]
    entity = payload["entity"]

    domain = payload["domain"]
    env = payload["environment"]
    zone = payload["zone"]
    layer = payload["layer"]

    output_type = payload.get("output", "ALL_FORMATS").upper()
    drift_policy = payload.get("drift_policy", DriftPolicy.WARN)
    #drift_policy = payload["drift_policy"]

    if not DriftPolicy.is_valid(drift_policy):
        raise ValueError(f"Invalid drift_policy: {drift_policy}")

    # --------------------------------------------------
    # Phase 1 – Format detection + Adapter dispatch + Source Schema Check
    # --------------------------------------------------
    detector = FormatDetector(file_path)
    input_format = detector.detect()

    adapter_cls = AdapterRegistry.get_adapter(input_format)
    adapter = adapter_cls(file_path, entity_name=entity)

    canonical_schema = adapter.parse()

    SYSTEM_COLUMN_NAMES = {
        col["name"]
        for col in get_standard_metadata_columns()
    }

    # Source schema check
    for table in canonical_schema.tables:
        if not table.fields:
            raise ValueError(
                "Source schema contains no fields.It should have atleast one field."
            )
        # Business column using system name
        for field in table.fields:
            if field.name in SYSTEM_COLUMN_NAMES and not getattr(field, "is_system", False):
                raise ValueError(
                    f"Column '{field.name}' uses a reserved system column name "
                    "but is defined as a business field."
                )

        # System-only schema
        if all(field.name in SYSTEM_COLUMN_NAMES for field in table.fields):
            raise ValueError(
                "Schema contains only system columns. "
                "At least one business column is required."
            )

    # Inject dataset identity from payload
    canonical_schema.dataset.update({
        "domain": domain,
        "environment": env,
        "zone": zone,
        "layer": layer,
    })


    # --------------------------------------------------
    # Phase 2 – Naming normalization
    # --------------------------------------------------
    canonical_schema = apply_naming_normalization(canonical_schema)
    rename_mappings = canonical_schema.rename_mappings

    # --------------------------------------------------
    # Phase 3 – Metadata injection
    # --------------------------------------------------
    canonical_schema = MetadataInjector().apply(canonical_schema)
    
    user_table_description = payload.get("table_description")
    if user_table_description:
        canonical_schema.description = user_table_description

    # --------------------------------------------------
    # Phase 4 – Partitioning (canonical-level, advisory)
    # --------------------------------------------------
    partitioning = generate_partitioning_suggestion(
        schema=canonical_schema,
        zone=zone,
    )

    partition_column = None
    if (
        partitioning
        and partitioning["partitioning_suggestion"]["strategy"] == "COLUMN"
    ):
        partition_column = partitioning["partitioning_suggestion"]["column"]

    # --------------------------------------------------
    # Phase 5 – Clustering (payload-level, advisory)
    # --------------------------------------------------
    payload_fields = [
        {
            "name": field.name,
            "type": field.data_type,
            "mode": "REPEATED" if getattr(field, "is_array", False) else "NULLABLE",
        }
        for table in canonical_schema.tables
        for field in table.fields
    ]

    clustering = generate_clustering_suggestion(
        schema=payload_fields,
        partition_column=partition_column,
        query_patterns=payload.get("query_patterns"),
        user_override=payload.get("clustering_override"),
    )

    # --------------------------------------------------
    # Phase 6 – BigQuery schema generation
    # --------------------------------------------------
    bq_schema = BigQuerySchema(
        canonical_schema=canonical_schema,
        table_name=f"{domain}_{entity}_{layer}",
    )
    fields = bq_schema.generate()

    security_analysis = {}

    for field in fields:
        if field.security and field.security.get("category") != "NON_PII":
            security_analysis[field.name] = field.security

    # --------------------------------------------------
    # Phase 7 – Validation
    # --------------------------------------------------
    BigQuerySchemaValidator(bq_schema=bq_schema).validate()

    # --------------------------------------------------
    # Phase 8 – Drift detection & enforcement
    # --------------------------------------------------
    registry = SchemaRegistry()
    enforcer = DriftPolicyEnforcer(policy=drift_policy, registry=registry)

    previous_entry = registry.get_entity(entity)
    is_initial_creation = previous_entry is None

    if previous_entry:
        previous_version = registry.get_current_version(entity)
        previous_schema = previous_entry["versions"][previous_version]["schema"]

        diff_report = SchemaDiff(
            old_schema=previous_schema,
            new_schema=bq_schema.to_dict(),
        ).diff()
    else:
        diff_report = {
            "breaking_changes": [],
            "non_breaking_changes": [],
        }
    
    has_breaking = bool(diff_report.get("breaking_changes"))
    has_non_breaking = bool(diff_report.get("non_breaking_changes"))

    entity_name, version = enforcer.enforce(
        entity=entity,
        diff_report=diff_report,
        new_schema=bq_schema.to_dict(),
    )

    if has_breaking and drift_policy == DriftPolicy.WARN:
        return {
            "status": "WARNING",
            "entity": entity_name,
            "version": version,
            "decision": "BREAKING",
            "message": (
                "Breaking schema change detected. "
                "Schema not updated due to WARN policy."
            ),
            "partitioning": partitioning,
            "clustering": clustering,
        }

    migration_ddls = []
    if has_non_breaking and not has_breaking:
        migration_ddls = BigQueryMigrationGenerator(
            dataset=f"{domain}_{env}_{zone}",
            table=f"{entity}_{version}",
        ).generate(diff_report)

    # --------------------------------------------------
    # Phase 9 – Outputs
    # --------------------------------------------------
    expose_schema = not (has_breaking and drift_policy == DriftPolicy.WARN)
    
    if is_initial_creation:
        decision = "CREATE"
    elif diff_report["breaking_changes"]:
        decision = "BREAKING"
    elif diff_report["non_breaking_changes"]:
        decision = "NON_BREAKING"
    else:
        decision = "NO_OP"


    response = {
        "status": "SUCCESS",
        "entity": entity_name,
        "version": version,
        "decision": decision
    }

    if decision == "NO_OP":
        response["message"] = (
            "Schema is unchanged. Returning current active BigQuery schema."
        )
    response["partitioning"] = partitioning
    response["clustering"] = clustering
    response["rename_mappings"] = rename_mappings
    response["security_analysis"] = security_analysis
    response["security_summary"] = {
        "pii_detected": any(
            f["category"] == "PII" for f in security_analysis.values()
        ),
        "sensitive_detected": any(
            f["category"] == "SENSITIVE" for f in security_analysis.values()
        ),
        "unknown_detected": any(
            f["category"] == "UNKNOWN" for f in security_analysis.values()
        ),
        "classified_columns": list(security_analysis.keys()),
    }

    if expose_schema and output_type in ("JSON", "ALL_FORMATS"):
        response["schema_json"] = BigQueryJSONSchemaExporter(
            bq_schema.to_dict()
        ).export()

    if expose_schema and output_type in ("YAML", "ALL_FORMATS"):
        response["schema_yaml"] = YAMLSchemaExporter(
            bq_schema.to_dict()
        ).export_to_string()

    if expose_schema and output_type in ("DDL", "ALL_FORMATS"):
        apply_partitioning = payload.get("apply_partitioning", False)
        apply_clustering = payload.get("apply_clustering", False)

        ddl_generator = BigQueryDDLGenerator(
            bq_schema=bq_schema,
            partitioning=partitioning if apply_partitioning else None,
            clustering=clustering if apply_clustering else None,
        )

        response["ddl"] = ddl_generator.generate(
            domain=domain,
            env=env,
            zone=zone,
            entity=entity,
            layer=layer,
        )

    if migration_ddls:
        response["migration_ddls"] = migration_ddls
    
    if expose_schema and output_type in ("DOCUMENTATION", "ALL_FORMATS"):
        doc_generator = DocumentationGenerator(
            bq_schema=bq_schema,
            partitioning=partitioning,
            clustering=clustering,
        )

        markdown = doc_generator.generate_markdown()

        if output_type == "DOCUMENTATION":
            return Response(
                content=markdown,
                media_type="text/markdown",
                headers={
                    "Content-Disposition": f'attachment; filename="{bq_schema.table_name}.md"'
                },
            )
        else:
            response["documentation"] = {
                "format": "markdown",
                "content": markdown,
            }

    return response