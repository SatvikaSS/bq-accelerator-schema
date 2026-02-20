from typing import Dict
from fastapi.responses import Response
from fastapi import Request

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
from app.governance.schema_registry import SchemaRegistry,compute_schema_hash

# ---------------- Outputs ----------------
from app.pipeline.bigquery_schema import BigQuerySchema
from app.outputs.bigquery_ddl import BigQueryDDLGenerator
from app.outputs.bigquery_migration import BigQueryMigrationGenerator
from app.outputs.bigquery_json_generator import BigQueryJSONSchemaExporter
from app.outputs.yaml_schema_generator import YAMLSchemaExporter
from app.outputs.documentation_generator import DocumentationGenerator

# ---------------- Observability ----------------
from app.observability.logger import (log_event,generate_request_id,RequestTimer,)
from app.observability.audit_logger import AuditLogger
from app.observability.identity import extract_user_identity


# Security hints
def build_security_summary(security_analysis: Dict) -> Dict:
    return {
        "pii_detected": any(f["category"] == "PII" for f in security_analysis.values()),
        "sensitive_detected": any(f["category"] == "SENSITIVE" for f in security_analysis.values()),
        "unknown_detected": any(f["category"] == "UNKNOWN" for f in security_analysis.values()),
        "classified_columns": list(security_analysis.keys()),
    }

# Logging Completion
def log_completion(
    timer,request_id,entity_name,version,decision,diff_report,security_summary,
):
    duration = timer.duration()

    log_event("SCHEMA_GENERATION_COMPLETED", {
        "request_id": request_id,
        "entity": entity_name,
        "version": version,
        "decision": decision,
        "duration_seconds": duration,
        "breaking_changes": len(diff_report.get("breaking_changes", [])),
        "non_breaking_changes": len(diff_report.get("non_breaking_changes", [])),
        "security_summary": security_summary,
    })


# ==========================================================
# ROUTER
# ==========================================================
def route(payload: Dict, request: Request) -> Dict:
    """
    Accelerator main entry point.

    Flow:
    Source → Canonical → Advisory Intelligence →
    BigQuery Schema → Validation → Governance → Outputs
    """

    request_id = generate_request_id()
    audit_logger = AuditLogger()
    user_id = extract_user_identity(request, payload)
    timer = RequestTimer()

    entity = payload.get("entity")
    preview_only = payload.get("preview_only") is True
    if not preview_only:
        log_event("SCHEMA_GENERATION_STARTED", {
            "request_id": request_id,
            "entity": entity,
            "metadata": {
                "domain": payload.get("domain"),
                "environment": payload.get("environment"),
                "zone": payload.get("zone"),
                "layer": payload.get("layer"),
            }
        })

    try:
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

        if not DriftPolicy.is_valid(drift_policy):
            raise ValueError(f"Invalid drift_policy: {drift_policy}")

        csv_override = payload.get("csv_override")
        type_overrides = payload.get("type_overrides")
        # --------------------------------------------------
        # Phase 1 – Format detection + Adapter dispatch + Source Schema Check
        # --------------------------------------------------
        detector = FormatDetector(file_path)
        input_format = detector.detect()

        adapter_cls = AdapterRegistry.get_adapter(input_format)
        if input_format.lower() == "csv":
            adapter = adapter_cls(
                file_path,
                entity_name=entity,
                override=csv_override,
                type_overrides=type_overrides,
            )
        else:
            adapter = adapter_cls(file_path, entity_name=entity)

        canonical_schema = adapter.parse()

        SYSTEM_COLUMN_NAMES = {
            col["name"]
            for col in get_standard_metadata_columns()
        }

        source_warning = (canonical_schema.metadata or {}).get("source_warning", {})
        if (
            source_warning.get("type") == "ROW_WIDTH_MISMATCH"
            and source_warning.get("confirm_required", False)
        ):
            return {
                "status": "WARNING",
                "decision": "USER_CONFIRMATION_REQUIRED",
                "message": (
                    "CSV row/header mismatch detected. Review mapping preview and confirm "
                    "by sending csv_override.confirm_malformed=true to proceed."
                ),
                "source_warning": source_warning,
            }
        # Source schema check
        for table in canonical_schema.tables:
            if not table.fields:
                raise ValueError("Source schema contains no fields.")

            # System-only schema
            if all(field.name in SYSTEM_COLUMN_NAMES for field in table.fields):
                raise ValueError("Schema contains only system columns.")
                
            # Business column using system name
            for field in table.fields:
                if field.name in SYSTEM_COLUMN_NAMES and not getattr(field, "is_system", False):
                    raise ValueError(
                        f"Column '{field.name}' uses a reserved system column name."
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

        if payload.get("table_description"):
            canonical_schema.description = payload["table_description"]

        # --------------------------------------------------
        # Phase 4 – Partitioning (canonical-level, advisory)
        # --------------------------------------------------
        partitioning = generate_partitioning_suggestion(
            schema=canonical_schema,
            zone=zone,
        )

        if input_format.lower() == "avro":
            partitioning.setdefault("partitioning_suggestion", {})
            existing_note = partitioning["partitioning_suggestion"].get("notes", "")
            avro_note = (
                "For Avro input, volume-based DAY/HOUR partition recommendation is heuristic "
                "in this run because exact row_count was not computed to avoid expensive full-file scan."
            )
            partitioning["partitioning_suggestion"]["notes"] = (existing_note + avro_note).strip()

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
                "stats": getattr(field, "stats", {}) or {},
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

        if input_format.lower() in {"avro", "parquet"}:
            clustering.setdefault("clustering", {})
            clustering["clustering"]["notes"] = (
                "Data-driven cardinality stats were not computed for Avro/Parquet in this run. "
                "Recommendation is based on schema/query heuristics only."
            )
        if preview_only:
            return {
                "status": "PREVIEW",
                "entity": entity,
                "partitioning": partitioning,
                "clustering": clustering,
            }

        # --------------------------------------------------
        # Phase 6 – BigQuery schema generation
        # --------------------------------------------------
        normalized_table_name = canonical_schema.tables[0].name
        bq_schema = BigQuerySchema(
            canonical_schema=canonical_schema,
            table_name=normalized_table_name,
        )

        fields = bq_schema.generate()

        security_analysis = {
            field.name: field.security
            for field in fields
            if field.security and field.security.get("category") != "NON_PII"
        }

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

        new_schema_dict = bq_schema.to_dict()

        if previous_entry:
            previous_version = registry.get_current_version(entity)
            previous_schema = previous_entry["versions"][previous_version]["schema"]

            # Fast no-op gate: skip full diff if schema hash is unchanged
            if compute_schema_hash(previous_schema) == compute_schema_hash(new_schema_dict):
                diff_report = {
                    "added_columns": [],
                    "removed_columns": [],
                    "modified_columns": [],
                    "breaking_changes": [],
                    "non_breaking_changes": [],
                }
            else:
                diff_report = SchemaDiff(
                    old_schema=previous_schema,
                    new_schema=new_schema_dict,
                ).diff()
        else:
            diff_report = {
                "added_columns": [],
                "removed_columns": [],
                "modified_columns": [],
                "breaking_changes": [],
                "non_breaking_changes": [],
            }

        has_breaking = bool(diff_report.get("breaking_changes"))
        has_non_breaking = bool(diff_report.get("non_breaking_changes"))

        entity_name, version = enforcer.enforce(
            entity=entity,
            diff_report=diff_report,
            new_schema=new_schema_dict,
        )


        security_summary = build_security_summary(security_analysis)

        if has_breaking and drift_policy == DriftPolicy.WARN:
            duration = timer.duration()

            log_event("SCHEMA_GENERATION_WARNING", {
                "request_id": request_id,
                "entity": entity,
                "breaking_changes": len(diff_report.get("breaking_changes", [])),
                "duration_seconds": duration,
            })

            audit_record = audit_logger.build_record(
                request_id=request_id,
                user_id=user_id,
                action="SCHEMA_BREAKING_REJECTED",
                entity=entity_name,
                version=version,
                decision="BREAKING",
                breaking_changes=len(diff_report.get("breaking_changes", [])),
                non_breaking_changes=len(diff_report.get("non_breaking_changes", [])),
                security_summary=security_summary,
            )

            audit_logger.persist(audit_record)

            return {
                "status": "WARNING",
                "entity": entity_name,
                "version": version,
                "decision": "BREAKING",
                "message": (
                "Breaking schema change detected. Schema not updated due to WARN policy."),
                "partitioning": partitioning,
                "clustering": clustering,
                "schema_drift": diff_report,
            }

        migration_ddls = []
        if has_non_breaking and not has_breaking:
            migration_ddls = BigQueryMigrationGenerator(
                dataset=f"{domain}_{env}_{zone}",
                table=f"{entity}_{version}",
            ).generate(diff_report)
        # --------------------------------------------------
        # Decision
        # --------------------------------------------------
        if is_initial_creation:
            decision = "CREATE"
        elif has_breaking:
            decision = "BREAKING"
        elif has_non_breaking:
            decision = "NON_BREAKING"
        else:
            decision = "NO_OP"

        response = {
            "status": "SUCCESS",
            "entity": entity_name,
            "version": version,
            "decision": decision,
            **(
                {
                    "message": "Schema is unchanged. Returning current active BigQuery schema."
                }
                if decision == "NO_OP"
                else {}
            ),
            "partitioning": partitioning,
            "clustering": clustering,
            "rename_mappings": rename_mappings,
            "security_analysis": security_analysis,
            "security_summary": security_summary,
            "schema_drift": diff_report,
        }

        # --------------------------------------------------
        # Audit Logging 
        # --------------------------------------------------
        audit_record = audit_logger.build_record(
            request_id=request_id,
            user_id=user_id,
            action="SCHEMA_GENERATION",
            entity=entity_name,
            version=version,
            decision=decision,
            breaking_changes=len(diff_report.get("breaking_changes", [])),
            non_breaking_changes=len(diff_report.get("non_breaking_changes", [])),
            security_summary=security_summary,
        )

        audit_logger.persist(audit_record)

        # --------------------------------------------------
        # Phase 9 – Outputs
        # --------------------------------------------------

        expose_schema = not (has_breaking and drift_policy == DriftPolicy.WARN)

        if expose_schema and output_type in ("JSON", "ALL_FORMATS"):
            response["schema_json"] = BigQueryJSONSchemaExporter(
                bq_schema.to_dict()
            ).export()

        if expose_schema and output_type in ("YAML", "ALL_FORMATS"):
            response["schema_yaml"] = YAMLSchemaExporter(
                bq_schema.to_dict()
            ).export_to_string()

        # For NON_BREAKING updates, return migration only (no CREATE TABLE DDL)
        emit_create_ddl = not (decision == "NON_BREAKING" and migration_ddls)
        if expose_schema and output_type in ("DDL", "ALL_FORMATS") and emit_create_ddl:
            apply_partitioning = payload.get("apply_partitioning", False)
            apply_clustering = payload.get("apply_clustering", False)

            ddl_generator = BigQueryDDLGenerator(
                bq_schema=bq_schema,
                partitioning=partitioning if apply_partitioning else None,
                clustering=clustering if apply_clustering else None,
            )

            ddl = ddl_generator.generate(
                domain=domain,
                env=env,
                zone=zone,
                entity=entity,
                layer=layer,
            )

            # If only DDL requested:
            # - API mode: return downloadable Response
            # - CLI mode: return dict so summary/rename_mappings are preserved
            if output_type == "DDL" and not payload.get("return_dict_for_ddl", False):
                sql_text = ddl["dataset_ddl"] + "\n\n" + ddl["table_ddl"]

                log_completion(timer,request_id,entity_name,version,decision,diff_report,security_summary,)

                return Response(
                    content=sql_text,
                    media_type="text/plain",
                    headers={
                        "Content-Disposition": f'attachment; filename="{entity}.sql"'
                    },
                )
            else:
                response["ddl"] = ddl


        if migration_ddls:
            response["migration_ddls"] = migration_ddls


        if expose_schema and output_type in ("DOCUMENTATION", "ALL_FORMATS"):
            doc_generator = DocumentationGenerator(
                bq_schema=bq_schema,
                partitioning=partitioning,
                clustering=clustering,
                security_analysis=security_analysis,
                rename_mappings=rename_mappings,
                entity=entity_name,
                version=version,
                decision=decision,
                drift_policy=drift_policy,
            )

            markdown = doc_generator.generate_markdown()

            if output_type == "DOCUMENTATION" and not payload.get("return_dict_for_documentation", False):
                log_completion(timer,request_id,entity_name,version,decision,diff_report,security_summary,)

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

        log_completion(timer,request_id,entity_name,version,decision,diff_report,security_summary,)

        return response

    except Exception as e:
        if not preview_only:
            audit_record = audit_logger.build_record(
                request_id=request_id,
                user_id=user_id,
                action="SCHEMA_GENERATION_FAILED",
                entity=entity if entity else "unknown",
                version="N/A",
                decision="FAILED",
                breaking_changes=0,
                non_breaking_changes=0,
                security_summary={},
            )

            audit_logger.persist(audit_record)

            log_event("SCHEMA_GENERATION_FAILED", {
                "request_id": request_id,
                "entity": entity,
                "error": str(e),
            })
        raise