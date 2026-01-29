from accelerator.governance.schema_registry import SchemaRegistry


class DriftPolicy:
    STRICT = "STRICT"
    WARN = "WARN"
    AUTO = "AUTO"


class DriftPolicyEnforcer:
    def __init__(self, policy: str, registry: SchemaRegistry):
        self.policy = policy
        self.registry = registry

    def enforce(
        self,
        entity: str,
        diff_report: dict,
        new_schema: list,
    ):
        breaking = diff_report["breaking_changes"]
        non_breaking = diff_report["non_breaking_changes"]

        # No changes
        if not breaking and not non_breaking:
            print("No schema changes detected")
            return entity, self.registry.get_current_version(entity)

        # First-time registration
        if not self.registry.get_entity(entity):
            version = self.registry.register_new_entity(
                entity, new_schema
            )
            print(f"Registered {entity} as {entity}_{version}")
            return entity, version

        if breaking:
            print("Breaking changes detected")

            if self.policy == DriftPolicy.STRICT:
                raise RuntimeError(
                    f"Breaking schema changes detected: {breaking}"
                )

            if self.policy == DriftPolicy.WARN:
                for b in breaking:
                    print("WARNING:", b)

            if self.policy == DriftPolicy.AUTO:
                new_version = self.registry.register_new_version(
                    entity=entity,
                    schema=new_schema,
                    breaking=True,
                    change_summary=breaking,
                )
                print(
                    f"New table version created: {entity}_{new_version}"
                )
                return entity, new_version

        # Non-breaking changes → same version
        current_version = self.registry.update_current_version_schema(
            entity=entity,
            schema=new_schema,
            change_summary=non_breaking,
        )

        print(
            f"Non-breaking changes applied to {entity}_{current_version}"
        )
        return entity, current_version
