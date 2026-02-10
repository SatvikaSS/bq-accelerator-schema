from app.governance.schema_registry import SchemaRegistry


class DriftPolicy:
    STRICT = "STRICT"
    WARN = "WARN"
    AUTO = "AUTO"

    @classmethod
    def is_valid(cls, policy: str) -> bool:
        return policy in {cls.STRICT, cls.WARN, cls.AUTO}


class DriftPolicyEnforcer:
    """
    Enforces schema drift policies using a schema registry.

    Returns:
        (entity_name, active_version)
    """

    def __init__(self, policy: str, registry: SchemaRegistry):
        if not DriftPolicy.is_valid(policy):
            raise ValueError(
                f"Invalid drift policy '{policy}'. "
                f"Allowed values: STRICT, WARN, AUTO"
            )

        self.policy = policy
        self.registry = registry

    def enforce(
        self,
        entity: str,
        diff_report: dict,
        new_schema: list,
    ):
        # --------------------------------------------------
        # First-time registration
        # --------------------------------------------------
        if not self.registry.get_entity(entity):
            version = self.registry.register_new_entity(
                entity=entity,
                schema=new_schema,
            )
            print(f"Registered new entity: {entity}_{version}")
            return entity, version

        breaking = diff_report.get("breaking_changes", [])
        non_breaking = diff_report.get("non_breaking_changes", [])

        current_version = self.registry.get_current_version(entity)

        # --------------------------------------------------
        # No changes
        # --------------------------------------------------
        if not breaking and not non_breaking:
            print(f"No schema changes detected for {entity}_{current_version}")
            return entity, current_version

        # --------------------------------------------------
        # Breaking changes detected
        # --------------------------------------------------
        if breaking:
            print("Breaking schema changes detected")

            if self.policy == DriftPolicy.STRICT:
                raise RuntimeError(
                    f"Breaking schema changes detected: {breaking}"
                )

            if self.policy == DriftPolicy.WARN:
                for change in breaking:
                    print("WARNING:", change)
                print(
                    f"Schema NOT updated due to WARN policy "
                    f"({entity}_{current_version})"
                )
                return entity, current_version

            if self.policy == DriftPolicy.AUTO:
                new_version = self.registry.register_new_version(
                    entity=entity,
                    schema=new_schema,
                    breaking=True,
                    change_summary=breaking + non_breaking,
                )
                print(f"New version created: {entity}_{new_version}")
                return entity, new_version

        # --------------------------------------------------
        # Non-breaking changes only
        # --------------------------------------------------
        updated_version = self.registry.update_current_version_schema(
            entity=entity,
            schema=new_schema,
            change_summary=non_breaking,
        )

        print(f"Non-breaking changes applied to {entity}_{updated_version}")
        return entity, updated_version