## Accelerator Architecture

Flow:
Input → Adapter → Canonical Schema → BigQuery Mapping → Validation → Drift & Versioning

Folders:
- input/: format detection
- adapters/: input parsers
- canonical/: cloud-agnostic schema
- outputs/: BigQuery-specific logic
- governance/: schema versioning & drift
