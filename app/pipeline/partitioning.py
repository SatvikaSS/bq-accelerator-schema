from typing import Dict, List, Optional

from app.canonical.schema import CanonicalSchema

"""
IMPORTANT DESIGN CONTRACT:

- Advisory only (NO DDL generation)
- Operates ONLY on canonical payload schema
- Ignores system / ingestion metadata columns
- Skips ARRAY fields (BigQuery does not allow partitioning on REPEATED)
- Deterministic and environment-aware
- User overrides (e.g., retention days) must be handled OUTSIDE this module
"""

# -----------------------------
# Type definitions
# -----------------------------

DATE_TYPES = {"DATE"}
TIMESTAMP_TYPES = {"TIMESTAMP", "DATETIME"}

BUSINESS_TIME_HINTS = (
    "date",
    "time",
    "created",
    "event",
    "order",
    "transaction",
)

# Default retention policy by zone
ZONE_RETENTION_POLICY = {
    "raw": 90,
    "curated": 365,
    "analytics": None,
}

# -----------------------------
# Public API
# -----------------------------

def generate_partitioning_suggestion(
    schema: CanonicalSchema,
    zone: Optional[str] = "raw",
) -> Optional[Dict]:
    """
    Generate a partitioning suggestion based on canonical PAYLOAD schema only.

    Returns:
        dict | None
    """

    zone = (zone or "raw").lower()

    if not schema.tables:
        return None

    table = schema.tables[0]

    date_columns: List[str] = []
    timestamp_columns: List[str] = []

    for field in table.fields:
        # Skip arrays (BigQuery restriction)
        if getattr(field, "is_array", False):
            continue

        field_type = field.data_type.upper()
        field_name = field.name

        if field_type in DATE_TYPES:
            date_columns.append(field_name)
        elif field_type in TIMESTAMP_TYPES:
            timestamp_columns.append(field_name)

    # -----------------------------
    # Case 1: DATE column (best)
    # -----------------------------
    if date_columns:
        column = _pick_best_column(date_columns)

        return _build_suggestion(
            strategy="COLUMN",
            column=column,
            column_type="DATE",
            confidence="HIGH",
            reason=[
                "DATE column with business meaning detected",
                "Time-based filtering is common for analytical workloads",
                "DAY partitioning provides optimal balance between cost and performance",
            ],
            zone=zone,
        )

    # -----------------------------
    # Case 2: TIMESTAMP column
    # -----------------------------
    if timestamp_columns:
        column = _pick_best_column(timestamp_columns)

        return _build_suggestion(
            strategy="COLUMN",
            column=column,
            column_type="TIMESTAMP",
            confidence="MEDIUM",
            reason=[
                "TIMESTAMP column detected",
                "Converted to DATE for partitioning",
                "DAY partitioning provides optimal balance between cost and performance",
            ],
            zone=zone,
        )

    # -----------------------------
    # Case 3: Fallback → ingestion-time
    # -----------------------------
    return _build_suggestion(
        strategy="INGESTION_TIME",
        column=None,
        column_type=None,
        confidence="LOW",
        reason=[
            "No DATE or TIMESTAMP column found in payload schema",
            "Ingestion-time partitioning can reduce full table scans",
            "Recommended only for large or append-only tables",
        ],
        zone=zone,
    )

# -----------------------------
# Helpers
# -----------------------------

def _pick_best_column(columns: List[str]) -> str:
    """
    Deterministic column selection:
    1. Prefer column with business-time semantics
    2. Fallback to lexicographically smallest name
    """
    for col in columns:
        if _has_business_semantics(col):
            return col
    return sorted(columns)[0]


def _has_business_semantics(column_name: str) -> bool:
    name = column_name.lower()
    return any(hint in name for hint in BUSINESS_TIME_HINTS)


def _build_suggestion(
    strategy: str,
    column: Optional[str],
    column_type: Optional[str],
    confidence: str,
    reason: List[str],
    zone: str,
) -> Dict:
    retention_days = ZONE_RETENTION_POLICY.get(zone)

    return {
        "partitioning_suggestion": {
            "strategy": strategy,
            "column": column,
            "column_type": column_type,
            "granularity": "DAY",
            "fallback": "INGESTION_TIME",
            "recommended_retention_days": retention_days,
            "confidence": confidence,
            "reason": reason,
            "notes": (
                "Partitioning is applied only at table creation time. "
                "Changing it later requires creating a new table. "
                "This is an advisory suggestion only."
            ),
        }
    }