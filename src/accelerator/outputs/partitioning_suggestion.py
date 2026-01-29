from typing import Dict, List, Optional

"""
IMPORTANT DESIGN CONTRACT:

- This module operates ONLY on PAYLOAD (canonical) schema.
- Input columns must represent business/source fields only.
- BigQuery/system/ingestion metadata columns must be excluded by the caller.
- Column names here are NOT required to be BigQuery-valid.
- Mapping to BigQuery column names must happen outside this module.

This module is warehouse-agnostic and advisory only.
"""


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


ZONE_RETENTION_POLICY = {
    "raw": 90,
    "curated": 365,
    "analytics": None,
}


def generate_partitioning_suggestion(
    schema: List[Dict],
    zone: Optional[str] = "raw",
) -> Optional[Dict]:
    """
    Generate a partitioning suggestion based on PAYLOAD schema only.

    Expected schema format (payload / canonical):
    [
        {"name": "signup date", "type": "DATE"},
        {"name": "email order", "type": "STRING"}
    ]

    - Column names may be raw / non-BigQuery-safe.
    - No system or ingestion metadata columns should be included.
    - This function does NOT mutate schema or generate DDL.
    """

    date_columns: List[str] = []
    timestamp_columns: List[str] = []

    for field in schema:
        col_type = field["type"].upper()
        col_name = field["name"]

        if col_type in DATE_TYPES:
            date_columns.append(col_name)
        elif col_type in TIMESTAMP_TYPES:
            timestamp_columns.append(col_name)

    # -----------------------------
    # Case 1: DATE column exists (highest priority)
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
    # Case 2: TIMESTAMP column exists
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
    # Case 3: No time column → ingestion-time fallback
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
    zone: Optional[str],
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
                "Partitioning is applied only at table creation time; "
                "changing it later requires creating a new table. "
                "It is only a suggestion."
            ),
        }
    }
