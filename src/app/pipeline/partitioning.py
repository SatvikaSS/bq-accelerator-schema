from typing import Dict, List, Optional

from app.canonical.schema import CanonicalSchema

"""
IMPORTANT DESIGN CONTRACT:

- Advisory only (NO DDL generation)
- Operates ONLY on canonical payload schema
- Ignores system / ingestion metadata columns
- Skips ARRAY fields (BigQuery does not allow partitioning on REPEATED)
- Deterministic and environment-aware
- Supports SKIP / MANUAL / AUTO modes
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

# BigQuery aligned limits/recommendations
MAX_PARTITIONS_PER_TABLE = 10_000
HOURLY_RECOMMENDED_MAX_DAYS = 180

# Enterprise tuning knobs (not BigQuery hard limits)
HOUR_ROWS_THRESHOLD = 50_000_000
DAILY_ROWS_THRESHOLD = 5_000_000

# Risk thresholds
DAY_PARTITION_WARN_THRESHOLD = 730
HOUR_PARTITION_WARN_THRESHOLD = 24 * HOURLY_RECOMMENDED_MAX_DAYS


def generate_partitioning_suggestion(
    schema: CanonicalSchema,
    zone: Optional[str] = "raw",
    mode: str = "AUTO",
    manual_config: Optional[Dict] = None,
) -> Optional[Dict]:
    zone = (zone or "raw").lower()
    mode = (mode or "AUTO").upper()

    if not schema.tables:
        return None

    # SKIP mode
    if mode == "SKIP":
        return {
            "partitioning_suggestion": {
                "strategy": None,
                "column": None,
                "column_type": None,
                "granularity": None,
                "granularity_policy": "SKIPPED_BY_USER",
                "fallback": "ingestion_timestamp",
                "estimated_partition_count": None,
                "cost_risk": "LOW",
                "cost_note": "Partitioning skipped explicitly by user.",
                "confidence": "SKIPPED",
                "reason": ["Partitioning explicitly skipped by user."],
                "notes": "No partitioning will be applied.",
            }
        }

    # MANUAL mode
    if mode == "MANUAL":
        if not manual_config:
            raise ValueError("Manual mode requires manual_config.")

        granularity = (manual_config.get("granularity") or "DAY").upper()
        retention_days = manual_config.get("recommended_retention_days")
        estimated_partitions = _estimate_partition_count(retention_days, granularity)

        return {
            "partitioning_suggestion": {
                "strategy": manual_config.get("strategy"),
                "column": manual_config.get("column"),
                "column_type": manual_config.get("column_type"),
                "granularity": granularity,
                "granularity_policy": "MANUAL",
                "fallback": "ingestion_timestamp",
                "estimated_partition_count": estimated_partitions,
                "cost_risk": _derive_cost_risk(retention_days, estimated_partitions, granularity),
                "cost_note": "Manual partitioning selected. System heuristics bypassed.",
                "confidence": "USER_DEFINED",
                "reason": ["Partitioning manually defined by user."],
                "notes": (
                    "Manual partitioning selected. "
                    "System cost heuristics were bypassed."
                ),
            }
        }

    # AUTO mode
    table = schema.tables[0]
    row_count = None
    if getattr(table, "metadata", None):
        row_count = table.metadata.get("row_count")

    retention_days = ZONE_RETENTION_POLICY.get(zone)

    date_columns: List[str] = []
    timestamp_columns: List[str] = []

    for field in table.fields:
        if getattr(field, "is_array", False):
            continue

        field_type = field.data_type.upper()
        field_name = field.name

        if field_type in DATE_TYPES:
            date_columns.append(field_name)
        elif field_type in TIMESTAMP_TYPES:
            timestamp_columns.append(field_name)

    # Case 1: DATE column
    if date_columns:
        column = _pick_best_column(date_columns)
        granularity = _select_granularity_by_volume(
            column_type="DATE",
            row_count=row_count,
            retention_days=retention_days,
        )

        return _build_suggestion(
            strategy="COLUMN",
            column=column,
            column_type="DATE",
            granularity=granularity,
            confidence="HIGH",
            reason=[
                "DATE column with business meaning detected",
                "Time-based filtering is common for analytical workloads",
                f"{granularity} partitioning selected based on policy and available volume signal",
            ],
            zone=zone,
        )

    # Case 2: TIMESTAMP/DATETIME column
    if timestamp_columns:
        column = _pick_best_column(timestamp_columns)
        granularity = _select_granularity_by_volume(
            column_type="TIMESTAMP",
            row_count=row_count,
            retention_days=retention_days,
        )

        return _build_suggestion(
            strategy="COLUMN",
            column=column,
            column_type="TIMESTAMP",
            granularity=granularity,
            confidence="MEDIUM" if granularity == "DAY" else "HIGH",
            reason=[
                "TIMESTAMP column detected",
                "Converted to time-based partitioning",
                f"{granularity} partitioning selected based on available volume signal",
            ],
            zone=zone,
        )

    # Fallback: ingestion-time
    return _build_suggestion(
        strategy="INGESTION_TIME",
        column=None,
        column_type=None,
        granularity="DAY",
        confidence="LOW",
        reason=[
            "No DATE or TIMESTAMP column found in payload schema",
            "Ingestion-time partitioning can reduce full table scans",
            "Recommended only for large or append-only tables",
        ],
        zone=zone,
    )


def _pick_best_column(columns: List[str]) -> str:
    scored = []
    for col in columns:
        score = sum(1 for hint in BUSINESS_TIME_HINTS if hint in col.lower())
        scored.append((col, score))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[0][0]


def _select_granularity_by_volume(
    column_type: str,
    row_count: Optional[int],
    retention_days: Optional[int],
) -> str:
    ctype = (column_type or "").upper()

    if ctype == "DATE":
        return "DAY"

    if ctype not in TIMESTAMP_TYPES:
        return "DAY"

    # BigQuery guidance: hourly partitions better for shorter windows
    if retention_days is not None and retention_days > HOURLY_RECOMMENDED_MAX_DAYS:
        return "DAY"

    if row_count is None or row_count <= 0:
        return "DAY"

    window_days = retention_days or 30
    daily_rows = row_count / max(window_days, 1)

    if row_count >= HOUR_ROWS_THRESHOLD or daily_rows >= DAILY_ROWS_THRESHOLD:
        return "HOUR"

    return "DAY"


def _estimate_partition_count(
    retention_days: Optional[int],
    granularity: str,
) -> Optional[int]:
    if retention_days is None:
        return None
    if granularity == "HOUR":
        return retention_days * 24
    return retention_days


def _derive_cost_risk(
    retention_days: Optional[int],
    estimated_partitions: Optional[int],
    granularity: str,
) -> str:
    if retention_days is None:
        return "HIGH"

    if estimated_partitions is None:
        return "MEDIUM"

    if granularity == "HOUR":
        if estimated_partitions <= 24 * 30:
            return "LOW"
        if estimated_partitions <= HOUR_PARTITION_WARN_THRESHOLD:
            return "MEDIUM"
        return "HIGH"

    if estimated_partitions <= 120:
        return "LOW"
    if estimated_partitions <= DAY_PARTITION_WARN_THRESHOLD:
        return "MEDIUM"
    return "HIGH"


def _build_cost_note(
    retention_days: Optional[int],
    estimated_partitions: Optional[int],
    granularity: str,
) -> str:
    if estimated_partitions is not None and estimated_partitions > MAX_PARTITIONS_PER_TABLE:
        return "Estimated partitions exceed BigQuery limit. Fallback recommendation applied."

    if retention_days is None:
        return "No retention policy set for this zone. Define retention to control long-term cost."

    if estimated_partitions is None:
        return "Partition estimate unavailable. Recommendation uses policy defaults."

    if granularity == "HOUR" and estimated_partitions > HOUR_PARTITION_WARN_THRESHOLD:
        return "Hourly partitions are high for this retention window. Consider DAY partitioning or lower retention."

    if granularity == "DAY" and estimated_partitions > DAY_PARTITION_WARN_THRESHOLD:
        return "High DAY partition count expected. Review retention policy for cost control."

    return "Retention and estimated partition count are within expected cost bounds."


def _build_suggestion(
    strategy: str,
    column: Optional[str],
    column_type: Optional[str],
    granularity: str,
    confidence: str,
    reason: List[str],
    zone: str,
) -> Dict:
    retention_days = ZONE_RETENTION_POLICY.get(zone)
    estimated_partitions = _estimate_partition_count(retention_days, granularity)

    # Hard safety: do not exceed BigQuery partition limit
    if estimated_partitions is not None and estimated_partitions > MAX_PARTITIONS_PER_TABLE:
        if granularity == "HOUR":
            granularity = "DAY"
            estimated_partitions = _estimate_partition_count(retention_days, granularity)
            reason.append("Hourly partition estimate exceeded BigQuery limit; downgraded to DAY.")

        if estimated_partitions is not None and estimated_partitions > MAX_PARTITIONS_PER_TABLE:
            strategy = "INGESTION_TIME"
            column = None
            column_type = None
            confidence = "LOW"
            reason.append("Estimated partitions exceed BigQuery maximum; fallback to ingestion-time partitioning.")

    cost_risk = _derive_cost_risk(retention_days, estimated_partitions, granularity)
    cost_note = _build_cost_note(retention_days, estimated_partitions, granularity)

    return {
        "partitioning_suggestion": {
            "strategy": strategy,
            "column": column,
            "column_type": column_type,
            "granularity": granularity,
            "fallback": "ingestion_timestamp",
            "estimated_partition_count": estimated_partitions,
            "cost_risk": cost_risk,
            "cost_note": cost_note,
            "confidence": confidence,
            "reason": reason,
            "notes": (
                "Partitioning is applied only at table creation time. "
                "Changing it later requires creating a new table. "
                "This is an advisory suggestion only."
            ),
        }
    }
