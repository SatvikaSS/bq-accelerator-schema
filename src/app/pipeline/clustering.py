from typing import Dict, List, Optional

"""
IMPORTANT DESIGN CONTRACT:

- Operates ONLY on PAYLOAD (canonical) schema.
- Input fields must represent business/source columns only.
- System / ingestion metadata columns must be excluded by the caller.
- Column names are NOT required to be BigQuery-safe.
- Mapping to warehouse-specific names happens elsewhere.

This module is:
- Advisory only
- Deterministic
- UI-editable
- Warehouse-agnostic
"""

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------

MAX_CLUSTER_COLUMNS = 4

# Semantic exclusions (columns that do not benefit from clustering)
EXCLUDED_TYPES = {
    "DATE",
    "TIMESTAMP",
    "DATETIME",
    "BOOLEAN",
    "BOOL",
    "FLOAT",
    "FLOAT64",
    "NUMERIC",
    "BIGNUMERIC",
}

# Name-based heuristics
METRIC_NAME_HINTS = (
    "amount",
    "price",
    "score",
    "metric",
    "value",
    "total",
    "avg",
    "sum",
)

LOW_CARDINALITY_HINTS = (
    "status",
    "type",
    "flag",
    "category",
    "level",
)

HIGH_CARDINALITY_HINTS = (
    "id",
    "uuid",
    "key",
)

# ------------------------------------------------------------------
# Public entry point
# ------------------------------------------------------------------

def generate_clustering_suggestion(
    schema: List[Dict],
    partition_column: Optional[str] = None,
    query_patterns: Optional[Dict[str, List[str]]] = None,
    user_override: Optional[List[str]] = None,
) -> Dict:
    """
    Generate clustering suggestions from canonical PAYLOAD schema.

    Expected input format:
    [
        {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "country", "type": "STRING"}
    ]

    - No mutation
    - No DDL
    - UI-friendly output
    """

    # -------------------------------------------------
    # User override (absolute priority)
    # -------------------------------------------------
    if user_override is not None:
        return _build_user_override_response(user_override)

    # -------------------------------------------------
    # Eligibility filtering
    # -------------------------------------------------
    eligible_columns: List[str] = []

    for field in schema:
        name = field["name"]
        col_type = field["type"].upper()
        mode = field.get("mode", "NULLABLE")

        if _is_excluded(
            name=name,
            col_type=col_type,
            mode=mode,
            partition_column=partition_column,
        ):
            continue

        eligible_columns.append(name)

    if not eligible_columns:
        return _no_clustering_reason(
            "No columns met the minimum clustering suitability threshold"
        )

    # -------------------------------------------------
    # Scoring
    # -------------------------------------------------
    scores: Dict[str, Dict] = {}

    for col in eligible_columns:
        scores[col] = _score_column(
            col,
            query_patterns=query_patterns,
        )

    # Remove low / negative signal columns
    scores = {k: v for k, v in scores.items() if v["total"] > 0}

    if not scores:
        return _no_clustering_reason(
            "All eligible columns had low or unknown clustering benefit"
        )

    # -------------------------------------------------
    # Ranking & selection
    # -------------------------------------------------
    ranked = sorted(
        scores.items(),
        key=lambda x: x[1]["total"],
        reverse=True,
    )

    selected = ranked[:MAX_CLUSTER_COLUMNS]
    columns = [c for c, _ in selected]

    confidence = _derive_confidence([s for _, s in selected])

    reasoning = {
        col: scores[col]["reason"]
        for col in columns
    }

    return {
        "clustering": {
            "suggested": True,
            "editable": True,
            "confidence": confidence,
            "columns": columns,
            "reasoning": reasoning,
        }
    }

# ------------------------------------------------------------------
# Scoring logic
# ------------------------------------------------------------------

def _score_column(
    column: str,
    query_patterns: Optional[Dict[str, List[str]]],
) -> Dict:
    score = 0
    reasons: List[str] = []

    # Cardinality heuristics
    if _has_high_cardinality(column):
        score += 3
        reasons.append("High cardinality indicator")
    elif _has_low_cardinality(column):
        score -= 2

    # Query pattern signals (if available)
    if query_patterns:
        if column in query_patterns.get("joins", []):
            score += 4
            reasons.append("Frequently used in joins")
        elif column in query_patterns.get("filters", []):
            score += 3
            reasons.append("Frequently used in filters")
        elif column in query_patterns.get("group_by", []):
            score += 1
            reasons.append("Used in GROUP BY")

    return {
        "total": score,
        "reason": "; ".join(reasons) if reasons else "Heuristic-based recommendation",
    }

# ------------------------------------------------------------------
# Eligibility rules
# ------------------------------------------------------------------

def _is_excluded(
    name: str,
    col_type: str,
    mode: str,
    partition_column: Optional[str],
) -> bool:
    lname = name.lower()

    # Partition column is never clustered
    if partition_column and name == partition_column:
        return True

    # Semantic exclusions
    if col_type in EXCLUDED_TYPES:
        return True

    # Arrays / repeated fields are not clusterable
    if mode == "REPEATED":
        return True

    # Metric-like columns give poor pruning
    if any(hint in lname for hint in METRIC_NAME_HINTS):
        return True

    return False


def _has_high_cardinality(name: str) -> bool:
    lname = name.lower()
    return any(hint in lname for hint in HIGH_CARDINALITY_HINTS)


def _has_low_cardinality(name: str) -> bool:
    lname = name.lower()
    return any(hint in lname for hint in LOW_CARDINALITY_HINTS)

# ------------------------------------------------------------------
# Confidence
# ------------------------------------------------------------------

def _derive_confidence(scores: List[Dict]) -> str:
    max_score = max(s["total"] for s in scores)

    if max_score >= 7:
        return "HIGH"
    if max_score >= 4:
        return "MEDIUM"
    return "LOW"

# ------------------------------------------------------------------
# Response builders
# ------------------------------------------------------------------

def _no_clustering_reason(reason: str) -> Dict:
    return {
        "clustering": {
            "suggested": False,
            "reason": reason,
        }
    }


def _build_user_override_response(columns: List[str]) -> Dict:
    if not columns:
        return _no_clustering_reason("User explicitly skipped clustering")

    return {
        "clustering": {
            "suggested": True,
            "editable": False,
            "confidence": "USER_DEFINED",
            "columns": columns[:MAX_CLUSTER_COLUMNS],
            "reasoning": {
                col: "User override"
                for col in columns[:MAX_CLUSTER_COLUMNS]
            },
        }
    }