from typing import Dict, List, Optional
from app.standards.metadata_columns import get_standard_metadata_columns

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
METADATA_COLUMN_NAMES = {
    c["name"].lower() for c in get_standard_metadata_columns()
}

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

CLUSTERING_ADVISORY_NOTE = (
    "Clustering is an advisory recommendation based on schema structure, "
    "optional data distribution signals, and usage hints."
)

def generate_clustering_suggestion(
    schema: List[Dict],
    partition_column: Optional[str] = None,
    query_patterns: Optional[Dict[str, List[str]]] = None,
    user_override: Optional[List[str]] = None,
) -> Dict:
    # keep your existing list-based override behavior
    if user_override is not None:
        return _build_user_override_response(user_override)

    eligible_fields: List[Dict] = []

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

        eligible_fields.append(field)

    if not eligible_fields:
        return _no_clustering_reason(
            "No columns met the minimum clustering suitability threshold"
        )

    scores: Dict[str, Dict] = {}
    for field in eligible_fields:
        scores[field["name"]] = _score_column(field, query_patterns=query_patterns)

    scores = {k: v for k, v in scores.items() if v["total"] > 0}
    if not scores:
        return _no_clustering_reason(
            "All eligible columns had low or unknown clustering benefit"
        )

    ranked = sorted(scores.items(), key=lambda x: x[1]["total"], reverse=True)
    selected = ranked[:MAX_CLUSTER_COLUMNS]
    columns = [c for c, _ in selected]
    confidence = _derive_confidence([s for _, s in selected])
    
    return {
        "clustering": {
            "suggested": True,
            "editable": True,
            "confidence": confidence,
            "columns": columns,
            "reasoning": {col: scores[col]["reason"] for col in columns},
            "notes": CLUSTERING_ADVISORY_NOTE,
        }
    }

# ------------------------------------------------------------------
# Scoring logic
# ------------------------------------------------------------------

def _score_column(
    field: Dict,
    query_patterns: Optional[Dict[str, List[str]]],
) -> Dict:
    score = 0
    reasons: List[str] = []

    col = field["name"]
    lname = col.lower()

    stats = field.get("stats", {})
    distinct_ratio = stats.get("distinct_ratio")
    null_ratio = stats.get("null_ratio")

    # 1) Data-driven (primary, if stats provided)
    if distinct_ratio is not None:
        if distinct_ratio > 0.7:
            score += 4
            reasons.append("High distinct ratio")
        elif distinct_ratio < 0.05:
            score -= 3
            reasons.append("Very low distinct ratio")

    if null_ratio is not None and null_ratio > 0.6:
        score -= 2
        reasons.append("High null ratio")

    # 2) Name heuristics (secondary)
    if _has_high_cardinality(lname):
        score += 1
        reasons.append("Identifier-like name")
    elif _has_low_cardinality(lname):
        score -= 1

    # 3) Query hints
    if query_patterns:
        if col in query_patterns.get("joins", []):
            score += 3
            reasons.append("Used in joins")
        if col in query_patterns.get("filters", []):
            score += 2
            reasons.append("Used in filters")
        if col in query_patterns.get("group_by", []):
            score += 1
            reasons.append("Used in GROUP BY")

    return {
        "total": score,
        "reason": "; ".join(reasons) if reasons else "Low signal",
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

    # Exclude platform metadata columns from clustering
    if lname in METADATA_COLUMN_NAMES:
        return True
    
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