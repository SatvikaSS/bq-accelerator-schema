from typing import Tuple
from app.canonical.field import CanonicalField


def map_canonical_to_bigquery(field: CanonicalField) -> Tuple[str, str]:
    """
    Map a CanonicalField to BigQuery (type, mode).
    """

    canonical_type = field.data_type.upper()

    # -------------------------------------------------
    # ARRAY → REPEATED
    # -------------------------------------------------
    if field.is_array:
        element_type = (field.element_type or canonical_type).upper()

        if element_type == "RECORD":
            return "RECORD", "REPEATED"

        bq_type, _ = _map_scalar_type(element_type, field)
        return bq_type, "REPEATED"

    # -------------------------------------------------
    # Scalar handling
    # -------------------------------------------------
    return _map_scalar_type(canonical_type, field)


def _map_scalar_type(
    canonical_type: str,
    field: CanonicalField
) -> Tuple[str, str]:

    mode = "NULLABLE" if field.nullable else "REQUIRED"

    if canonical_type == "INTEGER":
        return "INT64", mode

    if canonical_type == "FLOAT":
        return "FLOAT64", mode

    if canonical_type == "BOOLEAN":
        return "BOOL", mode

    if canonical_type == "STRING":
        return "STRING", mode

    if canonical_type == "DATE":
        return "DATE", mode

    if canonical_type == "TIMESTAMP":
        return "TIMESTAMP", mode

    if canonical_type == "DECIMAL":
        meta = field.numeric_metadata
        if meta:
            if meta.precision <= 38 and meta.scale <= 9:
                return "NUMERIC", mode
            return "BIGNUMERIC", mode
        return "NUMERIC", mode

    if canonical_type == "JSON":
        return "JSON", mode

    if canonical_type == "RECORD":
        return "RECORD", mode

    # Defensive fallback
    return "STRING", mode