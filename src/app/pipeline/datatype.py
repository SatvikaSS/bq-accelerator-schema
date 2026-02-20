from typing import Tuple
from app.canonical.field import CanonicalField
from app.standards.metadata_columns import get_standard_metadata_columns

_METADATA_MODE_BY_NAME = {
    c["name"]: c["mode"] for c in get_standard_metadata_columns()
}

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

    return _map_scalar_type(canonical_type, field)

def _default_mode(field: CanonicalField) -> str:
    return _METADATA_MODE_BY_NAME.get(field.name, "NULLABLE")

def _map_scalar_type(
    canonical_type: str,
    field: CanonicalField
) -> Tuple[str, str]:

    mode = _default_mode(field)

    if canonical_type == "INTEGER":
        return "INTEGER", mode

    if canonical_type == "FLOAT":
        return "FLOAT", mode

    if canonical_type == "BOOLEAN":
        return "BOOLEAN", mode

    if canonical_type == "STRING":
        return "STRING", mode

    if canonical_type == "DATE":
        return "DATE", mode

    if canonical_type == "TIMESTAMP":
        return "TIMESTAMP", mode
    
    if canonical_type == "GEOGRAPHY":
        return "GEOGRAPHY", mode

    if canonical_type == "RANGE_DATE":
        return "RANGE", mode

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