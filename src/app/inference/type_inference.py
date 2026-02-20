from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re


class NaiveTimestampError(ValueError):
    """Raised when timestamp has no timezone information."""
    pass


def _is_boolean(value: str) -> bool:
    """
    Check if value represents a boolean.
    """
    return str(value).strip().lower() in (
        "true", "false", "0", "1", "yes", "no", "y", "n", "t", "f"
    )


def is_ambiguous_boolean(values) -> bool:
    """
    True when BOOLEAN inference is based only on 0/1 tokens,
    which may actually represent categorical integers.
    """
    normalized = [
        str(v).strip().lower()
        for v in values
        if v not in (None, "", "NULL", "null", "Null")
    ]

    if not normalized:
        return False

    return all(v in {"0", "1"} for v in normalized)


def _is_integer(value: str) -> bool:
    """
    Check if value represents an integer.
    """
    try:
        int(str(value).strip())
        return True
    except Exception:
        return False


DECIMAL_PATTERN = re.compile(r"^[+-]?\d+(\.\d+)?$")


def _is_decimal(value: str) -> bool:
    """
    Check if value represents a base-10 decimal number.
    (No scientific notation here; handled by float fallback.)
    """
    v = str(value).strip()
    if not DECIMAL_PATTERN.fullmatch(v):
        return False
    try:
        Decimal(v)
        return True
    except (InvalidOperation, ValueError):
        return False


def _is_float(value: str) -> bool:
    try:
        float(str(value).strip())
        return True
    except Exception:
        return False


def _is_date(value: str) -> bool:
    """
    Check if value matches common date formats.
    """
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            datetime.strptime(str(value).strip(), fmt)
            return True
        except ValueError:
            continue
    return False


ISO_TZ_PATTERN = re.compile(r".*(Z|[+-]\d{2}:\d{2})$")


def _parse_timestamp_utc(value: str):
    """
    Parse timestamp and normalize to UTC if timezone is present.
    Returns datetime in UTC or None if invalid.
    """
    value = str(value).strip()

    try:
        # Zulu time
        if value.endswith("Z"):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc)

        # Offset time (+05:30, -04:00)
        if ISO_TZ_PATTERN.match(value):
            dt = datetime.fromisoformat(value)
            return dt.astimezone(timezone.utc)

        # No timezone -> ambiguous -> reject
        return None

    except Exception:
        return None


def _is_timestamp_utc(value: str) -> bool:
    """
    True only if timestamp has timezone and is convertible to UTC.
    """
    return _parse_timestamp_utc(value) is not None


def _is_naive_timestamp(value: str) -> bool:
    """
    Timestamp without timezone (ambiguous).
    """
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            datetime.strptime(str(value).strip(), fmt)
            return True
        except ValueError:
            continue
    return False

WKT_PREFIXES = (
    "POINT", "LINESTRING", "POLYGON",
    "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION",
)

RANGE_DATE_PATTERN = re.compile(
    r"^[\[\(]\s*\d{4}-\d{2}-\d{2}\s*,\s*\d{4}-\d{2}-\d{2}\s*[\)\]]$"
)

def _is_geography(value: str) -> bool:
    v = str(value).strip().upper()
    return any(v.startswith(p + "(") for p in WKT_PREFIXES)

def _is_range_date(value: str) -> bool:
    return RANGE_DATE_PATTERN.fullmatch(str(value).strip()) is not None


def infer_type(values):
    """
    Infer canonical data type from sampled values.

    Promotion order (safe & conservative):
    BOOLEAN -> INTEGER -> DECIMAL -> FLOAT -> TIMESTAMP -> DATE -> STRING

    Notes:
    - DECIMAL inference enables downstream NUMERIC/BIGNUMERIC mapping
      when precision/scale metadata is available.
    - Naive timestamps (no timezone) are rejected.
    """

    if not values:
        return "STRING"

    # Normalize values (strip whitespace, drop null-like markers)
    values = [
        str(v).strip()
        for v in values
        if v not in (None, "", "NULL", "null", "Null")
    ]

    if not values:
        return "STRING"

    # BOOLEAN
    if all(_is_boolean(v) for v in values):
        normalized = {str(v).strip().lower() for v in values}
        # numeric-only bool tokens are ambiguous -> keep as INTEGER
        if normalized.issubset({"0", "1"}):
            return "INTEGER"
        return "BOOLEAN"

    # INTEGER
    if all(_is_integer(v) for v in values):
        return "INTEGER"

    # DECIMAL 
    if all(_is_decimal(v) for v in values):
        if any("." in str(v) for v in values):
            return "DECIMAL"
        return "INTEGER"

    # FLOAT 
    if all(_is_float(v) for v in values):
        return "FLOAT"

    # TIMESTAMP (STRICT UTC ENFORCEMENT)
    if any(_is_naive_timestamp(v) for v in values):
        bad_values = [v for v in values if _is_naive_timestamp(v)]
        raise NaiveTimestampError(
            f"Naive timestamps detected (no timezone). "
            f"Examples: {bad_values[:3]}{'...' if len(bad_values) > 3 else ''}. "
            "Timestamps must include timezone (e.g. Z or +05:30)."
        )

    if all(_is_timestamp_utc(v) for v in values):
        return "TIMESTAMP"

    # DATE
    if all(_is_date(v) for v in values):
        return "DATE"
    
    if all(_is_geography(v) for v in values):
        return "GEOGRAPHY"

    if all(_is_range_date(v) for v in values):
        return "RANGE_DATE"

    # Default fallback
    return "STRING"
