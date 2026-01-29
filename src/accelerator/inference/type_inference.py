from datetime import datetime, timezone
import re

class NaiveTimestampError(ValueError):
    """Raised when timestamp has no timezone information."""
    pass


def _is_boolean(value: str) -> bool:
    """
    Check if value represents a boolean.
    """
    return str(value).strip().lower() in ("true", "false", "0", "1", "yes", "no","y","n","t","f")


def _is_integer(value: str) -> bool:
    """
    Check if value represents an integer.
    """
    try:
        int(value)
        return True
    except Exception:
        return False


def _is_float(value: str) -> bool:
    try:
        float(value)
        return True
    except Exception:
        return False


def _is_date(value: str) -> bool:
    """
    Check if value matches common date formats.
    """
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            datetime.strptime(str(value), fmt)
            return True
        except ValueError:
            continue
    return False

ISO_TZ_PATTERN = re.compile(
    r".*(Z|[+-]\d{2}:\d{2})$"
)


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

        # No timezone → ambiguous → reject
        return None

    except Exception:
        return None


def _is_timestamp_utc(value: str) -> bool:
    """
    True only if timestamp has timezone and is convertible to UTC.
    """
    return _parse_timestamp_utc(value) is not None

def _is_timestamp(value: str) -> bool:
    """
    Timestamp must be timezone-aware and convertible to UTC.
    """
    return _is_timestamp_utc(value)

def _is_naive_timestamp(value: str) -> bool:
    """
    Timestamp without timezone (ambiguous).
    """
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            datetime.strptime(str(value), fmt)
            return True
        except ValueError:
            continue
    return False



def infer_type(values):
    """
    Infer canonical data type from sampled values.

    Promotion order (safe & conservative):
    BOOLEAN → INTEGER → FLOAT → TIMESTAMP → DATE → STRING
    """

    if not values:
        return "STRING"

    # Normalize values (strip whitespace)
    values = [str(v).strip() for v in values if v not in (None, "", "NULL")]

    if not values:
        return "STRING"

    # BOOLEAN
    if all(_is_boolean(v) for v in values):
        return "BOOLEAN"

    # INTEGER
    if all(_is_integer(v) for v in values):
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

    # Default fallback
    return "STRING"
