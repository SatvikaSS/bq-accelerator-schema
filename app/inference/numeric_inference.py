from decimal import Decimal, InvalidOperation
from app.canonical.field import NumericMetadata


def infer_numeric_metadata(values):
    """
    Infer precision and scale from numeric values.

    Args:
        values (list): List of numeric values as strings

    Returns:
        NumericMetadata | None
    """

    max_precision = 0
    max_scale = 0

    for v in values:
        try:
            d = Decimal(str(v))
        except (InvalidOperation, ValueError):
            continue

        _, digits, exponent = d.as_tuple()

        precision = len(digits)
        scale = -exponent if exponent < 0 else 0

        max_precision = max(max_precision, precision)
        max_scale = max(max_scale, scale)

    if max_precision == 0:
        return None

    return NumericMetadata(
        precision=max_precision,
        scale=max_scale,
        max_integer_digits=max_precision - max_scale,
        signed=True
    )
