def infer_type(values):
    """
    Simple type inference:
    Order: integer → float → boolean → date → string
    """
    if not values:
        return "STRING"

    # Check integer
    try:
        for v in values:
            int(v)
        return "INTEGER"
    except:
        pass

    # Check float
    try:
        for v in values:
            float(v)
        return "FLOAT"
    except:
        pass

    # Future: boolean, date detection
    return "STRING"
