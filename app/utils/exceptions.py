class AcceleratorError(Exception):
    """
    Base exception for all accelerator errors
    """
    pass


class SchemaValidationError(AcceleratorError):
    """
    Raised when schema validation fails
    """
    pass
