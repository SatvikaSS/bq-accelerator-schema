import os

class UnsupportedFormatError(Exception):
    """Raised when input file format is not supported."""
    pass


class FormatDetector:
    """
    Detects the input file format based on extension.
    """

    SUPPORTED_FORMATS = {
        "csv": "CSV"
        # json, avro, parquet will be added later
    }

    def __init__(self, file_path: str):
        self.file_path = file_path

    def detect(self) -> str:
        """
        Detect input file format.

        Returns:
            str: Detected format (e.g., 'CSV')

        Raises:
            UnsupportedFormatError: If format is unsupported
        """
        if not self.file_path:
            raise UnsupportedFormatError("Input file path is empty")

        _, ext = os.path.splitext(self.file_path)

        if not ext:
            raise UnsupportedFormatError(
                "File has no extension. Unable to detect format."
            )

        ext = ext.lower().replace(".", "")

        if ext not in self.SUPPORTED_FORMATS:
            raise UnsupportedFormatError(
                f"Unsupported input format: {ext.upper()}. "
                f"Supported formats: {list(self.SUPPORTED_FORMATS.keys())}"
            )

        return self.SUPPORTED_FORMATS[ext]
