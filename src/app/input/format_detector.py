import os

class UnsupportedFormatError(Exception):
    """Raised when input file format is not supported."""
    pass


class FormatDetector:
    """
    Detects the input file format based on extension.
    """

    SUPPORTED_FORMATS = {
        "csv": "CSV",
        "avro": "Avro",
        "parquet": "Parquet",
        "json": "JSON",
        "jsonl": "JSONL",
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
        
        if not os.path.exists(self.file_path):
            raise UnsupportedFormatError("Input file does not exist")
        
        if os.path.getsize(self.file_path) == 0:
            raise UnsupportedFormatError("Input file is empty.No data available")

        _, ext = os.path.splitext(self.file_path)

        if not ext:
            raise UnsupportedFormatError(
                "File has no extension. Unable to detect format."
            )

        ext = ext.lower().lstrip(".")

        if ext not in self.SUPPORTED_FORMATS:
            raise UnsupportedFormatError(
                f"Unsupported input format: {ext}. "
                f"Supported formats: {list(self.SUPPORTED_FORMATS.keys())}"
            )

        return self.SUPPORTED_FORMATS[ext]
