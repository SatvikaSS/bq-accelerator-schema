from app.adapters.csv_adapter import CSVAdapter
from app.adapters.json_adapter import JSONAdapter
from app.adapters.parquet_adapter import ParquetAdapter
from app.adapters.avro_adapter import AvroAdapter


class AdapterRegistry:
    """
    Maps detected input formats to adapter implementations.
    """

    _REGISTRY = {
        "CSV": CSVAdapter,
        "JSON": JSONAdapter,
        "JSONL": JSONAdapter,
        "PARQUET": ParquetAdapter,
        "AVRO": AvroAdapter,
    }

    @classmethod
    def get_adapter(cls, format_name: str):
        if not format_name:
            raise ValueError("Format name must not be empty")

        key = format_name.upper()

        if key not in cls._REGISTRY:
            raise ValueError(
                f"No adapter registered for format: {format_name}"
            )

        return cls._REGISTRY[key]
