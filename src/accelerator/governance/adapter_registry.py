from accelerator.adapters.csv_adapter import CSVAdapter
# future:
# from accelerator.adapters.json_adapter import JSONAdapter
# from accelerator.adapters.parquet_adapter import ParquetAdapter


class AdapterRegistry:
    """
    Maps detected input formats to adapter implementations.
    """

    _REGISTRY = {
        "CSV": CSVAdapter,
        # "JSON": JSONAdapter,
        # "PARQUET": ParquetAdapter,
    }

    @classmethod
    def get_adapter(cls, format_name: str):
        if format_name not in cls._REGISTRY:
            raise ValueError(
                f"No adapter registered for format: {format_name}"
            )
        return cls._REGISTRY[format_name]
