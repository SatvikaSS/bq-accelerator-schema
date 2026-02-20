def get_standard_metadata_columns():
    """
    Standard platform metadata column definitions.
    Returned as dictionaries to avoid circular imports.
    """
    return [
        {
            "name": "_ingestion_timestamp",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
            "description": "Timestamp when the record was ingested into the platform",
        },
        {
            "name": "_source_system",
            "type": "STRING",
            "mode": "REQUIRED",
            "description": "Identifier of the source system providing the data",
        },
        {
            "name": "_batch_id",
            "type": "STRING",
            "mode": "REQUIRED",
            "description": "Unique identifier for the ingestion batch",
        },
        {
            "name": "_record_hash",
            "type": "STRING",
            "mode": "REQUIRED",
            "description": "Hash of record content used for change detection",
        },
        {
            "name": "_is_deleted",
            "type": "BOOLEAN",
            "mode": "REQUIRED",
            "description": "Logical delete indicator for soft-deleted records",
        },
        {
            "name": "_deleted_timestamp",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
            "description": "Timestamp when the record was logically deleted",
        },
        {
            "name": "_op_type",
            "type": "STRING",
            "mode": "REQUIRED",
            "description": "CDC operation type: INSERT, UPDATE, or DELETE",
        },
        {
            "name": "_op_ts",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
            "description": "Timestamp when the CDC operation occurred (UTC)",
        },
    ]
