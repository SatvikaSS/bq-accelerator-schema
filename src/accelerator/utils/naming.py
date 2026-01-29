def build_table_name(domain: str, entity: str, layer: str) -> str:
    """
    Build table name using {domain}_{entity}_{layer}
    """
    return f"{domain}_{entity}_{layer}".lower()

def build_dataset_name(domain: str, env: str, zone: str) -> str:
    """
    Build BigQuery dataset name using {domain}_{env}_{zone}
    """
    return f"{domain}_{env}_{zone}".lower()
