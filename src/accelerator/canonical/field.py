from dataclasses import dataclass
from typing import Optional

@dataclass
class CanonicalField:
    """
    Canonical representation of a column
    """

    def __init__(
        self,
        name: str,
        data_type: str,
        nullable: bool,
        description: str = None,
        max_length: int = None
    ):
        self.name = name
        self.data_type = data_type
        self.nullable = nullable
        self.description = description
        self.max_length = max_length

    def __repr__(self):
        return (
            f"CanonicalField(name='{self.name}', "
            f"data_type='{self.data_type}', "
            f"nullable={self.nullable}, "
            f"max_length={self.max_length})"
        )
