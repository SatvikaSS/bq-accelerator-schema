import re
from typing import Dict

from app.standards.data_classification_rules import DATA_CLASSIFICATION_RULES
from app.standards.security_controls import SECURITY_HINTS
from app.standards.metadata_columns import get_standard_metadata_columns


# Heuristic red-flag keywords for UNKNOWN detection
RED_FLAG_KEYWORDS = ["secret","key","hash","token","auth","credential","private","confidential","internal",]
METADATA_COLUMNS = {
    col["name"].lower()
    for col in get_standard_metadata_columns()
}

def _has_red_flag(text: str) -> bool:
    """
    Detect potentially sensitive columns not covered by explicit rules.
    """
    return any(keyword in text for keyword in RED_FLAG_KEYWORDS)


def classify_column(name: str, description: str | None = None) -> Dict[str, str]:
    """
    Detects:
    - PII (explicit rules)
    - SENSITIVE (explicit rules)
    - UNKNOWN (heuristic red flags)
    - NON_PII (confidently safe)
    """

    # Platform metadata bypass
    if name.lower() in METADATA_COLUMNS:
        return {
            "classification": "NON_PII",
            "category": "NON_PII",
            "confidence": "HIGH",
            "recommended_control": "NONE",
            "note": "Standard platform metadata column",
        }

    text = f"{name} {description or ''}".lower()

    # Explicit rule-based classification (PII / SENSITIVE)
    for classification, rule in DATA_CLASSIFICATION_RULES.items():
        for pattern in rule["patterns"]:
            if re.search(pattern, text):
                return {
                    "classification": classification,
                    "category": classification.split(".")[0],  # PII or SENSITIVE
                    "confidence": rule["confidence"],
                    "recommended_control": SECURITY_HINTS.get(
                        classification, "RESTRICTED_ACCESS"
                    ),
                }

    # Heuristic UNKNOWN detection (fail-safe)
    if _has_red_flag(text):
        return {
            "classification": "UNKNOWN",
            "category": "UNKNOWN",
            "confidence": "MEDIUM",
            "recommended_control": "REVIEW_REQUIRED",
            "note": "Potentially sensitive based on heuristic keywords",
        }

    # Confident NON_PII fallback
    return {
        "classification": "NON_PII",
        "category": "NON_PII",
        "confidence": "LOW",
        "recommended_control": "NONE",
    }